# async_stream_manager.py
import asyncio
import aiohttp
import json
import time
from collections import defaultdict
from unified_logger import get_logger
from typing import List, Callable, Coroutine

logger = get_logger(__name__)

# Almacén global de colas (Patrón Consumidor/Productor por símbolo)
symbol_queues = defaultdict(asyncio.Queue)

async def _connect_and_listen(
    stream_url: str, 
    session: aiohttp.ClientSession, 
    stop_event: asyncio.Event
):
    """
    Productor: Se conecta a un combined stream y encola los eventos kline_closed.
    Maneja la reconexión con exponential backoff.
    """
    backoff_time = 1
    while not stop_event.is_set():
        try:
            async with session.ws_connect(stream_url, heartbeat=30, timeout=20) as ws:
                logger.info(f"Conectado exitosamente al stream: {stream_url[:100]}...")
                backoff_time = 1  # Resetear backoff tras conexión exitosa
                
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        payload = json.loads(msg.data)
                        
                        # Manejar formato de combined stream: {'stream': '...', 'data': {...}}
                        data = payload.get('data')
                        if not data:
                            continue
                        
                        kline = data.get('k')
                        
                        # k.x == True significa que la vela está cerrada
                        if kline and kline.get('x'):
                            symbol = kline.get('s').upper()
                            if symbol in symbol_queues:
                                # Poner en la cola del símbolo específico
                                await symbol_queues[symbol].put(kline)
                                
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning(f"Stream desconectado: {stream_url[:100]}.")
                        break
            
        except asyncio.CancelledError:
            logger.info("Tarea de conexión cancelada.")
            break
        except Exception as e:
            logger.error(f"Error en WebSocket ({stream_url[:100]}): {e}.")
        
        if not stop_event.is_set():
            wait_time = min(backoff_time, 60)
            logger.info(f"Reconectando al stream en {wait_time}s...")
            await asyncio.sleep(wait_time)
            backoff_time *= 2  # Exponential backoff

async def _symbol_consumer(
    symbol: str, 
    queue: asyncio.Queue, 
    candle_processor_callback: Callable[[str, dict], Coroutine],
    stop_event: asyncio.Event
):
    """
    Consumidor: Procesa eventos de la cola de un símbolo específico.
    Garantiza idempotencia y llama al callback de lógica de trading.
    """
    last_kline_start_time = None
    log_extra = {'symbol': symbol}
    
    while not stop_event.is_set():
        try:
            # Esperar por el próximo item en la cola
            kline_data = await queue.get()
            
            k_start_time = kline_data.get('t')
            
            # Validación de Idempotencia (sugerida por el experto)
            if k_start_time == last_kline_start_time:
                logger.debug("Evento duplicado de vela ignorado.", extra=log_extra)
                continue
            last_kline_start_time = k_start_time
            
            # Programar la lógica de trading (no bloquear al consumidor)
            asyncio.create_task(candle_processor_callback(symbol, kline_data))
            
        except asyncio.CancelledError:
            logger.info(f"Consumidor para {symbol} detenido.", extra=log_extra)
            break
        except Exception as e:
            logger.error(f"Error en consumidor de {symbol}: {e}", exc_info=True, extra=log_extra)

def _chunk_symbols(symbols: List[str], chunk_size: int = 100) -> List[List[str]]:
    """Divide los símbolos en chunks para los combined streams."""
    return [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

async def start_kline_streams(
    symbols_list: List[str], 
    candle_processor_callback: Callable[[str, dict], Coroutine],
    stop_event: asyncio.Event
):
    """
    Punto de entrada: Inicia los productores (streams) y los consumidores (lógica).
    """
    
    # 1. Iniciar los consumidores (uno por símbolo)
    consumer_tasks = []
    for symbol in symbols_list:
        queue = symbol_queues[symbol]  # defaultdict la crea si no existe
        task = asyncio.create_task(
            _symbol_consumer(symbol, queue, candle_processor_callback, stop_event)
        )
        consumer_tasks.append(task)
    logger.info(f"Iniciados {len(consumer_tasks)} consumidores de símbolos.")

    # 2. Iniciar los productores (uno por chunk de combined stream)
    producer_tasks = []
    async with aiohttp.ClientSession() as session:
        
        # Agrupar símbolos en chunks (sugerencia del experto)
        for chunk in _chunk_symbols(symbols_list, 100):
            stream_names = [f"{s.lower()}@kline_1m" for s in chunk]
            stream_path = "/".join(stream_names)
            url = f"wss://stream.binance.com:9443/stream?streams={stream_path}"
            
            task = asyncio.create_task(_connect_and_listen(url, session, stop_event))
            producer_tasks.append(task)
        
        logger.info(f"Iniciando {len(producer_tasks)} conexiones de combined streams.")
        
        # Mantener los productores vivos
        await asyncio.gather(*producer_tasks)
            
    # Si los productores terminan (ej. stop_event), detenemos los consumidores
    logger.info("Deteniendo consumidores...")
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
    logger.info("Gestor de streams detenido limpiamente.")