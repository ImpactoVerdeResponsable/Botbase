# microstructure_manager.py
import asyncio
import aiohttp
import json
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional, Any

from unified_logger import get_logger

logger = get_logger(__name__)


class MicrostructureManager:
    """
    Gestiona dinámicamente las suscripciones a streams de microestructura (Order Book)
    para el modo Event-Driven.
    """

    def __init__(self):
        # Almacena las tareas de asyncio activas (una por símbolo)
        self._tasks: Dict[str, asyncio.Task] = {}
        # Almacena el snapshot más reciente del order book
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._stop_event = asyncio.Event()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Obtiene o crea la sesión aiohttp."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _listen_to_stream(self, symbol: str):
        """
        Tarea de larga duración que escucha el stream de un símbolo.
        Maneja la conexión, recepción de mensajes y reconexión.
        """
        log_extra = {'symbol': symbol, 'transaction_id': f'MS_WS_{symbol[:4]}'}
        stream_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth20@100ms"
        backoff_time = 1

        while not self._stop_event.is_set():
            try:
                session = await self._get_session()
                async with session.ws_connect(stream_url, heartbeat=30, timeout=20) as ws:
                    logger.info(f"Microstructure WS conectado para {symbol}", extra=log_extra)
                    backoff_time = 1  # Resetear backoff

                    async for msg in ws:
                        if self._stop_event.is_set():
                            break
                        
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._process_message(symbol, msg.data, log_extra)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning(f"Microstructure WS desconectado para {symbol}. Msg: {msg.data}", extra=log_extra)
                            break
                
            except asyncio.CancelledError:
                logger.info(f"Microstructure task para {symbol} cancelada.", extra=log_extra)
                break  # Salir del bucle while si la tarea se cancela
            except Exception as e:
                logger.error(f"Error en Microstructure WS ({symbol}): {e}.", extra=log_extra)

            if not self._stop_event.is_set():
                wait_time = min(backoff_time, 60)
                logger.info(f"Reconectando Microstructure WS para {symbol} en {wait_time}s...", extra=log_extra)
                await asyncio.sleep(wait_time)
                backoff_time *= 2  # Exponential backoff

    def _process_message(self, symbol: str, data: str, log_extra: dict):
        """Procesa el mensaje JSON y lo almacena en snapshots."""
        try:
            payload = json.loads(data)
            
            # Validar que es un snapshot de order book
            if 'bids' not in payload or 'asks' not in payload:
                logger.debug(f"Payload de microestructura inválido (sin bids/asks) para {symbol}", extra=log_extra)
                return

            # Convertir a Decimal para precisión (primeros 5 niveles)
            snapshot = {
                'bids': [[Decimal(b[0]), Decimal(b[1])] for b in payload['bids'][:5]],
                'asks': [[Decimal(a[0]), Decimal(a[1])] for a in payload['asks'][:5]],
                'lastUpdateId': payload.get('lastUpdateId'),
                'timestamp': time.time() # Usar timestamp de recepción
            }
            
            self._snapshots[symbol] = snapshot
            
        except json.JSONDecodeError:
            logger.warning(f"Error decodificando JSON de microestructura para {symbol}", extra=log_extra)
        except (InvalidOperation, TypeError) as e:
             logger.warning(f"Error convirtiendo datos de OB a Decimal para {symbol}: {e}", extra=log_extra)

    def subscribe(self, symbol: str):
        """
        Inicia la escucha del stream de microestructura para un símbolo.
        """
        if symbol in self._tasks:
            logger.debug(f"Suscripción de microestructura para {symbol} ya existe.", extra={'symbol': symbol})
            return

        log_extra = {'symbol': symbol, 'transaction_id': 'MS_SUB'}
        logger.info(f"Iniciando suscripción de microestructura para {symbol}", extra=log_extra)
        
        # Crear y almacenar la tarea de escucha
        task = asyncio.create_task(self._listen_to_stream(symbol))
        self._tasks[symbol] = task

    def unsubscribe(self, symbol: str):
        """
        Detiene la escucha del stream de microestructura para un símbolo.
        """
        task = self._tasks.pop(symbol, None)
        if task:
            log_extra = {'symbol': symbol, 'transaction_id': 'MS_UNSUB'}
            logger.info(f"Cancelando suscripción de microestructura para {symbol}", extra=log_extra)
            task.cancel()
            
        # Limpiar snapshot
        if symbol in self._snapshots:
            del self._snapshots[symbol]

    def get_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene el snapshot de order book más reciente para un símbolo.
        (No bloqueante)
        """
        return self._snapshots.get(symbol)

    async def stop_all(self):
        """Detiene todas las tareas y cierra la sesión."""
        self._stop_event.set()
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        if self._session and not self._session.closed:
            await self._session.close()
            
        self._tasks.clear()
        self._snapshots.clear()
        logger.info("Microstructure Manager detenido limpiamente.")


# Instancia global
microstructure_manager = MicrostructureManager()