import redis
import json
import ccxt
import time  # <--- IMPORTACIÓN CORREGIDA
import random
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
from datetime import datetime
from decimal import Decimal

# --- INICIO DE MODIFICACIÓN ---
# Importar el módulo de configuración correcto y sus funciones
import config
# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
from redis_manager import redis_manager
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
# Función 'connect_to_redis' eliminada y reemplazada por 'redis_manager'
def connect_to_redis():
    """Función simplificada usando RedisManager"""
    return redis_manager.get_connection()
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN ---
# Cargar la configuración usando get_param
# (Se mantiene esta lógica para la configuración local del engine)
try:
    slippage_percent = Decimal(config.get_param("PROFITABILITY_CONFIG.SLIPPAGE_PERCENT", "0.0005"))
    commission_percent = Decimal(config.get_param("PROFITABILITY_CONFIG.COMMISSION_PERCENT", "0.001"))
    max_trades = int(config.get_param("SHADOW_OPTUNA.WINDOW_VELAS", 500))
    # --- INICIO DE MODIFICACIÓN: (Paso 4) ---
    # Obtener valores de redis_manager
    redis_channel = redis_manager.get_config_value("REDIS_CHANNEL", "bot_signals")
    redis_list_key = redis_manager.get_config_value("SHADOW_TRADES_LIST", "shadow_trades_1m")
    # --- FIN DE MODIFICACIÓN ---
    
    log_extra_init = {'symbol': 'SHADOW_INIT'}
    logger.info(f"Configuración de Shadow Engine cargada.", extra=log_extra_init)
    logger.info(f"Slippage: {slippage_percent * 100}%, Comisión: {commission_percent * 100}%, Max Trades: {max_trades}, Canal: {redis_channel}, Lista: {redis_list_key}", extra=log_extra_init)
except Exception as e:
    log_extra_fail = {'symbol': 'SHADOW_INIT_FAIL'}
    logger.error(f"Error fatal al cargar la configuración: {e}", extra=log_extra_fail)
    logger.error("Asegúrese de que bot_settings.json exista y tenga las claves 'PROFITABILITY_CONFIG' y 'SHADOW_OPTUNA'.", extra=log_extra_fail)
    exit()  # Salir si la configuración no se puede cargar
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
# Conexión a Redis
r = redis_manager.get_connection()
if not r:
    logger.error("No se pudo establecer conexión con Redis. Saliendo.", extra={'symbol': 'REDIS_CONN_FAIL'})
    exit()
# --- FIN DE MODIFICACIÓN ---

# Configuración de CCXT (simulada, no necesita claves de API reales para esto)
exchange = ccxt.binance()


def simulate_trade(signal_data):
    """
    Simula la ejecución de una señal de trading, aplicando slippage y comisiones.
    """
    log_extra = {'transaction_id': signal_data.get('uid', 'N/A_SIM'), 'symbol': signal_data.get('symbol', 'N/A')}
    try:
        symbol = signal_data['symbol']
        side = signal_data['type']  # El logger usa 'type', no 'side'
        price = Decimal(signal_data['price'])
        # Asumir una cantidad fija para simulación si no viene
        quantity = Decimal(signal_data.get('quantity', '1.0')) 
        
        # 1. Simular Slippage (Deslizamiento)
        slippage = price * slippage_percent * (1 if side == 'LONG' else -1)  # Asumir LONG/SHORT
        simulated_price = price + slippage
        
        # 2. Simular Comisión
        commission = (quantity * simulated_price) * commission_percent
        
        # 3. Calcular PnL (placeholder)
        # El PnL real se calculará en Optuna al cerrar la posición virtual.
        
        trade_record = {
            "timestamp": time.time(),
            "symbol": symbol,
            "side": side,  # Guardar el tipo de señal
            "quantity": str(quantity),  # Guardar como string
            "original_price": str(price),  # Guardar como string
            "simulated_price": str(simulated_price),  # Guardar como string
            "commission": str(commission),  # Guardar como string
            "signal_id": signal_data.get('uid', 'N/A')  # Usar 'uid' del shadow_logger
        }
        
        logger.info(f"[SHADOW] {side} {symbol} @ {simulated_price:.4f} (Original: {price:.4f}, Comisión: {commission:.6f})", extra=log_extra)
        
        # --- INICIO DE MODIFICACIÓN: (Paso 4) ---
        # Guardar el trade simulado en Redis (usando la clave de config)
        r.lpush(redis_list_key, json.dumps(trade_record))
        
        # Mantener solo los N trades más recientes (según config)
        r.ltrim(redis_list_key, 0, max_trades - 1)
        # --- FIN DE MODIFICACIÓN ---
       
    except Exception as e:
        logger.error(f"Error al simular trade: {e}. Datos: {signal_data}", exc_info=True, extra=log_extra)


def listen_to_redis():
    """
    Escucha el canal 'bot_signals' en Redis y procesa las señales.
    """
    log_extra = {'symbol': 'REDIS_LISTEN'}
    
    # --- INICIO DE MODIFICACIÓN: (Paso 4) ---
    global r  # Asegurarse de usar la conexión global
    if not r:
        r = redis_manager.get_connection()
        if not r:
            logger.error("Conexión Redis no disponible. No se puede iniciar el listener.", extra=log_extra)
            return

    pubsub = r.pubsub()
    # (redis_channel ya se obtuvo de la config al inicio)
    # --- FIN DE MODIFICACIÓN ---
    
    pubsub.subscribe(redis_channel)
    logger.info(f"... Shadow Engine listo. Esperando señales en el canal '{redis_channel}' ...", extra=log_extra)
    
    # --- MODIFICACIÓN: La función listen() debe iterarse para que el Ctrl+C funcione.
    # El bucle de Redis PubSub es donde ocurre el bloqueo.
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                signal_data = json.loads(message['data'])
                log_extra_msg = {'transaction_id': signal_data.get('uid', 'N/A'), 'symbol': signal_data.get('symbol', 'N/A')}
                logger.info(f"Señal recibida: {signal_data.get('uid')}", extra=log_extra_msg)
                simulate_trade(signal_data)
            except json.JSONDecodeError:
                logger.error(f"Error al decodificar señal JSON: {message['data']}", extra=log_extra)
            except Exception as e:
                logger.error(f"Error procesando mensaje de Redis: {e}", extra=log_extra)


if __name__ == "__main__":
    log_extra_main = {'symbol': 'SHADOW_MAIN'}
    try:
        listen_to_redis()
    except KeyboardInterrupt:
        logger.info("🛑 Interrupción por teclado. Deteniendo Shadow Engine.", extra=log_extra_main)
    except Exception as e:
        logger.error(f"Error inesperado en el bucle principal: {e}", exc_info=True, extra=log_extra_main)
    finally:
        pass