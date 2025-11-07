import json
import redis  # Importar redis (aunque la conexión la maneja el manager)
import uuid
import pandas as pd
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
import config  # Importar config

# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
from redis_manager import redis_manager
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

# --- ################################# ---
# ---  INICIO DE LA MODIFICACIÓN (V4.3 -> V2.6) ---
# --- ################################# ---

# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
# Lógica de conexión (REDIS_HOST, REDIS_PORT, r = redis.Redis(...)) eliminada.
# 'redis_manager' la maneja.
# --- FIN DE MODIFICACIÓN ---

def log_signal(signal: dict, df, tx_id: str):
    log_extra = {'transaction_id': tx_id, 'symbol': df.attrs.get('symbol', 'N/A')}

    # --- INICIO DE MODIFICACIÓN: (Paso 4) ---
    r = redis_manager.get_connection()
    if r is None:
        logger.error("No hay conexión a Redis. No se puede loggear la señal shadow.", extra=log_extra)
        return
        
    CHANNEL = redis_manager.get_config_value('REDIS_CHANNEL')
    if CHANNEL is None:
        logger.error("No se pudo obtener REDIS_CHANNEL. No se puede loggear la señal shadow.", extra=log_extra)
        return
    # --- FIN DE MODIFICACIÓN ---

    signal['uid'] = str(uuid.uuid4())
    signal['ts'] = pd.Timestamp.now(tz='UTC').isoformat()
    signal['symbol'] = df.attrs.get('symbol', 'N/A')
    signal['price'] = str(df['close'].iloc[-1])
    
    try:
        # Publicar en el canal específico (bot_signals_1m)
        r.publish(CHANNEL, json.dumps(signal))
        logger.debug(f"[SHADOW-LOG] {signal['uid']} {signal['symbol']} {signal['type']}", extra=log_extra)
    except Exception as e:
        logger.error(f"Error al publicar señal shadow en Redis: {e}", exc_info=True, extra=log_extra)

# --- ################################# ---
# ---   FIN DE LA MODIFICACIÓN (V4.3 -> V2.6)   ---
# --- ################################# ---