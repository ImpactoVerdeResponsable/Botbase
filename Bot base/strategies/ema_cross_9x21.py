# strategies/ema_cross_9x21.py
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
from decimal import Decimal
import pandas as pd # Importar pandas
import technical_analysis as ta_module # Modificación 4: Usar TA Module

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

def get_signal(df, params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs):
    symbol = df.attrs.get('symbol', 'N/A')
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

    try:
        # --- INICIO DE LA MODIFICACIÓN (Modificación 4: Usar TA Module) ---
        fast     = int(params.get("FAST_EMA", 9))
        slow     = int(params.get("SLOW_EMA", 21))

        # Calcular EMAs usando technical_analysis.py (devuelve series de Decimal)
        ema_f_series = ta_module.get_ema(df, length=fast)
        ema_s_series = ta_module.get_ema(df, length=slow)
        
        # Verificar si hay suficientes datos
        # Se necesita al menos 'slow' periodos + 1 vela anterior para el cruce
        min_data_needed = slow + 1
        if ema_f_series is None or ema_s_series is None or len(ema_f_series.dropna()) < min_data_needed:
             logger.debug(f"Datos insuficientes para EMAcross9x21. Necesarios (dropna): {min_data_needed}, Disponibles: {len(ema_f_series.dropna()) if ema_f_series is not None else 0}", extra=log_extra)
             return None
             
        ema_f = ema_f_series.iloc[-1]
        ema_s = ema_s_series.iloc[-1]
        ema_f_prev = ema_f_series.iloc[-2]
        ema_s_prev = ema_s_series.iloc[-2]
        
        # Validación de NaNs (Regla 8)
        if any(v is None for v in [ema_f, ema_s, ema_f_prev, ema_s_prev]):
            logger.debug("Valores NaN encontrados al final de las series EMA.", extra=log_extra)
            return None

        # Lógica de cruce adaptada a Decimal
        cross_up = (ema_f > ema_s) and (ema_f_prev <= ema_s_prev)
       
        entry    = cross_up
        # --- FIN DE LA MODIFICACIÓN ---

        if entry:
            logger.info(f"🔥 EMAcross9x21 señal LONG detectada.", extra=log_extra)
            signal = {"type": "LONG"}
            if 'strategy_name' in params:
                 signal['strategy_name'] = params['strategy_name']
            return signal
        else:
            return None

    except Exception as e:
        logger.error(f"Error en EMAcross9x21 para {symbol}: {e}", exc_info=True, extra=log_extra)
        return None