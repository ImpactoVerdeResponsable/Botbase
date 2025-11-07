# strategies/volatility_strategy.py
from unified_logger import get_logger
from decimal import Decimal
import pandas as pd
import technical_analysis as ta_module
import dynamic_params
import config

logger = get_logger(__name__)


def get_signal(df, params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs):
    symbol = df.attrs.get('symbol', 'N/A')
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    
    try:
        # --- INICIO DE MODIFICACIÓN: (FIX v2.9.2 - Warning RSI_DYNAMIC_PARAMS) ---
        
        # 1. Obtener el bloque 'parameters' (según bot_settings.json v2.9.1)
        strategy_params = params.get("parameters", {})

        # 2. Buscar RSI_DYNAMIC_PARAMS dentro de 'parameters'
        rsi_dyn_params = strategy_params.get("RSI_DYNAMIC_PARAMS")
         
        if not rsi_dyn_params:
            # Fallback: buscar en el nivel superior (compatibilidad v2.9.0)
            rsi_dyn_params = params.get("RSI_DYNAMIC_PARAMS", {})
            if not rsi_dyn_params:
                logger.warning("No se encontró 'RSI_DYNAMIC_PARAMS' en la config. Usando valores por defecto.", extra=log_extra)
        
        momentum_th = Decimal(str(rsi_dyn_params.get("MOMENTUM_THRESHOLD_LONG", 65)))
        # --- FIN DE MODIFICACIÓN ---

        rsi_period = dynamic_params.get_dynamic_rsi_period(vol_ratio, symbol)
        
        rsi_series = ta_module.get_rsi(df, length=rsi_period)
        if rsi_series is None or len(rsi_series.dropna()) < 2: 
            logger.debug(f"Datos insuficientes para RSI. Requeridos (dropna): 2, Disponibles: {len(rsi_series.dropna()) if rsi_series is not None else 0}", extra=log_extra)
            return None

        rsi_current, rsi_previous = rsi_series.iloc[-1], rsi_series.iloc[-2]
        
        if not pd.notna(rsi_current) or not pd.notna(rsi_previous):
            logger.debug(f"Valores NaN en RSI (Actual: {rsi_current}, Prev: {rsi_previous}).", extra=log_extra)
            return None

        # Señal: RSI cruza el umbral de momentum
        if rsi_previous < momentum_th and rsi_current >= momentum_th:
            logger.info(f"✅ Filtro RSI Dinámico OK (RSI({rsi_period}): {rsi_current:.2f} cruzó umbral {momentum_th})", extra=log_extra)
            logger.info(f"🔥🔥🔥 SEÑAL DE COMPRA FINAL CONFIRMADA por 'ImpulsoDeVolatilidad' 🔥🔥🔥", extra=log_extra)
            
            signal = {"type": "BUY"}  # 'BUY' es consistente con el original
            if 'strategy_name' in params:
                signal['strategy_name'] = params['strategy_name']
            return signal

        logger.debug(f"Filtro RSI Dinámico RECHAZADO. Sin cruce de momentum (RSI({rsi_period}): {rsi_current:.2f})", extra=log_extra)
        return None

    except Exception as e:
        logger.error(f"Error en 'ImpulsoDeVolatilidad' para {symbol}: {e}", exc_info=True, extra=log_extra)
        return None