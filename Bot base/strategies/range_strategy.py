# strategies/range_strategy.py
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
from decimal import Decimal
import pandas as pd
import technical_analysis as ta_module
import dynamic_params # Mantenido para get_dynamic_rsi_period

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

def get_signal(df, params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs):
    symbol = df.attrs.get('symbol', 'N/A')
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

 
    try:
        # --- INICIO DE LA MODIFICACIÓN ---
        # Eliminado el filtro de volatilidad mínima (MIN_ATR_PERCENTAGE).
        # Este filtro prevenía que la estrategia de rango operara en
        # condiciones de baja volatilidad (lo cual es contradictorio).
        # min_atr_pct = Decimal(str(params.get("MIN_ATR_PERCENTAGE", "0.0")))
        # if min_atr_pct > 0:
        #     ... (lógica eliminada) ...
        # --- FIN DE LA MODIFICACIÓN ---

        # --- LÓGICA EXISTENTE DE LA ESTRATEGIA ---
        rsi_bb_params = params.get("RSI_BB_PARAMS", {})
        bb_len = int(rsi_bb_params.get("BBANDS_LENGTH", 20))
        bb_std = float(rsi_bb_params.get("BBANDS_STD_DEV", 2.0))
      
         
        # Usar el período de RSI dinámico (como en el original)
        dyn_rsi_period = dynamic_params.get_dynamic_rsi_period(vol_ratio, symbol)

        rsi_series = ta_module.get_rsi(df, length=dyn_rsi_period)
        if rsi_series is None or len(rsi_series.dropna()) < 2: 
            logger.debug(f"Datos insuficientes para RSI. Necesarios (dropna): 2, Disponibles: {len(rsi_series.dropna()) if rsi_series is not None else 0}", extra=log_extra)
            return None
        
     
        bb_bands = ta_module.get_bollinger_bands_on_series(rsi_series, length=bb_len, std_dev=bb_std, symbol_for_log=symbol)
        if bb_bands is None or len(bb_bands.dropna()) < 2: 
            logger.debug(f"Datos insuficientes para BB en RSI. Necesarios (dropna): 2, Disponibles: {len(bb_bands.dropna()) if bb_bands is not None else 0}", extra=log_extra)
            return None

        # Acceder a los últimos valores
        rsi_current, rsi_previous = rsi_series.iloc[-1], rsi_series.iloc[-2]
        bb_lower_current = bb_bands['lower'].iloc[-1]

        # Validación de NaNs (Regla 8)
        if not pd.notna(rsi_current) or not pd.notna(rsi_previous) or not pd.notna(bb_lower_current): 
            logger.debug(f"Valores NaN en RSI o BB(RSI). RSI: {rsi_current}, Prev: {rsi_previous}, BB_Low: {bb_lower_current}", extra=log_extra)
            return None

        # Señal: RSI cruza hacia ARRIBA su propia banda de Bollinger inferior
        if rsi_previous < bb_lower_current and rsi_current >= bb_lower_current:
            logger.info(f"🔥🔥🔥 SEÑAL DE COMPRA FINAL CONFIRMADA por 'ReversionEnRango' 🔥🔥🔥", extra=log_extra)
    
            
            signal = {"type": "BUY"} # 'BUY' es consistente con el original
            if 'strategy_name' in params:
                 signal['strategy_name'] = params['strategy_name']
            return signal
        
        return None
    except Exception as e:
        logger.error(f"Error en 'ReversionEnRango' para {symbol}: {e}", exc_info=True, extra=log_extra)
        return None