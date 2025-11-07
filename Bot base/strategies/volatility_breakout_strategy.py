# strategies/volatility_breakout_strategy.py
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
from decimal import Decimal
import pandas as pd
import technical_analysis as ta_module

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

def get_signal(df, params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs):
    symbol = df.attrs.get('symbol', 'N/A')
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

    try:
        bbands_len = int(params.get("BBANDS_LENGTH", 20))
        bbw_lookback = int(params.get("BBW_LOOKBACK", 100))
        
        # --- ################################# ---
        # ---      INICIO DE LA MODIFICACIÓN    ---
        # --- ################################# ---
        # (Cambio 3) Corrección de la errata 'SQUEZZE' -> 'SQUEEZE'
        bbw_percentile_key = "BBW_SQUEEZE_PERCENTILE"
        if "BBW_SQUEZZE_PERCENTILE" not in params:
             bbw_percentile_key = "BBW_SQUEZZE_PERCENTILE" # Fallback por si la errata sigue en el config
        bbw_percentile = int(params.get(bbw_percentile_key, 25))
        # --- ################################# ---
        # ---        FIN DE LA MODIFICACIÓN     ---
        # --- ################################# ---

        bbw = ta_module.get_bollinger_band_width(df, length=bbands_len)
        
        # Validación de datos mejorada
        min_data_bbw = bbw_lookback + bbands_len # Se necesita suficiente data para el lookback + el cálculo de la banda
        if bbw is None or len(bbw.dropna()) < bbw_lookback: 
            logger.debug(f"Datos insuficientes para BBW. Necesarios (dropna): {bbw_lookback}, Disponibles: {len(bbw.dropna()) if bbw is not None else 0}", extra=log_extra)
            return None

        bbw_slice_numeric = pd.to_numeric(bbw.dropna().tail(bbw_lookback), errors='coerce')
        if bbw_slice_numeric.empty:
            logger.debug("Slice numérico de BBW vacío después de coerción.", extra=log_extra)
            return None
            
        squeeze_threshold = Decimal(str(bbw_slice_numeric.quantile(bbw_percentile / 100)))
        
        # --- ################################# ---
        # ---      INICIO DE LA MODIFICACIÓN    ---
        # --- ################################# ---
        
        # Validación de índice (requiere al menos 2 velas para [-1] y [-2])
        if len(df) < 2 or len(bbw) < 2:
            logger.debug(f"Datos insuficientes para Squeeze/Breakout (requiere 2 velas). Disponibles: {len(df)}", extra=log_extra)
            return None
            
        # (Cambio 2) Condición 1: COMPRESIÓN (Squeeze) en la vela ANTERIOR
        bbw_anterior = bbw.iloc[-2]
        if not pd.notna(bbw_anterior):
            logger.debug("BBW anterior es NaN. Saltando.", extra=log_extra)
            return None
            
        is_squeeze = bbw_anterior <= squeeze_threshold
        if not is_squeeze:
            logger.debug(f"Filtro Squeeze RECHAZADO. BBW vela anterior ({bbw_anterior:.4f}) > {squeeze_threshold:.4f}", extra=log_extra)
            return None
        logger.info(f"✅ Filtro de Compresión BBW OK (BBW: {bbw_anterior:.4f} <= {squeeze_threshold:.4f})", extra=log_extra)

        # (Cambio 2) Condición 2: RUPTURA (Breakout) en la vela ACTUAL
        bb_bands = ta_module.get_bollinger_bands_on_series(df['close'], length=bbands_len)
        if bb_bands is None: 
            logger.warning("No se pudieron calcular BBands para el breakout.", extra=log_extra)
            return None
        
        precio_actual = df['close'].iloc[-1]
        bb_upper = bb_bands['upper'].iloc[-1]
        
        if not pd.notna(precio_actual) or not pd.notna(bb_upper):
            logger.warning(f"Precio actual o BB Upper son NaN (Precio: {precio_actual}, BB: {bb_upper}). Saltando.", extra=log_extra)
            return None
            
        is_breakout = precio_actual > bb_upper
        if not is_breakout:
            logger.debug(f"Filtro Breakout RECHAZADO. Precio ({precio_actual}) no superó BB Superior ({bb_upper})", extra=log_extra)
            return None
        logger.info(f"✅ Filtro de Ruptura de BB OK (Precio: {precio_actual} > BB Superior: {bb_upper})", extra=log_extra)

   
        # (Cambio 2) Condición 3: VOLUMEN en la vela ACTUAL
        vma_fast, vma_slow = ta_module.get_volume_oscillator(df, fast_period=1, slow_period=20)
        if vma_fast is None or vma_slow is None: 
            logger.warning("No se pudo calcular el oscilador de volumen.", extra=log_extra)
            return None
        
        vol_actual = vma_fast.iloc[-1]
        vol_medio = vma_slow.iloc[-1]
        
        if not pd.notna(vol_actual) or not pd.notna(vol_medio):
            logger.warning(f"Volumen actual o VMA son NaN (Vol: {vol_actual}, VMA: {vol_medio}). Saltando.", extra=log_extra)
            return None
            
        # Usar un multiplicador fijo simple (1.2x) como en la sugerencia original (Cargado desde params)
        vol_mult = Decimal(str(params.get("VOLUME_BREAKOUT_MULTIPLIER", "1.2")))
        is_volume_ok = vol_actual > (vol_medio * vol_mult)
        
        if not is_volume_ok:
            logger.info(f"Filtro de Volumen de Ruptura RECHAZADO. Vol: {vol_actual} no supera umbral ({vol_medio * vol_mult:.2f}).", extra=log_extra)
            return None
        logger.info("✅ Filtro de Volumen de Ruptura OK.", extra=log_extra)

        # (Cambio 1) Se eliminan todos los filtros de ADX, DMI, RSI y Estructura de Mercado
        
        # (Cambio 2) SEÑAL FINAL BASADA EN LAS 3 CONDICIONES
        # (La lógica de 'entry' ahora es implícita, ya que retornamos None si algo falla)
        
        logger.info(f"🔥🔥🔥 SEÑAL DE COMPRA FINAL CONFIRMADA por 'RupturaDeCompresion' 🔥🔥🔥", extra=log_extra)
        
        signal = {"type": "BUY"}
        if 'strategy_name' in params:
                signal['strategy_name'] = params['strategy_name']
        return signal
        # --- ################################# ---
        # ---        FIN DE LA MODIFICACIÓN     ---
        # --- ################################# ---

    except Exception as e:
        logger.error(f"Error en 'RupturaDeCompresion' para {symbol}: {e}", exc_info=True, extra=log_extra)
        return None