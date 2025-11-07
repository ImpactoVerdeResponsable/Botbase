# strategies/micro_scalping_enhanced.py
from unified_logger import get_logger
from decimal import Decimal
import pandas as pd
import technical_analysis as ta_module
import config

logger = get_logger(__name__)

def get_signal(df, params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs):
    """
    Implementación de MicroScalpingRSI_Enhanced (Fase 2 - Shadow)
    Híbrido: Core RSI + Filtro Sweep + Filtro Volatilidad
    """
    symbol = df.attrs.get('symbol', 'N/A')
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

    try:
        # --- 1. Cargar Parámetros ---
        
        # Core RSI v2.8 (similar a v3)
        rsi_len = int(params.get("RSI_LENGTH", 14))
        pullback_thresh = Decimal(str(params.get("PULLBACK_THRESHOLD", "0.7")))
        
        # Filtro Sweep
        cancel_ratio_thresh = Decimal(str(params.get("QUOTE_CANCEL_RATIO_THRESHOLD", 6)))
        
        # Filtro Volatilidad
        vol_thresh = Decimal(str(params.get("VOL_THRESHOLD", "1.2")))
        
        # (SPREAD_MAX_BP es manejado por strategy_hub/dynamic_params)

        # --- 2. Obtener Indicadores Core ---
        rsi_series = ta_module.get_rsi(df, length=rsi_len)

        min_data = rsi_len + 5
        if df is None or len(df) < min_data or rsi_series is None:
             logger.debug(f"Datos insuficientes para MSRSI_Enhanced.", extra=log_extra)
             return None

        rsi_current = rsi_series.iloc[-1]
        if rsi_current is None:
            logger.debug(f"Valor RSI inválido.", extra=log_extra)
            return None

        # --- 3. Obtener Datos de Microestructura (Inyectados/Stubs) ---
        microstructure_data = kwargs.get('microstructure_data', {})
        
        # Filtro Sweep
        quote_cancel_ratio = Decimal(str(microstructure_data.get('quote_cancel_ratio', 0)))
        # Usar CVD Stub (CVD >= 0)
        cvd_stub_val = ta_module.get_cvd_stub(df, length=20)
        if cvd_stub_val is None:
            logger.debug("CVD Stub no disponible.", extra=log_extra)
            return None
        
        # --- 4. Evaluar Condiciones ---

        # Condición 1: Core RSI v2.8 (RSI > 50)
        cond_rsi = (rsi_current > Decimal('50'))
        if not cond_rsi: return None # Fail fast

        # Condición 2: Core RSI v2.8 (Pullback Vela [-2])
        pullback_candle = df.iloc[-2]
        pb_high = pullback_candle['high']
        pb_low = pullback_candle['low']
        pb_close = pullback_candle['close']
        
        candle_range = pb_high - pb_low
        cond_pullback = False
        if candle_range > 0:
            pullback_depth = (pb_high - pb_close) / candle_range
            cond_pullback = (pullback_depth > pullback_thresh)
        
        if not cond_pullback: return None # Fail fast

        # Condición 3: Core RSI v2.8 (Trigger Vela [-1])
        entry_price = df['close'].iloc[-1]
        cond_trigger = (entry_price > pb_high)
        
        if not cond_trigger: return None # Fail fast

        # Condición 4: Filtro Sweep (Cancel Ratio)
        cond_sweep_cancel = (quote_cancel_ratio > cancel_ratio_thresh)
        if not cond_sweep_cancel:
            logger.debug(f"MSRSI_Enhanced RECHAZADO (Filtro Cancel Ratio: {quote_cancel_ratio} <= {cancel_ratio_thresh})", extra=log_extra)
            return None

        # Condición 5: Filtro Sweep (CVD Stub)
        cond_sweep_cvd = (cvd_stub_val >= 0)
        if not cond_sweep_cvd:
            logger.debug(f"MSRSI_Enhanced RECHAZADO (Filtro CVD Stub: {cvd_stub_val} < 0)", extra=log_extra)
            return None

        # Condición 6: Filtro Volatilidad (ATR Ratio)
        cond_vol = (vol_ratio > vol_thresh)
        if not cond_vol:
            logger.debug(f"MSRSI_Enhanced RECHAZADO (Filtro Volatilidad: {vol_ratio:.2f} <= {vol_thresh})", extra=log_extra)
            return None

        # --- 5. Entrada (Pasa todos los filtros) ---
        
        logger.info(f"🔥 MSRSI_Enhanced (Shadow) señal LONG detectada.", extra=log_extra)
        
        # (SL/TP no son necesarios para el shadow_engine v2.9)
        signal = {
            "type": "LONG"
        }
        if 'strategy_name' in params:
             signal['strategy_name'] = params['strategy_name']
        return signal

    except Exception as e:
        logger.error(f"Error en MSRSI_Enhanced (Shadow) para {symbol}: {e}", exc_info=True, extra=log_extra)
        return None