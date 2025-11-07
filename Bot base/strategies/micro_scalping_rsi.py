# strategies/micro_scalping_rsi.py
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
from decimal import Decimal
import pandas as pd # Importar pandas
import technical_analysis as ta_module # Modificación 4: Usar TA Module
# --- INICIO DE MODIFICACIÓN: (Fase 1 - v3) ---
import config
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

def get_signal(df, params, vol_ratio, macro_trend_is_up, transaction_id, **kwargs):
    """
    Implementación de MicroScalpingRSI_v3 (Fase 1)
    Lógica:
    1. RSI(14) > 50 (Régimen alcista)
    2. Vela [-2] (Pullback) debe cerrar en su 30% inferior (depth > 0.7)
    3. Vela [-1] (Trigger) debe cerrar por encima del HIGH de la vela [-2]
    4. SL/TP calculados dinámicamente por ATR(14)
    """
    symbol = df.attrs.get('symbol', 'N/A')
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

    try:
    
        # --- INICIO DE MODIFICACIÓN (Fase 1 - v3) ---
        
        # 1. Cargar Parámetros
        rsi_len = int(params.get("RSI_LENGTH", 14))
        pullback_thresh = Decimal(str(params.get("PULLBACK_THRESHOLD", "0.7")))
        atr_len = int(params.get("ATR_WINDOW", 14))
        
        # (SPREAD_MAX_BP es manejado por dynamic_params.apply_spread_filter)
        
      
        # 2. Obtener Indicadores
        rsi_series = ta_module.get_rsi(df, length=rsi_len)
        _, atr_value = ta_module.get_atr(df, length=atr_len)

        # 3. Validar datos suficientes
        min_data = max(rsi_len, atr_len) + 5
        if df is None or len(df) < min_data:
             logger.debug(f"Datos insuficientes para MicroScalpingRSI_v3. Necesarios: {min_data}, Disponibles: {len(df) if df is not None else 0}", extra=log_extra)
       
             return None

        rsi_current = rsi_series.iloc[-1]
        if rsi_current is None or atr_value is None or atr_value <= 0:
            logger.debug(f"Valores de indicador inválidos (RSI: {rsi_current}, ATR: {atr_value}).", extra=log_extra)
            return None

        # 4. Lógica de Señal (v3)

        # Condición 1: Régimen RSI (RSI > 50)
       
        cond_rsi = (rsi_current > Decimal('50'))

        # Validación de índice explícita (aunque min_data ya debería cubrirlo)
        if len(df) < 3:
            logger.debug(f"Datos insuficientes para pullback (requiere 3 velas). Disponibles: {len(df)}", extra=log_extra)
            return None

        # Condición 2: Pullback (Vela [-2] cerró en el 30% inferior)
        pullback_candle = df.iloc[-2]
        pb_high = pullback_candle['high']
        pb_low = pullback_candle['low']
        pb_close = pullback_candle['close']
        
        # Validación de NaNs en precios de pullback
        if not all(pd.notna([pb_high, pb_low, pb_close])):
            logger.warning("Valores NaN (Nulos) detectados en la vela de pullback. Abortando señal.", extra=log_extra)
            return None
            
        candle_range = pb_high - pb_low
        
        cond_pullback = False
        if candle_range > 0:
            pullback_depth = (pb_high - pb_close) / candle_range
            cond_pullback = (pullback_depth > pullback_thresh)
        
        # Condición 3: Trigger (Vela [-1] rompe el high de la vela pullback)
        entry_price = df['close'].iloc[-1]
        cond_trigger = (entry_price > pb_high)

    
        # 5. Entrada
        entry = cond_rsi and cond_pullback and cond_trigger
        
        # --- FIN DE LA MODIFICACIÓN ---
        
        if entry:
            logger.info(f"🔥 MicroScalpingRSI_v3 señal LONG detectada.", extra=log_extra)
            
            
 # --- INICIO DE MODIFICACIÓN (Fase 1 - SL/TP dinámico) ---
            # Implementar SL/TP dinámico según especificación (cargado desde params)
            sl_mult = Decimal(str(params.get("SL_ATR_MULTIPLIER", "2.5")))
            tp_mult_base = Decimal(str(params.get("TP_ATR_BASE_MULTIPLIER", "1.5")))
            rr = Decimal(str(config.get_param("RISK_MANAGEMENT_CONFIG.RISK_REWARD_RATIO", "1.5")))

            sl_price = entry_price - (atr_value * sl_mult)
            
 # TP = entry + (ATR * 1.5 * RR)
            tp_price = entry_price + (atr_value * tp_mult_base * rr)
            # --- FIN DE MODIFICACIÓN ---

            signal = {
                "type": "LONG",
                "stop_loss_price": sl_price,
      
                "take_profit_price": tp_price
            }
            if 'strategy_name' in params:
                 signal['strategy_name'] = params['strategy_name']
            return signal
        else:
            return None

    except Exception as e:
  
        logger.error(f"Error en MicroScalpingRSI_v3 para {symbol}: {e}", extra=log_extra)
        return None