# dynamic_params.py
from unified_logger import get_logger
from decimal import Decimal, InvalidOperation
import time
from typing import Optional, Dict

import technical_analysis as ta
import config
import binance_interaction as bi

logger = get_logger(__name__)

# --- INICIO DE MODIFICACIÓN: (FIX v2.9.2 - AttributeError) ---
_spread_cache: Dict[str, tuple] = {}
_SPREAD_CACHE_TTL_SECONDS = 20  # Cachear spread por 20 segundos


def get_cached_spread(symbol: str, transaction_id: str) -> Optional[Decimal]:
# --- FIN DE MODIFICACIÓN ---
    """
    Obtiene el spread (en %) desde un cache interno o desde la API.
    Resuelve el problema de llamadas repetidas (Solución 6) y bloqueo (Solución 5).
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    current_time = time.time()

    # 1. Verificar Cache
    if symbol in _spread_cache:
        cached_time, cached_spread = _spread_cache[symbol]
        if (current_time - cached_time) < _SPREAD_CACHE_TTL_SECONDS:
            logger.debug(f"Spread cache HIT para {symbol}: {cached_spread:.4f}%", extra=log_extra)
            return cached_spread

    # 2. Cache Miss o Expirado
    logger.debug(f"Spread cache MISS para {symbol}. Llamando a API.", extra=log_extra)
    try:
        # Esta es la llamada bloqueante (síncrona)
        current_spread_pct = bi.get_current_spread(symbol, transaction_id=f"{transaction_id}_SpreadCheck")

        if current_spread_pct == Decimal('Infinity') or current_spread_pct is None:
            logger.warning(f"No se pudo obtener spread válido para {symbol} desde la API.", extra=log_extra)
            return None  # No cachear un error

        # 3. Actualizar Cache
        _spread_cache[symbol] = (current_time, current_spread_pct)
        return current_spread_pct

    except Exception as e:
        logger.error(f"Error obteniendo spread para cache: {e}", exc_info=True, extra=log_extra)
        return None  # Fallback seguro


def calculate_volatility_ratio(df_klines, short_atr_period, long_atr_period, symbol, transaction_id):
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

    if not all([short_atr_period, long_atr_period]):
        logger.warning("Períodos de ATR para ratio de volatilidad no configurados. Saltando cálculo.", extra=log_extra)
        return Decimal("1.0")

    min_len = int(long_atr_period) + 1
    if df_klines is None or len(df_klines) < min_len:
        logger.debug(f"Datos insuficientes para ratio de volatilidad. Necesarios: {min_len}, Disponibles: {len(df_klines) if df_klines is not None else 0}", extra=log_extra)
        return Decimal("1.0")

    _, atr_short = ta.get_atr(df_klines, length=int(short_atr_period))
    _, atr_long = ta.get_atr(df_klines, length=int(long_atr_period))

    if atr_short is None or atr_long is None or atr_long <= 0:
        logger.warning("No se pudo calcular ATR para ratio de volatilidad.", extra=log_extra)
        return Decimal("1.0")

    volatility_ratio = atr_short / atr_long
    logger.debug(f"Ratio de volatilidad calculado: {volatility_ratio:.2f}", extra=log_extra)
    return volatility_ratio


def get_dynamic_risk_multiplier(vol_ratio, symbol):
    params = config.get_param("DYNAMIC_PARAMS_CONFIG.RISK_MANAGEMENT", symbol=symbol, return_dict_for_complex=True)
    base = Decimal(str(params.get("STOP_LOSS_ATR_MULTIPLIER_BASE", "2.5")))
    adj_high_vol = Decimal(str(params.get("STOP_LOSS_ATR_MULTIPLIER_ADJ_HIGH_VOL", "0.5")))

    high_vol_threshold = Decimal(str(config.get_param("DYNAMIC_PARAMS_CONFIG.VOL_RATIO_HIGH_THRESHOLD", "1.3")))

    if vol_ratio > high_vol_threshold:
        return base + adj_high_vol
    return base


def get_dynamic_rsi_period(vol_ratio, symbol):
    """
    Calcula un período de RSI dinámico basado en la volatilidad del mercado.
    """
    log_extra = {'symbol': symbol}
    params = config.get_param("DYNAMIC_PARAMS_CONFIG.RSI_PERIOD", symbol=symbol, return_dict_for_complex=True)
    if not params:
        logger.warning("No se encontró configuración para RSI dinámico. Usando período por defecto de 14.", extra=log_extra)
        return 14

    base = int(params.get("BASE_PERIOD", 14))
    adj_low = int(params.get("ADJUSTMENT_LOW_VOL", 4))
    adj_high = int(params.get("ADJUSTMENT_HIGH_VOL", -4))
    min_p = int(params.get("MIN_PERIOD", 8))
    max_p = int(params.get("MAX_PERIOD", 24))

    low_thresh = Decimal(str(config.get_param("DYNAMIC_PARAMS_CONFIG.VOL_RATIO_LOW_THRESHOLD", "0.8")))
    high_thresh = Decimal(str(config.get_param("DYNAMIC_PARAMS_CONFIG.VOL_RATIO_HIGH_THRESHOLD", "1.3")))

    if vol_ratio < low_thresh:
        period = base + adj_low
    elif vol_ratio > high_thresh:
        period = base + adj_high
    else:
        period = base

    final_period = max(min_p, min(period, max_p))
    logger.debug(f"Período de RSI dinámico calculado: {final_period} para {symbol}", extra={'symbol': symbol})
    return final_period


# --- INICIO DE MODIFICACIÓN: (v2.9.1) Solución 1 (Patch Experto) ---
# Helper de robustez de tipos (sugerido por Experto GPT)
# --- INICIO DE MODIFICACIÓN: (FIX v2.9.2 - AttributeError) ---
def to_decimal_safe(value, default=Decimal('0')):
# --- FIN DE MODIFICACIÓN ---
    try:
        if value is None:
            return default
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


def apply_spread_filter(symbol: str, strategy_name: str, original_signal: dict, transaction_id: str):
    """
    Filtra señales basado en condiciones de spread actuales.
    Retorna: signal modificado o None si el filtro bloquea la señal.
    Utiliza el spread cacheado (Solución 6).
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    try:
        # --- INICIO DE MODIFICACIÓN: (FIX v2.9.2 - AttributeError) ---
        # Obtener spread actual (en %) desde el cache (Solución 6)
        current_spread_pct = get_cached_spread(symbol, transaction_id)
        # --- FIN DE MODIFICACIÓN ---
        
        if current_spread_pct is None:
            logger.warning(f"Spread no disponible (None) para {symbol}. Permitiedo señal por fallback.", extra=log_extra)
            return original_signal

        # Convertir a Basis Points (BPs)
        current_spread_bp = current_spread_pct * 100

        # Obtener threshold de configuración (en BPs)
        max_spread_bp = config.get_param(
            f"STRATEGIES_CONFIG.{strategy_name}.parameters.MAX_SPREAD_BP",
            default_value=5.0
        )
        # --- INICIO DE MODIFICACIÓN: (FIX v2.9.2 - AttributeError) ---
        max_spread_bp_decimal = to_decimal_safe(max_spread_bp, default=Decimal('5.0'))
        # --- FIN DE MODIFICACIÓN ---

        if current_spread_bp > max_spread_bp_decimal:
            logger.info(
                f"🚫 Filtro de Spread RECHAZADO: {symbol} [{strategy_name}] - "
                f"Spread Actual: {float(current_spread_bp):.2f}bp > Máx Permitido: {float(max_spread_bp_decimal):.2f}bp",
                extra=log_extra
            )
            return None

        logger.debug(f"Filtro de Spread OK: {current_spread_bp:.2f}bp <= {max_spread_bp_decimal}bp", extra=log_extra)
        return original_signal

    except Exception as e:
        logger.error(f"Error en apply_spread_filter: {e}", extra=log_extra, exc_info=True)
        return original_signal