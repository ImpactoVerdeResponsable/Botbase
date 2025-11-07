# market_analyzer.py
from unified_logger import get_logger
from decimal import Decimal
import pandas as pd
import technical_analysis as ta_module
import datetime  # Necesario para la validación de tiempo (Punto Crítico 3)
import position_management as pm  # <-- MODIFICACIÓN (v2.9.1)

logger = get_logger(__name__)


# --- INICIO DE MODIFICACIÓN: (Punto Crítico 3.1 + v2.9.1) ---
def validate_market_data_quality(df: pd.DataFrame, symbol: str) -> bool:
    """Valida calidad de datos antes del análisis"""
    log_extra = {'symbol': symbol, 'transaction_id': 'DATA_QA'}

    if df.empty:
        logger.warning(f"Validación fallida: DataFrame vacío para {symbol}.", extra=log_extra)
        pm.increment_data_quality_failure(symbol)  # <-- MODIFICACIÓN (v2.9.1)
        return False

    # Asegurar que la columna 'timestamp' sea datetime para diff()
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        logger.warning(f"Validación fallida: Columna 'timestamp' no es datetime en {symbol}.", extra=log_extra)
        # Esto debería estar resuelto por binance_interaction.py, pero es una validación de seguridad
        pm.increment_data_quality_failure(symbol)  # <-- MODIFICACIÓN (v2.9.1)
        return False

    # 1. Verificar gaps temporales (Más de 2 minutos de gap, asumiendo velas de 1m)
    time_diff = df['timestamp'].diff().dt.total_seconds()
    if (time_diff > 120).any():
        logger.warning(f"Validación fallida: Gap temporal (>120s) detectado en {symbol}.", extra=log_extra)
        pm.increment_data_quality_failure(symbol)  # <-- MODIFICACIÓN (v2.9.1)
        return False

    # 2. Verificar outliers de precio (Cambio > 10%)
    price_change = df['close'].pct_change().abs()
    # Convertir a float para la comparación, ya que df['close'] es Decimal/None
    price_change_float = pd.to_numeric(price_change, errors='coerce')

    if (price_change_float > 0.1).any():  # Cambio > 10%
        logger.warning(f"Validación fallida: Posible outlier de precio (>10%) detectado en {symbol}.", extra=log_extra)
        pm.increment_data_quality_failure(symbol)  # <-- MODIFICACIÓN (v2.9.1)
        return False

    return True
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: (Solución Error KITEUSDT) ---
def count_crossings(series1, series2):
    """Cuenta cuántas veces dos series se han cruzado."""
    log_extra = {'symbol': 'COUNT_CROSSINGS'}
    try:
        # Convertir ambas series a numérico (float/NaN) e ignorar índices
        s1_numeric = pd.to_numeric(series1, errors='coerce').reset_index(drop=True)
        s2_numeric = pd.to_numeric(series2, errors='coerce').reset_index(drop=True)

        pos = s1_numeric > s2_numeric
        crosses = (pos != pos.shift(1)).astype(int).sum()
        return crosses
    except Exception as e:
        logger.error(f"Error en count_crossings: {e}", exc_info=True, extra=log_extra)
        return 0  # Devolver 0 si la comparación falla
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: Integración de la validación de calidad ---


def determine_market_state(df, config, log_extra):

    # Ejecutar validación de calidad de datos antes de análisis
    if not validate_market_data_quality(df, log_extra.get('symbol', 'N/A')):
        logger.warning(f"Saltando análisis de mercado para {log_extra.get('symbol')} debido a baja calidad de datos.", extra=log_extra)
        return "INCIERTA"  # Retornar INCIERTA si los datos no son fiables

    # --- Lógica existente ---
    try:
        ema_fast_p = int(config.get("EMA_FAST_PERIOD", 21))
        ema_medium_p = int(config.get("EMA_MEDIUM_PERIOD", 50))
        adx_p = int(config.get("ADX_PERIOD", 14))
        adx_trend_th = Decimal(str(config.get("ADX_TREND_THRESHOLD", 20)))

        choppy_lookback = int(config.get("CHOPPY_LOOKBACK_PERIOD", 100))
        choppy_threshold = int(config.get("CHOPPY_CROSSING_THRESHOLD", 10))

        ema_fast = ta_module.get_ema(df, length=ema_fast_p)
        ema_medium = ta_module.get_ema(df, length=ema_medium_p)
        adx, dmi_plus, dmi_minus = ta_module.get_adx_dmi(df, length=adx_p)

        if any(s is None or s.empty for s in [ema_fast, ema_medium, adx, dmi_plus, dmi_minus]):
            return "INCIERTA"

        if len(df) > choppy_lookback and len(ema_fast.dropna()) > choppy_lookback:
            crossings = count_crossings(df['close'].tail(choppy_lookback), ema_fast.tail(choppy_lookback))
            if crossings > choppy_threshold:
                logger.debug(f"Detectado mercado 'CHOPPY'. Cruces ({crossings}) > Umbral ({choppy_threshold})", extra=log_extra)
                return "CHOPPY"

        last_adx, last_dmi_plus, last_dmi_minus = adx.iloc[-1], dmi_plus.iloc[-1], dmi_minus.iloc[-1]
        last_ema_fast, last_ema_medium = ema_fast.iloc[-1], ema_medium.iloc[-1]

        if any(v is None for v in [last_adx, last_dmi_plus, last_dmi_minus, last_ema_fast, last_ema_medium]):
            return "INCIERTA"

        is_trending = last_adx > adx_trend_th
        is_ranging = last_adx <= adx_trend_th

        if is_trending:
            if last_ema_fast > last_ema_medium and last_dmi_plus > last_dmi_minus:
                return "FUERTE_ALCISTA"
            elif last_ema_fast < last_ema_medium and last_dmi_minus > last_dmi_plus:
                return "FUERTE_BAJISTA"
            elif last_dmi_plus > last_dmi_minus:
                return "INICIO_ALCISTA"
            else:
                return "INICIO_BAJISTA"

        if is_ranging:
            bbw = ta_module.get_bollinger_band_width(df)
            if bbw is not None and not bbw.empty:
                # Asegurar que no haya NaNs en el cálculo de percentil
                bbw_numeric = pd.to_numeric(bbw, errors='coerce').dropna()
                if not bbw_numeric.empty:
                    bbw_percentile = bbw_numeric.rank(pct=True).iloc[-1]
                    if bbw_percentile < 0.25:
                        return "COMPRESION_VOLATIL"

            if last_ema_fast > last_ema_medium:
                return "LATERAL_CON_SESGO_ALCISTA"
            elif last_ema_fast < last_ema_medium:
                return "LATERAL_CON_SESGO_BAJISTA"
            else:
                return "LATERAL_CONSOLIDACION"

        return "INCIERTA"

    except Exception as e:
        logger.error(f"Error determinando estado de mercado: {e}", exc_info=True, extra=log_extra)
        return "INCIERTA"

# --- Lógica de Event-Based Allocation (Tarea 6) ---


def is_high_impact_event_day():
    """
    Detectar días de eventos de alta volatilidad (CPI, FOMC)
    En producción, esto se conectaría a un calendario económico
    """
    # IMPLEMENTAR: Lógica para detectar eventos CPI/FOMC
    # Por ahora, placeholder para integración futura
    return False


def get_event_allocation_boost(strategy_name):
    """
    Retorna multiplier de allocation para eventos
    """
    event_boosts = {
        "ImpulsoDeVolatilidad": 1.5,  # +50% en eventos
        "RupturaDeCompresion": 1.3,   # +30% en eventos
        "MicroScalpingRSI": 1.0       # Sin boost
    }

    if is_high_impact_event_day():
        return event_boosts.get(strategy_name, 1.0)
    return 1.0
# --- FIN DE MODIFICACIÓN ---