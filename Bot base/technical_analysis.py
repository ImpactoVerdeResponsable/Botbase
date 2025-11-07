# technical_analysis.py
from unified_logger import get_logger
from decimal import Decimal
import pandas as pd
import pandas_ta as ta
import numpy as np
import hashlib
import json
from datetime import datetime, timedelta
from collections import OrderedDict
from exceptions import InsufficientDataError

logger = get_logger(__name__)

DECIMAL_PRECISION = Decimal('0.00000001')


# --- INICIO DE MODIFICACIÓN: Implementación de la Solución 1.3 (LRUCache) ---
class LRUCache:
    def __init__(self, max_size=1000):
        self.cache = OrderedDict()
        self.max_size = max_size

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def set(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.max_size:
            self.cache.popitem(last=False)

    def get_cache_key(self, symbol, indicator, params):
        """Genera clave única para cache"""
        param_str = json.dumps(params, sort_keys=True)
        key_str = f"{symbol}_{indicator}_{param_str}"
        return hashlib.md5(key_str.encode()).hexdigest()
# --- FIN DE MODIFICACIÓN ---


# Instancia global
indicator_cache = LRUCache(max_size=1000)


# --- INICIO DE MODIFICACIÓN: Implementación de la Solución 1.1 ---
def _cached_indicator_calculation(indicator_name: str, calculation_func: callable,
                                  symbol: str, params: dict, *args, **kwargs):
    """
    Patrón genérico para cálculo de indicadores con cache
    """
    log_extra = {'symbol': symbol, 'transaction_id': 'CACHE_WRAPPER'}
    cache_key = indicator_cache.get_cache_key(symbol, indicator_name, params)
    cached_result = indicator_cache.get(cache_key)

    if cached_result is not None:
        logger.debug(f"Cache HIT para {indicator_name}{params} en {symbol}", extra=log_extra)
        return cached_result

    logger.debug(f"Cache MISS para {indicator_name}{params} en {symbol}", extra=log_extra)
    # Los *args y **kwargs de la función original pueden ser usados si el cálculo los requiere.
    result = calculation_func(*args, **kwargs)

    if result is not None:
        # Manejar casos de tuplas (ej. ATR)
        if isinstance(result, tuple):
            # No cachear si el resultado es una tupla compuesta solo de None
            if all(v is None for v in result):
                pass
            else:
                indicator_cache.set(cache_key, result)
        else:
            indicator_cache.set(cache_key, result)

    return result
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_rsi (Solución 1.2) ---
def get_rsi(df_for_rsi, length=14):
    symbol_for_log = df_for_rsi.attrs.get('symbol', 'N/A_RSI_ATTR') if hasattr(df_for_rsi, 'attrs') else 'N/A_RSI_NO_ATTR'
    log_extra = {'symbol': symbol_for_log}
    params = {'length': length}

    def _calculate_rsi():
        min_data_needed = int(length) + 10
        if df_for_rsi is None or len(df_for_rsi) < min_data_needed:
            return None
        try:
            close_prices = pd.to_numeric(df_for_rsi['close'], errors='coerce')
            if close_prices.isnull().all():
                return None

            rsi_series = ta.rsi(close=close_prices, length=int(length))
            if rsi_series is None or rsi_series.empty:
                return None

            return rsi_series.apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
        except Exception as e:
            logger.error(f"Error calculando RSI para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None

    return _cached_indicator_calculation('RSI', _calculate_rsi, symbol_for_log, params)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_ema (Solución 1.2) ---
def get_ema(df, length=200):
    symbol_for_log = df.attrs.get('symbol', 'N/A_EMA_ATTR') if hasattr(df, 'attrs') else 'N/A_EMA_NO_ATTR'
    log_extra = {'symbol': symbol_for_log}
    params = {'length': length}

    def _calculate_ema():
        min_data_needed = int(length)
        if df is None or len(df) < min_data_needed:
            return None
        try:
            close_prices = pd.to_numeric(df['close'], errors='coerce')
            if close_prices.isnull().all():
                return None

            ema_series = ta.ema(close_prices, length=int(length))
            if ema_series is None or ema_series.empty:
                return None

            return ema_series.apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
        except Exception as e:
            logger.error(f"Error calculando EMA para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None

    return _cached_indicator_calculation('EMA', _calculate_ema, symbol_for_log, params)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_sma (Solución 1.2) ---
def get_sma(series_for_sma, length=50):
    log_extra = {'symbol': 'GENERIC_SERIES'}
    symbol_for_log = 'GENERIC_SERIES'
    if hasattr(series_for_sma, 'attrs'):
        symbol_for_log = series_for_sma.attrs.get('symbol', 'GENERIC_SERIES_ATTR')

    params = {'length': length}

    def _calculate_sma():
        min_data_needed = int(length)
        if series_for_sma is None or len(series_for_sma) < min_data_needed:
            return None
        try:
            numeric_series = pd.to_numeric(series_for_sma, errors='coerce')
            if numeric_series.isnull().all():
                return None

            sma_series = ta.sma(numeric_series, length=int(length))
            if sma_series is None or sma_series.empty:
                return None
            return sma_series.apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
        except Exception as e:
            logger.error(f"Error calculando SMA: {e}", exc_info=True, extra=log_extra)
            return None

    return _cached_indicator_calculation('SMA', _calculate_sma, symbol_for_log, params)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_atr (Solución 1.2) ---
def get_atr(df_for_atr, length=14):
    symbol_for_log = df_for_atr.attrs.get('symbol', 'N/A_ATR_ATTR') if hasattr(df_for_atr, 'attrs') else 'N/A_ATR_NO_ATTR'
    log_extra = {'symbol': symbol_for_log}
    params = {'length': length}

    def _calculate_atr():
        min_data_needed = int(length) + 1
        if df_for_atr is None or len(df_for_atr) < min_data_needed:
            return None, None
        try:
            high = pd.to_numeric(df_for_atr['high'], errors='coerce')
            low = pd.to_numeric(df_for_atr['low'], errors='coerce')
            close = pd.to_numeric(df_for_atr['close'], errors='coerce')

            if high.isnull().all() or low.isnull().all() or close.isnull().all():
                return None, None

            atr_series = ta.atr(high=high, low=low, close=close, length=int(length))
            if atr_series is None or atr_series.empty:
                return None, None

            atr_values = atr_series.apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
            current_last_atr = atr_values.iloc[-1] if not atr_values.empty else None

            return atr_values, current_last_atr
        except Exception as e:
            logger.error(f"Error calculando ATR para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None, None

    result = _cached_indicator_calculation('ATR', _calculate_atr, symbol_for_log, params)
    return result if result else (None, None)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_bollinger_bands_on_series (Solución 1.2) ---
def get_bollinger_bands_on_series(series_for_bb, length=20, std_dev="2.0", symbol_for_log="N/A_BB_SERIES_ATTR"):
    log_extra = {'symbol': symbol_for_log}
    params = {'length': length, 'std_dev': std_dev}

    def _calculate_bbands():
        min_data_needed = int(length)
        if series_for_bb is None or len(series_for_bb) < min_data_needed:
            return None
        try:
            numeric_series = pd.to_numeric(series_for_bb, errors='coerce')
            if numeric_series.isnull().all():
                return None

            bb_df = ta.bbands(close=numeric_series, length=int(length), std=float(std_dev))
            if bb_df is None or bb_df.empty:
                return None

            bb_lower_col = f'BBL_{int(length)}_{float(std_dev)}'
            bb_middle_col = f'BBM_{int(length)}_{float(std_dev)}'
            bb_upper_col = f'BBU_{int(length)}_{float(std_dev)}'

            if not all(col in bb_df.columns for col in [bb_lower_col, bb_middle_col, bb_upper_col]):
                return None

            return pd.DataFrame({
                'lower': bb_df[bb_lower_col].apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None),
                'middle': bb_df[bb_middle_col].apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None),
                'upper': bb_df[bb_upper_col].apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
            })
        except Exception as e:
            logger.error(f"Error calculando BBands en serie para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None

    return _cached_indicator_calculation('BBANDS_SERIES', _calculate_bbands, symbol_for_log, params)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_volume_oscillator (Solución 1.2) ---
def get_volume_oscillator(df, fast_period=5, slow_period=20):
    symbol_for_log = df.attrs.get('symbol', 'N/A_VMA_ATTR') if hasattr(df, 'attrs') else 'N/A_VMA_NO_ATTR'
    log_extra = {'symbol': symbol_for_log}
    params = {'fast': fast_period, 'slow': slow_period}

    def _calculate_vol_osc():
        min_data_needed = int(slow_period)
        if df is None or len(df) < min_data_needed:
            return None, None
        try:
            volume_series = pd.to_numeric(df['volume'], errors='coerce')
            if volume_series.isnull().all():
                return None, None

            vma_fast = ta.sma(volume_series, length=int(fast_period))
            vma_slow = ta.sma(volume_series, length=int(slow_period))
            if vma_fast is None or vma_slow is None:
                return None, None

            vma_fast = vma_fast.apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
            vma_slow = vma_slow.apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)

            return vma_fast, vma_slow
        except Exception as e:
            logger.error(f"Error calculando Oscilador de Volumen para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None, None

    result = _cached_indicator_calculation('VOL_OSC', _calculate_vol_osc, symbol_for_log, params)
    return result if result else (None, None)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_adx_dmi (Solución 1.2) ---
def get_adx_dmi(df_for_adx, length=14):
    symbol_for_log = df_for_adx.attrs.get('symbol', 'N/A_ADX_ATTR') if hasattr(df_for_adx, 'attrs') else 'N/A_ADX_NO_ATTR'
    log_extra = {'symbol': symbol_for_log}
    params = {'length': length}

    def _calculate_adx():
        min_data_needed = int(length) * 2
        if df_for_adx is None or len(df_for_adx) < min_data_needed:
            return None, None, None
        try:
            high = pd.to_numeric(df_for_adx['high'], errors='coerce')
            low = pd.to_numeric(df_for_adx['low'], errors='coerce')
            close = pd.to_numeric(df_for_adx['close'], errors='coerce')

            if high.isnull().all() or low.isnull().all() or close.isnull().all():
                return None, None, None

            adx_df = ta.adx(high=high, low=low, close=close, length=int(length))
            if adx_df is None or adx_df.empty:
                return None, None, None

            adx_col, dip_col, dim_col = f"ADX_{length}", f"DMP_{length}", f"DMN_{length}"

            if not all(col in adx_df.columns for col in [adx_col, dip_col, dim_col]):
                return None, None, None

            adx_series = adx_df[adx_col].apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
            dip_series = adx_df[dip_col].apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
            dim_series = adx_df[dim_col].apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)

            return adx_series, dip_series, dim_series
        except Exception as e:
            logger.error(f"Error calculando ADX/DMI para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None, None, None

    result = _cached_indicator_calculation('ADX_DMI', _calculate_adx, symbol_for_log, params)
    return result if result else (None, None, None)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_stochastic_rsi (Solución 1.2) ---
def get_stochastic_rsi(df, length=14, rsi_length=14, k=3, d=3):
    symbol_for_log = df.attrs.get('symbol', 'N/A_STOCH_ATTR') if hasattr(df, 'attrs') else 'N/A_STOCH_NO_ATTR'
    log_extra = {'symbol': symbol_for_log}
    params = {'length': length, 'rsi_length': rsi_length, 'k': k, 'd': d}

    def _calculate_stoch_rsi():
        min_data_needed = int(length) + int(rsi_length) + 1
        if df is None or len(df) < min_data_needed:
            return None, None
        try:
            close_prices = pd.to_numeric(df['close'], errors='coerce')
            if close_prices.isnull().all():
                return None, None

            stoch_rsi_df = ta.stochrsi(close=close_prices, length=int(length), rsi_length=int(rsi_length), k=int(k), d=int(d))
            if stoch_rsi_df is None or stoch_rsi_df.empty:
                return None, None

            # stochrsi devuelve típicamente 2 columnas: %K y %D (pueden tener nombres distintos)
            k_col, d_col = stoch_rsi_df.columns[0], stoch_rsi_df.columns[1]
            k_series = stoch_rsi_df[k_col].apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)
            d_series = stoch_rsi_df[d_col].apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)

            return k_series, d_series
        except Exception as e:
            logger.error(f"Error calculando StochRSI para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None, None

    result = _cached_indicator_calculation('STOCH_RSI', _calculate_stoch_rsi, symbol_for_log, params)
    return result if result else (None, None)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_bollinger_band_width (Solución 1.2) ---
def get_bollinger_band_width(df_or_series, length=20, std_dev=2.0):
    log_extra = {'symbol': 'BBW_SERIES'}
    symbol_for_log = 'BBW_SERIES'

    if isinstance(df_or_series, pd.DataFrame):
        close_series = df_or_series['close']
        symbol_for_log = df_or_series.attrs.get('symbol', 'BBW_DF_ATTR') if hasattr(df_or_series, 'attrs') else 'BBW_DF_ATTR'
    else:
        close_series = df_or_series
        if hasattr(close_series, 'attrs'):
            symbol_for_log = close_series.attrs.get('symbol', 'BBW_SERIES_ATTR')

    log_extra['symbol'] = symbol_for_log
    params = {'length': length, 'std_dev': std_dev}

    def _calculate_bbw():
        min_data_needed = int(length)
        if close_series is None or len(close_series) < min_data_needed:
            return None

        try:
            bb_df = ta.bbands(close=pd.to_numeric(close_series, errors='coerce'), length=int(length), std=float(std_dev))
            if bb_df is None or bb_df.empty:
                return None

            upper_band = bb_df[f'BBU_{int(length)}_{float(std_dev)}']
            lower_band = bb_df[f'BBL_{int(length)}_{float(std_dev)}']
            middle_band = bb_df[f'BBM_{int(length)}_{float(std_dev)}']

            # Evitar división por cero
            middle_band_safe = middle_band.replace(0, np.nan)
            bbw_series = (upper_band - lower_band) / middle_band_safe

            return bbw_series.apply(lambda x: Decimal(str(x)).quantize(DECIMAL_PRECISION) if pd.notna(x) else None)

        except Exception as e:
            logger.error(f"Error calculando Bollinger Band Width para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None

    return _cached_indicator_calculation('BBW', _calculate_bbw, symbol_for_log, params)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Refactorización de get_atr_percent (Solución 1.2) ---
def get_atr_percent(df, length=14):
    symbol_for_log = df.attrs.get('symbol', 'N/A_ATRP_ATTR') if hasattr(df, 'attrs') else 'N/A_ATRP_ATTR'
    log_extra = {'symbol': symbol_for_log}
    params = {'length': length}

    def _calculate_atrp():
        # Llama a la versión cacheadora de get_atr
        _, atr_val = get_atr(df, length=length)
        if atr_val is None:
            return None

        # atr_val podría ser Decimal o None
        try:
            # Obtener el último close de forma segura y convertir a Decimal
            last_close_raw = df['close'].iloc[-1]
            last_close = Decimal(str(last_close_raw)) if pd.notna(last_close_raw) else None
        except Exception:
            last_close = None

        if last_close is None or last_close <= 0:
            return None

        try:
            # Si atr_val es una Serie, tomar su último valor; si es Decimal, usarlo
            if isinstance(atr_val, pd.Series):
                atr_last_raw = atr_val.iloc[-1]
                atr_last = Decimal(str(atr_last_raw)) if pd.notna(atr_last_raw) else None
            else:
                atr_last = Decimal(str(atr_val)) if atr_val is not None else None
        except Exception:
            atr_last = None

        if atr_last is None or atr_last <= 0:
            return None

        atr_percent = (atr_last / last_close) * Decimal('100')
        # Cuantizar resultado
        return atr_percent.quantize(DECIMAL_PRECISION)

    return _cached_indicator_calculation('ATRP', _calculate_atrp, symbol_for_log, params)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Tarea 2.1 (Fase 2) - Stub CVD ---
def get_cvd_stub(df, length=20):
    """
    Stub para Cumulative Volume Delta (CVD) solicitado en Fase 2.
    Implementación simple: SMA(Volume) como proxy.
    En una implementación real, esto requeriría datos de TAPE (trades).
    """
    symbol_for_log = df.attrs.get('symbol', 'N/A_CVD_ATTR') if hasattr(df, 'attrs') else 'N/A_CVD_ATTR'
    log_extra = {'symbol': symbol_for_log}
    params = {'length': length}

    def _calculate_cvd_stub():
        try:
            # Usar SMA(Volume) como proxy stub
            volume_series = pd.to_numeric(df['volume'], errors='coerce')
            if volume_series.isnull().all():
                return None

            sma_vol = ta.sma(volume_series, length=int(length))
            if sma_vol is None or sma_vol.empty:
                return None

            # El requerimiento es CVD >= 0.
            # Asumimos que si el volumen actual > SMA, el delta es positivo.
            current_vol_raw = volume_series.iloc[-1]
            sma_vol_current_raw = sma_vol.iloc[-1]

            if pd.isna(current_vol_raw) or pd.isna(sma_vol_current_raw):
                return Decimal('0')

            current_vol = Decimal(str(current_vol_raw))
            sma_vol_current = Decimal(str(sma_vol_current_raw))

            # Retorna 1 (positivo) si vol > sma, -1 (negativo) si no.
            return Decimal('1') if current_vol > sma_vol_current else Decimal('-1')

        except Exception as e:
            logger.error(f"Error calculando CVD Stub para {symbol_for_log}: {e}", exc_info=True, extra=log_extra)
            return None

    return _cached_indicator_calculation('CVD_STUB', _calculate_cvd_stub, symbol_for_log, params)
# --- FIN DE MODIFICACIÓN ---
