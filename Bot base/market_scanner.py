# market_scanner.py
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
import time
import math
from decimal import Decimal
import pandas as pd # Asegurar que pandas esté importado si se usa en _calculate_recent_change

import binance_interaction as bi
import config # Importar el módulo config
import utils
from exceptions import InsufficientDataError

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

_scanner_cache = {
    "last_scan_time": 0,
    "top_symbols": []
}

def get_top_symbols(transaction_id="SCANNER"):
    log_extra = {'transaction_id': transaction_id, 'symbol': 'SCANNER'}

    scanner_config = config.get_param("MARKET_SCANNER_CONFIG", return_dict_for_complex=True)
    if not scanner_config or not scanner_config.get("ENABLED", False):
        # Fallback a lista estática si existe en settings (aunque no está en el ejemplo actual)
        static_symbols = config.get_param("GLOBAL_SETTINGS.SYMBOLS", default_value=[])
        if static_symbols:
            logger.warning("Scanner desactivado. Usando lista estática de símbolos.", extra=log_extra)
            return static_symbols
        else:
            logger.error("Scanner desactivado y no hay lista estática de símbolos. No se pueden obtener símbolos.", extra=log_extra)
            return []


    interval_minutes = int(scanner_config.get("SCAN_INTERVAL_MINUTES", 15))
    current_time = time.time()

    if current_time - _scanner_cache["last_scan_time"] < interval_minutes * 60 and _scanner_cache["top_symbols"]:
        logger.debug(f"Usando resultados de escáner desde caché.", extra=log_extra)
        return _scanner_cache["top_symbols"]

    logger.info(f"Ejecutando escaneo de mercado... (Intervalo: {interval_minutes} min)", extra=log_extra)
    try:
        tickers = bi.get_all_tickers_data(transaction_id=f"{transaction_id}_Fetch")
        if not tickers:
            raise InsufficientDataError("No se recibieron datos de tickers de Binance.")

        # Filtrar tickers
        filtered_pairs = _filter_pairs(tickers, scanner_config, log_extra)

        # Enriquecer con cambio reciente
        # Pasar el diccionario scanner_config aquí
        enriched_pairs = _calculate_recent_change(filtered_pairs, scanner_config, log_extra)

        # Rankear pares
        ranked_pairs = _rank_pairs(enriched_pairs, scanner_config, log_extra)

        # Seleccionar el top N
        num_symbols_to_trade = int(scanner_config.get("NUMBER_OF_SYMBOLS_TO_TRADE", 3))
        top_n_symbols = [pair['symbol'] for pair in ranked_pairs[:num_symbols_to_trade]]

        _scanner_cache["top_symbols"] = top_n_symbols
        _scanner_cache["last_scan_time"] = current_time

        # Guardar resultados si está habilitado
        if scanner_config.get("SAVE_SCAN_RESULTS", False):
            utils.save_scan_results_to_file(ranked_pairs, f"{transaction_id}_SaveScan")

        logger.info(f"Escaneo completado. Símbolos seleccionados: {top_n_symbols}", extra=log_extra)
        return top_n_symbols

    except Exception as e:
        logger.error(f"Error durante el escaneo de mercado: {e}. Usando última lista de caché si está disponible.", exc_info=True, extra=log_extra)
        # Devolver la caché anterior si el escaneo falla para no detener el bot
        return _scanner_cache["top_symbols"]

def _filter_pairs(tickers, scanner_cfg, log_extra): # Renombrado a scanner_cfg
    min_volume_usdt = Decimal(str(scanner_cfg.get("MINIMUM_24H_VOLUME_USDT", 10000000)))
    quote_asset = scanner_cfg.get("QUOTE_ASSET_FILTER", "USDT")
    blacklist = set(scanner_cfg.get("BLACKLIST_SYMBOLS", []))
    top_n_volume = int(scanner_cfg.get("TOP_N_BY_VOLUME", 50))

    valid_pairs = []
    for ticker in tickers:
        symbol = ticker.get('symbol')
        quote_volume_str = ticker.get('quoteVolume')

        # Validaciones básicas
        if not symbol or not quote_volume_str: continue
        if symbol in blacklist: continue
        if not symbol.endswith(quote_asset): continue

        try:
            quote_volume = Decimal(quote_volume_str)
            if quote_volume >= min_volume_usdt:
                valid_pairs.append(ticker)
        except Exception:
            # Ignorar tickers con volumen inválido
            continue

    # Ordenar por volumen y tomar el top N
    valid_pairs.sort(key=lambda x: Decimal(x.get('quoteVolume', '0')), reverse=True)
    top_volume_pairs = valid_pairs[:top_n_volume]
    logger.debug(f"Filtrados {len(top_volume_pairs)} pares por volumen >= {min_volume_usdt} {quote_asset} y top {top_n_volume}.", extra=log_extra)
    return top_volume_pairs

# --- CORRECCIÓN AQUÍ: Renombrar el argumento 'config' a 'scanner_cfg' ---
def _calculate_recent_change(pairs, scanner_cfg, log_extra):
    enriched_pairs = []
    # Usar el argumento renombrado para obtener el intervalo
    interval = scanner_cfg.get("CHANGE_TIMEFRAME", "15m")

    # --- CORRECCIÓN AQUÍ: Acceder a VALID_BINANCE_INTERVALS desde el módulo config importado ---
    if interval not in config.VALID_BINANCE_INTERVALS:
        logger.warning(f"Intervalo '{interval}' para CHANGE_TIMEFRAME inválido. Usando '15m'.", extra=log_extra)
        interval = "15m"

    for pair in pairs:
        pair['recent_change_percent'] = Decimal('0') # Inicializar
        try:
            # Pedir solo 2 velas para calcular el cambio
            klines = bi.get_klines_data(pair['symbol'], interval, limit=2, transaction_id=log_extra.get('transaction_id', 'CalcChange'))
            # Asegurar que klines es un DataFrame y tiene al menos 2 filas y las columnas necesarias
            if klines is not None and isinstance(klines, pd.DataFrame) and not klines.empty and len(klines) >= 2 and 'open' in klines.columns and 'close' in klines.columns:
                # Usar la vela *anterior* completa para el open y la *última* (puede estar incompleta) para el close
                open_price = klines['open'].iloc[0]
                last_price = klines['close'].iloc[-1]
                if open_price is not None and last_price is not None and open_price > 0:
                    change = ((last_price - open_price) / open_price) * 100
                    pair['recent_change_percent'] = change # Guardar como Decimal
                else:
                    # Log si los precios son inválidos
                    logger.debug(f"Precios inválidos (open={open_price}, close={last_price}) para {pair['symbol']} en {interval}.", extra=log_extra)
            else:
                 logger.debug(f"Datos insuficientes o inválidos para calcular cambio reciente de {pair['symbol']} en {interval}. Klines len: {len(klines) if klines is not None else 'None'}", extra=log_extra)

        except Exception as e:
            logger.warning(f"No se pudo calcular cambio reciente para {pair['symbol']}: {e}", extra=log_extra)
            # No añadir exc_info=True aquí para no llenar el log si falla a menudo
        finally:
             # Siempre añadir el par, incluso si falla el cálculo del cambio (tendrá 0%)
             enriched_pairs.append(pair)

    return enriched_pairs


def _rank_pairs(pairs, scanner_cfg, log_extra): # Renombrado a scanner_cfg
    w_change = Decimal(str(scanner_cfg.get("WEIGHT_PERCENT_CHANGE", "0.6")))
    w_volume = Decimal(str(scanner_cfg.get("WEIGHT_24H_VOLUME", "0.4")))

    ranked_pairs = []
    for pair in pairs:
        try:
            # Asegurar que 'recent_change_percent' sea Decimal
            change_score = Decimal(pair.get('recent_change_percent', '0'))

            # Asegurar que 'quoteVolume' sea convertible a float para log10
            quote_volume_str = pair.get('quoteVolume', '1')
            # Intentar convertir, usar 1.0 como fallback si falla o es inválido
            try:
                 quote_volume_float = float(quote_volume_str)
                 if quote_volume_float <= 0:
                      quote_volume_float = 1.0 # Evitar log(0) o log(negativo)
            except (ValueError, TypeError):
                 quote_volume_float = 1.0

            # Calcular score de volumen (log10)
            volume_score = Decimal(math.log10(quote_volume_float))

            # Calcular score final
            pair['score'] = (w_change * change_score) + (w_volume * volume_score)
            ranked_pairs.append(pair)

        except Exception as e:
            logger.error(f"Error al rankear el par {pair.get('symbol', 'N/A')}: {e}. Omitiendo par.", exc_info=True, extra=log_extra)
            pair['score'] = Decimal('-Infinity') # Darle un score muy bajo si falla
            ranked_pairs.append(pair)

    # Ordenar por score descendente
    ranked_pairs.sort(key=lambda x: x.get('score', Decimal('-Infinity')), reverse=True)
    return ranked_pairs