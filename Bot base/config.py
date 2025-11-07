# config.py
# -*- coding: utf-8 -*-

import os
import json
import logging  # <-- Se mantiene logging estándar
import traceback
from decimal import Decimal, Context, ROUND_DOWN, InvalidOperation, ConversionSyntax
from dotenv import load_dotenv
import copy
import time

import path_manager

logger = logging.getLogger(__name__)

BASE_DIR = path_manager.get_project_dir()

# --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
_log_extra_config_init = {'symbol': 'CONFIG_INIT', 'transaction_id': 'CONFIG_INIT'}
try:
    dotenv_path = path_manager.get_path("dotenv")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    else:
        logger.debug(f"Archivo .env no encontrado en {dotenv_path}. Confiando en variables de entorno.", extra=_log_extra_config_init)
except Exception as e_dotenv:
    logger.error(f"Error al cargar .env desde {dotenv_path}: {e_dotenv}", extra=_log_extra_config_init)
# --- FIN DE MODIFICACIÓN ---


VALID_BINANCE_INTERVALS = [
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M"
]

ALL_POSSIBLE_TREND_STATES = sorted(list(set([
    "FUERTE_ALCISTA", "INICIO_ALCISTA", "LATERAL_CON_SESGO_ALCISTA",
    "LATERAL_CONSOLIDACION", "LATERAL_CON_SESGO_BAJISTA",
    "INICIO_BAJISTA", "FUERTE_BAJISTA", "INCIERTA"
])))

PRODUCTION_MONITORING = {
    "HEALTH_CHECK_INTERVAL": 30,
    "MEMORY_USAGE_THRESHOLD_MB": 500,
    "CPU_USAGE_THRESHOLD": 80,
    "MAX_CONCURRENT_THREADS": 10
}

ADDITIONAL_CONFIGS = {
    "PORTFOLIO_ALLOCATION": {
        "RupturaDeCompresion": 0.25,
        "MicroScalpingRSI": 0.20,
        "ImpulsoDeVolatilidad": 0.20,
        "Variant_MicroScalpX": 0.15,
        "CASH_RESERVE": 0.20
    },
    "TRADING_HOURS": {
        "OPTIMAL_RANGE": [8, 16],  # 08:00-16:00 UTC
        "REDUCED_RANGE": [2, 4],   # 02:00-04:00 UTC
        "WEEKEND_REDUCTION": 0.5   # 50% domingos
    }
}

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_TESTNET_STR = os.getenv("BINANCE_TESTNET", "false")
BINANCE_TESTNET = BINANCE_TESTNET_STR.lower() == "true"

_CONFIG_CACHE = {}
_LAST_LOAD_TIME = 0
_CONFIG_FILE_MTIME = 0
CONFIG_FILE_PATH = path_manager.get_path("bot_settings")


# --- ################################# ---
# ---  INICIO DE LA MODIFICACIÓN (V3.0) ---
# --- ################################# ---

# --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
def _set_nested_val(config_dict, path_keys, value, log_extra):
# --- FIN DE MODIFICACIÓN ---
    """
    Función auxiliar para establecer un valor en un diccionario anidado
    basado en una lista de claves.
    Ej: _set_nested_val(cache, ['STRATEGIES_CONFIG', 'MicroScalpingRSI', 'RSI_LENGTH'], 12)
    """
    d = config_dict
    for i, key in enumerate(path_keys):
        if i == len(path_keys) - 1:
            d[key] = value
        else:
            if key not in d or not isinstance(d[key], dict):
                # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
                # Añadir el transaction_id faltante desde log_extra
                logger.warning(
                    f"Ruta anidada '{key}' no existía en la config base al aplicar Optuna. Creándola.",
                    extra={'symbol': 'CONFIG_OPTUNA', 'transaction_id': log_extra.get('transaction_id', 'N/A_OPTUNA')}
                )
                # --- FIN DE MODIFICACIÓN ---
                d[key] = {}
            d = d[key]


def _apply_optuna_overrides(config_cache, log_extra):
    """
    Busca parámetros optimizados por Optuna y los "parchea" sobre
    la configuración de estrategias cargada.
    """
    try:
        optuna_config = config_cache.get("SHADOW_OPTUNA", {})
        if not optuna_config.get("ENABLED", False):
            return

        last_best_params = optuna_config.get("LAST_BEST", {})
        if not last_best_params:
            logger.debug("Optuna activado, pero aún no hay 'LAST_BEST' params. Usando defaults.", extra=log_extra)
            return

        logger.info("🧠 Aplicando parámetros optimizados por OPTUNA...", extra=log_extra)

        for flat_key, value in last_best_params.items():
            path_keys = flat_key.split('.')
            full_dest_path = ['STRATEGIES_CONFIG'] + path_keys
            
            try:
                # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
                # Pasar log_extra a la función hija
                _set_nested_val(config_cache, full_dest_path, value, log_extra)
                # --- FIN DE MODIFICACIÓN ---
                logger.info(f"  -> {flat_key} = {value}", extra=log_extra)
            except Exception as e_set:
                logger.error(f"Error al aplicar el parámetro de Optuna '{flat_key}': {e_set}", extra=log_extra)

        logger.info("✅ Parámetros de Optuna aplicados al caché de configuración.", extra=log_extra)

    except Exception as e:
        logger.error(f"Error CRÍTICO al aplicar overrides de Optuna: {e}", exc_info=True, extra=log_extra)


# --- ################################# ---
# ---   FIN DE LA MODIFICACIÓN (V3.0)   ---
# --- ################################# ---


def load_config(force_reload=False, current_transaction_id="N/A_LOAD_CONFIG"):
    global _CONFIG_CACHE, _LAST_LOAD_TIME, _CONFIG_FILE_MTIME
    log_extra_load = {'transaction_id': current_transaction_id, 'symbol': 'CONFIG_LOAD'}

    current_time = time.time()
    config_reload_interval = 30

    current_mtime = 0
    try:
        if os.path.exists(CONFIG_FILE_PATH):
            current_mtime = os.path.getmtime(CONFIG_FILE_PATH)
    except OSError as e:
        logger.warning(f"No se pudo obtener mtime de {CONFIG_FILE_PATH}: {e}", extra=log_extra_load)

    if (force_reload or not _CONFIG_CACHE or
            (current_time - _LAST_LOAD_TIME > config_reload_interval) or
            (current_mtime != 0 and current_mtime != _CONFIG_FILE_MTIME)):

        try:
            if not os.path.exists(CONFIG_FILE_PATH):
                raise FileNotFoundError(f"Archivo de configuración '{CONFIG_FILE_PATH}' no encontrado.")

            with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
                new_config_data = json.load(f)

            _CONFIG_CACHE = new_config_data
            _CONFIG_FILE_MTIME = current_mtime
            _LAST_LOAD_TIME = current_time

            _apply_optuna_overrides(_CONFIG_CACHE, log_extra_load)

            logger.info("Configuración recargada exitosamente (Hot-Reload)", extra=log_extra_load)
            return _CONFIG_CACHE

        except Exception as e:
            logger.error(f"Error recargando configuración: {e}", exc_info=True, extra=log_extra_load)
            if not _CONFIG_CACHE:
                logger.critical("Fallo al cargar la configuración inicial. No hay caché. Lanzando excepción.", extra=log_extra_load)
                raise
            else:
                logger.warning("Fallo al recargar config. Se continuará usando la última configuración válida en caché.", extra=log_extra_load)

    return _CONFIG_CACHE


def _traverse_path(dictionary, path_keys):
    """Función auxiliar para navegar un diccionario con una lista de claves."""
    val = dictionary
    for key in path_keys:
        if not isinstance(val, dict) or key not in val:
            return None
        val = val[key]
    return val


def get_param(key_path, symbol=None, default_value=None, return_dict_for_complex=False):
    global _CONFIG_CACHE
    if not _CONFIG_CACHE:
        load_config(force_reload=True)
    if not _CONFIG_CACHE:
        return default_value

    keys = key_path.split('.')

    base_value = _traverse_path(_CONFIG_CACHE, keys)

    symbol_value = None
    if symbol:
        symbol_path = ["SYMBOL_SETTINGS", symbol] + keys
        symbol_value = _traverse_path(_CONFIG_CACHE, symbol_path)

    value_to_return = default_value

    if isinstance(base_value, dict) and isinstance(symbol_value, dict):
        merged_value = copy.deepcopy(base_value)
        merged_value.update(symbol_value)
        value_to_return = merged_value
    elif symbol_value is not None:
        value_to_return = symbol_value
    elif base_value is not None:
        value_to_return = base_value

    if return_dict_for_complex:
        return value_to_return if isinstance(value_to_return, dict) else default_value
    else:
        return value_to_return if not isinstance(value_to_return, dict) else default_value


def get_config_cache():
    """Devuelve una copia profunda del caché de configuración actual."""
    if not _CONFIG_CACHE:
        load_config(force_reload=False)
    return copy.deepcopy(_CONFIG_CACHE)


load_config(current_transaction_id="CONFIG_MODULE_INIT_MAIN")

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    critical_msg_apikeys = "CRÍTICO: Las claves API de Binance no están disponibles."
    # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
    logger.critical(
        critical_msg_apikeys,
        extra={'symbol': 'API_KEYS', 'transaction_id': 'API_KEYS_INIT'}
    )
    # --- FIN DE MODIFICACIÓN ---