# test_strategy_optimizations.py
import pytest
from unittest.mock import patch, MagicMock
from decimal import Decimal

# Importar los módulos que vamos a testear
import config
import dynamic_params
import advanced_risk_management
from exceptions import CriticalBinanceError # Para simular errores
from unified_logger import get_logger

# Configurar un logger para este módulo de test (opcional pero buena práctica)
logger = get_logger(__name__)

# --- Tarea 4: Test de Validación (v2.9.1) ---

# Fixture para asegurar que la config esté cargada (aunque se carga al importar)
@pytest.fixture(scope="module", autouse=True)
def load_test_config():
    config.load_config(force_reload=True)

def test_strategy_disabled():
    """
    Validación 3: Verifica que MicroScalpingRSI esté deshabilitado.
    """
    log_extra = {'transaction_id': 'TEST', 'symbol': 'CONFIG'}
    logger.info("\n--- Test: test_strategy_disabled ---", extra=log_extra)
    config_data = config.get_config_cache()
    
    # Validar que la estrategia está deshabilitada
    assert "MicroScalpingRSI" in config_data["STRATEGIES_CONFIG"], "MicroScalpingRSI no está en la config"
    assert config_data["STRATEGIES_CONFIG"]["MicroScalpingRSI"]["enabled"] == False, "MicroScalpingRSI.enabled no es False"
    
    # Validar que el allocation también sea cero (según el JSON modificado)
    assert "PORTFOLIO_ALLOCATION" in config_data, "PORTFOLIO_ALLOCATION no está en la config"
    assert config_data["PORTFOLIO_ALLOCATION"]["MicroScalpingRSI"] == 0.0, "PORTFOLIO_ALLOCATION[MicroScalpingRSI] no es 0.0"
    logger.info("✅ test_strategy_disabled: OK", extra=log_extra)


# --- Validación 1: Sistema de Filtros de Spread ---

@patch('binance_interaction.get_current_spread')
def test_spread_filter_allows_signal(mock_get_spread):
    """
    Verifica que el filtro de spread permita una señal si 
    el spread está dentro de los límites.
    """
    log_extra = {'transaction_id': 'TEST_SPREAD_OK', 'symbol': 'ZENUSDT'}
    logger.info("\n--- Test: test_spread_filter_allows_signal ---", extra=log_extra)
    
    # Simular spread (en %) que resulta en 2.0bp
    mock_get_spread.return_value = Decimal('0.02') 
    
    signal_in = {"type": "LONG", "strategy_name": "MicrostructureSweep"}
    
    # Llamar a la función (MAX_SPREAD_BP = 3.0 en bot_settings.json)
    signal_out = dynamic_params.apply_spread_filter(
        "ZENUSDT", "MicrostructureSweep", signal_in, "TEST_SPREAD_OK"
    )
    
    assert signal_out is not None, "La señal fue filtrada incorrectamente (debió pasar)"
    assert signal_out == signal_in
    logger.info("✅ test_spread_filter_allows_signal: OK", extra=log_extra)


@patch('binance_interaction.get_current_spread')
def test_spread_filter_blocks_signal(mock_get_spread):
    """
    Verifica que el filtro de spread bloquee (retorne None) si
    el spread excede los límites.
    """
    log_extra = {'transaction_id': 'TEST_SPREAD_BLOCK', 'symbol': 'ZENUSDT'}
    logger.info("\n--- Test: test_spread_filter_blocks_signal ---", extra=log_extra)
    
    # Simular spread (en %) que resulta en 4.0bp
    mock_get_spread.return_value = Decimal('0.04') 
    
    signal_in = {"type": "LONG", "strategy_name": "MicrostructureSweep"}
    
    # Llamar a la función (MAX_SPREAD_BP = 3.0 en bot_settings.json)
    signal_out = dynamic_params.apply_spread_filter(
        "ZENUSDT", "MicrostructureSweep", signal_in, "TEST_SPREAD_BLOCK"
    )
    
    assert signal_out is None, "La señal no fue filtrada (debió ser None)"
    logger.info("✅ test_spread_filter_blocks_signal: OK", extra=log_extra)


@patch('binance_interaction.get_current_spread')
def test_spread_filter_fallback_on_error(mock_get_spread):
    """
    Verifica que el filtro de spread permita la señal (Fallback) 
    si ocurre un error al obtener el spread.
    """
    log_extra = {'transaction_id': 'TEST_SPREAD_FAIL', 'symbol': 'ZENUSDT'}
    logger.info("\n--- Test: test_spread_filter_fallback_on_error ---", extra=log_extra)
    
    # Simular un error de API
    mock_get_spread.side_effect = CriticalBinanceError("Error de API simulado")
    
    signal_in = {"type": "LONG", "strategy_name": "MicrostructureSweep"}
    
    signal_out = dynamic_params.apply_spread_filter(
        "ZENUSDT", "MicrostructureSweep", signal_in, "TEST_SPREAD_FAIL"
    )
    
    assert signal_out is not None, "La señal fue filtrada por un error (debió pasar por fallback)"
    assert signal_out == signal_in
    logger.info("✅ test_spread_filter_fallback_on_error: OK", extra=log_extra)


# --- Validación 2: Sistema de Allocation Dinámico ---

@patch('dynamic_params._get_cached_spread') # Mockear el helper de cache
def test_allocation_normal(mock_get_cached_spread):
    """
    Verifica que el multiplicador de allocation sea 1.0 en condiciones normales.
    """
    log_extra = {'transaction_id': 'TEST_ALLOC_NORMAL', 'symbol': 'BTCUSDT'}
    logger.info("\n--- Test: test_allocation_normal ---", extra=log_extra)
    
    mock_get_cached_spread.return_value = Decimal('0.02') # Spread bajo (2bp)
    
    multiplier = advanced_risk_management.get_dynamic_allocation_multiplier(
        "BTCUSDT", "ImpulsoDeVolatilidad", "NORMAL", "TEST_ALLOC_NORMAL"
    )
    
    assert multiplier == Decimal('1.0')
    logger.info("✅ test_allocation_normal: OK", extra=log_extra)


@patch('dynamic_params._get_cached_spread') # Mockear el helper de cache
def test_allocation_reduced(mock_get_cached_spread):
    """
    Verifica que el multiplicador se reduzca a 0.5 en 
    condiciones de HIGH Vol + HIGH Spread.
    """
    log_extra = {'transaction_id': 'TEST_ALLOC_REDUCED', 'symbol': 'BTCUSDT'}
    logger.info("\n--- Test: test_allocation_reduced ---", extra=log_extra)
    
    mock_get_cached_spread.return_value = Decimal('0.05') # Spread alto (5bp > 4bp threshold)
    
    multiplier = advanced_risk_management.get_dynamic_allocation_multiplier(
        "BTCUSDT", "ImpulsoDeVolatilidad", "HIGH", "TEST_ALLOC_REDUCED"
    )
    
    assert multiplier == Decimal('0.5')
    logger.info("✅ test_allocation_reduced: OK", extra=log_extra)


@patch('dynamic_params._get_cached_spread') # Mockear el helper de cache
def test_allocation_high_vol_low_spread(mock_get_cached_spread):
    """
    Verifica que el multiplicador sea 1.0 si la volatilidad es alta
    pero el spread es bajo.
    """
    log_extra = {'transaction_id': 'TEST_ALLOC_HV_LS', 'symbol': 'BTCUSDT'}
    logger.info("\n--- Test: test_allocation_high_vol_low_spread ---", extra=log_extra)
    
    mock_get_cached_spread.return_value = Decimal('0.02') # Spread bajo (2bp < 4bp threshold)
    
    multiplier = advanced_risk_management.get_dynamic_allocation_multiplier(
        "BTCUSDT", "ImpulsoDeVolatilidad", "HIGH", "TEST_ALLOC_HV_LS"
    )
    
    assert multiplier == Decimal('1.0')
    logger.info("✅ test_allocation_high_vol_low_spread: OK", extra=log_extra)


@patch('dynamic_params._get_cached_spread') # Mockear el helper de cache
def test_allocation_fallback_on_error(mock_get_cached_spread):
    """
    Verifica que el multiplicador sea 1.0 (Fallback) si
    ocurre un error al obtener el spread.
    """
    log_extra = {'transaction_id': 'TEST_ALLOC_FAIL', 'symbol': 'BTCUSDT'}
    logger.info("\n--- Test: test_allocation_fallback_on_error ---", extra=log_extra)
    
    mock_get_cached_spread.side_effect = CriticalBinanceError("Error de API simulado")
    
    multiplier = advanced_risk_management.get_dynamic_allocation_multiplier(
        "BTCUSDT", "ImpulsoDeVolatilidad", "HIGH", "TEST_ALLOC_FAIL"
    )
    
    assert multiplier == Decimal('1.0')
    logger.info("✅ test_allocation_fallback_on_error: OK", extra=log_extra)