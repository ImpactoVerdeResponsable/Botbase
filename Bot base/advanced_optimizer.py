# advanced_optimizer.py
import logging
from collections import defaultdict
import pandas as pd  # Asumido, ya que se manejan dataframes
import numpy as np  # Asumido para cálculos estadísticos
from decimal import Decimal

logger = logging.getLogger(__name__)


class AdvancedOptimizer:
    def __init__(self):
        self.regime_data = {}
        self.performance_history = defaultdict(list)

    def regime_based_optimization(self, data: pd.DataFrame, strategy_name: str, market_regime: str):
        """
        Optimización separada por:
        - Mercado alcista/bajista
        - Alta/baja volatilidad
        - Horarios específicos (Asia/London/NY)
        """
        # Filtrar datos por régimen
        regime_data = self._filter_data_by_regime(data, market_regime)

        if len(regime_data) < 100:  # Mínimo de datos
            return self._get_default_parameters(strategy_name)

        # Optimización por régimen específico
        if market_regime == "BULL_HIGH_VOL":
            return self._optimize_bull_high_vol(regime_data, strategy_name)
        elif market_regime == "BEAR_HIGH_VOL":
            return self._optimize_bear_high_vol(regime_data, strategy_name)
        elif market_regime == "RANGING_LOW_VOL":
            return self._optimize_ranging_low_vol(regime_data, strategy_name)
        else:
            return self._optimize_general(regime_data, strategy_name)

    def real_time_parameter_adjustment(self, strategy_name: str, recent_performance: dict):
        """
        Ajuste en tiempo real basado en:
        - Win rate rolling (últimas 50 operaciones)
        - Sharpe ratio reciente
        - Drawdown actual
        """
        current_params = self._get_current_parameters(strategy_name)

        # Ajustar basado en performance reciente
        win_rate = recent_performance.get('win_rate_50', 0.5)
        sharpe_ratio = recent_performance.get('sharpe_30', 0)
        current_drawdown = recent_performance.get('drawdown_current', 0)

        adjustment_factors = {
            'aggressiveness': 1.0,
            'position_size_multiplier': 1.0,
            'stop_loss_multiplier': 1.0
        }

        # Ajustar agresividad basado en win rate
        if win_rate > 0.6:
            adjustment_factors['aggressiveness'] = 1.2
            adjustment_factors['position_size_multiplier'] = 1.1
        elif win_rate < 0.4:
            adjustment_factors['aggressiveness'] = 0.8
            adjustment_factors['position_size_multiplier'] = 0.7

        # Ajustar stop loss basado en drawdown
        # Nota: current_drawdown se espera como negativo si hay pérdida (ej. -0.05 para -5%)
        if isinstance(current_drawdown, (int, float)) and current_drawdown > -0.05:
            # Drawdown > -5% (es decir, menor pérdida) -> stops más ajustados
            adjustment_factors['stop_loss_multiplier'] = 0.8

        return self._apply_parameter_adjustments(current_params, adjustment_factors)

    # --- INICIO DE MODIFICACIÓN: (Fase 2) ---
    def adaptive_filtering(self, base_thresholds: dict, market_regime: str) -> dict:
        """
        (Fase 2) Ajusta los thresholds de los filtros (Spread, Vol, CVD)
        basado en el régimen de mercado actual.
        """
        adjusted_thresholds = dict(base_thresholds)  # copy

        # "low vol: relajar 10%"
        if market_regime == "RANGING_LOW_VOL":
            logger.debug(f"Aplicando ajuste adaptativo para {market_regime}: Relajando thresholds 10%")

            if 'VOL_THRESHOLD' in adjusted_thresholds:
                # Relajar = Bajar el umbral de volatilidad requerido
                adjusted_thresholds['VOL_THRESHOLD'] = Decimal(str(adjusted_thresholds['VOL_THRESHOLD'])) * Decimal('0.9')

            if 'SPREAD_MAX_BP' in adjusted_thresholds:
                # Relajar = Permitir un spread mayor
                adjusted_thresholds['SPREAD_MAX_BP'] = Decimal(str(adjusted_thresholds['SPREAD_MAX_BP'])) * Decimal('1.1')

            # (No se puede relajar CVD >= 0, se mantiene)

        elif market_regime == "BULL_HIGH_VOL":
            logger.debug(f"Aplicando ajuste adaptativo para {market_regime}: Ajustando thresholds")

            if 'SPREAD_MAX_BP' in adjusted_thresholds:
                # Ajustar = Requerir un spread menor
                adjusted_thresholds['SPREAD_MAX_BP'] = Decimal(str(adjusted_thresholds['SPREAD_MAX_BP'])) * Decimal('0.8')

        return adjusted_thresholds
    # --- FIN DE MODIFICACIÓN ---

    def _filter_data_by_regime(self, data: pd.DataFrame, market_regime: str) -> pd.DataFrame:
        """Filtra datos históricos por régimen de mercado"""
        regime_filters = {
            "BULL_HIGH_VOL": lambda df: (df['trend'] > 0) & (df['volatility'] > df['volatility'].median()),
            "BEAR_HIGH_VOL": lambda df: (df['trend'] < 0) & (df['volatility'] > df['volatility'].median()),
            "RANGING_LOW_VOL": lambda df: (df['trend'].abs() < 0.1) & (df['volatility'] < df['volatility'].median())
        }

        filter_func = regime_filters.get(market_regime)
        if filter_func and 'trend' in data.columns and 'volatility' in data.columns:
            try:
                return data[filter_func(data)]
            except Exception as e:
                logger.debug(f"Error aplicando filter_func para {market_regime}: {e}")
                return data

        return data

    # --- INICIO DE MODIFICACIÓN: (Fase 1.5) ---
    def _get_default_parameters(self, strategy_name: str) -> dict:
        logger.warning(f"Usando parámetros default para {strategy_name}, datos insuficientes.")
        # Retornar un stub válido
        return {
            "RSI_LENGTH": 14,
            "FAST_EMA": 9,
            "SLOW_EMA": 21
        }

    def _optimize_bull_high_vol(self, data: pd.DataFrame, strategy_name: str) -> dict:
        logger.info(f"Optimizando {strategy_name} para BULL_HIGH_VOL... (Stub)")
        # Stub: Parámetros más agresivos
        return {
            "RSI_LENGTH": 10,
            "FAST_EMA": 7,
            "SLOW_EMA": 18,
            "RISK_REWARD_RATIO": 2.0
        }

    def _optimize_bear_high_vol(self, data: pd.DataFrame, strategy_name: str) -> dict:
        logger.info(f"Optimizando {strategy_name} para BEAR_HIGH_VOL... (Stub)")
        # Stub: Parámetros defensivos (el bot actual es LONG only, así que esto no se usaría)
        return {"enabled": False}

    def _optimize_ranging_low_vol(self, data: pd.DataFrame, strategy_name: str) -> dict:
        logger.info(f"Optimizando {strategy_name} para RANGING_LOW_VOL... (Stub)")
        # Stub: Parámetros de rango
        return {
            "RSI_LENGTH": 18,
            "BBANDS_STD_DEV": 2.0
        }

    def _optimize_general(self, data: pd.DataFrame, strategy_name: str) -> dict:
        logger.info(f"Optimizando {strategy_name} para régimen general... (Stub)")
        return self._get_default_parameters(strategy_name)

    def _get_current_parameters(self, strategy_name: str) -> dict:
        # Stub: Debería leer desde config o un estado interno
        logger.debug(f"Obteniendo parámetros actuales (stub) para {strategy_name}")
        return {
            "RSI_LENGTH": 14,
            "position_size_multiplier": 1.0,
            "stop_loss_multiplier": 1.0
        }

    def _apply_parameter_adjustments(self, params: dict, factors: dict) -> dict:
        # Stub: Aplica los factores de ajuste
        adjusted_params = dict(params)

        # Ejemplo de ajuste:
        if 'RSI_LENGTH' in adjusted_params and 'aggressiveness' in factors:
            # Menor agresividad = período RSI más largo (menos señales)
            try:
                # evitar división por cero o tipos inválidos
                aggress = float(factors['aggressiveness']) if factors['aggressiveness'] else 1.0
                adjusted_params['RSI_LENGTH'] = max(1, int(adjusted_params['RSI_LENGTH'] / aggress))
            except Exception:
                pass

        if 'position_size_multiplier' in factors:
            adjusted_params['position_size_multiplier'] = factors['position_size_multiplier']

        if 'stop_loss_multiplier' in factors:
            adjusted_params['stop_loss_multiplier'] = factors['stop_loss_multiplier']

        logger.debug(f"Parámetros ajustados (stub): {adjusted_params}")
        return adjusted_params
    # --- FIN DE MODIFICACIÓN ---
