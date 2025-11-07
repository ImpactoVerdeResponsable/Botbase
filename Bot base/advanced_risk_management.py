# advanced_risk_management.py
import logging
import numpy as np
from collections import defaultdict
from decimal import Decimal, InvalidOperation
import binance_interaction as bi
import config

# --- INICIO DE MODIFICACIÓN: (FIX v2.9.3 - NameError) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---

# Importar el helper de cache de spread
from dynamic_params import get_cached_spread, to_decimal_safe


logger = get_logger(__name__)

# Helper de robustez de tipos (sugerido por Experto GPT)
# (Movido a dynamic_params.py ya que es compartido)
# def _to_decimal_safe(value, default=Decimal('0')):
#     ...


class AdvancedRiskManager:
    def __init__(self):
        self.portfolio_exposure = 0.0
        self.volatility_regime = "NORMAL"
        self.correlation_matrix = {}
        self.position_sizes = {}

    def dynamic_position_sizing(self, symbol, strategy_confidence, volatility_regime, current_exposure):
        """
        Tamaño de posición basado en:
        - Regímenes de volatilidad
        - Correlación entre assets
        - Exposure total del portfolio
        """
        base_size = self._get_base_position_size()

        # Ajuste por régimen de volatilidad
        vol_multiplier = self._get_volatility_multiplier(volatility_regime)

        # Ajuste por confianza de estrategia
        confidence_multiplier = strategy_confidence

        # Ajuste por exposición actual
        exposure_multiplier = self._calculate_exposure_multiplier(current_exposure)

        # Ajuste por correlación
        correlation_multiplier = self._get_correlation_multiplier(symbol)

        final_size = (base_size * vol_multiplier * confidence_multiplier * exposure_multiplier * correlation_multiplier)

        # Límites de tamaño
        min_size = self._get_minimum_position_size()
        max_size = self._get_maximum_position_size(symbol)

        return max(min_size, min(final_size, max_size))

    def portfolio_hedging(self, active_positions, market_regime):
        """
        Hedging automático basado en exposición
        - Delta hedging para posiciones grandes
        - Correlation hedging entre assets
        """
        if not active_positions:
            return None

        total_exposure = sum(float(pos.get('exposure', 0)) for pos in active_positions.values())
        net_direction = self._calculate_net_direction(active_positions)

        hedging_suggestions = []

        # Hedging por exposición excesiva
        if total_exposure > 0.3:  # 30% del capital
            hedge_size = total_exposure * 0.5  # Hedge 50%
            hedging_suggestions.append({
                'type': 'EXPOSURE_HEDGE',
                'size': hedge_size,
                'direction': 'SHORT' if net_direction > 0 else 'LONG',
                'reason': f'High exposure: {total_exposure:.1%}'
            })

        # Hedging por correlación
        correlation_hedge = self._calculate_correlation_hedge(active_positions)
        if correlation_hedge:
            hedging_suggestions.append(correlation_hedge)

        return hedging_suggestions

    def _get_volatility_multiplier(self, volatility_regime):
        """Multiplicador basado en régimen de volatilidad"""
        multipliers = {
            "LOW": 1.3,    # Más agresivo en baja volatilidad
            "NORMAL": 1.0, # Tamaño normal
            "HIGH": 0.6,   # Más conservador en alta volatilidad
            "EXTREME": 0.3 # Muy conservador en volatilidad extrema
        }
        return multipliers.get(volatility_regime, 1.0)

    def _calculate_exposure_multiplier(self, current_exposure):
        """Reduce tamaño cuando exposición es alta"""
        if current_exposure < 0.1:    # <10% exposure
            return 1.2
        elif current_exposure < 0.2:  # <20% exposure
            return 1.0
        elif current_exposure < 0.3:  # <30% exposure
            return 0.7
        else:                         # >=30% exposure
            return 0.4

    # --- Métodos Placeholder (requieren implementación real) ---
    def _get_base_position_size(self): return 1000.0  # Placeholder
    def _get_minimum_position_size(self): return 100.0  # Placeholder
    def _get_maximum_position_size(self, symbol): return 10000.0  # Placeholder
    def _get_correlation_multiplier(self, symbol): return 1.0  # Placeholder

    def _calculate_net_direction(self, active_positions):
        """
        Stub: Calcula la dirección neta del portfolio.
        """
        try:
            total_exposure = sum(to_decimal_safe(pos.get('exposure', '0')) for pos in active_positions.values())
            if total_exposure > 0:
                return 1.0  # Neto LONG
            elif total_exposure < 0:
                return -1.0  # Neto SHORT
            return 0.0  # Neto FLAT
        except Exception as e:
            logger.warning(f"Error en stub _calculate_net_direction: {e}", extra={'symbol':'ADV_RISK'})
            return 0.0  # Fallback

    def _calculate_correlation_hedge(self, active_positions):
        """
        Stub: Requiere una matriz de correlación en tiempo real.
        """
        return None  # Placeholder


def get_dynamic_allocation_multiplier(symbol, strategy_name, volatility_regime, transaction_id):
    """
    Calcula multiplicador de allocation basado en condiciones de mercado.
    (Implementación Solución 3 y 6 del Experto GPT)
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    base_allocation = Decimal('1.0')

    try:
        spread_threshold = Decimal('4.0')  # 4 bp threshold

        if volatility_regime.upper() == "HIGH":
            # Solución 6: Usar el cache, no llamar a bi.get_current_spread directamente
            current_spread_pct = get_cached_spread(symbol, transaction_id=f"{transaction_id}_DynAlloc")
            
            # Solución 3: Manejo de tipos robusto
            if current_spread_pct is None:
                logger.warning(f"Spread no disponible (None) para allocation dinámico. Usando 1.0x", extra=log_extra)
                return base_allocation

            current_spread_bp = current_spread_pct * 100 # Convertir % a BPs

            if current_spread_bp > spread_threshold:
                logger.info(
                    f"Reducción de Allocation: {symbol} [{strategy_name}] - "
                    f"Vol: {volatility_regime}, Spread: {float(current_spread_bp):.2f}bp > {spread_threshold}bp. "
                    "Aplicando 0.5x",
                    extra=log_extra
                )
                return Decimal('0.5')  # 50% de allocation normal

    except Exception as e:
        logger.warning(f"Error calculando allocation dinámico: {e}", exc_info=True, extra=log_extra)

    # Fallback: Retornar 1.0x si las condiciones no se cumplen o si hay un error
    return base_allocation