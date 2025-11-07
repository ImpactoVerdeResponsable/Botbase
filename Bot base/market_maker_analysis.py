# market_maker_analysis.py
import logging
from collections import deque, defaultdict
import numpy as np

# Se asume que 'binance_microstructure' (Paso 4) está disponible para importación
# import binance_microstructure

logger = logging.getLogger(__name__)


class MarketMakerAnalysis:
    def __init__(self):
        self.orderbook_snapshots = defaultdict(lambda: deque(maxlen=50))
        self.spoofing_patterns = defaultdict(list)

    def detect_spoofing(self, symbol, orderbook_history):
        """Detección de órdenes falsas (spoofing) en tiempo real"""
        if len(orderbook_history) < 10:
            return 0.0

        current_ob = orderbook_history[-1]
        spoofing_indicators = []

        # 1. Large orders que desaparecen
        disappearing_orders = self._detect_disappearing_orders(orderbook_history)
        spoofing_indicators.append(disappearing_orders)

        # 2. Patrones repetitivos
        repetitive_patterns = self._detect_repetitive_patterns(orderbook_history)
        spoofing_indicators.append(repetitive_patterns)

        # 3. Análisis temporal de cancelaciones
        cancellation_analysis = self._analyze_cancellations(orderbook_history)
        spoofing_indicators.append(cancellation_analysis)

        spoofing_score = np.mean(spoofing_indicators)
        return min(1.0, spoofing_score)

    def identify_liquidity_zones(self, symbol, trades_data, orderbook_data):
        """Identifica donde están los market makers operando"""
        # 1. Precios con alta densidad de trades
        trade_density = self._calculate_trade_density(trades_data)

        # 2. Áreas de acumulación/distribución
        accumulation_zones = self._find_accumulation_zones(trades_data, orderbook_data)

        # 3. Niveles de stop loss clusters
        stop_clusters = self._estimate_stop_clusters(orderbook_data, trades_data)

        return {
            'high_trade_density': trade_density,
            'accumulation_zones': accumulation_zones.get('accumulation', []),
            'distribution_zones': accumulation_zones.get('distribution', []),
            'estimated_stop_clusters': stop_clusters,
            'market_maker_zones': self._identify_mm_zones(orderbook_data)
        }

    def _detect_disappearing_orders(self, orderbook_history):
        """Detecta órdenes grandes que aparecen y desaparecen rápidamente"""
        if len(orderbook_history) < 3:
            return 0.0

        current_ob = orderbook_history[-1]
        previous_ob = orderbook_history[-2]

        # Buscar órdenes grandes en previous que no están en current
        large_orders_prev = self._find_large_orders(previous_ob, threshold=0.1)
        large_orders_curr = self._find_large_orders(current_ob, threshold=0.1)

        disappeared_orders = [order for order in large_orders_prev if order not in large_orders_curr]

        spoofing_likelihood = min(1.0, len(disappeared_orders) / 3.0)
        return spoofing_likelihood

    # --- INICIO DE MODIFICACIÓN: (Fase 1.5) ---
    def _detect_repetitive_patterns(self, orderbook_history):
        """Stub: Detección de patrones repetitivos (ej. 'layering')."""
        return 0.0  # Placeholder

    def _analyze_cancellations(self, orderbook_history):
        """Stub: Análisis de tasa de cancelación."""
        return 0.0  # Placeholder

    def _calculate_trade_density(self, trades_data):
        """Stub: Cálculo de densidad de trades (requiere clustering)."""
        return []  # Placeholder

    def _find_accumulation_zones(self, trades_data, orderbook_data):
        """Stub: Detección de zonas de acumulación/distribución."""
        return {}  # Placeholder

    def _estimate_stop_clusters(self, orderbook_data, trades_data):
        """Stub: Estimación de clusters de stop loss."""
        return []  # Placeholder

    def _identify_mm_zones(self, orderbook_data):
        """Stub: Identificación de zonas de Market Maker."""
        return []  # Placeholder
    # --- FIN DE MODIFICACIÓN ---

    def _find_large_orders(self, orderbook, threshold=0.1):
        # Helper para _detect_disappearing_orders
        large_orders = []
        try:
            # --- MODIFICACIÓN: Añadir chequeo de orderbook vacío ---
            if not orderbook.get('bids') or not orderbook.get('asks'):
                return set()
            # --- FIN DE MODIFICACIÓN ---
            
            mid_price = (float(orderbook['bids'][0][0]) + float(orderbook['asks'][0][0])) / 2
            if mid_price == 0: return set() # Evitar división por cero
            
            threshold_qty = mid_price * threshold  # Asumiendo que threshold es un % del valor
            
            for bid in orderbook['bids']:
                if float(bid[1]) > threshold_qty:
                    large_orders.append(tuple(bid))
            for ask in orderbook['asks']:
                if float(ask[1]) > threshold_qty:
                    large_orders.append(tuple(ask))
        except Exception as e:
            logger.warning(f"Error en _find_large_orders: {e}")

        return set(large_orders)