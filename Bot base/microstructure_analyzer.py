# microstructure_analyzer.py
import logging
import numpy as np
from decimal import Decimal # <-- MODIFICACIÓN: Importar Decimal

# Se asume que 'binance_microstructure' (Paso 4) está disponible para importación
# import binance_microstructure

logger = logging.getLogger(__name__)


class MicrostructureAnalyzer:
    def __init__(self):
        self.orderbook_cache = {}

    def calculate_bid_ask_imbalance(self, orderbook):
        """Calcula imbalance entre bids y asks"""
        if (not orderbook.get('bids') or not orderbook.get('asks')):
            logger.debug("Datos de orderbook insuficientes para imbalance.")
            return 0.0

        try:
            total_bid_volume = sum(float(bid[1]) for bid in orderbook.get('bids', []))
            total_ask_volume = sum(float(ask[1]) for ask in orderbook.get('asks', []))

            if total_bid_volume + total_ask_volume == 0:
                return 0.0

            imbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume)
            return imbalance
        except Exception as e:
            logger.warning(f"Error calculando imbalance: {e}")
            return 0.0

    def detect_large_orders(self, orderbook, threshold=0.1):
        """Detecta órdenes grandes en el orderbook"""
        large_bids = []
        large_asks = []

        if (not orderbook.get('bids') or not orderbook.get('asks') or
                not orderbook['bids'][0] or not orderbook['asks'][0]):
            logger.debug("Datos de orderbook insuficientes para detectar large orders.")
            return [], []

        try:
            mid_price = (float(orderbook['bids'][0][0]) + float(orderbook['asks'][0][0])) / 2
            if mid_price == 0: return [], []

            # Threshold basado en % del precio (Valor Nocional)
            threshold_value = mid_price * threshold

            for bid in orderbook['bids']:
                order_value = float(bid[0]) * float(bid[1])
                if order_value > threshold_value:
                    large_bids.append(bid)

            for ask in orderbook['asks']:
                order_value = float(ask[0]) * float(ask[1])
                if order_value > threshold_value:
                    large_asks.append(ask)

            return large_bids, large_asks
        except Exception as e:
            logger.warning(f"Error detectando large orders: {e}")
            return [], []

    # --- INICIO DE MODIFICACIÓN: (Fase 1.5) ---
    def calculate_liquidity_density(self, orderbook):
        """
        Stub: Calcula la densidad de liquidez cerca del mid-price
        sumando el volumen (cantidad de asset) en los primeros 5 niveles.
        """
        if (not orderbook.get('bids') or not orderbook.get('asks')):
            logger.debug("Datos de orderbook insuficientes para densidad.")
            return 0.0
            
        try:
            # Sumar la cantidad (índice 1) de los primeros 5 niveles
            bid_density = sum(Decimal(bid[1]) for bid in orderbook['bids'][:5])
            ask_density = sum(Decimal(ask[1]) for ask in orderbook['asks'][:5])
            
            total_density = float(bid_density + ask_density)
            return total_density
            
        except Exception as e:
            logger.warning(f"Error en stub calculate_liquidity_density: {e}")
            return 0.0
    # --- FIN DE MODIFICACIÓN ---