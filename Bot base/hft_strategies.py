# hft_strategies.py
import logging
import numpy as np
from binance_microstructure import BinanceMicrostructure  # Importar módulo (Paso 4)

logger = logging.getLogger(__name__)


class HFT1MinuteStrategies:
    def __init__(self):
        self.microstructure = BinanceMicrostructure()
        self.performance_tracker = {}

    def liquidity_grab_strategy(self, df, orderbook_data, trade_data):
        """
        Estrategia: Detectar cuando un actor grande toma liquidez
        y seguir el momentum inmediato
        """
        # 1. Detectar large market orders
        large_buys, large_sells = self._detect_large_orders(trade_data)

        # 2. Medir impacto en orderbook
        # (Se asume que _measure_orderbook_impact es una función auxiliar)
        orderbook_impact = self._measure_orderbook_impact(orderbook_data, large_buys, large_sells)

        # 3. Señal basada en microstructure
        if large_buys and orderbook_impact['buy_pressure'] > 0.8:
            signal_strength = min(1.0, len(large_buys) * 0.2 + orderbook_impact['buy_pressure'])
            return {
                "type": "LONG",
                "strategy_name": "LiquidityGrab",
                "confidence": signal_strength,
                "reason": f"Large buy orders detected: {len(large_buys)}",
                "expected_duration": 3  # velas
            }
        elif large_sells and orderbook_impact['sell_pressure'] > 0.8:
            signal_strength = min(1.0, len(large_sells) * 0.2 + orderbook_impact['sell_pressure'])
            return {
                "type": "SHORT",
                "strategy_name": "LiquidityGrab",
                "confidence": signal_strength,
                "reason": f"Large sell orders detected: {len(large_sells)}",
                "expected_duration": 3
            }

        return None

    def mean_reversion_micro(self, df, orderbook_data, volatility_regime):
        """
        Mean reversion basado en microstructure
        - Reversión después de liquidity grabs
        - Fading a movimientos excesivos
        """
        current_price = df['close'].iloc[-1]

        # Calcular fair value basado en orderbook
        fair_value = self._calculate_fair_value(orderbook_data)

        if fair_value is None or fair_value == 0:
            logger.debug("No se pudo calcular Fair Value para MeanReversionMicro.")
            return None

        price_deviation = (current_price - fair_value) / fair_value

        # Umbrales dinámicos basados en volatilidad
        if volatility_regime == "HIGH":
            threshold = 0.008  # 0.8%
        else:
            threshold = 0.004  # 0.4%

        if price_deviation > threshold:
            return {
                "type": "SHORT",
                "strategy_name": "MeanReversionMicro",
                "confidence": min(0.9, abs(price_deviation) / 0.02),
                "reason": f"Price deviation: {price_deviation:.3%}",
                "expected_duration": 5
            }
        elif price_deviation < -threshold:
            return {
                "type": "LONG",
                "strategy_name": "MeanReversionMicro",
                "confidence": min(0.9, abs(price_deviation) / 0.02),
                "reason": f"Price deviation: {price_deviation:.3%}",
                "expected_duration": 5
            }

        return None

    def _detect_large_orders(self, trade_data):
        """Detecta órdenes grandes anormales"""
        if not trade_data or len(trade_data) < 10:
            return [], []

        try:
            volumes = [float(trade['quantity']) for trade in trade_data]
            avg_volume = np.mean(volumes)
            std_volume = np.std(volumes)

            large_buys = [trade for trade in trade_data
                          if trade['is_buyer_maker'] is False and  # Taker buy
                          float(trade['quantity']) > avg_volume + 2 * std_volume]

            large_sells = [trade for trade in trade_data
                           if trade['is_buyer_maker'] is True and  # Taker sell
                           float(trade['quantity']) > avg_volume + 2 * std_volume]

            return large_buys, large_sells
        except Exception as e:
            logger.warning(f"Error detectando large orders: {e}")
            return [], []

    def _calculate_fair_value(self, orderbook_data):
        """Calcula fair value basado en orderbook imbalance"""
        bids = orderbook_data.get('bids', [])
        asks = orderbook_data.get('asks', [])

        if not bids or not asks:
            return None

        try:
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            mid_price = (best_bid + best_ask) / 2

            # Calcular weighted mid price basado en profundidad
            bid_volume = sum(float(bid[1]) for bid in bids[:5])
            ask_volume = sum(float(ask[1]) for ask in asks[:5])

            if bid_volume + ask_volume > 0:
                imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
                fair_value = mid_price * (1 + imbalance * 0.001)  # Ajuste pequeño
                return fair_value

            return mid_price
        except Exception as e:
            logger.warning(f"Error calculando fair value: {e}")
            return None

    # --- INICIO DE MODIFICACIÓN: (Fase 1.5) ---
    def _measure_orderbook_impact(self, orderbook_data, large_buys, large_sells):
        """
        Stub: Mide el impacto de órdenes grandes.
        Retorna un dict con presiones simuladas.
        """
        impact = {'buy_pressure': 0.5, 'sell_pressure': 0.5}
        if large_buys:
            impact['buy_pressure'] = 0.9  # Simular alta presión
        if large_sells:
            impact['sell_pressure'] = 0.9  # Simular alta presión
        return impact
    # --- FIN DE MODIFICACIÓN ---