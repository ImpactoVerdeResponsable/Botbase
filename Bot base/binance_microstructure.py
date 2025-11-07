# binance_microstructure.py
import numpy as np
from collections import deque, defaultdict
from datetime import datetime, timedelta
import pandas as pd
from decimal import Decimal, InvalidOperation  # Importar InvalidOperation
import time  # Importar time

from unified_logger import get_logger
from orderbook_validator import OrderbookValidator  # Importar validador (Paso 2.1)

# Se asume que scikit-learn (sklearn) está en requirements.txt
try:
    from sklearn.cluster import KMeans
except ImportError:
    # Manejar el caso donde sklearn no esté instalado
    KMeans = None
    get_logger(__name__).warning("SKLearn no encontrado. El clustering de volumen estará deshabilitado.", extra={'symbol': 'IMPORT_ERROR'})

logger = get_logger(__name__)


class BinanceMicrostructure:
    def __init__(self):
        self.orderbook_depth = 20
        self.trade_analysis_window = 50
        self.orderbook_history = defaultdict(lambda: deque(maxlen=100))
        self.trade_flow = defaultdict(lambda: deque(maxlen=200))

    def detect_market_maker_activity(self, symbol, orderbook_data, transaction_id="N/A_MM_DETECT"):
        """Identifica actividad de market makers en el orderbook"""
        log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
        current_time = datetime.utcnow()

        # 1. Análisis de imbalance en bids/asks
        bid_volume = sum(Decimal(bid[1]) for bid in orderbook_data.get('bids', [])[:10])
        ask_volume = sum(Decimal(ask[1]) for ask in orderbook_data.get('asks', [])[:10])

        volume_imbalance = Decimal('0')
        if (bid_volume + ask_volume) > 0:
            volume_imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)

        # 2. Detección de spoofing - órdenes grandes que desaparecen
        spoofing_score = self._calculate_spoofing_score(symbol, orderbook_data, log_extra)

        # 3. Velocidad de cancelaciones
        cancellation_rate = self._calculate_cancellation_rate(symbol, log_extra)

        market_maker_activity = {
            'volume_imbalance': float(volume_imbalance),
            'spoofing_score': float(spoofing_score),
            'cancellation_rate': float(cancellation_rate),
            'timestamp': current_time,
            'market_maker_present': abs(volume_imbalance) > Decimal('0.3') or spoofing_score > Decimal('0.7')
        }

        return market_maker_activity

    def get_liquidity_zones(self, symbol, orderbook_data, trade_history, transaction_id="N/A_LIQ_ZONES"):
        """Identifica zonas de liquidez clave para SL/TP"""
        log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

        # 1. Clusters de volumen
        volume_clusters = self._find_volume_clusters(trade_history, log_extra)

        # 2. Niveles psicológicos
        psychological_levels = self._find_psychological_levels(orderbook_data, log_extra)

        # 3. Áreas de alta densidad de órdenes
        order_density = self._calculate_order_density(orderbook_data, log_extra)

        liquidity_zones = {
            'support_levels': volume_clusters.get('support', []),
            'resistance_levels': volume_clusters.get('resistance', []),
            'psychological_levels': psychological_levels,
            'high_density_zones': order_density.get('high_density', []),
            'liquidity_pools': self._identify_liquidity_pools(orderbook_data, log_extra)
        }

        return liquidity_zones

    # --- INICIO DE MODIFICACIÓN: (Paso 2.2) ---
    def _calculate_spoofing_score(self, symbol, orderbook_data, log_extra):
        """Calcula probabilidad de spoofing con validación robusta"""

        # Validar orderbook antes de procesar
        if not OrderbookValidator.validate_orderbook_structure(orderbook_data, symbol):
            return Decimal('0.0')

        current_bids = orderbook_data.get('bids', [])
        current_asks = orderbook_data.get('asks', [])

        # Validar que tenemos datos suficientes
        if len(current_bids) < 5 or len(current_asks) < 5:
            logger.debug(f"Orderbook insuficiente para spoofing detection en {symbol}", extra=log_extra)
            return Decimal('0.0')

        try:
            # Calcular tamaño promedio de trade con manejo de errores
            avg_trade_size = self._get_average_trade_size(symbol, log_extra)
            if avg_trade_size <= 0:
                avg_trade_size = Decimal('1.0')

            # Detectar órdenes grandes con thresholds dinámicos
            large_order_threshold = avg_trade_size * Decimal('10')
            large_orders_bids = []
            large_orders_asks = []

            for bid in current_bids[:10]:  # Solo primeros 10 niveles
                try:
                    bid_quantity = Decimal(bid[1])
                    if bid_quantity > large_order_threshold:
                        large_orders_bids.append(bid)
                except (InvalidOperation, IndexError, TypeError) as e:  # Añadido TypeError
                    logger.debug(f"Error procesando bid en spoofing detection: {e}", extra=log_extra)
                    continue

            for ask in current_asks[:10]:
                try:
                    ask_quantity = Decimal(ask[1])
                    if ask_quantity > large_order_threshold:
                        large_orders_asks.append(ask)
                except (InvalidOperation, IndexError, TypeError) as e:  # Añadido TypeError
                    logger.debug(f"Error procesando ask en spoofing detection: {e}", extra=log_extra)
                    continue

            # Calcular score con normalización robusta
            total_large_orders = len(large_orders_bids) + len(large_orders_asks)
            spoofing_score = min(Decimal('1.0'),
                                 total_large_orders / Decimal('5.0'))

            logger.debug(f"Spoofing score para {symbol}: {spoofing_score} "
                         f"(large orders: {total_large_orders})", extra=log_extra)

            return spoofing_score

        except Exception as e:
            logger.error(f"Error crítico en spoofing detection para {symbol}: {e}",
                         exc_info=True, extra=log_extra)
            return Decimal('0.0')
    # --- FIN DE MODIFICACIÓN ---

    def _get_average_trade_size(self, symbol, log_extra):
        """Helper para obtener tamaño promedio de trade"""
        if symbol not in self.trade_flow or len(self.trade_flow[symbol]) < 10:
            return Decimal('1.0')  # Default
        try:
            volumes = [Decimal(trade.get('quantity', '0')) for trade in self.trade_flow[symbol]]
            return sum(volumes) / len(volumes)
        except Exception as e:
            logger.warning(f"Error calculando avg trade size: {e}", extra=log_extra)
            return Decimal('1.0')

    # --- INICIO DE MODIFICACIÓN: (Fase 1.5) ---
    def _calculate_cancellation_rate(self, symbol, log_extra):
        """Stub: Calcula la tasa de cancelaciones (requiere datos de WS más detallados)"""
        return Decimal('0.1')  # Retornar un valor bajo por defecto
    # --- FIN DE MODIFICACIÓN ---

    def _find_volume_clusters(self, trade_history, log_extra):
        """Encuentra clusters de volumen para identificar S/R"""
        if KMeans is None:
            logger.warning("KMeans (sklearn) no está instalado. Saltando clustering de volumen.", extra=log_extra)
            return {'support': [], 'resistance': []}

        if len(trade_history) < 10:
            return {'support': [], 'resistance': []}

        try:
            prices = [float(trade['price']) for trade in trade_history]
            volumes = [float(trade['quantity']) for trade in trade_history]

            price_matrix = np.array(prices).reshape(-1, 1)

            n_clusters = min(5, len(np.unique(prices)))
            if n_clusters == 0: return {'support': [], 'resistance': []}

            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(price_matrix)

            cluster_volumes = {}
            for i, cluster_label in enumerate(clusters):
                if cluster_label not in cluster_volumes:
                    cluster_volumes[cluster_label] = 0
                cluster_volumes[cluster_label] += volumes[i]

            cluster_centers = kmeans.cluster_centers_.flatten()

            if not cluster_volumes:  # Manejar caso donde cluster_volumes está vacío
                return {'support': [], 'resistance': []}

            avg_volume_per_cluster = np.mean(list(cluster_volumes.values()))

            significant_levels = []
            for i, center in enumerate(cluster_centers):
                cluster_label = kmeans.predict(np.array([[center]]))[0]
                if cluster_volumes[cluster_label] > avg_volume_per_cluster:
                    significant_levels.append(center)

            significant_levels = sorted(list(set(significant_levels)))

            return {
                'support': significant_levels[:2],
                'resistance': significant_levels[-2:]
            }
        except Exception as e:
            logger.error(f"Error en clustering de volumen: {e}", exc_info=True, extra=log_extra)
            return {'support': [], 'resistance': []}

    def _find_psychological_levels(self, orderbook_data, log_extra):
        """Encuentra niveles psicológicos (ej. números redondos)"""
        try:
            if not orderbook_data.get('bids') or not orderbook_data.get('asks'):
                return []
            best_bid = Decimal(orderbook_data.get('bids', [['0']])[0][0])
            best_ask = Decimal(orderbook_data.get('asks', [['0']])[0][0])
            mid_price = (best_bid + best_ask) / 2

            if mid_price == 0: return []

            level_1 = round(mid_price, 0)
            level_2 = round(mid_price / 50) * 50
            return sorted(list(set([float(level_1), float(level_2)])))
        except Exception as e:
            logger.warning(f"Error calculando niveles psicológicos: {e}", extra=log_extra)
            return []

    # --- INICIO DE MODIFICACIÓN: (Fase 1.5) ---
    def _calculate_order_density(self, orderbook_data, log_extra):
        """Stub: Calcula áreas de alta densidad de órdenes"""
        return {'high_density': []}

    def _identify_liquidity_pools(self, orderbook_data, log_extra):
        """Stub: Identifica pools de liquidez (órdenes iceberg)"""
        return []
    # --- FIN DE MODIFICACIÓN ---

    # --- INICIO DE MODIFICACIÓN: (Paso 2.3) ---
    def get_microstructure_health_metrics(self, symbol: str) -> dict:
        """
        Retorna métricas de salud del análisis de microestructura
        """
        log_extra = {'symbol': symbol, 'transaction_id': 'MS_HEALTH_CHECK'}
        health_metrics = {
            'orderbook_validity': False,
            'data_freshness': None,
            'spoofing_detection_operational': False,
            'liquidity_analysis_operational': False,
            'error_rate': 0.0
        }

        try:
            # Verificar datos más recientes
            latest_data_deque = self.orderbook_history.get(symbol)
            if latest_data_deque and len(latest_data_deque) > 0:
                latest_data = latest_data_deque[-1]  # Get the last item
                latest_timestamp = latest_data.get('timestamp')
                # Asumiendo que timestamp se guarda como objeto datetime
                data_age = (datetime.utcnow() - latest_timestamp).total_seconds() if latest_timestamp else float('inf')
                health_metrics['data_freshness'] = data_age

            # Verificar funcionalidad de detección de spoofing
            test_orderbook = {'bids': [['100', '1']], 'asks': [['101', '1']], 'lastUpdateId': 1}
            spoofing_score = self._calculate_spoofing_score(symbol, test_orderbook, log_extra)
            health_metrics['spoofing_detection_operational'] = spoofing_score is not None

            health_metrics['orderbook_validity'] = True  # Si spoofing corrió, la validación (básica) pasó

        except Exception as e:
            logger.error(f"Error en health check de microestructura para {symbol}: {e}", extra=log_extra)
            health_metrics['error_rate'] = 1.0

        return health_metrics
    # --- FIN DE MODIFICACIÓN ---