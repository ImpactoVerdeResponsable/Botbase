# advanced_metrics.py
import logging
import numpy as np
from collections import defaultdict
from decimal import Decimal

# Se asume que scipy está en requirements.txt
try:
    from scipy import stats
except ImportError:
    stats = None
    logging.warning("Scipy no encontrado. El cálculo de 'edge_probability' estará deshabilitado.")

logger = logging.getLogger(__name__)


class AdvancedMetrics:
    def __init__(self):
        self.performance_data = defaultdict(list)
        self.regime_analysis = {}

    def calculate_strategy_quality(self, strategy_name, trades_data):
        """Métricas avanzadas de calidad de estrategia"""
        if len(trades_data) < 10:
            return self._get_default_metrics(strategy_name)

        returns = [float(trade['net_profit_quote']) for trade in trades_data]

        metrics = {
            "probabilidad_edge_real": self.calculate_edge_probability(returns),
            "capacidad_de_scaling": self.assess_scaling_capacity(trades_data),
            "sensibilidad_a_slippage": self.slippage_sensitivity(trades_data, returns), # Modificado para pasar returns
            "correlacion_con_mercado": self.market_correlation(trades_data),
            "consistencia_temporal": self.temporal_consistency(trades_data),
            "robustez_regimenes": self.regime_robustness(trades_data)
        }

        return metrics

    def regime_performance_analysis(self, trades_data):
        """Rendimiento separado por condiciones de mercado"""
        regime_performance = {}

        # Asumir que 'market_regime' se añade a trades_data en algún punto
        # --- MODIFICACIÓN: (Fase 1.5) Añadir fallback para 'comment' si 'market_regime' no existe ---
        # (Asumiendo que 'comment' puede tener el estado del mercado si 'market_regime' no se inyectó)
        all_regimes = set([t.get('market_regime', t.get('comment')) for t in trades_data if t.get('market_regime') or t.get('comment')])
        if not all_regimes:
            logger.warning("No se encontraron 'market_regime' ni 'comment' en trades_data. Saltando análisis de régimen.")
            return {}

        for regime in all_regimes:
            if not regime: continue
            regime_trades = [t for t in trades_data if t.get('market_regime', t.get('comment')) == regime]

            if len(regime_trades) >= 5:
                regime_returns = [float(t['net_profit_quote']) for t in regime_trades]
                
                # Evitar división por cero si len(regime_trades) es 0 (aunque ya chequeamos >= 5)
                if not regime_trades: continue

                regime_performance[regime] = {
                    'win_rate': len([t for t in regime_trades if float(t['net_profit_quote']) > 0]) / len(regime_trades),
                    'avg_profit': np.mean(regime_returns),
                    'profit_factor': self.calculate_profit_factor(regime_trades),
                    'sharpe_ratio': self.calculate_sharpe(regime_returns),
                    'trade_count': len(regime_trades)
                }

        return regime_performance

    def calculate_edge_probability(self, returns):
        """Probabilidad de que la estrategia tenga edge real"""
        if stats is None or len(returns) < 20:
            return 0.5

        positive_returns = [r for r in returns if r > 0]
        if not returns: return 0.0  # Evitar división por cero

        win_rate = len(positive_returns) / len(returns)

        # Test de significancia estadística
        try:
            t_stat, p_value = stats.ttest_1samp(returns, 0)
            if p_value > 0.5:  # Si p_value es alto, el edge es insignificante
                edge_probability = win_rate * (1 - p_value)
            else:
                edge_probability = win_rate * (1 - p_value / 2)  # Ajuste
        except Exception as e:
            logger.warning(f"Error en T-Test: {e}")
            p_value = 1.0
            edge_probability = 0.0

        return min(0.95, max(0.0, edge_probability))

    # --- Métodos Auxiliares y Placeholders Implementados ---

    def _get_default_metrics(self, strategy_name):
        logger.debug(f"Datos insuficientes para métricas avanzadas de {strategy_name}.")
        return {
            "probabilidad_edge_real": 0.5,
            "capacidad_de_scaling": 0.0,
            "sensibilidad_a_slippage": 0.0,
            "correlacion_con_mercado": 0.5, # Dummy
            "consistencia_temporal": 0.0,
            "robustez_regimenes": 0.0
        }

    # --- INICIO DE MODIFICACIÓN: (Fase 1.5) ---
    def assess_scaling_capacity(self, trades_data):
        """
        Stub: Estima la capacidad de escalado basándose en el
        tamaño promedio de la posición en USD.
        (Un valor más bajo podría implicar mejor escalabilidad)
        """
        try:
            # Asume que trades_data tiene 'position_size_quote' (del excel_logger)
            sizes = [float(t.get('position_size_quote', 0)) for t in trades_data]
            if not sizes:
                return 0.0
            return np.mean(sizes)
        except Exception as e:
            logger.warning(f"Error en stub assess_scaling_capacity: {e}")
            return 0.0

    def slippage_sensitivity(self, trades_data, returns):
        """
        Stub: Estima la sensibilidad al slippage usando la desviación
        estándar de los retornos (PnL).
        (Mayor std dev = mayor sensibilidad a la ejecución)
        """
        try:
            if not returns:
                return 0.0
            std_dev = np.std(returns)
            return std_dev
        except Exception as e:
            logger.warning(f"Error en stub slippage_sensitivity: {e}")
            return 0.0

    def market_correlation(self, trades_data):
        """
        Stub: Placeholder. Calcular correlación real requiere
        datos de mercado (ej. BTC) que no están disponibles aquí.
        """
        return 0.5  # Retornar valor dummy neutro

    def temporal_consistency(self, trades_data):
        """
        Stub: Compara el win rate de la primera mitad de los trades
        vs la segunda mitad. Un valor cercano a 1.0 es consistente.
        """
        try:
            half_point = len(trades_data) // 2
            if half_point < 5:  # No hay suficientes datos para comparar
                return 0.0

            first_half = trades_data[:half_point]
            second_half = trades_data[half_point:]

            wins1 = len([t for t in first_half if float(t.get('net_profit_quote', 0)) > 0])
            wins2 = len([t for t in second_half if float(t.get('net_profit_quote', 0)) > 0])

            wr1 = wins1 / len(first_half)
            wr2 = wins2 / len(second_half)
            
            # 1.0 - (diferencia normalizada)
            consistency = 1.0 - abs(wr1 - wr2)
            return max(0.0, consistency)
            
        except Exception as e:
            logger.warning(f"Error en stub temporal_consistency: {e}")
            return 0.0

    def regime_robustness(self, trades_data):
        """
        Stub: Analiza la robustez de la estrategia calculando la
        desviación estándar de los win-rates entre regímenes.
        """
        try:
            regime_perf = self.regime_performance_analysis(trades_data)
            if not regime_perf or len(regime_perf) < 2:
                # No se puede medir robustez si solo opera en 1 régimen
                return 0.0 

            win_rates = [v['win_rate'] for v in regime_perf.values() if v and 'win_rate' in v]
            if len(win_rates) < 2:
                return 0.0

            std_dev_wr = np.std(win_rates)
            
            # Normalizar: 1.0 = perfecto (std dev 0), 0.0 = malo (std dev 0.5 o más)
            robustness = max(0.0, 1.0 - (std_dev_wr * 2))
            return robustness
        except Exception as e:
            logger.warning(f"Error en stub regime_robustness: {e}")
            return 0.0
    # --- FIN DE MODIFICACIÓN ---

    def calculate_profit_factor(self, trades):
        """Calcula el Profit Factor"""
        gross_profit = sum(float(t['net_profit_quote']) for t in trades if float(t['net_profit_quote']) > 0)
        gross_loss = abs(sum(float(t['net_profit_quote']) for t in trades if float(t['net_profit_quote']) < 0))

        if gross_loss == 0:
            return 999.0 if gross_profit > 0 else 1.0

        return gross_profit / gross_loss

    def calculate_sharpe(self, returns, risk_free_rate=0.0):
        """Calcula el Sharpe Ratio (simplificado)"""
        if not returns or np.std(returns) == 0:
            return 0.0

        avg_return = np.mean(returns)
        std_dev = np.std(returns)
        if std_dev == 0:
             return 0.0

        # Asumir que 'returns' ya está en la periodicidad deseada
        sharpe = (avg_return - risk_free_rate) / std_dev

        # Anualizar (asumiendo trades de 1m, 96 velas de 15m/día, 365 días)
        # Este factor de anualización es especulativo
        # (El original era 365 * 96 * 4, que es 140160. sqrt(140160) ~= 374)
        # (Usaremos un valor estándar para trading de alta frecuencia)
        annualization_factor = np.sqrt(252 * 6.5 * 60) # (Días * Horas * Minutos)
        return sharpe * annualization_factor