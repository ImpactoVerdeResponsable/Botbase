# performance_monitor.py
from unified_logger import get_logger
import numpy as np
import pandas as pd
from collections import defaultdict
from decimal import Decimal
import time  # Importar time para _trigger_alert
from datetime import datetime  # <-- MODIFICACIÓN (Fase 1.5)

# Importar el cache de indicadores para las métricas del sistema
from technical_analysis import indicator_cache

logger = get_logger(__name__)

# Se asume que este módulo se integrará con el dashboard o un logger
# y que tendrá acceso a la BBDD de trades (ej. trades.xlsx)


class PerformanceMonitor:
    def __init__(self):
        self.metrics_history = []
        self.performance_cache = {}
        self.all_trades_df = None  # DataFrame para almacenar trades reales

    def load_trades_data(self, trades_df):
        """Carga el dataframe de trades reales."""
        log_extra = {'symbol': 'PERF_MONITOR'}
        if trades_df is None or trades_df.empty:
            logger.warning("PerformanceMonitor: No se cargaron datos de trades.", extra=log_extra)
            self.all_trades_df = pd.DataFrame()
        else:
            self.all_trades_df = trades_df.copy()
            # Asegurar tipos de datos
            self.all_trades_df['net_profit_quote'] = pd.to_numeric(self.all_trades_df['net_profit_quote'], errors='coerce').fillna(0)
            self.all_trades_df['close_timestamp'] = pd.to_datetime(self.all_trades_df['close_timestamp'], errors='coerce')

    def calculate_strategy_metrics(self, strategy_name):
        """Calcula métricas específicas por estrategia"""
        log_extra = {'symbol': strategy_name, 'transaction_id': 'CALC_METRICS'}

        if self.all_trades_df is None or self.all_trades_df.empty:
            return None  # No hay datos

        strategy_trades_df = self.all_trades_df[self.all_trades_df['comment'] == strategy_name]

        if len(strategy_trades_df) < 5:
            logger.debug(f"Datos insuficientes para métricas de {strategy_name} (Trades: {len(strategy_trades_df)})", extra=log_extra)
            return None

        strategy_trades_list = strategy_trades_df.to_dict('records')
        returns = strategy_trades_df['net_profit_quote'].tolist()

        metrics = {
            'win_rate': self.calculate_win_rate(strategy_trades_list),
            'avg_win': self.calculate_average_win(strategy_trades_list),
            'avg_loss': self.calculate_average_loss(strategy_trades_list),
            'profit_factor': self.calculate_profit_factor(strategy_trades_list),
            'sharpe_ratio': self.calculate_sharpe(returns),
            'max_drawdown': self.calculate_max_drawdown(returns),
            'expectancy': self.calculate_expectancy(strategy_trades_list),
            'trade_count': len(strategy_trades_list)
        }
        self.performance_cache[strategy_name] = metrics
        return metrics

    def get_recommendations(self):
        """Genera recomendaciones basadas en métricas"""
        recommendations = []
        log_extra = {'symbol': 'PERF_RECOMMEND'}

        if not self.performance_cache:
            logger.info("Calculando métricas de performance antes de generar recomendaciones...", extra=log_extra)
            if self.all_trades_df is not None:
                all_strategies = self.all_trades_df['comment'].unique()
                for strategy in all_strategies:
                    if strategy: self.calculate_strategy_metrics(strategy)

        if not self.performance_cache:
            return ["No hay datos de performance cacheados."]

        for strategy, metrics in self.performance_cache.items():
            if metrics['win_rate'] < 0.4 and metrics['trade_count'] > 10:
                recommendations.append(f"Considerar desactivar {strategy} (Win Rate: {metrics['win_rate']:.1%})")
            elif metrics['profit_factor'] < 1.2 and metrics['trade_count'] > 10:
                recommendations.append(f"Revisar parámetros de {strategy} (Profit Factor: {metrics['profit_factor']:.2f})")
            elif metrics['max_drawdown'] < -100.0:  # Asumiendo DD en USD
                recommendations.append(f"Alta MaxDrawdown en {strategy} ({metrics['max_drawdown']:.2f} USD)")

        return recommendations

    # --- Funciones de Cálculo de Métricas ---

    def get_trades_by_strategy(self, strategy_name):
        """Helper para filtrar trades (usado por calculate_strategy_metrics)"""
        if self.all_trades_df is None:
            return []
        strategy_trades_df = self.all_trades_df[self.all_trades_df['comment'] == strategy_name]
        return strategy_trades_df.to_dict('records')

    def calculate_win_rate(self, trades):
        wins = len([t for t in trades if t['net_profit_quote'] > 0])
        if not trades: return 0.0
        return wins / len(trades)

    def calculate_average_win(self, trades):
        wins = [t['net_profit_quote'] for t in trades if t['net_profit_quote'] > 0]
        if not wins: return 0.0
        return np.mean(wins)

    def calculate_average_loss(self, trades):
        losses = [t['net_profit_quote'] for t in trades if t['net_profit_quote'] < 0]
        if not losses: return 0.0
        return np.mean(losses)

    def calculate_profit_factor(self, trades):
        gross_profit = sum(t['net_profit_quote'] for t in trades if t['net_profit_quote'] > 0)
        gross_loss = abs(sum(t['net_profit_quote'] for t in trades if t['net_profit_quote'] < 0))

        if gross_loss == 0:
            return 999.0 if gross_profit > 0 else 1.0
        return gross_profit / gross_loss

    def calculate_sharpe(self, returns, risk_free_rate=0.0):
        if not returns or np.std(returns) == 0:
            return 0.0

        avg_return = np.mean(returns)
        std_dev = np.std(returns)
        if std_dev == 0:
            return 0.0
            
        sharpe = (avg_return - risk_free_rate) / std_dev
        return sharpe  # Devolver Sharpe por trade (sin anualizar)

    def calculate_max_drawdown(self, returns):
        if not returns:
            return 0.0
        cumulative = np.cumsum(returns)
        peak = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - peak)
        return np.min(drawdown)

    def calculate_expectancy(self, trades):
        win_rate = self.calculate_win_rate(trades)
        avg_win = self.calculate_average_win(trades)
        avg_loss = self.calculate_average_loss(trades)

        loss_rate = 1.0 - win_rate
        expectancy = (win_rate * avg_win) + (loss_rate * avg_loss)
        return expectancy


# --- INICIO DE MODIFICACIÓN: (Paso 3.1) ---
class EnhancedPerformanceMonitor(PerformanceMonitor):
    def __init__(self):
        super().__init__()
        self.performance_trends = defaultdict(list)
        self.alert_thresholds = self._load_alert_thresholds()
        self.degradation_detected = False

    def _load_alert_thresholds(self):
        """Carga thresholds de alerta desde configuración"""
        # (En una implementación real, esto podría venir de config.py)
        return {
            'sharpe_drop_pct': Decimal('0.3'),  # 30% drop
            'win_rate_drop_pct': Decimal('0.25'),  # 25% drop
            'max_drawdown_increase_pct': Decimal('0.5'),  # 50% increase
            'consecutive_losses': 3,
            'volume_decrease_pct': Decimal('0.4')  # 40% volume drop
        }

    def calculate_advanced_metrics(self, strategy_name: str) -> dict:
        """Calcula métricas avanzadas de performance"""
        basic_metrics = self.calculate_strategy_metrics(strategy_name)
        if not basic_metrics:
            return None

        # Métricas de tendencia
        trend_metrics = self._calculate_trend_metrics(strategy_name, basic_metrics)

        # Métricas de riesgo ajustado
        risk_metrics = self._calculate_risk_adjusted_metrics(basic_metrics)

        # Métricas de consistencia
        consistency_metrics = self._calculate_consistency_metrics(strategy_name)

        advanced_metrics = {
            **basic_metrics,
            'trend_analysis': trend_metrics,
            'risk_adjusted_metrics': risk_metrics,
            'consistency_metrics': consistency_metrics,
            'performance_score': self._calculate_performance_score(basic_metrics, trend_metrics)
        }

        # Verificar degradación y añadir alertas al dict
        advanced_metrics['alerts'] = self._check_performance_degradation(strategy_name, advanced_metrics)

        return advanced_metrics

    def _calculate_trend_metrics(self, strategy_name: str, current_metrics: dict) -> dict:
        """Analiza tendencias de performance"""
        self.performance_trends[strategy_name].append(current_metrics)

        if len(self.performance_trends[strategy_name]) < 5:
            return {'trend_available': False}

        recent_data = self.performance_trends[strategy_name][-5:]

        # Calcular tendencias usando regresión lineal simple
        win_rate_trend = self._calculate_linear_trend(
            [m['win_rate'] for m in recent_data]
        )
        sharpe_trend = self._calculate_linear_trend(
            [m['sharpe_ratio'] for m in recent_data]
        )

        return {
            'trend_available': True,
            'win_rate_trend': win_rate_trend,
            'sharpe_trend': sharpe_trend,
            'trend_direction': 'improving' if sharpe_trend > 0 else 'deteriorating',
            'trend_strength': abs(sharpe_trend)
        }

    # --- INICIO DE MODIFICACIÓN: (Paso 3.2) ---
    def _check_performance_degradation(self, strategy_name: str, metrics: dict):
        """Verifica degradación de performance y genera alertas"""
        alerts = []

        # Alertas basadas en thresholds
        if metrics['win_rate'] < 0.4:
            alerts.append({
                'type': 'CRITICAL',
                'message': f"Win rate críticamente bajo para {strategy_name}: {metrics['win_rate']:.1%}",
                'metric': 'win_rate',
                'value': metrics['win_rate']
            })

        # Alertas basadas en tendencias
        trend_metrics = metrics.get('trend_analysis', {})
        if trend_metrics.get('trend_available', False):
            if trend_metrics['sharpe_trend'] < -0.1:  # Sharpe decreasing
                alerts.append({
                    'type': 'WARNING',
                    'message': f"Sharpe ratio en deterioro para {strategy_name}",
                    'metric': 'sharpe_trend',
                    'value': trend_metrics['sharpe_trend']
                })

        # Alertas de drawdown
        if metrics['max_drawdown'] < -100.0:  # Drawdown > 100 USD
            alerts.append({
                'type': 'WARNING',
                'message': f"Drawdown significativo para {strategy_name}: {metrics['max_drawdown']:.2f} USD",
                'metric': 'max_drawdown',
                'value': metrics['max_drawdown']
            })

        # Procesar alertas
        for alert in alerts:
            self._trigger_alert(strategy_name, alert)

        if alerts:
            self.degradation_detected = True

        return alerts  # Devolver la lista de alertas

    def _trigger_alert(self, strategy_name: str, alert: dict):
        """Dispara alerta a través del sistema de logging unificado"""
        log_extra = {
            'symbol': 'PERFORMANCE_ALERT',
            'transaction_id': f"alert_{int(time.time())}",
            'alert_data': alert
        }

        if alert['type'] == 'CRITICAL':
            logger.critical(f"🚨 {alert['message']}", extra=log_extra)
        else:
            logger.warning(f"⚠️ {alert['message']}", extra=log_extra)

        # Opcional: Integrar con sistema de notificaciones externo
        self._send_external_alert(strategy_name, alert)
    # --- FIN DE MODIFICACIÓN ---

    # --- INICIO DE MODIFICACIÓN: (Paso 3.3) ---
    def get_comprehensive_performance_dashboard(self) -> dict:
        """Genera dashboard completo de performance"""
        dashboard = {
            'timestamp': datetime.utcnow().isoformat(),
            'overall_health': 'HEALTHY',
            'strategy_performance': {},
            'system_metrics': {},
            'alerts': [],
            'recommendations': []
        }

        # Métricas por estrategia
        for strategy in self.get_all_tracked_strategies():
            advanced_metrics = self.calculate_advanced_metrics(strategy)
            if advanced_metrics:
                dashboard['strategy_performance'][strategy] = advanced_metrics

                # Recolectar alertas
                if advanced_metrics.get('alerts'):
                    dashboard['alerts'].extend(advanced_metrics['alerts'])

        # Métricas del sistema
        dashboard['system_metrics'] = {
            'cache_performance': indicator_cache.get_performance_stats(),
            'memory_usage': self._get_memory_usage(),
            'processing_latency': self._get_processing_latency()
        }

        # Determinar salud general
        if any('CRITICAL' in alert.get('type', '') for alert in dashboard['alerts']):
            dashboard['overall_health'] = 'CRITICAL'
        elif dashboard['alerts']:
            dashboard['overall_health'] = 'WARNING'

        # Generar recomendaciones
        dashboard['recommendations'] = self._generate_optimization_recommendations(dashboard)

        return dashboard
    # --- FIN DE MODIFICACIÓN ---

    # --- INICIO DE MODIFICACIÓN: (Fase 1.5) ---
    def _calculate_risk_adjusted_metrics(self, basic_metrics):
        """Stub: Calcula métricas de riesgo (ej. Sortino)"""
        # Simular Sortino como un % de Sharpe
        sortino = basic_metrics.get('sharpe_ratio', 0) * 1.2
        return {
            'sortino_ratio': sortino,
            'cagr': 0.0 # Requiere análisis temporal
        }

    def _calculate_consistency_metrics(self, strategy_name):
        """Stub: Calcula métricas de consistencia"""
        return {
            'expectancy_consistency': 0.5 # Dummy
        }

    def _calculate_performance_score(self, basic_metrics, trend_metrics):
        """Stub: Calcula un score de performance general"""
        score = 0.0
        try:
            score += basic_metrics.get('win_rate', 0) * 50
            score += basic_metrics.get('profit_factor', 1) * 10
            
            if trend_metrics.get('trend_available', False):
                 # Premiar tendencia positiva de sharpe
                 score += trend_metrics.get('sharpe_trend', 0) * 100
            
            return max(0, min(100, score)) # Normalizar 0-100
        except Exception:
            return 0.0

    def _calculate_linear_trend(self, data):
        """Implementación existente (no es un stub)"""
        try:
            if len(data) < 2: return 0.0
            # Regresión lineal simple (pendiente)
            x = np.arange(len(data))
            y = np.array(data)
            slope, _ = np.polyfit(x, y, 1)
            return slope
        except Exception:
            return 0.0

    def _send_external_alert(self, strategy_name, alert):
        """Stub: Envía alerta externa (ej. Slack, Telegram)"""
        logger.debug(f"Alerta externa (STUB): {alert['message']}", extra={'symbol': strategy_name})
        pass # Placeholder

    def get_all_tracked_strategies(self):
        if self.all_trades_df is not None:
            return self.all_trades_df['comment'].unique()
        return []

    def _get_memory_usage(self):
        """Stub: Retorna uso de memoria (debería usar psutil)"""
        return 100.0  # Dummy 100.0 MB

    def _get_processing_latency(self):
        """Stub: Retorna latencia de procesamiento (debería medirse en main loop)"""
        return 0.1  # Dummy 0.1s

    def _generate_optimization_recommendations(self, dashboard):
        """Stub: Genera recomendaciones de optimización"""
        recs = []
        if dashboard['overall_health'] == 'CRITICAL':
            recs.append("Revisar estrategias con alertas CRITICAL. Considerar desactivación.")
        
        # Lógica de get_recommendations (original)
        for strategy, metrics in self.performance_cache.items():
            if metrics['win_rate'] < 0.4 and metrics['trade_count'] > 10:
                recs.append(f"Considerar desactivar {strategy} (Win Rate: {metrics['win_rate']:.1%})")
            elif metrics['profit_factor'] < 1.2 and metrics['trade_count'] > 10:
                recs.append(f"Revisar parámetros de {strategy} (Profit Factor: {metrics['profit_factor']:.2f})")
        
        return list(set(recs)) # Devolver únicas
    # --- FIN DE MODIFICACIÓN ---