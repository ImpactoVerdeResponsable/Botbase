# redis_manager.py
import logging
import redis
from typing import Optional
import config

logger = logging.getLogger(__name__)

# --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
# Se define un log_extra estático para este módulo
_log_extra_redis = {'symbol': 'REDIS_MGR', 'transaction_id': 'REDIS_CONN'}
# --- FIN DE MODIFICACIÓN ---

class RedisManager:
    """
    Gestor centralizado de conexiones Redis
    Proporciona conexión única y configuración consistente
    """
    
    _instance: Optional['RedisManager'] = None
    _connection: Optional[redis.Redis] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
    
    def _connect(self):
        """Establece conexión con Redis con estrategia de fallback"""
        hosts_to_try = [
            {'host': 'redis', 'port': 6379},  # Docker
            {'host': 'localhost', 'port': config.get_param("SHADOW_OPTUNA.REDIS_PORT", 6379)},
            {'host': '127.0.0.1', 'port': config.get_param("SHADOW_OPTUNA.REDIS_PORT", 6379)}
        ]
        
        for host_config in hosts_to_try:
            try:
                self._connection = redis.Redis(
                    host=host_config['host'],
                    port=host_config['port'],
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                self._connection.ping()
                # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
                logger.info(f"✅ Conectado a Redis en {host_config['host']}:{host_config['port']}", extra=_log_extra_redis)
                # --- FIN DE MODIFICACIÓN ---
                break
            except (redis.ConnectionError, redis.TimeoutError) as e:
                # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
                logger.debug(f"❌ No se pudo conectar a {host_config['host']}:{host_config['port']} - {e}", extra=_log_extra_redis)
                # --- FIN DE MODIFICACIÓN ---
                self._connection = None
                continue
            except Exception as e:
                # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
                logger.debug(f"❌ Error inesperado con {host_config['host']}:{host_config['port']} - {e}", extra=_log_extra_redis)
                # --- FIN DE MODIFICACIÓN ---
                self._connection = None
                continue
        else:
            # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
            logger.error("❌ No se pudo conectar a Redis en ningún host disponible", extra=_log_extra_redis)
            # --- FIN DE MODIFICACIÓN ---
            self._connection = None
    
    def get_connection(self) -> Optional[redis.Redis]:
        """
        Obtiene la conexión Redis activa
        """
        if self._connection is None:
            # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
            logger.warning("⚠️  Intentando conectar/reconectar Redis...", extra=_log_extra_redis)
            # --- FIN DE MODIFICACIÓN ---
            self._connect()
        
        if self._connection:
            try:
                self._connection.ping()
                return self._connection
            except (redis.ConnectionError, redis.TimeoutError):
                # --- INICIO DE MODIFICACIÓN: (Fase 1.1 - FIX KeyError) ---
                logger.warning("⚠️  Conexión Redis perdida, reconectando...", extra=_log_extra_redis)
                # --- FIN DE MODIFICACIÓN ---
                self._connect()
                return self._connection
        
        return None
    
    def is_connected(self) -> bool:
        """Verifica si Redis está conectado"""
        conn = self.get_connection()
        return conn is not None
    
    def get_config_value(self, key: str, default: any = None) -> any:
        """
        Obtiene valor de configuración Redis-related de forma centralizada
        """
        config_map = {
            'SHADOW_TRADES_LIST': config.get_param("SHADOW_OPTUNA.LIST_KEY", "shadow_trades_1m"),
            'REDIS_CHANNEL': config.get_param("SHADOW_OPTUNA.CHANNEL", "bot_signals"),
            'REDIS_PORT': config.get_param("SHADOW_OPTUNA.REDIS_PORT", 6379),
            'REDIS_HOST': config.get_param("SHADOW_OPTUNA.REDIS_HOST", "localhost")
        }
        return config_map.get(key, default)

# Instancia global
redis_manager = RedisManager()