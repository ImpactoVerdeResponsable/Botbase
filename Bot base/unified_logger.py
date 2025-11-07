# unified_logger.py
import logging
import sys
import os
from typing import Any, Dict, Optional
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler  # <-- MODIFICACIÓN (Fase 1.3 / 4.0)

# --- INICIO DE MODIFICACIÓN: (Fase 1.2 / 4.0) ---
# Importar el nuevo gestor de rutas
import path_manager
# --- FIN DE MODIFICACIÓN ---


class UnifiedLogger:
    """
    Logger unificado para todo el sistema de trading
    Proporciona interfaz consistente para todos los módulos
    """

    _handlers_configured = False  # Flag de clase para configurar handlers solo una vez

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self._ensure_handlers()

    def _ensure_handlers(self):
        """
        Garantiza que el logger raíz tenga el handler de CONSOLA configurado.
        Los handlers de archivos se configuran por separado en setup_logging_from_config.
        """
        if not UnifiedLogger._handlers_configured:
            root_logger = logging.getLogger()

            # Limpiar handlers existentes si los hubiera (para evitar duplicados en recargas)
            if root_logger.hasHandlers():
                for h in root_logger.handlers[:]:
                    root_logger.removeHandler(h)

            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s.%(msecs)03d - %(levelname)s - [%(name)s] - [%(transaction_id)s|%(symbol)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
            root_logger.setLevel(logging.INFO)  # Nivel base, será sobreescrito por config
            UnifiedLogger._handlers_configured = True

    def _enrich_log_record(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Enriquece el registro de log con información contextual"""
        base_extra = {
            'transaction_id': 'N/A',
            'symbol': 'N/A'
        }
        if extra:
            base_extra.update(extra)
        return base_extra

    def info(self, message: str, extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log nivel INFO con formato unificado"""
        enriched_extra = self._enrich_log_record(extra)
        self.logger.info(message, extra=enriched_extra, **kwargs)

    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log nivel DEBUG con formato unificado"""
        enriched_extra = self._enrich_log_record(extra)
        self.logger.debug(message, extra=enriched_extra, **kwargs)

    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log nivel WARNING con formato unificado"""
        enriched_extra = self._enrich_log_record(extra)
        self.logger.warning(message, extra=enriched_extra, **kwargs)

    def error(self, message: str, extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log nivel ERROR con formato unificado"""
        enriched_extra = self._enrich_log_record(extra)
        self.logger.error(message, extra=enriched_extra, **kwargs)

    def critical(self, message: str, extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log nivel CRITICAL con formato unificado"""
        enriched_extra = self._enrich_log_record(extra)
        self.logger.critical(message, extra=enriched_extra, **kwargs)


# Función helper para obtener logger unificado
def get_logger(name: str) -> UnifiedLogger:
    """
    Obtiene una instancia de logger unificado

    Args:
        name: Nombre del módulo (generalmente __name__)

    Returns:
        UnifiedLogger: Instancia configurada del logger
    """
    return UnifiedLogger(name)


# --- INICIO DE MODIFICACIÓN: (Fase 1.3 / 4.0) ---
# Lógica de configuración de logging (antes en utils.py) movida aquí
# y adaptada para las nuevas rutas de Fase 4.
def setup_logging_from_config(config_module):
    """
    Ajusta el nivel del logger RAÍZ y añade HANDLERS DE ARCHIVO
    basándose en el archivo de configuración.
    """
    _logger = get_logger(__name__)
    
    try:
        log_level_str = config_module.get_param('GLOBAL_SETTINGS.LOGGING_LEVEL', default_value='INFO')
        log_level = getattr(logging, log_level_str.upper(), logging.INFO)

        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # --- Formateador estándar para archivos ---
        file_formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d - %(levelname)s - [%(name)s] - [%(transaction_id)s|%(symbol)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # --- 1. Handler para System Logs (INFO y superior) ---
        # (Cumple con /bot_1m_nvo/logs/system/system_logs/)
        try:
            # path_manager ya crea el directorio
            system_log_dir = path_manager.get_path("system_logs_dir")
            system_log_filepath = os.path.join(system_log_dir, "system.log")
            
            system_handler = TimedRotatingFileHandler(
                system_log_filepath,
                when="midnight",
                interval=1,
                backupCount=30,
                encoding='utf-8'
            )
            system_handler.setLevel(log_level)
            system_handler.setFormatter(file_formatter)
            
            if not any(isinstance(h, TimedRotatingFileHandler) and "system.log" in h.baseFilename for h in root_logger.handlers):
                root_logger.addHandler(system_handler)
                _logger.info(f"Logging del sistema (nivel {log_level_str}) activado en: {system_log_filepath}", extra={'symbol': 'LOG_SETUP'})
                
        except Exception as e:
            _logger.error(f"Error al configurar el system_handler: {e}. Logs de sistema solo en consola.", extra={'symbol': 'LOG_SETUP'}, exc_info=True)

        # --- 2. Handler para Error Logs (ERROR y superior) ---
        # (Cumple con /bot_1m_nvo/logs/errors/error_logs/)
        try:
            # path_manager ya crea el directorio
            error_log_dir = path_manager.get_path("error_logs_dir")
            error_log_filepath = os.path.join(error_log_dir, "error.log")

            error_handler = TimedRotatingFileHandler(
                error_log_filepath,
                when="midnight",
                interval=1,
                backupCount=30,
                encoding='utf-8'
            )
            error_handler.setLevel(logging.ERROR) # <-- Solo Nivel ERROR
            error_handler.setFormatter(file_formatter)

            if not any(isinstance(h, TimedRotatingFileHandler) and "error.log" in h.baseFilename for h in root_logger.handlers):
                root_logger.addHandler(error_handler)
                _logger.info(f"Logging de errores activado en: {error_log_filepath}", extra={'symbol': 'LOG_SETUP'})

        except Exception as e:
            _logger.error(f"Error al configurar el error_handler: {e}. Logs de error solo en consola/system.", extra={'symbol': 'LOG_SETUP'}, exc_info=True)

    except Exception as e:
        _logger.critical(f"Error fatal al aplicar nivel de log desde config: {e}", exc_info=True, extra={'symbol': 'LOG_SETUP'})
# --- FIN DE MODIFICACIÓN ---