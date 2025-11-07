# path_manager.py
import os
import logging
from typing import Dict

# Usamos el logger estándar de Python aquí para evitar ciclos de importación.
# unified_logger configurará el handler raíz, por lo que estos logs
# serán capturados y formateados correctamente una vez que el bot inicie.
_logger = logging.getLogger(__name__)

# --- Definiciones de Directorios Base ---

# PROJECT_DIR es la carpeta raíz donde se encuentran main.py, config.py, etc.
PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))

# BOT_OUTPUT_ROOT es el directorio "sandboxed" (bot_1m_nvo) donde se
# guardarán todos los archivos generados (logs, data, reports),
# según el plan de Fase 4.
_BOT_OUTPUT_ROOT = os.path.join(PROJECT_DIR, "bot_1m_nvo")


# --- Fuente Única de Verdad (SSOT) para todas las rutas ---

_PATHS: Dict[str, str] = {
    # --- Archivos de Configuración/Estado (En el Project Root) ---
    "bot_settings": os.path.join(PROJECT_DIR, "bot_settings.json"),
    "active_positions": os.path.join(PROJECT_DIR, "active_positions.json"),
    "dotenv": os.path.join(PROJECT_DIR, ".env"),

    # --- FASE 4: Directorios Raíz de Salida ---
    "data_root": os.path.join(_BOT_OUTPUT_ROOT, "data"),
    "logs_root": os.path.join(_BOT_OUTPUT_ROOT, "logs"),
    "reports_root": os.path.join(_BOT_OUTPUT_ROOT, "reports"),
    "config_output_root": os.path.join(_BOT_OUTPUT_ROOT, "config"),

    # --- FASE 4: Rutas de Datos (/data) ---
    "trades_excel": os.path.join(_BOT_OUTPUT_ROOT, "data", "trades", "trades.xlsx"),
    "performance_metrics": os.path.join(_BOT_OUTPUT_ROOT, "data", "performance", "performance_metrics.json"),
    "backtest_results_dir": os.path.join(_BOT_OUTPUT_ROOT, "data", "backtest_results"),
    "orderbook_snapshots_dir": os.path.join(_BOT_OUTPUT_ROOT, "data", "market_data", "orderbook_snapshots"),
    "tape_data_dir": os.path.join(_BOT_OUTPUT_ROOT, "data", "market_data", "tape_data"),
    "market_regime_metrics": os.path.join(_BOT_OUTPUT_ROOT, "data", "market_analysis", "market_regime_metrics.json"),
    "participant_behavior": os.path.join(_BOT_OUTPUT_ROOT, "data", "market_analysis", "participant_behavior.json"),
    "microstructure_analysis_dir": os.path.join(_BOT_OUTPUT_ROOT, "data", "microstructure_analysis"),

    # --- FASE 4: Rutas de Logs (/logs) ---
    "execution_logs": os.path.join(_BOT_OUTPUT_ROOT, "logs", "execution", "execution_logs.csv"),
    "system_logs_dir": os.path.join(_BOT_OUTPUT_ROOT, "logs", "system"), # Para TimedRotatingFileHandler
    "error_logs_dir": os.path.join(_BOT_OUTPUT_ROOT, "logs", "errors"), # Para TimedRotatingFileHandler
    "latency_metrics_dir": os.path.join(_BOT_OUTPUT_ROOT, "logs", "performance", "latency_metrics"),
    "memory_profiles_dir": os.path.join(_BOT_OUTPUT_ROOT, "logs", "performance", "memory_profiles"),
    "api_call_logs_dir": os.path.join(_BOT_OUTPUT_ROOT, "logs", "api"),
    "decision_logs": os.path.join(_BOT_OUTPUT_ROOT, "logs", "decisions", "decision_logs.json"),

    # --- FASE 4: Rutas de Reportes (/reports) ---
    "performance_dashboards_dir": os.path.join(_BOT_OUTPUT_ROOT, "reports", "dashboards", "performance_dashboards"),
    "team_reports_dir": os.path.join(_BOT_OUTPUT_ROOT, "reports", "team", "team_reports"),
    "daily_summary": os.path.join(_BOT_OUTPUT_ROOT, "reports", "daily", "daily_summary.json"),
    
    # --- FASE 4: Rutas de Configuración de Salida (/config) ---
    "strategy_configs_dir": os.path.join(_BOT_OUTPUT_ROOT, "config", "strategies"),
    "risk_parameters": os.path.join(_BOT_OUTPUT_ROOT, "config", "risk", "risk_parameters.json"),
    
    # --- Rutas del Proyecto Original que ahora se mapean a Fase 4 ---
    # (Mapea las claves antiguas a las nuevas rutas de Fase 4)
    "log_directory": _BOT_OUTPUT_ROOT, # Para unified_logger y utils
    "scan_results_directory": os.path.join(_BOT_OUTPUT_ROOT, "data", "scan_results"), # Mapeo de config original
    "backup_destination_path": os.path.join(_BOT_OUTPUT_ROOT, "data", "trades", "backups"), # Mapeo de config original
}


def get_path(key: str, **kwargs) -> str:
    """
    Obtiene una ruta centralizada y asegura que el directorio exista.

    Esta función es la única responsable de crear directorios en el sistema.

    Args:
        key: La clave del diccionario _PATHS (e.g., 'trades_excel').
        **kwargs: Para formatear rutas dinámicas (no se usa actualmente,
                  pero es útil para (ej. 'backtest_results_dir', 
                  'backtest_2025-11-04.json')).

    Returns:
        str: La ruta absoluta al archivo o directorio.

    Raises:
        KeyError: Si la clave de ruta no se encuentra.
        IOError: Si no se pueden crear los directorios.
    """
    if key not in _PATHS:
        _logger.error(f"Error Crítico de Path: La clave '{key}' no existe en path_manager._PATHS.")
        raise KeyError(f"Clave de ruta no encontrada: '{key}'")

    path = _PATHS[key]

    if kwargs:
        try:
            path = path.format(**kwargs)
        except KeyError as e:
            _logger.error(f"Error de formato en get_path para clave '{key}': Falta argumento {e}")
            raise e

    # Determinar si la ruta es un archivo o un directorio
    # Asumimos que si tiene una extensión (ej. .json, .xlsx), es un archivo.
    _, extension = os.path.splitext(path)
    
    if extension:
        # Es un archivo, asegurar que el directorio padre exista
        directory = os.path.dirname(path)
    else:
        # Es un directorio, asegurarlo
        directory = path

    if not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
            _logger.debug(f"Directorio creado: {directory}")
        except OSError as e:
            _logger.error(f"Error de E/S al crear directorio '{directory}': {e}")
            raise IOError(f"No se pudo crear el directorio: {directory}") from e
        except Exception as e:
            _logger.error(f"Error inesperado al crear directorio '{directory}': {e}")
            raise IOError(f"Error inesperado creando directorio: {directory}") from e
            
    return path

def get_project_dir() -> str:
    """Retorna el directorio raíz del proyecto (donde vive main.py)."""
    return PROJECT_DIR

def get_bot_output_root() -> str:
    """Retorna el directorio raíz de salida (la carpeta 'bot_1m_nvo')."""
    get_path('data_root') # Asegura que el root exista al ser llamado
    return _BOT_OUTPUT_ROOT