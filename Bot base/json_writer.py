# json_writer.py
import datetime
from unified_logger import get_logger
from io_worker import io_worker, TASK_APPEND_JSON_LIST, TASK_SAVE_JSON
import path_manager  # Importar path_manager para la clave de ruta

logger = get_logger(__name__)


def log_decision(symbol: str, strategy: str, signal_type: str, vol_ratio: float, market_state: str, price: float, transaction_id: str):
    """
    Encola el registro de una decisión de trading (señal generada)
    para ser guardada en /bot_1m_nvo/logs/decisions/decision_logs.json.

    Esta función es NO bloqueante.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}
    
    try:
        # 1. Formatear los datos según el Anexo (Fase 4)
        decision_data = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "symbol": symbol,
            "strategy": strategy,
            "signal_type": signal_type,
            "price": float(price) if price else 0.0,
            "market_state": market_state,
            "vol_ratio": float(vol_ratio),
            "transaction_id": transaction_id  # Trazabilidad
        }
        
        # 2. Definir el payload para el IOWorker
        payload = {
            "path_key": "decision_logs",  # Clave de path_manager.py
            "item_data": decision_data     # El item a añadir a la lista
        }
        
        # 3. Encolar la tarea
        io_worker.add_task(
            task_type=TASK_APPEND_JSON_LIST,
            payload=payload,
            transaction_id=transaction_id
        )
        
        logger.debug(f"Decisión de trading encolada para {symbol}", extra=log_extra)
        
    except Exception as e:
        logger.error(f"Error al encolar decisión de trading: {e}", exc_info=True, extra=log_extra)


def write_json_file(path_key: str, data: dict, transaction_id: str):
    """
    Encola el guardado (SOBRESCRIBIR) de un archivo JSON.
    Usado para reportes como performance_metrics.json, daily_summary.json, etc.
    
    Payload esperado: {'path_key': str, 'data': dict}
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': 'JSON_WRITER'}
    try:
        payload = {
            "path_key": path_key,
            "data": data
        }
        
        io_worker.add_task(
            task_type=TASK_SAVE_JSON,  # Usar la tarea de sobrescritura
            payload=payload,
            transaction_id=transaction_id
        )
        logger.debug(f"Archivo JSON encolado para escritura (Sobrescribir) en {path_key}", extra=log_extra)
    except Exception as e:
        logger.error(f"Error al encolar escritura JSON (Sobrescribir) para {path_key}: {e}", exc_info=True, extra=log_extra)