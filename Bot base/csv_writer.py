# csv_writer.py
import datetime
from unified_logger import get_logger
from io_worker import io_worker, TASK_APPEND_CSV
import path_manager  # Importar path_manager para la clave de ruta

logger = get_logger(__name__)


def log_execution(
    transaction_id: str,
    symbol: str,
    side: str,
    quantity: str,
    price: str,
    status: str,
    error_message: str = None
):
    """
    Encola el registro de una ejecución de orden (intento o éxito)
    para ser guardada en /bot_1m_nvo/logs/execution/execution_logs.csv.

    Esta función es NO bloqueante.
    """
    log_extra = {'transaction_id': transaction_id, 'symbol': symbol}

    try:
        # 1. Formatear los datos según el Anexo (Fase 4)
        execution_row = {
            "Timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "TransactionID": transaction_id,
            "Symbol": symbol,
            "Side": side,
            "Quantity": quantity,
            "Price": price, # Precio objetivo (para market) o límite
            "Status": status,
            "Error": error_message if error_message else "None"
        }

        # 2. Definir el payload para el IOWorker
        payload = {
            "path_key": "execution_logs",  # Clave de path_manager.py
            "data_row": execution_row      # El dict que representa la fila
        }

        # 3. Encolar la tarea
        io_worker.add_task(
            task_type=TASK_APPEND_CSV,
            payload=payload,
            transaction_id=transaction_id
        )

        logger.debug(f"Log de ejecución encolado para {symbol} (Status: {status})", extra=log_extra)

    except Exception as e:
        logger.error(f"Error al encolar log de ejecución: {e}", exc_info=True, extra=log_extra)

# (Se pueden añadir otras funciones de log CSV aquí, ej. log_api_call)