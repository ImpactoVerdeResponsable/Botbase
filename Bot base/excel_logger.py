# excel_logger.py
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
import os
import pandas as pd
import datetime
import pytz  # Importar pytz
# from filelock import FileLock, Timeout # <-- MODIFICACIÓN (Fase 1.4): Eliminado
from decimal import Decimal  # Importar Decimal

# --- INICIO DE MODIFICACIÓN: (Fase 1.2, 1.4) ---
import path_manager
from io_worker import io_worker, TASK_SAVE_EXCEL
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

# EXCEL_FILE_NAME = "trades.xlsx" # <-- MODIFICACIÓN (Fase 1.2): Ya no se usa
SHEET_NAME = "Trading_Log"
HEADERS = [
    "trade_id", "bot_name", "symbol", "open_timestamp", "close_timestamp",
    "trade_duration_sec", "trade_type", "entry_price", "exit_price",
    "position_size_asset", "position_size_quote", "stop_loss_price",
    "take_profit_price", "commission", "swap", "gross_profit_quote",
    "net_profit_quote", "close_reason", "comment"
]


def log_trade(trade_data_dict):
    transaction_id = trade_data_dict.get('trade_id', 'N/A')
    log_extra = {'transaction_id': str(transaction_id), 'symbol': trade_data_dict.get('symbol', 'N/A')}

    # --- INICIO DE MODIFICACIÓN: (Fase 1.4) ---
    # La lógica de bloqueo y escritura (FileLock, pd.read_excel, pd.to_excel)
    # se elimina de esta función. Ahora se delega al IOWorker.
    # --- FIN DE MODIFICACIÓN ---

    try:
        # --- Robustecer manejo de timestamps ANTES de encolar la tarea ---
        # El IOWorker debe recibir datos limpios listos para escribir.
        trade_data_copy = trade_data_dict.copy()  # Trabajar sobre una copia

        # Formatear open_timestamp (espera datetime object)
        open_ts_dt = trade_data_copy.get('open_timestamp')
        if isinstance(open_ts_dt, datetime.datetime):
            # Convertir a UTC y hacerlo naive para Excel
            if open_ts_dt.tzinfo is not None:
                open_ts_naive = open_ts_dt.astimezone(pytz.utc).replace(tzinfo=None)
            else:  # Ya es naive (asumir UTC)
                open_ts_naive = open_ts_dt
            trade_data_copy['open_timestamp'] = open_ts_naive
        elif open_ts_dt is not None:
            logger.warning(f"open_timestamp para {transaction_id} no es datetime object: {open_ts_dt}. Se intentará convertir.", extra=log_extra)
            try:
                # Intentar parsear si es string
                open_ts_dt_parsed = pd.to_datetime(open_ts_dt, errors='coerce', utc=True)
                if pd.notna(open_ts_dt_parsed):
                    trade_data_copy['open_timestamp'] = open_ts_dt_parsed.tz_localize(None)  # Hacer naive
                else:
                    trade_data_copy['open_timestamp'] = None  # Falló el parseo
            except Exception:
                trade_data_copy['open_timestamp'] = None

        # Formatear close_timestamp (espera datetime object)
        close_ts_dt = trade_data_copy.get('close_timestamp')
        if isinstance(close_ts_dt, datetime.datetime):
            if close_ts_dt.tzinfo is not None:
                close_ts_naive = close_ts_dt.astimezone(pytz.utc).replace(tzinfo=None)
            else:
                close_ts_naive = close_ts_dt
            trade_data_copy['close_timestamp'] = close_ts_naive
        elif close_ts_dt is not None:
            logger.warning(f"close_timestamp para {transaction_id} no es datetime object: {close_ts_dt}. Se intentará convertir.", extra=log_extra)
            try:
                close_ts_dt_parsed = pd.to_datetime(close_ts_dt, errors='coerce', utc=True)
                if pd.notna(close_ts_dt_parsed):
                    trade_data_copy['close_timestamp'] = close_ts_dt_parsed.tz_localize(None)  # Hacer naive
                else:
                    trade_data_copy['close_timestamp'] = None  # Falló el parseo
            except Exception:
                trade_data_copy['close_timestamp'] = None

        # --- INICIO DE MODIFICACIÓN: (Fase 1.4) ---
        # Crear payload para el IOWorker
        
        # Asegurar que solo los headers definidos se pasen al DataFrame
        # que creará el io_worker
        final_trade_data_for_queue = {header: trade_data_copy.get(header) for header in HEADERS}

        payload = {
            "path_key": "trades_excel",  # Clave de path_manager.py
            "trade_data": final_trade_data_for_queue
        }

        # Encolar la tarea en lugar de escribirla
        io_worker.add_task(TASK_SAVE_EXCEL, payload, transaction_id)

        logger.info(f"Operación {transaction_id} encolada para registro en Excel.", extra=log_extra)
        # --- FIN DE MODIFICACIÓN ---

    except Exception as e:
        # Captura errores DURANTE la preparación de los datos
        logger.error(f"Error inesperado al PREPARAR el trade {transaction_id} para el IOWorker: {e}", exc_info=True, extra=log_extra)