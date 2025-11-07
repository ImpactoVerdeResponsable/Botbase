# io_worker.py
import queue
import threading
import time
import pandas as pd
from filelock import FileLock, Timeout
import json
import os

from unified_logger import get_logger
import path_manager

logger = get_logger(__name__)

# --- Constantes del Worker ---
STOP_SENTINEL = None  # Señal para detener el hilo
FLUSH_TIMEOUT_SECONDS = 5  # Tiempo máximo de espera al cerrar
MAX_QUEUE_SIZE = 1000  # Prevenir uso excesivo de memoria

# --- Tipos de Tareas ---
# (Usamos dicts para pasar payloads de tareas)
TASK_SAVE_JSON = "SAVE_JSON"  # Sobrescritura atómica
TASK_SAVE_EXCEL = "SAVE_EXCEL"  # Añadir (append) a Excel
TASK_APPEND_JSON_LIST = "APPEND_JSON_LIST"  # Añadir (append) a una lista JSON
# --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
TASK_APPEND_CSV = "APPEND_CSV"  # Añadir (append) una fila a un CSV
# --- FIN DE MODIFICACIÓN ---


class IOWorker:
    """
    Gestiona un hilo dedicado para operaciones de E/S (escritura de archivos)
    para evitar bloquear el bucle de trading principal.
    Utiliza un patrón de productor-consumidor con una única cola.
    """

    def __init__(self):
        self._queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread_started = False
        self.log_extra = {'symbol': 'IO_WORKER', 'transaction_id': 'IO_THREAD'}

    def start(self):
        """Inicia el hilo del worker."""
        if not self._thread_started:
            self._thread.start()
            self._thread_started = True
            logger.info("I/O Worker thread iniciado.", extra=self.log_extra)

    def stop(self):
        """Solicita la detención del hilo y espera a que la cola se vacíe."""
        if not self._thread_started:
            return

        logger.info(f"Deteniendo I/O Worker. Esperando {FLUSH_TIMEOUT_SECONDS}s para vaciar la cola...", extra=self.log_extra)
        try:
            # Enviar señal de detención
            self._queue.put(STOP_SENTINEL, timeout=FLUSH_TIMEOUT_SECONDS)
            # Esperar a que el hilo termine
            self._thread.join(timeout=FLUSH_TIMEOUT_SECONDS + 1)

            if self._thread.is_alive():
                logger.warning("I/O Worker thread no se detuvo a tiempo.", extra=self.log_extra)
        except queue.Full:
            logger.error("I/O Worker queue está llena, no se pudo enviar STOP_SENTINEL.", extra=self.log_extra)
        except Exception as e:
            logger.error(f"Error al detener I/O Worker: {e}", extra=self.log_extra, exc_info=True)

    def add_task(self, task_type: str, payload: dict, transaction_id: str = "N/A"):
        """
        Añade una tarea de escritura a la cola de forma no bloqueante.

        Args:
            task_type (str): Tipo de tarea (e.g., TASK_SAVE_JSON).
            payload (dict): Datos necesarios para la tarea.
            transaction_id (str): ID de la transacción para trazabilidad.
        """
        if not self._thread_started:
            logger.error("I/O Worker no iniciado. Tarea descartada.", extra=self.log_extra)
            return

        task = {
            "type": task_type,
            "payload": payload,
            "transaction_id": transaction_id,
            "timestamp": time.time()
        }

        try:
            self._queue.put_nowait(task)
        except queue.Full:
            logger.warning(f"I/O Worker queue está llena. Tarea {task_type} (ID: {transaction_id}) descartada.",
                           extra={'symbol': 'IO_QUEUE_FULL', 'transaction_id': transaction_id})

    def _run(self):
        """Bucle principal del hilo. Espera tareas y las procesa."""
        while True:
            try:
                task = self._queue.get()

                if task is STOP_SENTINEL:
                    logger.info("STOP_SENTINEL recibido. Vaciando cola y terminando.", extra=self.log_extra)
                    # Procesar tareas restantes antes de salir
                    while not self._queue.empty():
                        task = self._queue.get_nowait()
                        if task is STOP_SENTINEL: continue
                        self._process_task(task)
                    break  # Salir del bucle while True

                self._process_task(task)

            except queue.Empty:
                continue
            except Exception as e:
                logger.critical(f"Error crítico en el hilo I/O Worker: {e}", extra=self.log_extra, exc_info=True)
                time.sleep(1)

        logger.info("I/O Worker thread detenido limpiamente.", extra=self.log_extra)

    def _process_task(self, task: dict):
        """Enruta la tarea al handler de escritura correspondiente."""
        task_type = task.get("type")
        task_id = task.get("transaction_id", "N/A")

        wait_time = time.time() - task.get("timestamp", time.time())
        if wait_time > 1.0:
            logger.warning(f"Alta latencia de cola en I/O Worker: {wait_time:.2f}s", extra={'transaction_id': task_id})

        log_extra = {'transaction_id': task_id, 'symbol': 'IO_PROCESS'}

        try:
            if task_type == TASK_SAVE_JSON:
                self._handle_save_json(task['payload'], log_extra)
            elif task_type == TASK_APPEND_JSON_LIST:
                self._handle_append_json_list(task['payload'], log_extra)
            # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
            elif task_type == TASK_APPEND_CSV:
                self._handle_append_csv(task['payload'], log_extra)
            # --- FIN DE MODIFICACIÓN ---
            elif task_type == TASK_SAVE_EXCEL:
                self._handle_save_excel(task['payload'], log_extra)
            else:
                logger.error(f"Tipo de tarea I/O desconocida: {task_type}", extra=log_extra)
        except Exception as e:
            logger.error(f"Error al procesar tarea I/O ({task_type}): {e}", extra=log_extra, exc_info=True)
        finally:
            self._queue.task_done()

    # --- Handlers de Escritura Específicos ---

    def _handle_save_json(self, payload: dict, log_extra: dict):
        """
        Escribe datos JSON de forma atómica (SOBRESCRIBIR).
        Payload esperado: {'path_key': str, 'data': dict}
        """
        path_key = payload.get("path_key")
        data = payload.get("data")

        if not path_key or not isinstance(data, (dict, list)):
            logger.error(f"Payload JSON inválido. Key: {path_key}, Tipo data: {type(data)}", extra=log_extra)
            return

        try:
            filepath = path_manager.get_path(path_key)
            lock_path = filepath + ".lock"

            with FileLock(lock_path, timeout=10):
                temp_filepath = filepath + ".tmp"
                with open(temp_filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4, default=str)
                os.replace(temp_filepath, filepath)

            logger.debug(f"JSON (overwrite) guardado exitosamente en {filepath}", extra=log_extra)

        except Timeout:
            logger.error(f"Timeout al intentar bloquear {lock_path} para guardado JSON.", extra=log_extra)
        except Exception as e:
            logger.error(f"Error al guardar JSON en {path_key}: {e}", extra=log_extra, exc_info=True)

    def _handle_append_json_list(self, payload: dict, log_extra: dict):
        """
        Añade un item a una lista en un archivo JSON (Lectura-Modificación-Escritura atómica).
        Payload esperado: {'path_key': str, 'item_data': dict_or_list_item}
        """
        path_key = payload.get("path_key")
        item_data = payload.get("item_data")

        if not path_key or item_data is None:
            logger.error(f"Payload JSON Append inválido. Key: {path_key}", extra=log_extra)
            return

        try:
            filepath = path_manager.get_path(path_key)
            lock_path = filepath + ".lock"

            with FileLock(lock_path, timeout=10):
                # 1. Leer (Read)
                current_data = []
                if os.path.exists(filepath):
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            current_data = json.load(f)
                        if not isinstance(current_data, list):
                            logger.warning(f"Archivo JSON en {filepath} no es una lista. Se sobrescribirá.", extra=log_extra)
                            current_data = []
                    except json.JSONDecodeError:
                        logger.warning(f"Archivo JSON en {filepath} corrupto. Se sobrescribirá.", extra=log_extra)
                        current_data = []

                # 2. Modificar (Modify)
                if isinstance(item_data, list):
                    current_data.extend(item_data)
                else:
                    current_data.append(item_data)

                # 3. Escribir (Write)
                temp_filepath = filepath + ".tmp"
                with open(temp_filepath, 'w', encoding='utf-8') as f:
                    json.dump(current_data, f, indent=4, default=str)
                os.replace(temp_filepath, filepath)

            logger.debug(f"JSON (append) guardado exitosamente en {filepath}", extra=log_extra)

        except Timeout:
            logger.error(f"Timeout al intentar bloquear {lock_path} para guardado JSON (Append).", extra=log_extra)
        except Exception as e:
            logger.error(f"Error al guardar JSON (Append) en {path_key}: {e}", extra=log_extra, exc_info=True)

    # --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
    def _handle_append_csv(self, payload: dict, log_extra: dict):
        """
        Añade una fila de datos a un archivo CSV.
        Maneja la creación del header si el archivo no existe.
        Payload esperado: {'path_key': str, 'data_row': dict}
        """
        path_key = payload.get("path_key")
        data_row = payload.get("data_row") # Debe ser un dict

        if not path_key or not isinstance(data_row, dict):
            logger.error(f"Payload CSV Append inválido. Key: {path_key}, Tipo data: {type(data_row)}", extra=log_extra)
            return

        try:
            filepath = path_manager.get_path(path_key)
            lock_path = filepath + ".lock"
            
            with FileLock(lock_path, timeout=10):
                
                # 1. Verificar si el header es necesario
                file_exists = os.path.exists(filepath)
                
                # 2. Convertir el dict a un DataFrame de una fila
                df_row = pd.DataFrame([data_row])
                
                # 3. Escribir (Append)
                df_row.to_csv(
                    filepath,
                    mode='a', # 'a' para append
                    header=not file_exists, # Escribir header solo si el archivo no existe
                    index=False,
                    encoding='utf-8'
                )
            
            logger.debug(f"CSV (append) guardado exitosamente en {filepath}", extra=log_extra)

        except Timeout:
            logger.error(f"Timeout al intentar bloquear {lock_path} para guardado CSV (Append).", extra=log_extra)
        except Exception as e:
            logger.error(f"Error al guardar CSV (Append) en {path_key}: {e}", extra=log_extra, exc_info=True)
    # --- FIN DE MODIFICACIÓN ---

    def _handle_save_excel(self, payload: dict, log_extra: dict):
        """
        Escribe/Actualiza un archivo Excel (trades.xlsx).
        Payload esperado: {'path_key': str, 'trade_data': dict}
        """
        path_key = payload.get("path_key")
        trade_data = payload.get("trade_data")

        if not path_key or not isinstance(trade_data, dict):
            logger.error(f"Payload Excel inválido. Key: {path_key}, Tipo data: {type(trade_data)}", extra=log_extra)
            return

        try:
            filepath = path_manager.get_path(path_key)
            lock_path = filepath + ".lock"

            new_trade_df = pd.DataFrame([trade_data])

            with FileLock(lock_path, timeout=10):
                if not os.path.exists(filepath):
                    existing_df = pd.DataFrame(columns=new_trade_df.columns)
                else:
                    try:
                        existing_df = pd.read_excel(filepath, sheet_name="Trading_Log")
                    except Exception as e_read:
                        logger.error(f"Error al leer Excel existente '{filepath}', se sobrescribirá: {e_read}", extra=log_extra)
                        existing_df = pd.DataFrame(columns=new_trade_df.columns)

                combined_df = pd.concat([existing_df, new_trade_df], ignore_index=True)

                for col in ['open_timestamp', 'close_timestamp']:
                    if col in combined_df.columns:
                        combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce')

                combined_df.to_excel(
                    filepath,
                    sheet_name="Trading_Log",
                    index=False,
                    header=True,
                    engine='openpyxl'
                )

            logger.info(f"Trade {log_extra.get('transaction_id')} registrado en {filepath}", extra=log_extra)

        except Timeout:
            logger.error(f"Timeout al intentar bloquear {lock_path} para guardado Excel.", extra=log_extra)
        except Exception as e:
            logger.error(f"Error al guardar Excel en {path_key}: {e}", extra=log_extra, exc_info=True)


# --- Instancia Global ---
io_worker = IOWorker()