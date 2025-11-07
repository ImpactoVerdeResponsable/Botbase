import os
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---

# La misma ruta exacta que tienes en tu bot_settings.json
directorio_logs = r"C:\Users\usuario\Desktop\DIEGO\BOTS\VELAS\Bot Vela 1m Multipar\Log TXT"
ruta_archivo_test = os.path.join(directorio_logs, "test.txt")

try:
    log_extra = {'symbol': 'PERMISSIONS_TEST'}
    logger.info(f"Intentando escribir en: {ruta_archivo_test}", extra=log_extra)
    os.makedirs(directorio_logs, exist_ok=True)
    with open(ruta_archivo_test, 'w') as f:
        f.write("Prueba de escritura exitosa.")
    logger.info("✅ ¡Éxito! El archivo de prueba fue creado correctamente.", extra=log_extra)
    
except PermissionError:
    logger.error("❌ ERROR DE PERMISOS: El script no tiene permiso para escribir en esta carpeta.", extra=log_extra)
except Exception as e:
    logger.error(f"❌ Ocurrió un error inesperado: {e}", exc_info=True, extra=log_extra)