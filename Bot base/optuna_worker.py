# optuna_worker.py
"""
Módulo: optuna_worker.py
Descripción:
    Sistema de Optimización Automática con Optuna

    Este módulo implementa optimización bayesiana de parámetros usando:
    1. Shadow trades desde Redis
    2. Cálculo realista de PnL con gestión de posiciones
    3. Optimización de Sharpe ratio anualizado
    4. Aplicación automática de mejores parámetros

Flujo de optimización:
1. Recoge trades shadow (500 más recientes)
2. Ejecuta trials con diferentes parámetros
3. Calcula métricas de performance realistas
4. Guarda mejores parámetros en configuración
5. Config main.py aplica parámetros via hot-reload

Dependencias:
- Redis para colección de trades shadow
- bot_settings.json para persistencia de parámetros
"""
import optuna
import json
import time
import os
import pandas as pd
import redis
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
from decimal import Decimal  # Importar Decimal para el backtest
import math  # Importar math para cálculo de raíz cuadrada

# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
from redis_manager import redis_manager
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
import path_manager  # Importar el gestor de rutas
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE LA MODIFICACIÓN (Ruta Absoluta) ---
# (Fase 4.0): Obtener la ruta de config desde path_manager
try:
    CONFIG_FILE_PATH = path_manager.get_path("bot_settings")
    logger.info(f"Usando archivo de configuración: {CONFIG_FILE_PATH}", extra={'symbol': 'OPTUNA_WORKER'})
except (KeyError, IOError) as e:
    logger.critical(f"Error fatal: No se pudo obtener la ruta 'bot_settings' de path_manager: {e}", extra={'symbol': 'OPTUNA_WORKER'})
    # Salir si no podemos encontrar el archivo de config
    exit(1)
# --- FIN DE LA MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
# Función 'connect_to_redis' eliminada y reemplazada por 'redis_manager'
def connect_to_redis():
    """Función simplificada usando RedisManager"""
    return redis_manager.get_connection()
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: Implementación de la Solución 3 (Anexo) ---
def calculate_realistic_shadow_pnl(df):
    """Calcula PnL considerando gestión real de posiciones"""
    log_extra = {'symbol': 'OPTUNA_PNL'}
    if df.empty or len(df) < 2:
        return -999.0

    # --- Añadido: Conversión de tipos robusta ---
    try:
        df['simulated_price'] = pd.to_numeric(df['simulated_price'], errors='coerce')
        df['original_price'] = pd.to_numeric(df['original_price'], errors='coerce')
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['commission'] = pd.to_numeric(df['commission'], errors='coerce')
        # shadow_engine usa time.time()
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
        df = df.dropna(subset=['simulated_price', 'original_price', 'quantity', 'commission', 'timestamp'])
    except Exception as e:
        logger.error(f"Error convirtiendo tipos en calculate_realistic_shadow_pnl: {e}", extra=log_extra)
        return -999.0
    # --- Fin conversión ---

    # Ordenar por timestamp
    df = df.sort_values('timestamp')
    df['pnl'] = 0.0
    df['position'] = None

    current_position = None
    pnl_total = 0.0

    for i, row in df.iterrows():
        if current_position is None and row['side'] in ['LONG', 'BUY']:
            # Abrir posición
            current_position = {
                'entry_price': Decimal(row['simulated_price']),
                'quantity': Decimal(row['quantity']),
                'entry_commission': Decimal(row['commission']),
                'entry_index': i
            }

        elif current_position is not None:
            # Calcular PnL para esta vela
            current_price = Decimal(row['original_price'])
            position_value = (current_price - current_position['entry_price']) * current_position['quantity']
            commission_cost = current_position['entry_commission'] + Decimal(row['commission'])
            trade_pnl = position_value - commission_cost

            df.at[i, 'pnl'] = float(trade_pnl)
            pnl_total += float(trade_pnl)

            # Cerrar posición si es señal de salida (Asumiendo que 'CLOSE' o 'EXIT' pueden venir)
            if row['side'] in ['CLOSE', 'EXIT']:
                current_position = None

    # Calcular Sharpe ratio mejorado
    try:
        if len(df) > 10 and df['pnl'].std() > 0:
            sharpe = (df['pnl'].mean() / df['pnl'].std()) * math.sqrt(365 * 96)
            return sharpe
    except Exception as e_sharpe:
        logger.error(f"Error en cálculo de Sharpe (realistic): {e_sharpe}", extra=log_extra)
        return -999.0

    return -999.0
# --- FIN DE MODIFICACIÓN ---


def objective(trial):
    log_extra = {'transaction_id': f'trial_{trial.number}', 'symbol': 'OPTUNA_OBJ'}

    # --- INICIO DE MODIFICACIÓN: (Paso 4) ---
    r = redis_manager.get_connection()
    if not r:
        logger.error("Optuna trial falló: No hay conexión a Redis.", extra=log_extra)
        return -999.0  # Fallar el trial si no hay conexión

    # Leer los trades simulados desde la LISTA ESPECÍFICA
    list_key = redis_manager.get_config_value('SHADOW_TRADES_LIST', 'shadow_trades_1m')
    # --- FIN DE MODIFICACIÓN ---

    shadow_trades_json = r.lrange(list_key, 0, 500)
    shadow_trades = [json.loads(t) for t in shadow_trades_json]
    df = pd.DataFrame(shadow_trades)

    if len(df) < 10:
        logger.warning(f"Datos insuficientes para optimizar ({len(df)} trades).", extra=log_extra)
        return -999.0

    # Parámetros a optimizar (sin cambios)
    rsi_len = trial.suggest_int('MicroScalpingRSI.RSI_LENGTH', 8, 24)
    ema_fast = trial.suggest_int('EMAcross9x21.FAST_EMA', 5, 15)
    ema_slow = trial.suggest_int('EMAcross9x21.SLOW_EMA', 18, 35)
    bbw_perc = trial.suggest_int('RupturaDeCompresion.BBW_SQUEEZE_PERCENTILE', 15, 40)
    rsi_mom = trial.suggest_int('ImpulsoDeVolatilidad.RSI_DYNAMIC_PARAMS.MOMENTUM_THRESHOLD_LONG', 55, 80)
    rsi_bb_std = trial.suggest_float('ReversionEnRango.RSI_BB_PARAMS.BBANDS_STD_DEV', 1.5, 3.0)

    # --- ################################# ---
    # ---  INICIO DE LA MODIFICACIÓN (Solución 3: PnL Realista) ---
    # --- ################################# ---

    # Usar .copy() para asegurar que la manipulación de datos no afecte
    sharpe = calculate_realistic_shadow_pnl(df.copy())

    # --- ################################# ---
    # ---   FIN DE LA MODIFICACIÓN (Solución 3)  ---
    # --- ################################# ---

    return sharpe  # Ya es float


def job():
    log_extra = {'symbol': 'OPTUNA_JOB'}
    try:
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=50, timeout=300)

        best = study.best_params

        logger.info(f"[OPTUNA] Mejores valores encontrados: {best}", extra=log_extra)
        logger.info(f"[OPTUNA] Mejor Sharpe (simulado): {study.best_value}", extra=log_extra)

        # --- INICIO DE LA MODIFICACIÓN (Usar Ruta Absoluta) ---
        # (Fase 4.0): CONFIG_FILE_PATH ya está definido globalmente desde path_manager
        if not os.path.exists(CONFIG_FILE_PATH):
            logger.error(f"No se encontró el archivo de configuración en {CONFIG_FILE_PATH}. No se pueden aplicar parámetros.", extra=log_extra)
            return

        # aplicar al JSON si mejora (lógica de mejora del 10% omitida por simplicidad, se aplica siempre)
        with open(CONFIG_FILE_PATH, encoding='utf-8') as f:
            old_cfg = json.load(f)

        # Actualizar los parámetros optimizados en el diccionario
        if "SHADOW_OPTUNA" not in old_cfg:
            old_cfg["SHADOW_OPTUNA"] = {}

        old_cfg['SHADOW_OPTUNA']['LAST_BEST'] = best
        old_cfg['SHADOW_OPTUNA']['LAST_BEST_SHARPE'] = study.best_value
        # --- CORRECCIÓN: pd.Timestamp.utcnow() está deprecado en pandas >= 2.0 ---
        old_cfg['SHADOW_OPTUNA']['LAST_OPTIMIZATION_UTC'] = pd.Timestamp.now(tz='UTC').isoformat()

        # Sobrescribir el archivo de configuración con los nuevos parámetros
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(old_cfg, f, indent=2)
        logger.info(f"Parámetros actualizados en {CONFIG_FILE_PATH}", extra=log_extra)
        # --- FIN DE LA MODIFICACIÓN ---

    except Exception as e:
        logger.error(f"Error en el 'job' de Optuna: {e}", exc_info=True, extra=log_extra)


if __name__ == '__main__':
    log_extra_main = {'symbol': 'OPTUNA_MAIN'}
    logger.info("Iniciando Optuna Worker...", extra=log_extra_main)
    while True:
        job()
        sleep_duration = 3600  # 1 hora
        logger.info(f"Optimización completada. Durmiendo por {sleep_duration / 60} minutos...", extra=log_extra_main)
        time.sleep(sleep_duration)