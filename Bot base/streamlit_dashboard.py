# streamlit_dashboard.py
"""
Módulo: streamlit_dashboard.py
Descripción:
    Dashboard de Monitoreo en Tiempo Real

Componentes:
- Métricas PnL: Real vs Simulado
- Gráfico de performance comparativa
- Estado de optimización Optuna
- Parámetros actuales optimizados
- Listado de trades recientes

Fuentes de datos:
- Trades reales: Excel (trades.xlsx)
- Trades simulados: Redis LIST
- Parámetros Optuna: bot_settings.json
- Configuración: config.py

Actualización:
- Auto-refresh cada 30 segundos
- Cache de datos por 10 segundos
- Conexión Redis con fallback
"""
import streamlit as st
import redis
import json
import pandas as pd
import plotly.graph_objects as go
# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
from unified_logger import get_logger
# --- FIN DE MODIFICACIÓN ---
import time
import os  # Importar OS para leer config
import config  # Importar config para leer ajustes

# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
from redis_manager import redis_manager
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: (Fase 4.0) ---
import path_manager  # Importar el gestor de rutas
# --- FIN DE MODIFICACIÓN ---

# --- INICIO DE MODIFICACIÓN: (Paso 3) ---
logger = get_logger(__name__)
# --- FIN DE MODIFICACIÓN ---


# --- INICIO DE MODIFICACIÓN: (Paso 4) ---
# Lógica de conexión Docker-aware (Paso 4)
def connect_to_redis():
    """Función simplificada usando RedisManager"""
    return redis_manager.get_connection()


r = connect_to_redis()  # Reemplazar la conexión simple
# --- FIN DE MODIFICACIÓN ---

# Configuración de la página
st.set_page_config(layout="wide", page_title="📈 Bot Vela 1m")


# Título del Dashboard
# --- MODIFICACIÓN (Fase 5): Lee v2.9 de config ---
st.title(f"📈 {config.get_param('GLOBAL_SETTINGS.BOT_NAME', 'Bot Vela 1m')}")  # Título dinámico

# --- Contenedores de Métricas (Placeholders globales) ---
col1, col2 = st.columns(2)
pnl_real_placeholder = col1.empty()
pnl_shadow_placeholder = col2.empty()
plot_placeholder = st.empty()  # Placeholder para el gráfico


@st.cache_data(ttl=10)  # Cachear la carga de datos por 10s
def load_data():
    """
    Carga los trades simulados (shadow) y los trades reales desde Redis y Excel.
    """
    log_extra = {'symbol': 'DASH_LOAD'}

    # --- INICIO DE MODIFICACIÓN: (Fase 4.0) Usar path_manager ---
    # Obtener rutas centralizadas
    try:
        CONFIG_FILE_PATH = path_manager.get_path("bot_settings")
        excel_file_path = path_manager.get_path("trades_excel")
    except (KeyError, IOError) as e:
        logger.error(f"Error crítico al obtener rutas de path_manager: {e}", extra=log_extra)
        st.error(f"Error fatal al cargar rutas de path_manager: {e}")
        return pd.DataFrame(), pd.DataFrame(), {}
        
    SHADOW_TRADES_LIST = redis_manager.get_config_value("SHADOW_TRADES_LIST", "shadow_trades_1m")
    # --- FIN DE MODIFICACIÓN ---

    # Cargar trades simulados desde la LISTA ESPECÍFICA
    shadow_df = pd.DataFrame()
    if r:
        try:
            shadow_trades_json = r.lrange(SHADOW_TRADES_LIST, 0, 500)
            shadow_trades = [json.loads(t) for t in shadow_trades_json]
            shadow_df = pd.DataFrame(shadow_trades)
        except Exception as e_redis:
            logger.error(f"Error cargando trades shadow desde Redis: {e_redis}", extra=log_extra)
    else:
        logger.warning("No hay conexión a Redis, no se cargarán trades shadow.", extra=log_extra)

    # --- INICIO DE LA MODIFICACIÓN: Cargar trades reales desde Excel ---
    real_df = pd.DataFrame()  # Init empty
    try:
        # La ruta (excel_file_path) ahora viene de path_manager
        if os.path.exists(excel_file_path):
            real_df = pd.read_excel(excel_file_path)
            logger.info(f"Cargados {len(real_df)} trades reales desde {excel_file_path}", extra=log_extra)
        else:
            logger.warning(f"Archivo de trades reales no encontrado en: {excel_file_path}", extra=log_extra)
    except Exception as e:
        logger.error(f"Error al cargar trades reales desde Excel: {e}", extra=log_extra)
    # --- FIN DE LA MODIFICACIÓN ---

    # Cargar última actualización de Optuna (Directamente de bot_settings.json)
    optuna_info = {}
    try:
        # La ruta (CONFIG_FILE_PATH) ahora viene de path_manager
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            cfg_data = json.load(f)
        optuna_data = cfg_data.get("SHADOW_OPTUNA", {})
        if "LAST_BEST" in optuna_data:
            optuna_info = {
                "params": optuna_data.get("LAST_BEST"),
                "sharpe": optuna_data.get("LAST_BEST_SHARPE"),
                "timestamp": optuna_data.get("LAST_OPTIMIZATION_UTC")
            }
    except Exception as e:
        logger.error(f"No se pudo leer {CONFIG_FILE_PATH} para datos de Optuna: {e}", extra=log_extra)

    return shadow_df, real_df, optuna_info


# --- INICIO DE LA MODIFICACIÓN: Lógica de PnL separada y correcta ---
def calculate_shadow_pnl(df):
    """
    Calcula el PnL acumulado para el dataframe de shadow trading.
    Replica la lógica de PnL de optuna_worker.py
    """
    if df.empty or len(df) < 2:
        df['pnl'] = 0.0
        df['cumulative_pnl'] = 0.0
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.NaT
        return df

    # 1. Convertir (copiado de optuna_worker.py)
    try:
        df['simulated_price'] = pd.to_numeric(df['simulated_price'], errors='coerce')
        df['original_price'] = pd.to_numeric(df['original_price'], errors='coerce')
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['commission'] = pd.to_numeric(df['commission'], errors='coerce')
        df = df.fillna(0)

        # shadow_engine usa time.time() (timestamp UNIX), lrange los devuelve del más nuevo al más viejo
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df = df.sort_values(by='timestamp', ascending=True)  # Ordenar cronológicamente
    except Exception as e:
        logger.error(f"Error convirtiendo tipos en PnL shadow: {e}", extra={'symbol': 'DASH_PNL_SHADOW'})
        df['pnl'] = 0.0
        df['cumulative_pnl'] = 0.0
        return df

    # 2. Calcular PnL (copiado de optuna_worker.py)
    df['exit_price'] = df['original_price'].shift(-1)
    df['exit_commission'] = df['commission'].shift(-1)
    df['pnl'] = 0.0  # 'pnl' en lugar de 'ret'

    is_entry = (df['side'] == 'LONG') | (df['side'] == 'BUY')

    # Calcular PnL neto para los trades de ENTRADA
    df.loc[is_entry, 'pnl'] = (
        (df['exit_price'] - df['simulated_price']) * df['quantity']
    ) - (df['commission'] + df['exit_commission'])

    df['pnl'] = df['pnl'].fillna(0)  # Llenar el PnL de la última fila (que es NaN)
    df['cumulative_pnl'] = df['pnl'].cumsum()
    return df


def process_real_pnl(df):
    """
    Procesa el PnL acumulado para el dataframe de trades reales (leído de Excel).
    """
    if df.empty:
        df['pnl'] = 0.0
        df['cumulative_pnl'] = 0.0
        df['timestamp'] = pd.NaT
        return df

    # El Excel ya tiene PnL y timestamps
    df['timestamp'] = pd.to_datetime(df['close_timestamp'])  # Usar 'close_timestamp'
    df['pnl'] = pd.to_numeric(df['net_profit_quote'], errors='coerce').fillna(0)
    df = df.sort_values(by='timestamp')
    df['cumulative_pnl'] = df['pnl'].cumsum()
    return df
# --- FIN DE LA MODIFICACIÓN ---


def plot_pnl(placeholder, shadow_df, real_df):
    """
    Dibuja el gráfico de PnL acumulado.
    """
    fig = go.Figure()

    if not shadow_df.empty and 'cumulative_pnl' in shadow_df.columns:
        fig.add_trace(go.Scatter(
            x=shadow_df['timestamp'],
            y=shadow_df['cumulative_pnl'],
            mode='lines+markers',
            name='PnL Simulado (Shadow - Lógica Optuna)'
        ))

    if not real_df.empty and 'cumulative_pnl' in real_df.columns:
        fig.add_trace(go.Scatter(
            x=real_df['timestamp'],
            y=real_df['cumulative_pnl'],
            mode='lines+markers',
            name='PnL Real (Desde Excel)'
        ))

    fig.update_layout(
        title="PnL Acumulado: Real vs. Simulado",
        xaxis_title="Tiempo",
        yaxis_title="PnL (USDT)",
        hovermode="x unified"
    )
    placeholder.plotly_chart(fig, use_container_width=True)


# --- Bucle Principal del Script ---
try:
    # --- INICIO DE MODIFICACIÓN: Comprobar conexión Redis ---
    if r is None:
        st.error(f"Error de conexión: No se puede conectar a Redis. ¿El servicio 'redis' está funcionando?")
    else:
        # --- FIN DE MODIFICACIÓN ---
        # Cargar datos
        shadow_df, real_df, optuna_info = load_data()

        # --- MODIFICACIÓN: Llamar a las funciones correctas ---
        shadow_df = calculate_shadow_pnl(shadow_df)
        real_df = process_real_pnl(real_df)
        # --- FIN DE MODIFICACIÓN ---

        # Actualizar Métricas
        pnl_real = real_df['pnl'].sum() if not real_df.empty else 0
        pnl_shadow = shadow_df['pnl'].sum() if not shadow_df.empty else 0

        pnl_real_placeholder.metric("PnL Real Total", f"{pnl_real:.2f} USDT")
        pnl_shadow_placeholder.metric("PnL Simulado Total (Lógica Optuna)", f"{pnl_shadow:.2f} USDT")

        # Mostrar Métricas de Optuna en el sidebar
        with st.sidebar:
            st.subheader("Estado de Optimización (Optuna)")
            if optuna_info and optuna_info.get("timestamp"):
                try:
                    last_update_time = pd.to_datetime(optuna_info.get('timestamp'))
                    st.metric("Última Optimización", f"{last_update_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    st.metric("Mejor Sharpe (Simulado)", f"{optuna_info.get('sharpe', -999):.4f}")
                except Exception as e:
                    logger.warning(f"Error parseando timestamp de Optuna: {e}", extra={'symbol': 'DASH_OPTUNA'})
                    st.metric("Última Optimización", "Error de fecha")
            else:
                st.metric("Última Optimización", "Pendiente...")

            st.subheader("Parámetros Optimizados")
            st.json(optuna_info.get('params', {}))

            st.subheader("Últimos Trades Simulados")
            if not shadow_df.empty:
                # Mostrar los 10 más recientes (ordenados por timestamp)
                st.dataframe(shadow_df.sort_values(by='timestamp', ascending=False).head(10))
            else:
                st.write("Aún no hay trades simulados.")

        # Actualizar Gráfico
        plot_pnl(plot_placeholder, shadow_df, real_df)

except redis.exceptions.ConnectionError:
    st.error(f"Error de conexión: No se puede conectar a Redis. ¿El servicio 'redis' está funcionando?")
except Exception as e:
    log_extra_main = {'symbol': 'DASH_MAIN'}
    logger.error(f"Error en el dashboard: {e}", exc_info=True, extra=log_extra_main)
    st.error(f"Error: {e}")

# ⚠️ NOTA: En Streamlit, NO se debe usar time.sleep() + st.rerun() al final.
# Streamlit se re-ejecuta automáticamente gracias al @st.cache_data(ttl=10).
# Eliminamos estas líneas para evitar bloqueos y comportamientos inesperados.