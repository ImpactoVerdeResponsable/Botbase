# --- Etapa 1: Builder ---
FROM python:3.11 AS builder
WORKDIR /app

# Actualizar pip y herramientas de instalación
RUN pip install --upgrade pip setuptools wheel

# Instalar pandas-ta desde fuente local
COPY pandas-ta/ ./pandas-ta/
RUN pip install --no-cache-dir ./pandas-ta

# Copiar requirements e instalar dependencias (sin pandas-ta)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Etapa 2: Runtime ---
FROM python:3.11-slim
WORKDIR /app

# Copiar paquetes instalados desde builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copiar archivos del proyecto
COPY requirements.txt .
COPY bot_settings.json .
COPY *.py .
COPY strategies/ ./strategies/
