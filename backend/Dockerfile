FROM python:3.13-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Actualizar pip y instalar setuptools
RUN pip install --upgrade pip setuptools wheel

# Copiar archivos de configuración
COPY pyproject.toml ./
COPY main.py ./
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh

# Copiar código fuente
COPY src/ ./src/

# Instalar dependencias Python (sin bdns-core por ahora)
# Primero instalamos las dependencias principales
RUN pip install --no-cache-dir \
    fastapi>=0.115.0 \
    uvicorn[standard]>=0.34.0 \
    pydantic>=2.10.0 \
    pydantic-settings>=2.7.0 \
    sqlalchemy>=2.0.36 \
    psycopg2-binary>=2.9.10 \
    python-jose[cryptography]>=3.3.0 \
    passlib[bcrypt]>=1.7.4 \
    bcrypt==4.0.1 \
    python-multipart>=0.0.17

# Agregar src al PYTHONPATH para imports
ENV PYTHONPATH=/app/src:$PYTHONPATH

# Instalar bdns_core (se instalará desde el volumen montado al iniciar)
# El volumen se monta en /app/bdns_core_package
RUN echo "bdns_core se instalará desde volumen montado"

# Exponer puerto
EXPOSE 8001

# Usar entrypoint para instalar bdns_core antes de iniciar
ENTRYPOINT ["/app/entrypoint.sh"]

# Comando por defecto
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
