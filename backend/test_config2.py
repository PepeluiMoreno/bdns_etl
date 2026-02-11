#!/usr/bin/env python
"""Script para diagnosticar problemas de carga de configuración - v2."""
import os
import sys
from pathlib import Path

# Configurar PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configurar variables de entorno ANTES de importar
os.environ["ENVIRONMENT"] = "development"
os.chdir(Path(__file__).parent)

print(f"Working directory: {os.getcwd()}")
print(f".env.development exists: {Path('.env.development').exists()}")
print(f"ENVIRONMENT: {os.getenv('ENVIRONMENT')}")
print()

# Cargar configuración SIN cache
from bdns_core.config.etl import ETLSettings

# Crear instancia DIRECTA sin cache
settings = ETLSettings()

print(f"Loaded settings (no cache):")
print(f"  PORT: {settings.PORT}")
print(f"  HOST: {settings.HOST}")
print(f"  DEBUG: {settings.DEBUG}")
print(f"  DATABASE_URL: {settings.DATABASE_URL}")
print(f"  ENVIRONMENT: {settings.ENVIRONMENT}")
