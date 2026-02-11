#!/usr/bin/env python
"""Script para diagnosticar problemas de carga de configuración."""
import os
import sys
from pathlib import Path

# Configurar PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configurar variables de entorno
os.environ["ENVIRONMENT"] = "development"
os.chdir(Path(__file__).parent)

print(f"Working directory: {os.getcwd()}")
print(f".env.development exists: {Path('.env.development').exists()}")
print(f"ENVIRONMENT: {os.getenv('ENVIRONMENT')}")
print()

# Cargar configuración
from bdns_core.config import get_etl_settings

settings = get_etl_settings()

print(f"Loaded settings:")
print(f"  PORT: {settings.PORT}")
print(f"  HOST: {settings.HOST}")
print(f"  DEBUG: {settings.DEBUG}")
print(f"  DATABASE_URL: {settings.DATABASE_URL}")
print(f"  ENVIRONMENT: {settings.ENVIRONMENT}")
