#!/usr/bin/env python3
"""
bdns_etl/etl_scripts/run_etl.py

Orquestador ETL completo para concesiones BDNS (versión JSONL).
Reemplaza al antiguo orquestador CSV/JSON.

Flujo:
1. EXTRACT: 4 endpoints API → 4 JSONL (concesiones, minimis, ayudas_estado, partidos_politicos)
2. TRANSFORM: Une 4 JSONL, resuelve FKs, aplica prioridad de enriquecimiento → JSONL unificado
3. LOAD: COPY nativo PostgreSQL con INSERT/UPDATE

Uso:
    python ./run_etl.py --year 2024
    python ./run_etl.py --year 2024 --skip-extract
    python ./run_etl.py --year 2024 --skip-extract --skip-transform
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from uuid import UUID

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEEDING_DIR = PROJECT_ROOT / "seeding"


def update_execution_status(execution_id: str, phase: str, progress: int, message: str = ""):
    """Actualiza estado en BD para que el backend pueda leerlo."""
    try:
        from bdns_core.db.session import get_session
        from bdns_core.db.etl_models import EtlExecution
        
        with get_session() as session:
            execution = session.get(EtlExecution, UUID(execution_id))
            if execution:
                execution.current_phase = phase
                execution.progress_percentage = progress
                execution.current_operation = message
                session.commit()
                logger.info(f"[{phase}] {progress}% - {message}")
    except Exception as e:
        logger.error(f"Error actualizando estado: {e}")


def run_extract(year: int, execution_id: str) -> bool:
    """Ejecuta los 4 extractores."""
    update_execution_status(execution_id, "extract", 0, "Iniciando extracciones")
    
    extractors = [
        ("concesiones", SEEDING_DIR / "concesiones" / "extract" / "extract_concesiones.py"),
        ("minimis", SEEDING_DIR / "minimis" / "extract_minimis.py"),
        ("ayudas_estado", SEEDING_DIR / "ayudas_estado" / "extract_ayudas_estado.py"),
        ("partidos_politicos", SEEDING_DIR / "partidos_politicos" / "extract_partidos_politicos.py"),
    ]
    
    for i, (name, script) in enumerate(extractors):
        progress = int((i / len(extractors)) * 100)
        update_execution_status(execution_id, "extract", progress, f"Extrayendo {name}...")
        
        if not script.exists():
            logger.error(f"Script no encontrado: {script}")
            return False
        
        result = subprocess.run(
            [sys.executable, str(script), "--year", str(year)],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"ERROR en {name}: {result.stderr}")
            return False
        
        logger.info(f"✓ {name} completado")
    
    update_execution_status(execution_id, "extract", 100, "Extracciones completadas")
    return True


def run_transform(year: int, execution_id: str) -> bool:
    """Ejecuta transformación unificada."""
    update_execution_status(execution_id, "transform", 0, "Iniciando transformación")
    
    script = SEEDING_DIR / "concesiones" / "transform" / "transform_concesiones.py"
    
    result = subprocess.run(
        [sys.executable, str(script), "--year", str(year)],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error(f"ERROR en transform: {result.stderr}")
        return False
    
    update_execution_status(execution_id, "transform", 100, "Transformación completada")
    return True


def run_load(year: int, execution_id: str) -> bool:
    """Ejecuta carga de beneficiarios y concesiones."""
    update_execution_status(execution_id, "load", 0, "Iniciando carga")
    
    # 1. Cargar beneficiarios primero
    update_execution_status(execution_id, "load", 10, "Cargando beneficiarios...")
    
    beneficiarios_jsonl = SEEDING_DIR / "concesiones" / "data" / "jsonl" / "transformed" / f"beneficiarios_pendientes_{year}.jsonl"
    
    if beneficiarios_jsonl.exists():
        script = SEEDING_DIR / "beneficiarios" / "load" / "load_beneficiarios_jsonl.py"
        result = subprocess.run(
            [sys.executable, str(script), "--jsonl", str(beneficiarios_jsonl)],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            logger.error(f"ERROR cargando beneficiarios: {result.stderr}")
            # No es fatal, podemos continuar
    
    # 2. Cargar concesiones
    update_execution_status(execution_id, "load", 50, "Cargando concesiones...")
    
    concesiones_jsonl = SEEDING_DIR / "concesiones" / "data" / "jsonl" / "transformed" / f"concesiones_{year}.jsonl"
    
    script = SEEDING_DIR / "concesiones" / "load" / "load_concesiones_jsonl.py"
    result = subprocess.run(
        [sys.executable, str(script), "--jsonl", str(concesiones_jsonl)],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error(f"ERROR cargando concesiones: {result.stderr}")
        return False
    
    update_execution_status(execution_id, "load", 100, "Carga completada")
    return True


def main():
    parser = argparse.ArgumentParser(description="Orquestador ETL JSONL para concesiones BDNS")
    parser.add_argument("--year", type=int, required=True, help="Año a procesar")
    parser.add_argument("--execution-id", type=str, required=True, help="ID de ejecución en BD")
    parser.add_argument("--skip-extract", action="store_true", help="Saltar extracción")
    parser.add_argument("--skip-transform", action="store_true", help="Saltar transformación")
    
    args = parser.parse_args()
    
    logger.info(f"=== ETL JSONL Concesiones {args.year} ===")
    logger.info(f"Execution ID: {args.execution_id}")
    
    start_total = time.time()
    success = True
    
    try:
        # FASE 1: EXTRACT
        if not args.skip_extract:
            if not run_extract(args.year, args.execution_id):
                success = False
        else:
            logger.info("Saltando EXTRACT")
        
        # FASE 2: TRANSFORM
        if success and not args.skip_transform:
            if not run_transform(args.year, args.execution_id):
                success = False
        else:
            logger.info("Saltando TRANSFORM")
        
        # FASE 3: LOAD (siempre, si las anteriores fueron bien o se saltaron)
        if success:
            if not run_load(args.year, args.execution_id):
                success = False
        
        elapsed = time.time() - start_total
        
        if success:
            logger.info(f"=== COMPLETADO en {elapsed:.1f}s ===")
            return 0
        else:
            logger.error(f"=== FALLÓ en {elapsed:.1f}s ===")
            return 1
            
    except Exception as e:
        logger.exception("Error inesperado")
        return 1


if __name__ == "__main__":
    sys.exit(main())