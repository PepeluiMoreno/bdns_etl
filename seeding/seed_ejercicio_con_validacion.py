#!/usr/bin/env python3
"""
Script de ejemplo: Seeding de ejercicio con validación automática de catálogos.

Este script demuestra cómo integrar la validación y sincronización automática
de catálogos ANTES de procesar datos de un ejercicio específico.

La lógica es simple:
1. Validar si catálogos están obsoletos para el ejercicio solicitado
2. Si están obsoletos, sincronizarlos automáticamente desde API BDNS
3. Proceder con el seeding de convocatorias/concesiones

Uso:
    python seed_ejercicio_con_validacion.py --year 2025
    python seed_ejercicio_con_validacion.py --year 2024 --skip-catalog-validation
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Añadir sync/ al path para imports
sync_path = Path(__file__).resolve().parent.parent / "sync"
if str(sync_path) not in sys.path:
    sys.path.insert(0, str(sync_path))

from bdns_core.db.session import SessionLocal
from catalog_sync_validator import validar_y_sincronizar_catalogos

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def seed_convocatorias_ejercicio(session, year: int):
    """
    Placeholder: Aquí iría la lógica real de seeding de convocatorias.

    En la implementación real, esto llamaría a:
    - extract_convocatorias(year)
    - transform_convocatorias(year)
    - load_convocatorias(year)
    """
    logger.info(f"📥 Iniciando seeding de convocatorias para ejercicio {year}...")
    # TODO: Implementar lógica real de seeding
    logger.info(f"✅ Seeding de convocatorias {year} completado")


def seed_concesiones_ejercicio(session, year: int):
    """
    Placeholder: Aquí iría la lógica real de seeding de concesiones.

    En la implementación real, esto llamaría a:
    - orchestrate_all_concesiones(year)
    - transform_concesiones(year)
    - load_concesiones(year)
    """
    logger.info(f"📥 Iniciando seeding de concesiones para ejercicio {year}...")
    # TODO: Implementar lógica real de seeding
    logger.info(f"✅ Seeding de concesiones {year} completado")


def seed_ejercicio(year: int, skip_catalog_validation: bool = False):
    """
    Ejecuta seeding completo para un ejercicio, con validación de catálogos.

    Args:
        year: Año del ejercicio a procesar
        skip_catalog_validation: Si es True, omite validación de catálogos (no recomendado)

    Returns:
        0 si éxito, 1 si error
    """
    logger.info("=" * 80)
    logger.info(f"SEEDING DE EJERCICIO {year}")
    logger.info("=" * 80)

    inicio = datetime.now()

    with SessionLocal() as session:
        try:
            # ✅ PASO 1: VALIDAR Y SINCRONIZAR CATÁLOGOS
            if not skip_catalog_validation:
                logger.info("\n🔍 Paso 1: Validación de catálogos")
                logger.info("-" * 80)

                validar_y_sincronizar_catalogos(session, year)

                logger.info("✅ Catálogos validados y actualizados")
            else:
                logger.warning(
                    "⚠️  Validación de catálogos omitida (--skip-catalog-validation). "
                    "Esto puede causar errores de integridad referencial."
                )

            # ✅ PASO 2: SEEDING DE CONVOCATORIAS
            logger.info("\n📋 Paso 2: Seeding de convocatorias")
            logger.info("-" * 80)

            seed_convocatorias_ejercicio(session, year)

            # ✅ PASO 3: SEEDING DE CONCESIONES
            logger.info("\n💰 Paso 3: Seeding de concesiones")
            logger.info("-" * 80)

            seed_concesiones_ejercicio(session, year)

            # Completado
            fin = datetime.now()
            duracion = (fin - inicio).total_seconds()

            logger.info("\n" + "=" * 80)
            logger.info(f"✅ SEEDING DE EJERCICIO {year} COMPLETADO")
            logger.info(f"Duración total: {duracion:.2f} segundos")
            logger.info("=" * 80)

            return 0

        except Exception as e:
            logger.error("\n" + "=" * 80)
            logger.error(f"❌ ERROR EN SEEDING DE EJERCICIO {year}")
            logger.error(f"Error: {e}")
            logger.error("=" * 80)
            return 1


def main():
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description="Seeding de ejercicio con validación automática de catálogos"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Año del ejercicio a procesar (ej: 2025)"
    )
    parser.add_argument(
        "--skip-catalog-validation",
        action="store_true",
        help="Omitir validación de catálogos (no recomendado)"
    )

    args = parser.parse_args()

    # Ejecutar seeding
    exit_code = seed_ejercicio(args.year, args.skip_catalog_validation)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
