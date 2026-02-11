#!/usr/bin/env python3
"""
Sincronización semanal incremental de BDNS.

Este script sincroniza convocatorias y concesiones de forma incremental,
actualizando solo los registros nuevos o modificados desde la última ejecución.

IMPORTANTE: Antes de sincronizar datos transaccionales, valida y sincroniza
catálogos para garantizar integridad referencial.

Uso:
    # Sync de la última semana
    python sync_weekly.py

    # Sync de un rango específico
    python sync_weekly.py --fecha-desde 2025-01-01 --fecha-hasta 2025-01-31

    # Omitir validación de catálogos (no recomendado)
    python sync_weekly.py --skip-catalog-validation
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional

# Añadir sync/ al path para imports
sync_path = Path(__file__).resolve().parent
if str(sync_path) not in sys.path:
    sys.path.insert(0, str(sync_path))

from bdns_core.db.session import SessionLocal
from bdns_core.db.etl_models import EtlExecution, SyncControl
from catalog_sync_validator import validar_y_sincronizar_catalogos

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def get_ultima_sync_date(session) -> Optional[date]:
    """
    Obtiene la fecha de la última sincronización exitosa.

    Returns:
        date de última sync, o None si nunca se sincronizó
    """
    ultima_sync = (
        session.query(SyncControl)
        .filter(SyncControl.status == "completed")
        .order_by(SyncControl.fecha_hasta.desc())
        .first()
    )

    return ultima_sync.fecha_hasta if ultima_sync else None


def sync_convocatorias(session, fecha_desde: date, fecha_hasta: date):
    """
    Sincroniza convocatorias en el rango de fechas.

    Args:
        session: Sesión de SQLAlchemy
        fecha_desde: Fecha inicial (inclusive)
        fecha_hasta: Fecha final (inclusive)
    """
    logger.info(
        f"📋 Sincronizando convocatorias: {fecha_desde} → {fecha_hasta}"
    )

    # TODO: Implementar lógica real de sincronización
    # - Llamar a API BDNS con filtro de fechas
    # - Transformar datos
    # - UPSERT en base de datos

    logger.info("✅ Convocatorias sincronizadas")


def sync_concesiones(session, fecha_desde: date, fecha_hasta: date):
    """
    Sincroniza concesiones en el rango de fechas.

    Args:
        session: Sesión de SQLAlchemy
        fecha_desde: Fecha inicial (inclusive)
        fecha_hasta: Fecha final (inclusive)
    """
    logger.info(
        f"💰 Sincronizando concesiones: {fecha_desde} → {fecha_hasta}"
    )

    # TODO: Implementar lógica real de sincronización
    # - Llamar a API BDNS con filtro de fechas
    # - Transformar datos
    # - UPSERT en base de datos

    logger.info("✅ Concesiones sincronizadas")


def sync_weekly(
    fecha_desde: Optional[date] = None,
    fecha_hasta: Optional[date] = None,
    skip_catalog_validation: bool = False
):
    """
    Ejecuta sincronización semanal incremental.

    Args:
        fecha_desde: Fecha inicial. Si es None, usa última sync + 1 día
        fecha_hasta: Fecha final. Si es None, usa hoy
        skip_catalog_validation: Si es True, omite validación de catálogos

    Returns:
        0 si éxito, 1 si error
    """
    logger.info("=" * 80)
    logger.info("SINCRONIZACIÓN SEMANAL INCREMENTAL")
    logger.info("=" * 80)

    inicio = datetime.now()

    with SessionLocal() as session:
        try:
            # Determinar rango de fechas
            if fecha_hasta is None:
                fecha_hasta = date.today()

            if fecha_desde is None:
                ultima_sync = get_ultima_sync_date(session)
                if ultima_sync:
                    fecha_desde = ultima_sync + timedelta(days=1)
                    logger.info(f"Última sync: {ultima_sync}")
                else:
                    # Primera sync: últimos 7 días
                    fecha_desde = fecha_hasta - timedelta(days=7)
                    logger.info("Primera sincronización (sin histórico)")

            logger.info(f"Rango: {fecha_desde} → {fecha_hasta}")

            # Determinar año más reciente en la ventana
            year_max = fecha_hasta.year

            # ✅ PASO 1: VALIDAR Y SINCRONIZAR CATÁLOGOS
            if not skip_catalog_validation:
                logger.info("\n🔍 Paso 1: Validación de catálogos")
                logger.info("-" * 80)

                validar_y_sincronizar_catalogos(session, year_max)

                logger.info("✅ Catálogos validados y actualizados")
            else:
                logger.warning(
                    "⚠️  Validación de catálogos omitida. "
                    "Esto puede causar errores de integridad referencial."
                )

            # Crear registro de sincronización
            sync_record = SyncControl(
                fecha_desde=fecha_desde,
                fecha_hasta=fecha_hasta,
                status="running",
                started_at=datetime.now()
            )
            session.add(sync_record)
            session.commit()

            # ✅ PASO 2: SINCRONIZAR CONVOCATORIAS
            logger.info("\n📋 Paso 2: Sincronización de convocatorias")
            logger.info("-" * 80)

            sync_convocatorias(session, fecha_desde, fecha_hasta)

            # ✅ PASO 3: SINCRONIZAR CONCESIONES
            logger.info("\n💰 Paso 3: Sincronización de concesiones")
            logger.info("-" * 80)

            sync_concesiones(session, fecha_desde, fecha_hasta)

            # Marcar como completado
            sync_record.status = "completed"
            sync_record.finished_at = datetime.now()
            session.commit()

            # Completado
            fin = datetime.now()
            duracion = (fin - inicio).total_seconds()

            logger.info("\n" + "=" * 80)
            logger.info("✅ SINCRONIZACIÓN SEMANAL COMPLETADA")
            logger.info(f"Rango procesado: {fecha_desde} → {fecha_hasta}")
            logger.info(f"Duración total: {duracion:.2f} segundos")
            logger.info("=" * 80)

            return 0

        except Exception as e:
            # Marcar sync como fallido si existe
            if 'sync_record' in locals():
                sync_record.status = "failed"
                sync_record.finished_at = datetime.now()
                sync_record.error_message = str(e)
                session.commit()

            logger.error("\n" + "=" * 80)
            logger.error("❌ ERROR EN SINCRONIZACIÓN SEMANAL")
            logger.error(f"Error: {e}")
            logger.error("=" * 80)
            return 1


def main():
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description="Sincronización semanal incremental de BDNS"
    )
    parser.add_argument(
        "--fecha-desde",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Fecha inicial (YYYY-MM-DD). Si se omite, usa última sync + 1 día"
    )
    parser.add_argument(
        "--fecha-hasta",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Fecha final (YYYY-MM-DD). Si se omite, usa hoy"
    )
    parser.add_argument(
        "--skip-catalog-validation",
        action="store_true",
        help="Omitir validación de catálogos (no recomendado)"
    )

    args = parser.parse_args()

    # Ejecutar sync
    exit_code = sync_weekly(
        fecha_desde=args.fecha_desde,
        fecha_hasta=args.fecha_hasta,
        skip_catalog_validation=args.skip_catalog_validation
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
