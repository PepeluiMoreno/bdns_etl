"""
Validador de sincronización de catálogos.

Este módulo implementa la lógica de detección de obsolescencia de catálogos
basada en el ejercicio que se está procesando.

Regla: Los catálogos se consideran obsoletos si la última sincronización
es anterior al año del ejercicio que se va a procesar.
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from bdns_core.db.etl_models import EtlExecution

logger = logging.getLogger(__name__)


def get_ultima_sync_catalogos(session: Session) -> Optional[datetime]:
    """
    Retorna fecha de la última sincronización exitosa de catálogos.

    Args:
        session: Sesión de SQLAlchemy

    Returns:
        datetime de última sync, o None si nunca se sincronizó
    """
    ultima_sync = (
        session.query(EtlExecution)
        .filter(
            EtlExecution.execution_type == "sync_catalogos",
            EtlExecution.status == "completed"
        )
        .order_by(EtlExecution.finished_at.desc())
        .first()
    )

    return ultima_sync.finished_at if ultima_sync else None


def catalogos_obsoletos_para_ejercicio(session: Session, year: int) -> bool:
    """
    Determina si los catálogos están obsoletos para un ejercicio dado.

    Los catálogos se consideran obsoletos si:
    - Nunca se han sincronizado
    - La última sincronización es de un año anterior al ejercicio solicitado

    Args:
        session: Sesión de SQLAlchemy
        year: Año del ejercicio a validar

    Returns:
        True si catálogos necesitan actualización, False si están vigentes

    Examples:
        >>> # Última sync: 2024-06-01, Ejercicio: 2025
        >>> catalogos_obsoletos_para_ejercicio(session, 2025)
        True

        >>> # Última sync: 2025-01-15, Ejercicio: 2025
        >>> catalogos_obsoletos_para_ejercicio(session, 2025)
        False

        >>> # Última sync: 2025-12-31, Ejercicio: 2024
        >>> catalogos_obsoletos_para_ejercicio(session, 2024)
        False  # Catálogos más recientes, no obsoletos
    """
    ultima_sync = get_ultima_sync_catalogos(session)

    # Si nunca se sincronizaron, están obsoletos
    if not ultima_sync:
        logger.warning(
            f"No hay registro de sincronización de catálogos. "
            f"Se requiere sincronización inicial."
        )
        return True

    # Si la última sync es anterior al ejercicio, están obsoletos
    year_ultima_sync = ultima_sync.year

    if year_ultima_sync < year:
        logger.warning(
            f"Catálogos obsoletos: última sync {ultima_sync.date()} "
            f"(año {year_ultima_sync}) < ejercicio {year}"
        )
        return True

    logger.info(
        f"Catálogos vigentes: última sync {ultima_sync.date()} "
        f"(año {year_ultima_sync}) >= ejercicio {year}"
    )
    return False


def validar_y_sincronizar_catalogos(session: Session, year: int) -> None:
    """
    Valida si catálogos están vigentes para el ejercicio.
    Si están obsoletos, ejecuta sincronización automática.

    Esta función se debe llamar ANTES de cualquier proceso de seeding
    o sincronización de datos transaccionales (convocatorias, concesiones).

    Args:
        session: Sesión de SQLAlchemy
        year: Año del ejercicio a procesar

    Raises:
        Exception: Si sync_catalogos() falla

    Examples:
        >>> # En seeding de convocatorias 2025
        >>> with SessionLocal() as session:
        ...     validar_y_sincronizar_catalogos(session, 2025)
        ...     # Ahora sí, proceder con seeding
        ...     seed_convocatorias_ejercicio(2025)

        >>> # En sync semanal (fecha_hasta = 2025-02-10)
        >>> year_max = fecha_hasta.year
        >>> validar_y_sincronizar_catalogos(session, year_max)
        >>> sync_convocatorias(session, fecha_desde, fecha_hasta)
    """
    if catalogos_obsoletos_para_ejercicio(session, year):
        logger.warning(
            f"⚠️  Catálogos obsoletos para ejercicio {year}. "
            f"Sincronizando desde API BDNS..."
        )

        # Importar aquí para evitar circular imports
        from .sync_catalogos import sync_catalogos

        try:
            sync_catalogos(session)
            logger.info(
                f"✅ Catálogos sincronizados exitosamente. "
                f"Procediendo con ejercicio {year}."
            )
        except Exception as e:
            logger.error(
                f"❌ Error sincronizando catálogos: {e}. "
                f"No se puede proceder con ejercicio {year}."
            )
            raise
    else:
        logger.info(f"✅ Catálogos vigentes para ejercicio {year}.")
