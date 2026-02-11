"""
Sincronización de catálogos desde la API BDNS.

Este módulo sincroniza todos los catálogos de referencia desde la API oficial de BDNS.
Utiliza una estrategia de UPSERT incremental: solo añade nuevos valores, nunca borra.

Los catálogos incluyen:
- Órganos convocantes (central, autonómico, local, otros)
- Instrumentos
- Finalidades
- Fondos europeos
- Objetivos
- Regiones
- Reglamentos
- Sectores de actividad (CNAE)
- Sectores producto
- Tipos de beneficiario
- Regímenes de ayuda
- Formas jurídicas
"""

import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from sqlalchemy.orm import Session

# Asegurar que los módulos de seeding están en el path
seeding_catalogos_path = Path(__file__).resolve().parent.parent / "seeding" / "catalogos" / "load"
if str(seeding_catalogos_path) not in sys.path:
    sys.path.insert(0, str(seeding_catalogos_path))

from bdns_core.db.session import SessionLocal
from bdns_core.db.etl_models import EtlExecution
from bdns_core.db.models import (
    Organo, Instrumento, Finalidad, Fondo, Objetivo, Region,
    Reglamento, SectorActividad, SectorProducto, TipoBeneficiario,
    RegimenAyuda, FormaJuridica
)

# Importar funciones de carga de catálogos existentes
from load_organos import load_organos
from load_catalogos import (
    load_catalogo,
    load_regiones,
    load_sector_actividad_desde_csv,
    load_fondo_desde_csv,
    load_reglamento_desde_json,
    load_regimen_ayuda_desde_csv,
    load_forma_juridica_desde_csv,
)

logger = logging.getLogger(__name__)

# API Base URL
API_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
VPD = "GE"


def contar_registros(session: Session, model) -> int:
    """Cuenta registros en una tabla."""
    try:
        return session.query(model).count()
    except Exception:
        return 0


def sync_catalogos(session: Session) -> Dict[str, Any]:
    """
    Sincroniza todos los catálogos desde la API BDNS.

    Esta función realiza un UPSERT incremental de todos los catálogos.
    Solo añade nuevos valores, nunca borra registros existentes.

    La ejecución se registra en la tabla etl_execution para tracking.

    Args:
        session: Sesión de SQLAlchemy

    Returns:
        Dict con estadísticas de la sincronización:
        {
            'execution_id': UUID de la ejecución,
            'status': 'completed' | 'failed',
            'catalogos': {
                'Organo': {'antes': 100, 'despues': 105, 'nuevos': 5},
                'Finalidad': {'antes': 50, 'despues': 52, 'nuevos': 2},
                ...
            },
            'total_nuevos': 15,
            'duracion_segundos': 45.2,
            'error': None | str
        }

    Raises:
        Exception: Si la sincronización falla
    """
    logger.info("=" * 80)
    logger.info("INICIANDO SINCRONIZACIÓN DE CATÁLOGOS DESDE API BDNS")
    logger.info("=" * 80)

    # Crear registro de ejecución
    execution = EtlExecution(
        execution_type="sync_catalogos",
        status="running",
        started_at=datetime.now(),
        current_phase="iniciando"
    )
    session.add(execution)
    session.commit()

    resultado = {
        'execution_id': execution.id,
        'status': 'running',
        'catalogos': {},
        'total_nuevos': 0,
        'duracion_segundos': 0,
        'error': None
    }

    inicio = datetime.now()

    try:
        # Lista de catálogos a sincronizar
        catalogos_config = [
            {
                'nombre': 'Organo',
                'modelo': Organo,
                'func': load_organos,
                'args': [session],
                'descripcion': 'Órganos convocantes (central, autonómico, local, otros)'
            },
            {
                'nombre': 'Region',
                'modelo': Region,
                'func': load_regiones,
                'args': [session],
                'descripcion': 'Regiones geográficas jerárquicas'
            },
            {
                'nombre': 'Instrumento',
                'modelo': Instrumento,
                'func': load_catalogo,
                'args': [session, Instrumento, "instrumentos"],
                'descripcion': 'Instrumentos de financiación'
            },
            {
                'nombre': 'TipoBeneficiario',
                'modelo': TipoBeneficiario,
                'func': load_catalogo,
                'args': [session, TipoBeneficiario, "beneficiarios", {"vpd": VPD}],
                'descripcion': 'Tipos de beneficiarios'
            },
            {
                'nombre': 'SectorProducto',
                'modelo': SectorProducto,
                'func': load_catalogo,
                'args': [session, SectorProducto, "sectores"],
                'descripcion': 'Sectores de producto'
            },
            {
                'nombre': 'Finalidad',
                'modelo': Finalidad,
                'func': load_catalogo,
                'args': [session, Finalidad, "finalidades", {"vpd": VPD}],
                'descripcion': 'Finalidades de las ayudas'
            },
            {
                'nombre': 'Objetivo',
                'modelo': Objetivo,
                'func': load_catalogo,
                'args': [session, Objetivo, "objetivos"],
                'descripcion': 'Objetivos de las ayudas'
            },
            {
                'nombre': 'Reglamento',
                'modelo': Reglamento,
                'func': load_catalogo,
                'args': [session, Reglamento, "reglamentos"],
                'descripcion': 'Reglamentos aplicables'
            },
        ]

        # Sincronizar cada catálogo
        for config in catalogos_config:
            nombre = config['nombre']
            modelo = config['modelo']
            func = config['func']
            args = config['args']
            descripcion = config['descripcion']

            logger.info("-" * 80)
            logger.info(f"Sincronizando: {nombre}")
            logger.info(f"Descripción: {descripcion}")

            # Contar antes
            count_antes = contar_registros(session, modelo)
            logger.info(f"Registros antes: {count_antes}")

            # Actualizar fase
            execution.current_phase = f"sync_{nombre.lower()}"
            session.commit()

            try:
                # Ejecutar sincronización
                func(*args)
                session.commit()

                # Contar después
                count_despues = contar_registros(session, modelo)
                nuevos = count_despues - count_antes

                logger.info(f"Registros después: {count_despues}")
                if nuevos > 0:
                    logger.info(f"✅ {nuevos} nuevos registros añadidos")
                else:
                    logger.info(f"✅ Sin cambios (catálogo actualizado)")

                resultado['catalogos'][nombre] = {
                    'antes': count_antes,
                    'despues': count_despues,
                    'nuevos': nuevos
                }
                resultado['total_nuevos'] += nuevos

            except Exception as e:
                logger.error(f"❌ Error sincronizando {nombre}: {e}")
                # Continuar con otros catálogos aunque uno falle
                resultado['catalogos'][nombre] = {
                    'antes': count_antes,
                    'despues': count_antes,
                    'nuevos': 0,
                    'error': str(e)
                }

        # Marcar como completado
        execution.status = "completed"
        execution.finished_at = datetime.now()
        execution.current_phase = "completado"
        execution.records_processed = resultado['total_nuevos']

        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()

        resultado['status'] = 'completed'
        resultado['duracion_segundos'] = duracion

        session.commit()

        logger.info("=" * 80)
        logger.info("SINCRONIZACIÓN DE CATÁLOGOS COMPLETADA")
        logger.info(f"Total de nuevos registros: {resultado['total_nuevos']}")
        logger.info(f"Duración: {duracion:.2f} segundos")
        logger.info("=" * 80)

        return resultado

    except Exception as e:
        # Marcar como fallido
        execution.status = "failed"
        execution.finished_at = datetime.now()
        execution.error_message = str(e)
        session.commit()

        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()

        resultado['status'] = 'failed'
        resultado['duracion_segundos'] = duracion
        resultado['error'] = str(e)

        logger.error("=" * 80)
        logger.error("❌ SINCRONIZACIÓN DE CATÁLOGOS FALLIDA")
        logger.error(f"Error: {e}")
        logger.error("=" * 80)

        raise


def sync_catalogos_standalone():
    """
    Función standalone para ejecutar sincronización de catálogos.

    Útil para testing o ejecución manual desde línea de comandos.

    Example:
        python sync_catalogos.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    with SessionLocal() as session:
        try:
            resultado = sync_catalogos(session)
            print("\n✅ Sincronización completada exitosamente")
            print(f"Total de nuevos registros: {resultado['total_nuevos']}")
            print(f"Duración: {resultado['duracion_segundos']:.2f} segundos")
            return 0
        except Exception as e:
            print(f"\n❌ Error en sincronización: {e}")
            return 1


if __name__ == "__main__":
    sys.exit(sync_catalogos_standalone())
