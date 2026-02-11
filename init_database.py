#!/usr/bin/env python3
"""
Script de inicialización completa de la base de datos BDNS.

Ejecuta en orden:
1. Crea schemas (public, etl_admin)
2. Configura permisos
3. Crea todas las tablas (Base.metadata.create_all)
4. Ejecuta seeding de catálogos

Uso:
    python init_database.py --db-url postgresql://bdns:bdns@localhost:5244/bdns
"""

import sys
import argparse
import logging
from pathlib import Path
from sqlalchemy import create_engine, text

# Añadir paths necesarios
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "bdns_core" / "bdns_core" / "src"))
sys.path.insert(0, str(project_root / "bdns_etl" / "seeding"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_sql_file(engine, sql_file_path):
    """Ejecuta un archivo SQL."""
    logger.info(f"Ejecutando SQL desde: {sql_file_path}")
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    with engine.connect() as conn:
        # Ejecutar cada statement por separado
        for statement in sql_content.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    conn.execute(text(statement))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Error en statement (puede ser esperado): {e}")


def create_tables(engine):
    """Crea todas las tablas usando SQLAlchemy."""
    logger.info("Creando tablas con SQLAlchemy...")
    from bdns_core.db.base import Base
    # Importar modelos para registrarlos en metadata
    import bdns_core.db.models
    import bdns_core.db.etl_models

    Base.metadata.create_all(bind=engine)
    logger.info("✅ Tablas creadas exitosamente")


def seed_catalogos():
    """Ejecuta el seeding de catálogos."""
    logger.info("Iniciando seeding de catálogos...")
    catalogos_script = Path(__file__).parent / "seeding" / "catalogos" / "load" / "load_all_catalogos.py"

    if not catalogos_script.exists():
        logger.warning(f"Script de catálogos no encontrado: {catalogos_script}")
        return

    # Importar y ejecutar el main del script de catálogos
    import sys
    sys.path.insert(0, str(catalogos_script.parent))

    try:
        from load_all_catalogos import main as catalogos_main
        catalogos_main()
        logger.info("✅ Catálogos poblados exitosamente")
    except Exception as e:
        logger.error(f"Error poblando catálogos: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Inicializa la base de datos BDNS con schemas, tablas y catálogos"
    )
    parser.add_argument(
        "--db-url",
        default="postgresql://bdns:bdns@localhost:5244/bdns",
        help="URL de conexión a PostgreSQL"
    )
    parser.add_argument(
        "--skip-schemas",
        action="store_true",
        help="Omitir creación de schemas (si ya existen)"
    )
    parser.add_argument(
        "--skip-tables",
        action="store_true",
        help="Omitir creación de tablas"
    )
    parser.add_argument(
        "--skip-catalogos",
        action="store_true",
        help="Omitir seeding de catálogos"
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Inicializando Base de Datos BDNS")
    logger.info("=" * 60)

    engine = create_engine(args.db_url, echo=False)

    try:
        # Paso 1: Crear schemas y permisos
        if not args.skip_schemas:
            sql_file = Path(__file__).parent / "init_db.sql"
            run_sql_file(engine, sql_file)
            logger.info("✅ Schemas creados")

        # Paso 2: Crear tablas
        if not args.skip_tables:
            create_tables(engine)

        # Paso 3: Poblar catálogos
        if not args.skip_catalogos:
            seed_catalogos()

        logger.info("=" * 60)
        logger.info("✅ Inicialización completa exitosa")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"❌ Error durante la inicialización: {e}")
        sys.exit(1)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
