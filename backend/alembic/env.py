"""Alembic environment configuration for BDNS ETL Backend.

Gestiona migraciones del esquema etl_control (infraestructura ETL).
"""
from logging.config import fileConfig
import sys
from pathlib import Path

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# Añadir paths para importar bdns_core
project_root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(project_root / "bdns_core" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

# Importar Base y modelos
from bdns_core.db.base import Base
from bdns_core.db import etl_models  # Importar modelos ETL
from bdns_core.config import get_etl_settings

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# Configurar URL de base de datos desde settings
settings = get_etl_settings()
config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)


def include_object(object, name, type_, reflected, compare_to):
    """
    Incluir solo objetos del esquema bdns_etl.

    Esto evita que Alembic intente gestionar tablas de otros esquemas.
    """
    if type_ == "table":
        # Solo incluir tablas del esquema bdns_etl
        return object.schema == "bdns_etl"
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        include_schemas=True,
        version_table_schema="bdns_etl",  # Tabla de versiones en etl_control
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            include_schemas=True,
            version_table_schema="bdns_etl",  # Tabla de versiones en etl_control
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
