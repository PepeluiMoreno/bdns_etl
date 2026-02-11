"""initial_bdns_etl_schema

Revision ID: 001_etl_control
Revises: None
Create Date: 2026-02-10 15:00:00

Migración inicial del esquema bdns_etl (infraestructura ETL).

Crea:
- Schema bdns_etl
- Tabla etl_job (jobs individuales de ETL)
- Tabla etl_execution (historial de ejecuciones)
- Tabla sync_control (control de sincronizaciones)
- Índices y constraints

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '001_etl_control'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Crear esquema bdns_etl."""

    # ============================================================
    # 1. SCHEMA BDNS_ETL (ya creado por init_db.sh)
    # ============================================================

    # El esquema ya fue creado por init_db.sh antes de ejecutar migraciones
    op.execute("COMMENT ON SCHEMA bdns_etl IS 'Infraestructura ETL: control de ejecuciones, jobs y sincronización'")

    # ============================================================
    # 2. TABLA ETL_JOB
    # ============================================================

    op.create_table(
        'etl_job',
        sa.Column('id', sa.Uuid(), server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('entity', sa.String(length=50), nullable=False),
        sa.Column('year', sa.Integer(), nullable=False),
        sa.Column('mes', sa.Integer(), nullable=True),
        sa.Column('tipo', sa.String(length=1), nullable=True),
        sa.Column('stage', sa.String(length=20), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='pending'),
        sa.Column('retries', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('entity', 'year', 'mes', 'tipo', 'stage', name='uq_etl_job'),
        schema='bdns_etl'
    )

    op.create_index('ix_etl_job_pending', 'etl_job', ['status', 'stage'], unique=False, schema='bdns_etl')

    # ============================================================
    # 3. TABLA ETL_EXECUTION
    # ============================================================

    op.create_table(
        'etl_execution',
        sa.Column('id', sa.Uuid(), server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('execution_type', sa.String(length=20), nullable=False),
        sa.Column('entity', sa.String(length=50), nullable=True),
        sa.Column('year', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=False),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.Column('entrypoint', sa.String(length=255), nullable=True),
        sa.Column('current_phase', sa.String(length=50), nullable=True),
        sa.Column('current_operation', sa.Text(), nullable=True),
        sa.Column('progress_percentage', sa.Integer(), server_default='0'),
        sa.Column('records_processed', sa.Integer(), server_default='0'),
        sa.Column('records_inserted', sa.Integer(), server_default='0'),
        sa.Column('records_updated', sa.Integer(), server_default='0'),
        sa.Column('records_errors', sa.Integer(), server_default='0'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('execution_time_seconds', sa.Integer(), nullable=True),
        sa.Column('window_start', sa.DateTime(), nullable=True),
        sa.Column('window_end', sa.DateTime(), nullable=True),
        sa.Column('window_months', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='bdns_etl'
    )

    op.create_index('ix_etl_execution_type', 'etl_execution', ['execution_type'], unique=False, schema='bdns_etl')
    op.create_index('ix_etl_execution_status', 'etl_execution', ['status'], unique=False, schema='bdns_etl')
    op.create_index('ix_etl_execution_phase', 'etl_execution', ['current_phase'], unique=False, schema='bdns_etl')

    # ============================================================
    # 4. TABLA SYNC_CONTROL
    # ============================================================

    op.create_table(
        'sync_control',
        sa.Column('id', sa.Uuid(), server_default=sa.text('uuid_generate_v4()'), nullable=False),
        sa.Column('fecha_desde', sa.Date(), nullable=False),
        sa.Column('fecha_hasta', sa.Date(), nullable=False),
        sa.Column('estado', sa.String(length=20), server_default='running', nullable=False),
        sa.Column('inserts_detectados', sa.Integer(), server_default='0'),
        sa.Column('updates_detectados', sa.Integer(), server_default='0'),
        sa.Column('deletes_detectados', sa.Integer(), server_default='0'),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_by', sa.String(length=50), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('updated_by', sa.String(length=50), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='bdns_etl'
    )

    op.create_index('ix_sync_control_estado', 'sync_control', ['estado'], unique=False, schema='bdns_etl')

    # ============================================================
    # 5. COMENTARIOS
    # ============================================================

    op.execute("""
        COMMENT ON TABLE bdns_etl.etl_job IS 'Jobs individuales del ETL (extract, transform, load por entidad/año/mes)';
        COMMENT ON TABLE bdns_etl.etl_execution IS 'Historial de ejecuciones completas (seeding y sync)';
        COMMENT ON TABLE bdns_etl.sync_control IS 'Control de sincronizaciones incrementales con API BDNS';

        COMMENT ON COLUMN bdns_etl.etl_execution.current_phase IS 'Fase actual: extracting, transforming, loading, validating';
        COMMENT ON COLUMN bdns_etl.etl_execution.progress_percentage IS 'Progreso general 0-100%';
        COMMENT ON COLUMN bdns_etl.sync_control.estado IS 'Estado: running, completed, failed';
    """)


def downgrade() -> None:
    """Downgrade schema - Eliminar esquema bdns_etl."""

    # Eliminar tablas en orden inverso
    op.drop_table('sync_control', schema='bdns_etl')
    op.drop_table('etl_execution', schema='bdns_etl')
    op.drop_table('etl_job', schema='bdns_etl')

    # Eliminar schema
    op.execute("DROP SCHEMA IF EXISTS bdns_etl CASCADE")
