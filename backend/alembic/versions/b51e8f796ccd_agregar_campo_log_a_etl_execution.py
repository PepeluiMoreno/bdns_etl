"""agregar_campo_log_a_etl_execution

Revision ID: b51e8f796ccd
Revises: 001_etl_control
Create Date: 2026-02-10 21:24:03.066039

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b51e8f796ccd'
down_revision: Union[str, Sequence[str], None] = '001_etl_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'etl_execution',
        sa.Column('log', sa.Text(), nullable=True),
        schema='bdns_etl'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('etl_execution', 'log', schema='bdns_etl')
