"""Fix unique key on statistical_aggregations to (batch_code, aggregation_level, school_id)

Revision ID: 6b7db1a2a4f1
Revises: 11292e9137da
Create Date: 2025-09-11 17:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6b7db1a2a4f1'
down_revision: Union[str, Sequence[str], None] = '11292e9137da'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Ensure unique key matches business rule: one row per (batch_code, level, school_id)."""
    conn = op.get_bind()
    # Detect existing unique constraints on the table
    existing_keys = set()
    try:
        res = conn.execute(sa.text("SHOW INDEX FROM statistical_aggregations WHERE Non_unique=0"))
        for row in res.fetchall():
            existing_keys.add(row[2])  # Key_name
    except Exception:
        existing_keys = set()

    # Drop incorrect unique key if present
    if 'uk_batch_level_school_name' in existing_keys:
        try:
            op.drop_constraint('uk_batch_level_school_name', 'statistical_aggregations', type_='unique')
        except Exception:
            pass

    # Create correct unique key if missing
    if 'uk_batch_level_school' not in existing_keys:
        op.create_unique_constraint(
            'uk_batch_level_school',
            'statistical_aggregations',
            ['batch_code', 'aggregation_level', 'school_id']
        )


def downgrade() -> None:
    # Best-effort revert: drop the corrected key and (optionally) recreate the old one
    conn = op.get_bind()
    existing_keys = set()
    try:
        res = conn.execute(sa.text("SHOW INDEX FROM statistical_aggregations WHERE Non_unique=0"))
        for row in res.fetchall():
            existing_keys.add(row[2])
    except Exception:
        existing_keys = set()

    if 'uk_batch_level_school' in existing_keys:
        try:
            op.drop_constraint('uk_batch_level_school', 'statistical_aggregations', type_='unique')
        except Exception:
            pass

    # Re-create the old key if it previously existed
    if 'uk_batch_level_school_name' not in existing_keys:
        try:
            op.create_unique_constraint(
                'uk_batch_level_school_name',
                'statistical_aggregations',
                ['batch_code', 'aggregation_level', 'school_id', 'school_name']
            )
        except Exception:
            pass

