"""Add generated column school_id_norm and unique index on normalized key

Revision ID: a1c23d7b4e3d
Revises: 6b7db1a2a4f1
Create Date: 2025-09-12 12:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1c23d7b4e3d'
down_revision: Union[str, Sequence[str], None] = '6b7db1a2a4f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(conn, table: str, column: str) -> bool:
    try:
        res = conn.execute(sa.text("SHOW COLUMNS FROM %s LIKE :col" % table), {"col": column})
        return res.fetchone() is not None
    except Exception:
        return False


def _index_exists(conn, table: str, index_name: str) -> bool:
    try:
        res = conn.execute(sa.text("SHOW INDEX FROM %s WHERE Key_name = :k" % table), {"k": index_name})
        return res.fetchone() is not None
    except Exception:
        return False


def _assert_no_normalized_duplicates(conn) -> None:
    # Detect duplicates across normalized key (batch_code, aggregation_level, COALESCE(school_id,'REGIONAL'))
    sql = sa.text(
        """
        SELECT COUNT(*) AS dup_cnt
        FROM (
            SELECT batch_code, aggregation_level, COALESCE(school_id,'REGIONAL') AS school_id_norm, COUNT(*) AS c
            FROM statistical_aggregations
            GROUP BY batch_code, aggregation_level, school_id_norm
            HAVING c > 1
        ) t
        """
    )
    dup_cnt = conn.execute(sql).scalar() or 0
    if dup_cnt > 0:
        raise RuntimeError(
            f"Found {dup_cnt} duplicate groups on normalized key. "
            "Please run 'python scripts/fix_regional_duplicates.py --all' (or targeted --batch) to clean duplicates first."
        )


def upgrade() -> None:
    conn = op.get_bind()

    # 1) Ensure no duplicates that would violate the new unique index
    _assert_no_normalized_duplicates(conn)

    # 2) Add generated column if missing
    if not _column_exists(conn, 'statistical_aggregations', 'school_id_norm'):
        op.execute(
            sa.text(
                """
                ALTER TABLE `statistical_aggregations`
                ADD COLUMN `school_id_norm` VARCHAR(60)
                GENERATED ALWAYS AS (COALESCE(`school_id`,'REGIONAL')) STORED
                """
            )
        )

    # 3) Create unique index on normalized key if missing
    if not _index_exists(conn, 'statistical_aggregations', 'uk_batch_level_school_norm'):
        op.execute(
            sa.text(
                """
                CREATE UNIQUE INDEX `uk_batch_level_school_norm`
                ON `statistical_aggregations` (`batch_code`, `aggregation_level`, `school_id_norm`)
                """
            )
        )


def downgrade() -> None:
    conn = op.get_bind()

    # Drop unique index if exists
    if _index_exists(conn, 'statistical_aggregations', 'uk_batch_level_school_norm'):
        try:
            op.execute(sa.text("DROP INDEX `uk_batch_level_school_norm` ON `statistical_aggregations`"))
        except Exception:
            pass

    # Drop generated column if exists
    if _column_exists(conn, 'statistical_aggregations', 'school_id_norm'):
        try:
            op.execute(sa.text("ALTER TABLE `statistical_aggregations` DROP COLUMN `school_id_norm`"))
        except Exception:
            pass

