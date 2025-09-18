"""创建独立题目选项分布表

Revision ID: c8f4d9e2b1a5
Revises: 6b7db1a2a4f1
Create Date: 2025-01-15 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'c8f4d9e2b1a5'
down_revision = '6b7db1a2a4f1'
branch_labels = None
depends_on = None


def upgrade():
    """升级：创建独立的题目选项分布表"""
    # 创建问卷题目选项分布表
    op.create_table(
        'questionnaire_option_distribution',
        sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column('batch_code', mysql.VARCHAR(50), nullable=False, comment='批次代码'),
        sa.Column('school_id', mysql.VARCHAR(50), nullable=False, comment='学校ID'),
        sa.Column('subject_name', mysql.VARCHAR(100), nullable=False, comment='科目名称'),
        sa.Column('question_id', mysql.VARCHAR(100), nullable=False, comment='题目ID'),
        sa.Column('option_level', mysql.TINYINT(), nullable=False, comment='选项等级'),
        sa.Column('option_label', mysql.VARCHAR(100), nullable=True, comment='选项标签'),
        sa.Column('count', sa.Integer(), nullable=False, server_default=sa.text('0'), comment='选择人数'),
        sa.Column('n_total', sa.Integer(), nullable=False, server_default=sa.text('0'), comment='总答题人数'),
        sa.Column('pct', mysql.DECIMAL(7, 4), nullable=False, server_default=sa.text('0'), comment='百分比(0-100, 4位小数)'),
        sa.Column('created_at', mysql.TIMESTAMP(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', mysql.TIMESTAMP(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')),
        sa.Index('idx_batch_school_subject', 'batch_code', 'school_id', 'subject_name'),
        sa.Index('idx_question_option', 'question_id', 'option_level'),
        sa.UniqueConstraint('batch_code', 'school_id', 'subject_name', 'question_id', 'option_level', name='uk_questionnaire_option_distribution'),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4',
        mysql_collate='utf8mb4_unicode_ci',
        comment='问卷题目选项分布统计表'
    )


def downgrade():
    """降级：删除独立的题目选项分布表"""
    op.drop_table('questionnaire_option_distribution')
