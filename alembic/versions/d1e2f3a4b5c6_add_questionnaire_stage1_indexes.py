"""Add questionnaire stage-1 composite indexes

Revision ID: d1e2f3a4b5c6
Revises: c8f4d9e2b1a5
Create Date: 2025-09-17 03:05:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd1e2f3a4b5c6'
down_revision = 'c8f4d9e2b1a5'
branch_labels = None
depends_on = None


def upgrade():
    # 1) student_cleaned_scores(batch_code, subject_type, subject_name, school_code)
    try:
        op.create_index(
            'idx_scs_q',
            'student_cleaned_scores',
            ['batch_code', 'subject_type', 'subject_name', 'school_code'],
            unique=False,
        )
    except Exception:
        # index may already exist in some environments
        pass

    # 2) questionnaire_question_scores(batch_code, subject_name, school_id, question_id)
    try:
        op.create_index(
            'idx_qqs_q',
            'questionnaire_question_scores',
            ['batch_code', 'subject_name', 'school_id', 'question_id'],
            unique=False,
        )
    except Exception:
        pass

    # 3) questionnaire_option_distribution(batch_code, subject_name, question_id, option_level)
    try:
        op.create_index(
            'idx_qod_reg',
            'questionnaire_option_distribution',
            ['batch_code', 'subject_name', 'question_id', 'option_level'],
            unique=False,
        )
    except Exception:
        pass

    # 4) questionnaire_option_distribution(batch_code, subject_name, school_id, question_id, option_level)
    # note: unique index exists with different column order; this is an additional covering index
    try:
        op.create_index(
            'idx_qod_school',
            'questionnaire_option_distribution',
            ['batch_code', 'subject_name', 'school_id', 'question_id', 'option_level'],
            unique=False,
        )
    except Exception:
        pass


def downgrade():
    for name, table in [
        ('idx_qod_school', 'questionnaire_option_distribution'),
        ('idx_qod_reg', 'questionnaire_option_distribution'),
        ('idx_qqs_q', 'questionnaire_question_scores'),
        ('idx_scs_q', 'student_cleaned_scores'),
    ]:
        try:
            op.drop_index(name, table_name=table)
        except Exception:
            pass

