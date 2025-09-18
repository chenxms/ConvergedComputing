#!/usr/bin/env python3
import os
from sqlalchemy import create_engine, text

BATCH = os.getenv('BATCH_CODE', 'G4-2025')
SUBJECT = os.getenv('SUBJECT_NAME', '问卷')
URL = os.getenv('DATABASE_URL')


def main():
    if not URL:
        print('ERROR: DATABASE_URL not set')
        return 1
    engine = create_engine(URL)
    with engine.begin() as conn:
        try:
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 300"))
        except Exception:
            pass

        # 1) 清理旧明细
        conn.execute(text(
            """
            DELETE FROM questionnaire_question_scores
            WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s
            """
        ), {'b': BATCH, 's': SUBJECT})

        # 2) 插入问卷明细（包含标准化学校信息）
        ins = text(
            """
            INSERT INTO questionnaire_question_scores
              (batch_code, subject_name, student_id, school_id, school_code, school_name,
               question_id, original_score, max_score, scale_level, instrument_type, is_reverse)
            SELECT ssd.batch_code,
                   ssd.subject_name,
                   CAST(ssd.student_id AS UNSIGNED),
                   smd.school_id,
                   smd.school_id AS school_code,
                   smd.standard_school_name,
                   sqc.question_id,
                   CAST(JSON_UNQUOTE(JSON_EXTRACT(ssd.subject_scores, CONCAT('$."', sqc.question_id, '"'))) AS DECIMAL(10,2)) AS original_score,
                   sqc.max_score,
                   CASE
                     WHEN sqc.instrument_id LIKE '%10%' THEN 10
                     WHEN sqc.instrument_id LIKE '%7%'  THEN 7
                     WHEN sqc.instrument_id LIKE '%5%'  THEN 5
                     ELSE 4
                   END AS scale_level,
                   sqc.instrument_id AS instrument_type,
                   0 AS is_reverse
            FROM student_score_detail ssd
            INNER JOIN school_master_data smd
              ON smd.batch_code COLLATE utf8mb4_unicode_ci = ssd.batch_code COLLATE utf8mb4_unicode_ci
             AND smd.school_id COLLATE utf8mb4_unicode_ci = ssd.school_id COLLATE utf8mb4_unicode_ci
             AND smd.status = 'ACTIVE'
            JOIN subject_question_config sqc
              ON BINARY sqc.batch_code=BINARY ssd.batch_code
             AND BINARY sqc.subject_name=BINARY ssd.subject_name
             AND sqc.question_type_enum='questionnaire'
            WHERE BINARY ssd.batch_code=BINARY :b
              AND BINARY ssd.subject_name=BINARY :s
              AND JSON_EXTRACT(ssd.subject_scores, CONCAT('$."', sqc.question_id, '"')) IS NOT NULL
              AND ssd.student_id REGEXP '^[0-9]+$'
              AND smd.school_id IS NOT NULL
            """
        )
        res = conn.execute(ins, {'b': BATCH, 's': SUBJECT})
        inserted = res.rowcount or 0

        # 3) 重建学校级选项分布（写入完整列）
        conn.execute(text(
            "DELETE FROM questionnaire_option_distribution WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s"
        ), {'b': BATCH, 's': SUBJECT})
        conn.execute(text(
            """
            INSERT INTO questionnaire_option_distribution
              (batch_code, school_id, subject_name, question_id, option_level, option_label, count, n_total, pct)
            SELECT d.batch_code,
                   d.school_id,
                   d.subject_name,
                   d.question_id,
                   d.option_level,
                   NULL AS option_label,
                   d.cnt AS count,
                   t.total AS n_total,
                   ROUND(d.cnt * 100.0 / NULLIF(t.total, 0), 2) AS pct
            FROM (
              SELECT qqs.batch_code,
                     qqs.school_id,
                     qqs.subject_name,
                     qqs.question_id,
                     GREATEST(
                       1,
                       LEAST(
                         qqs.scale_level,
                         ROUND(COALESCE(qqs.original_score,0) / NULLIF(qqs.max_score,0) * qqs.scale_level, 0)
                       )
                     ) AS option_level,
                     COUNT(*) AS cnt
              FROM questionnaire_question_scores qqs
              WHERE BINARY qqs.batch_code=BINARY :b AND BINARY qqs.subject_name=BINARY :s AND qqs.school_id IS NOT NULL
              GROUP BY qqs.batch_code, qqs.school_id, qqs.subject_name, qqs.question_id,
                       GREATEST(
                         1,
                         LEAST(
                           qqs.scale_level,
                           ROUND(COALESCE(qqs.original_score,0) / NULLIF(qqs.max_score,0) * qqs.scale_level, 0)
                         )
                       )
            ) d
            JOIN (
              SELECT qqs.batch_code, qqs.school_id, qqs.subject_name, qqs.question_id, COUNT(*) AS total
              FROM questionnaire_question_scores qqs
              WHERE BINARY qqs.batch_code=BINARY :b AND BINARY qqs.subject_name=BINARY :s AND qqs.school_id IS NOT NULL
              GROUP BY qqs.batch_code, qqs.school_id, qqs.subject_name, qqs.question_id
            ) t
              ON t.batch_code=d.batch_code AND t.school_id=d.school_id AND t.subject_name=d.subject_name AND t.question_id=d.question_id
            """
        ), {'b': BATCH, 's': SUBJECT})

        print(f"Questionnaire cleaned. Inserted detail rows: {inserted}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
