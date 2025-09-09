#!/usr/bin/env python3
import os
from sqlalchemy import create_engine, text

BATCH = os.getenv('BATCH_CODE', 'G4-2025')
SUBJECT = os.getenv('SUBJECT_NAME', '科学')
URL = os.getenv('DATABASE_URL')

def main():
    if not URL:
        print('ERROR: DATABASE_URL not set')
        return 1
    engine = create_engine(URL)
    with engine.begin() as conn:
        # session settings to reduce lock waits
        try:
            conn.execute(text("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"))
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 300"))
            conn.execute(text("SET SESSION tx_isolation='READ-COMMITTED'"))
        except Exception:
            pass

        # 1) 科目满分（仅考试题目）
        subject_max = conn.execute(text(
            """
            SELECT COALESCE(SUM(max_score),0)
            FROM subject_question_config
            WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s AND question_type_enum='exam'
            """
        ), {'b': BATCH, 's': SUBJECT}).scalar() or 0

        # 2) 临时表保存该批次该学科每个学生的逐题总分
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_subject_totals"))
        conn.execute(text(
            """
            CREATE TEMPORARY TABLE tmp_subject_totals (
              batch_code VARCHAR(50)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
              subject_id VARCHAR(64)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
              student_id VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
              total_score DECIMAL(10,2) NOT NULL,
              PRIMARY KEY (batch_code, subject_id, student_id)
            ) ENGINE=Memory
            """
        ))

        conn.execute(text(
            """
            INSERT INTO tmp_subject_totals (batch_code, subject_id, student_id, total_score)
            SELECT ssd.batch_code,
                   ssd.subject_id,
                   ssd.student_id,
                   ROUND(SUM(
                     COALESCE(
                       CAST(
                         JSON_UNQUOTE(
                           JSON_EXTRACT(ssd.subject_scores, CONCAT('$."', sqc.question_id, '"'))
                         ) AS DECIMAL(10,2)
                       ),
                       0
                     )
                   ), 2) AS total_score
            FROM student_score_detail ssd
            JOIN subject_question_config sqc
              ON BINARY sqc.batch_code=BINARY ssd.batch_code
             AND BINARY sqc.subject_name=BINARY ssd.subject_name
             AND sqc.question_type_enum='exam'
            WHERE BINARY ssd.batch_code=BINARY :b AND BINARY ssd.subject_name=BINARY :s
            GROUP BY ssd.batch_code, ssd.subject_id, ssd.student_id
            """
        ), {'b': BATCH, 's': SUBJECT})

        # 3) 更新清洗表中对应批次+学科的总分与满分
        conn.execute(text(
            """
            UPDATE student_cleaned_scores scs
            JOIN tmp_subject_totals t ON BINARY t.batch_code = BINARY scs.batch_code
                                     AND BINARY t.subject_id = BINARY scs.subject_id
                                     AND BINARY t.student_id = BINARY scs.student_id
            SET scs.total_score = t.total_score,
                scs.max_score   = :subject_max
            WHERE BINARY scs.batch_code=BINARY :b AND BINARY scs.subject_name=BINARY :s
            """
        ), {'b': BATCH, 's': SUBJECT, 'subject_max': float(subject_max)})

        # 4) 清理
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_subject_totals"))

        print(f"Science fix done. max={float(subject_max):.2f}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())

