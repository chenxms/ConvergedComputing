#!/usr/bin/env python3
import os
import sys
from sqlalchemy import create_engine, text


def main():
    if len(sys.argv) < 3:
        print("Usage: python fix_subject_scores.py <batch_code> <subject_name>")
        sys.exit(1)

    batch = sys.argv[1]
    subject = sys.argv[2]
    url = os.getenv('DATABASE_URL')
    if not url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(2)

    engine = create_engine(url)
    with engine.begin() as conn:
        # 尝试统一连接排序规则，避免等号比较时报错
        try:
            conn.execute(text("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"))
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 300; SET SESSION tx_isolation='READ-COMMITTED'"))
        except Exception:
            pass
        # 1) 科目满分（仅统计考试题目）
        max_q = text(
            """
            SELECT COALESCE(SUM(max_score),0)
            FROM subject_question_config
            WHERE batch_code=:b AND subject_name=:s AND question_type_enum='exam'
            """
        )
        subject_max = float(conn.execute(max_q, {'b': batch, 's': subject}).scalar() or 0.0)

        # 2) 建临时表，计算每个学生考试题目得分总和
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_subject_totals"))
        conn.execute(text(
            """
            CREATE TEMPORARY TABLE tmp_subject_totals (
              student_id VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
              subject_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
              total_score DECIMAL(10,2) NOT NULL,
              PRIMARY KEY (student_id, subject_id)
            ) ENGINE=Memory
            """
        ))

        insert_totals = text(
            """
            INSERT INTO tmp_subject_totals (student_id, subject_id, total_score)
            SELECT ssd.student_id,
                   ssd.subject_id,
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
            GROUP BY ssd.student_id, ssd.subject_id
            """
        )
        conn.execute(insert_totals, {'b': batch, 's': subject})

        # 3) 更新清洗表中的科学总分与满分
        # 分批：先取目标 id，再按 id 受限更新，避免 UPDATE+ORDER BY 限制
        select_ids = text(
            """
            SELECT scs.id
            FROM student_cleaned_scores scs
            JOIN tmp_subject_totals t ON BINARY t.student_id = BINARY scs.student_id
                                     AND BINARY t.subject_id = BINARY scs.subject_id
            LIMIT 500
            """
        )
        total_updated = 0
        while True:
            rows = conn.execute(select_ids).fetchall()
            ids = [str(r[0]) for r in rows]
            if not ids:
                break
            ids_list = ",".join(ids)
            upd_sql = f"""
                UPDATE student_cleaned_scores scs
                JOIN tmp_subject_totals t ON BINARY t.student_id = BINARY scs.student_id
                                         AND BINARY t.subject_id = BINARY scs.subject_id
                SET scs.total_score = t.total_score,
                    scs.max_score = :subject_max
                WHERE scs.id IN ({ids_list})
            """
            res = conn.execute(text(upd_sql), {'subject_max': subject_max})
            cnt = res.rowcount or 0
            total_updated += cnt
        print(f"Updated rows: {total_updated}")

        # 4) 清理临时表（可选）
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_subject_totals"))

        print(f"Fixed subject '{subject}' for batch {batch}. Set max_score={subject_max} and recalculated total_score per student.")


if __name__ == '__main__':
    main()
