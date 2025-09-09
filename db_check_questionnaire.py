#!/usr/bin/env python3
import os, json
from collections import defaultdict
from sqlalchemy import create_engine, text

BATCH = os.getenv('BATCH_CODE', 'G4-2025')
URL = os.getenv('DATABASE_URL')

def main():
    if not URL:
        print(json.dumps({'error': 'DATABASE_URL not set'}, ensure_ascii=False))
        return 1
    engine = create_engine(URL)
    out = {'batch': BATCH}
    with engine.connect() as conn:
        # 1) 题目配置中的问卷科目
        q_cfg = text(
            """
            SELECT subject_name,
                   COUNT(DISTINCT question_id) AS question_count,
                   SUM(max_score) AS total_max
            FROM subject_question_config
            WHERE batch_code=:b AND question_type_enum='questionnaire'
            GROUP BY subject_name
            ORDER BY subject_name
            """
        )
        cfg_rows = [dict(r._mapping) for r in conn.execute(q_cfg, {'b': BATCH}).fetchall()]
        out['questionnaire_subjects'] = cfg_rows

        subjects = [r['subject_name'] for r in cfg_rows]

        # 2) 明细表问卷记录统计
        qqs_stats = []
        if subjects:
            # 通过配置JOIN，确保仅统计问卷题目所属科目
            q_qqs = text(
                """
                SELECT subject_name,
                       COUNT(*) AS row_count,
                       COUNT(DISTINCT student_id) AS students,
                       MIN(original_score) AS min_score,
                       MAX(original_score) AS max_score
                FROM questionnaire_question_scores\nWHERE batch_code=:b\nGROUP BY subject_name
                """
            )
            qqs_stats = [dict(r._mapping) for r in conn.execute(q_qqs, {'b': BATCH}).fetchall()]
        out['qqs_stats'] = qqs_stats

        # 3) 清洗汇总是否存在问卷（student_cleaned_scores）
        scs_stats = []
        scs_stats_any = []
        if subjects:
            # 存在正确标记为问卷的科目
            q_scs = text(
                """
                SELECT scs.subject_name,
                       COUNT(*) AS scs_rows,
                       COUNT(DISTINCT scs.student_id) AS scs_students,
                       MAX(scs.max_score) AS scs_max
                FROM student_cleaned_scores scs
                WHERE batch_code=:b AND subject_type='questionnaire'
                GROUP BY scs.subject_name
                """
            )
            scs_stats = [dict(r._mapping) for r in conn.execute(q_scs, {'b': BATCH}).fetchall()]
            # 任意标记（即使subject_type未设置为问卷）
            q_scs_any = text(
                """
                SELECT scs.subject_name,
                       COUNT(*) AS scs_rows,
                       COUNT(DISTINCT scs.student_id) AS scs_students,
                       MAX(scs.max_score) AS scs_max
                FROM student_cleaned_scores scs
                WHERE batch_code=:b
                GROUP BY scs.subject_name
                """
            )
            scs_stats_any = [dict(r._mapping) for r in conn.execute(q_scs_any, {'b': BATCH}).fetchall()]
        out['scs_stats'] = scs_stats
        out['scs_stats_any'] = scs_stats_any

        # 4) 一致性抽查：按学生聚合问卷总分 vs scs.total_score（若存在）
        consistency = []
        for s in subjects:
            # 聚合问卷总分
            q_agg = text(
                """
                SELECT subject_name, student_id, ROUND(SUM(original_score),2) AS total_score
                FROM questionnaire_question_scores\nWHERE batch_code=:b\nGROUP BY subject_name, student_id
                LIMIT 50
                """
            )
            agg = [dict(r._mapping) for r in conn.execute(q_agg, {'b': BATCH, 's': s}).fetchall()]
            if not agg:
                consistency.append({'subject_name': s, 'sample_checked': 0, 'mismatch': 0})
                continue
            # 取 scs 对应记录
            q_scs_rows = text(
                """
                SELECT student_id, ROUND(total_score,2) AS total_score
                FROM student_cleaned_scores
                WHERE batch_code=:b AND subject_name=:s
                """
            )
            scs_map = {r._mapping['student_id']: float(r._mapping['total_score']) for r in conn.execute(q_scs_rows, {'b': BATCH, 's': s}).fetchall()}
            mismatch = 0
            for row in agg:
                sid = row['student_id']
                calc = float(row['total_score'])
                if sid in scs_map and abs(scs_map[sid] - calc) > 1e-6:
                    mismatch += 1
            consistency.append({'subject_name': s, 'sample_checked': len(agg), 'mismatch': mismatch})
        out['consistency'] = consistency

        # 5) 样例抽查：各问卷科目明细3条 + scs汇总3条
        samples = {'qqs': {}, 'scs': {}}
        for s in subjects:
            q_sample_qqs = text(
                """
                SELECT student_id, subject_name, question_id, original_score, option_level, max_score
                FROM questionnaire_question_scores
                WHERE batch_code=:b AND subject_name=:s
                LIMIT 3
                """
            )
            samples['qqs'][s] = [dict(r._mapping) for r in conn.execute(q_sample_qqs, {'b': BATCH, 's': s}).fetchall()]

            q_sample_scs = text(
                """
                SELECT student_id, subject_name, total_score, max_score, subject_type
                FROM student_cleaned_scores
                WHERE batch_code=:b AND subject_name=:s
                LIMIT 3
                """
            )
            samples['scs'][s] = [dict(r._mapping) for r in conn.execute(q_sample_scs, {'b': BATCH, 's': s}).fetchall()]
        out['samples'] = samples

    print(json.dumps(out, ensure_ascii=False, default=str))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())




