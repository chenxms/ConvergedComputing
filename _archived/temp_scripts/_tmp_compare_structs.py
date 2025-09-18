import os, json
from sqlalchemy import create_engine, text

DB_HOST = os.getenv('DATABASE_HOST', '117.72.14.166')
DB_PORT = os.getenv('DATABASE_PORT', '23506')
DB_USER = os.getenv('DATABASE_USER', 'root')
DB_PASS = os.getenv('DATABASE_PASSWORD', 'mysql_Lujing2022')
DB_NAME = os.getenv('DATABASE_NAME', 'appraisal_test')
url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

engine = create_engine(url, future=True)

batches = ['G7-2025','G4-2025']

with engine.connect() as conn:
    for b in batches:
        print('='*80)
        print(f'批次: {b} 学校级抽样(按 school_id 升序取3条)')
        q = text("""
            SELECT school_id, school_name, statistics_data
            FROM statistical_aggregations
            WHERE batch_code = :b AND aggregation_level = 'SCHOOL'
            ORDER BY CAST(school_id AS UNSIGNED), school_id
            LIMIT 3
        """)
        rows = conn.execute(q, {'b': b}).fetchall()
        if not rows:
            print('  无学校级物化数据')
            continue
        for i, r in enumerate(rows, 1):
            sid = str(r[0]) if r[0] is not None else 'NULL'
            sname = r[1]
            raw = r[2]
            try:
                data = raw if isinstance(raw, dict) else json.loads(raw)
            except Exception as e:
                print(f'  学校[{sid} {sname}] JSON解析失败: {e}')
                continue
            top_keys = sorted(list(data.keys()))
            subj0 = data.get('subjects', [{}])[0] if isinstance(data.get('subjects'), list) and data.get('subjects') else {}
            subj_keys = sorted(list(subj0.keys())) if subj0 else []
            metrics_keys = sorted(list(subj0.get('metrics', {}).keys())) if subj0.get('metrics') else []
            legacy = [k for k in ['academic_subjects','non_academic_subjects'] if k in data]
            suspects = []
            if 'subjects' not in data:
                suspects.append('缺少subjects')
            if 'schema_version' in data and data.get('schema_version') != 'v1.2':
                suspects.append(f"schema_version={data.get('schema_version')}")
            if 'aggregation_level' in data and data.get('aggregation_level') != 'SCHOOL':
                suspects.append(f"aggregation_level={data.get('aggregation_level')}")
            has_rank = False
            for s in data.get('subjects', []):
                m = s.get('metrics', {})
                if 'rank' in m:
                    has_rank = True
                    break
            has_sr = any('school_rankings' in s for s in data.get('subjects', []))
            has_region_rank = any(('region_rank' in s or 'total_schools' in s) for s in data.get('subjects', []))
            print(f'  [{i}] 学校 {sid} {sname}')
            print(f'    顶层键: {top_keys}')
            if legacy:
                print(f'    旧结构字段: {legacy}')
            print(f'    subjects[0] 键: {subj_keys}')
            print(f'    metrics 键: {metrics_keys}')
            print(f'    是否含 metrics.rank: {has_rank}')
            print(f'    是否含 school_rankings: {has_sr}; 是否含 region_rank/total_schools: {has_region_rank}')
            if suspects:
                print(f'    可疑: {suspects}')
