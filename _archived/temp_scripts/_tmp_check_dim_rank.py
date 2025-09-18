import os, json
from sqlalchemy import create_engine, text

DB_HOST = os.getenv('DATABASE_HOST', '117.72.14.166')
DB_PORT = os.getenv('DATABASE_PORT', '23506')
DB_USER = os.getenv('DATABASE_USER', 'root')
DB_PASS = os.getenv('DATABASE_PASSWORD', 'mysql_Lujing2022')
DB_NAME = os.getenv('DATABASE_NAME', 'appraisal_test')
url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
engine = create_engine(url, future=True)

for b in ['G7-2025','G4-2025']:
    print('='*60)
    print(f'批次 {b} 学校级：维度rank检查(3所)')
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT school_id, school_name, statistics_data
            FROM statistical_aggregations
            WHERE batch_code=:b AND aggregation_level='SCHOOL'
            ORDER BY CAST(school_id AS UNSIGNED), school_id
            LIMIT 3
        """), {'b': b}).fetchall()
        if not rows:
            print('  无学校级数据')
            continue
        for r in rows:
            sid = str(r[0])
            sname = r[1]
            data = r[2] if isinstance(r[2], dict) else json.loads(r[2])
            has_dim_rank = False
            for s in data.get('subjects', []):
                for d in s.get('dimensions', []) or []:
                    if 'rank' in d:
                        has_dim_rank = True
                        break
                if has_dim_rank: break
            print(f'  学校 {sid} {sname}: 维度rank = {has_dim_rank}')
