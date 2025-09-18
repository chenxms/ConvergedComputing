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
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT statistics_data
            FROM statistical_aggregations
            WHERE batch_code=:b AND aggregation_level='REGIONAL'
            LIMIT 1
        """), {'b': b}).fetchone()
        print('='*60)
        print(f'批次 {b} 问卷/考试类型检查')
        if not row:
            print('  无区域级数据')
            continue
        data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        types = {}
        for s in data.get('subjects', []):
            t = s.get('type')
            types[t] = types.get(t, 0) + 1
        print(f'  subjects 类型分布: {types}')
        # 检查问卷的维度里 option_distribution 是否存在
        q_dims_has_dist = False
        for s in data.get('subjects', []):
            if s.get('type') == 'questionnaire':
                for d in s.get('dimensions', []) or []:
                    if 'option_distribution' in d:
                        q_dims_has_dist = True
                        break
        print(f'  问卷维度是否带 option_distribution: {q_dims_has_dist}')
