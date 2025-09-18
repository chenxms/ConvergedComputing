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
    print('='*80)
    print(f'批次: {b} 区域级抽样')
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT statistics_data
            FROM statistical_aggregations
            WHERE batch_code = :b AND aggregation_level='REGIONAL'
            LIMIT 1
        """), {'b': b}).fetchone()
        if not row:
            print('  无区域级物化数据')
            continue
        raw = row[0]
        try:
            data = raw if isinstance(raw, dict) else json.loads(raw)
        except Exception as e:
            print(f'  JSON解析失败: {e}')
            continue
        top_keys = sorted(list(data.keys()))
        print(f'  顶层键: {top_keys}')
        if not data.get('subjects'):
            print('  缺少subjects或为空')
            continue
        s0 = data['subjects'][0]
        s0_keys = sorted(list(s0.keys()))
        m0_keys = sorted(list(s0.get('metrics', {}).keys()))
        has_sr = 'school_rankings' in s0
        sr_len = len(s0.get('school_rankings', [])) if has_sr else 0
        print(f'  subjects[0] 键: {s0_keys}')
        print(f'  metrics 键: {m0_keys}')
        print(f'  是否含 school_rankings: {has_sr} (len={sr_len})')
