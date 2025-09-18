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
        for lvl in ['REGIONAL','SCHOOL']:
            row = conn.execute(text("""
                SELECT statistics_data
                FROM statistical_aggregations
                WHERE batch_code=:b AND aggregation_level=:lvl
                LIMIT 1
            """), {'b': b, 'lvl': lvl}).fetchone()
            if not row: 
                print(f'{b}/{lvl}: 无数据')
                continue
            data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            print(f"{b}/{lvl}: schema_version={data.get('schema_version')}, data_version={data.get('data_version')}")
