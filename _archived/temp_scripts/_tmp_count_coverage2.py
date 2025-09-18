import os
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
        total_schools = conn.execute(text("""
            SELECT COUNT(DISTINCT school_id) FROM student_score_detail WHERE batch_code=:b
        """), {'b': b}).scalar()
        mat_schools = conn.execute(text("""
            SELECT COUNT(DISTINCT school_id) FROM statistical_aggregations
            WHERE batch_code=:b AND aggregation_level='SCHOOL'
        """), {'b': b}).scalar()
        print(f"{b}: 已物化学校 {mat_schools} / 基础数据学校数 {total_schools}")
