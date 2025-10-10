import os
from sqlalchemy import create_engine, text

url=os.getenv('DATABASE_URL') or 'mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
engine=create_engine(url)
with engine.connect() as conn:
    res=conn.execute(text("SELECT aggregation_level, school_id, statistics_data FROM statistical_aggregations WHERE batch_code='G4-2025' AND aggregation_level='REGIONAL' LIMIT 1"))
    for level, school_id, payload in res:
        print(level, school_id)
        print(payload)
