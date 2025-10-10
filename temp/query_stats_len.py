import os
from sqlalchemy import create_engine, text

url=os.getenv('DATABASE_URL') or 'mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
engine=create_engine(url)
with engine.connect() as conn:
    res=conn.execute(text("SELECT aggregation_level, school_id, LENGTH(statistics_data) FROM statistical_aggregations WHERE batch_code='G4-2025' AND aggregation_level='REGIONAL'"))
    for level, school_id, length in res:
        print(level, school_id, length)
