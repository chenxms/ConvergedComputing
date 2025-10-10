import os
from sqlalchemy import create_engine, text
url=os.getenv('DATABASE_URL') or 'mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
engine=create_engine(url)
with engine.connect() as conn:
    res = conn.execute(text("SELECT subject_name, COUNT(*) FROM question_dimension_mapping WHERE batch_code='G7-2025' GROUP BY subject_name"))
    for row in res:
        print(row)
