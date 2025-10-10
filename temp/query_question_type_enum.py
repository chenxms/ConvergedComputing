import os
from sqlalchemy import create_engine, text
url=os.getenv('DATABASE_URL') or 'mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
engine=create_engine(url)
with engine.connect() as conn:
    res = conn.execute(text("SELECT DISTINCT question_type_enum FROM subject_question_config WHERE batch_code='G7-2025' AND subject_name='问卷'")).fetchall()
    print(res)
