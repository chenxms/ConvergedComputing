import os
from sqlalchemy import create_engine, text
url=os.getenv('DATABASE_URL') or 'mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
engine=create_engine(url)
with engine.connect() as conn:
    res = conn.execute(text("SELECT TABLE_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND COLUMN_NAME LIKE '%questionnaire%' GROUP BY TABLE_NAME"))
    for row in res:
        print(row[0])
