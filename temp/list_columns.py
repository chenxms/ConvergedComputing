import os
from sqlalchemy import create_engine, inspect
url=os.getenv('DATABASE_URL') or 'mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
engine=create_engine(url)
inspector = inspect(engine)
columns = inspector.get_columns('subject_question_config')
print([col['name'] for col in columns])
