# 数据库连接配置
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os
import logging
from typing import Generator

# 日志配置
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据库连接配置
DATABASE_HOST = os.getenv("DATABASE_HOST", "117.72.14.166")
DATABASE_PORT = os.getenv("DATABASE_PORT", "23506")
DATABASE_USER = os.getenv("DATABASE_USER", "root")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "mysql_Lujing2022")
DATABASE_NAME = os.getenv("DATABASE_NAME", "appraisal_stats")

# 构建数据库URL
DATABASE_URL = (
    f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}"
    f"@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
    "?charset=utf8mb4"
)

# 创建数据库引擎
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,                    # 连接池大小
    max_overflow=30,                 # 最大溢出连接
    pool_pre_ping=True,              # 连接健康检查
    pool_recycle=3600,               # 连接回收时间(1小时)
    echo=False,                      # 生产环境关闭SQL日志
    future=True,                     # 启用SQLAlchemy 2.0特性
    connect_args={
        "charset": "utf8mb4",
        "autocommit": False,
    }
)

# 创建会话工厂
SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine,
    future=True
)

# 创建声明性基类
Base = declarative_base()


def get_db() -> Generator:
    """获取数据库会话"""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()


def test_connection() -> bool:
    """测试数据库连接"""
    try:
        from sqlalchemy import text
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return False


def create_tables():
    """创建所有表"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("All tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create tables: {str(e)}")
        raise