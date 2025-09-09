# 数据库连接配置
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os
import logging
from typing import Generator, Optional
from contextlib import contextmanager

from .cache import create_cache_manager, StatisticalDataCache
from .monitoring import DatabaseConnectionMonitor

# 日志配置
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 优先使用完整的 DATABASE_URL，其次拼装各组件
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    DATABASE_HOST = os.getenv("DATABASE_HOST", "117.72.14.166")
    DATABASE_PORT = os.getenv("DATABASE_PORT", "23506")
    DATABASE_USER = os.getenv("DATABASE_USER", "root")
    DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "mysql_Lujing2022")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "appraisal_test")
    DATABASE_URL = (
        f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}"
        f"@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
        "?charset=utf8mb4"
    )

# 优化的数据库连接配置
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=25,                    # 增加连接池大小
    max_overflow=35,                 # 增加最大溢出连接
    pool_pre_ping=True,              # 连接健康检查
    pool_recycle=3600,               # 连接回收时间(1小时)
    query_cache_size=1200,           # SQL查询缓存
    echo=False,                      # 生产环境关闭SQL日志
    future=True,                     # 启用SQLAlchemy 2.0特性
    connect_args={
        "charset": "utf8mb4",
        "connect_timeout": 30,
        "read_timeout": 300,
        "write_timeout": 300,
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

# 全局连接监控器
connection_monitor = DatabaseConnectionMonitor()

# 全局缓存管理器
cache_manager: Optional[StatisticalDataCache] = None

try:
    cache_manager = create_cache_manager()
except Exception as e:
    logger.warning(f"Failed to initialize cache manager: {str(e)}")
    cache_manager = None

# 创建声明性基类
Base = declarative_base()


def get_db() -> Generator:
    """获取数据库会话"""
    import time
    
    start_time = time.time()
    db = SessionLocal()
    connection_time = time.time() - start_time
    
    connection_monitor.record_connection_created(connection_time)
    
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {str(e)}")
        connection_monitor.record_connection_error()
        db.rollback()
        raise
    finally:
        connection_monitor.record_connection_closed()
        db.close()


@contextmanager
def get_db_context():
    """上下文管理器方式获取数据库连接"""
    import time
    
    start_time = time.time()
    db = SessionLocal()
    connection_time = time.time() - start_time
    
    connection_monitor.record_connection_created(connection_time)
    
    try:
        yield db
        db.commit()
    except Exception as e:
        logger.error(f"Database transaction error: {str(e)}")
        connection_monitor.record_connection_error()
        db.rollback()
        raise
    finally:
        connection_monitor.record_connection_closed()
        db.close()


def get_cache_manager() -> Optional[StatisticalDataCache]:
    """获取缓存管理器"""
    return cache_manager


def get_connection_stats() -> dict:
    """获取连接统计信息"""
    return connection_monitor.get_connection_stats()


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


def get_database_info() -> dict:
    """获取数据库信息"""
    try:
        with engine.connect() as conn:
            result = conn.execute("SELECT VERSION()")
            db_version = result.fetchone()[0]
            
            pool = engine.pool
            
            return {
                "database_version": db_version,
                "pool_size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                "invalid": pool.invalid()
            }
    except Exception as e:
        logger.error(f"Failed to get database info: {str(e)}")
        return {"error": str(e)}


def check_database_health() -> dict:
    """检查数据库健康状态"""
    health_info = {
        "status": "unknown",
        "connection_test": False,
        "response_time_ms": 0,
        "pool_info": {},
        "connection_stats": {},
        "cache_info": {}
    }
    
    try:
        # 测试数据库连接
        import time
        start_time = time.time()
        
        connection_test = test_connection()
        response_time = (time.time() - start_time) * 1000
        
        health_info.update({
            "status": "healthy" if connection_test else "unhealthy",
            "connection_test": connection_test,
            "response_time_ms": response_time,
            "pool_info": get_database_info(),
            "connection_stats": get_connection_stats()
        })
        
        # 检查缓存状态
        if cache_manager:
            try:
                cache_stats = cache_manager.get_cache_stats()
                health_info["cache_info"] = {
                    "status": "available",
                    "stats": cache_stats
                }
            except Exception as e:
                health_info["cache_info"] = {
                    "status": "error",
                    "error": str(e)
                }
        else:
            health_info["cache_info"] = {
                "status": "disabled"
            }
            
    except Exception as e:
        health_info.update({
            "status": "error",
            "error": str(e)
        })
        logger.error(f"Database health check failed: {str(e)}")
    
    return health_info
