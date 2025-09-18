"""Database connection helpers and session utilities."""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from .cache import StatisticalDataCache, create_cache_manager
from .monitoring import DatabaseConnectionMonitor

# Configure module level logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Build database URL from environment variables when not provided directly
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

# Initialise SQLAlchemy engine and session factory
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=25,
    max_overflow=35,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=False,
    future=True,
    connect_args={
        "charset": "utf8mb4",
        "connect_timeout": 30,
        "read_timeout": 900,
        "write_timeout": 900,
        "autocommit": False,
    },
)

SessionLocal = sessionmaker(
    autocommit=False,
    expire_on_commit=False,
    autoflush=False,
    bind=engine,
    future=True,
)

connection_monitor = DatabaseConnectionMonitor()

try:
    cache_manager: Optional[StatisticalDataCache] = create_cache_manager()
except Exception as exc:  # pragma: no cover - defensive guard
    logger.warning("Failed to initialize cache manager: %s", exc)
    cache_manager = None

Base = declarative_base()


def _tune_session(session: Session) -> None:
    """Apply session level timeout tuning to reduce idle disconnects."""
    try:
        session.execute(text("SET SESSION wait_timeout=900"))
        session.execute(text("SET SESSION interactive_timeout=900"))
        session.execute(text("SET SESSION net_read_timeout=900"))
        session.execute(text("SET SESSION net_write_timeout=900"))
    except Exception:  # pragma: no cover - best effort tuning
        pass


def get_db() -> Generator[Session, None, None]:
    """Yield a database session and capture connection metrics."""
    start_time = time.time()
    db = SessionLocal()
    connection_time = time.time() - start_time
    connection_monitor.record_connection_created(connection_time)

    try:
        _tune_session(db)
        yield db
    except Exception as exc:
        logger.error("Database session error: %s", exc)
        connection_monitor.record_connection_error()
        db.rollback()
        raise
    finally:
        connection_monitor.record_connection_closed()
        db.close()


@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """Context manager wrapper around `get_db` for synchronous flows."""
    start_time = time.time()
    db = SessionLocal()
    connection_time = time.time() - start_time
    connection_monitor.record_connection_created(connection_time)

    try:
        _tune_session(db)
        yield db
        db.commit()
    except Exception as exc:
        logger.error("Database transaction error: %s", exc)
        connection_monitor.record_connection_error()
        db.rollback()
        raise
    finally:
        connection_monitor.record_connection_closed()
        db.close()


def get_cache_manager() -> Optional[StatisticalDataCache]:
    """Expose the lazily instantiated statistical cache manager."""
    return cache_manager


def get_connection_stats() -> Dict[str, Any]:
    """Return aggregated connection metrics captured by the monitor."""
    return connection_monitor.get_connection_stats()


def test_connection() -> bool:
    """Probe the database connection with a lightweight SELECT."""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Database connection successful")
        return True
    except Exception as exc:
        logger.error("Database connection failed: %s", exc)
        return False


def create_tables() -> None:
    """Create all mapped tables if they do not already exist."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("All tables created successfully")
    except Exception as exc:
        logger.error("Failed to create tables: %s", exc)
        raise


def get_database_info() -> Dict[str, Any]:
    """Collect lightweight metadata about the current database pool."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT VERSION()"))
            db_version = result.fetchone()[0]

        pool = engine.pool
        return {
            "database_version": db_version,
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid(),
        }
    except Exception as exc:
        logger.error("Failed to get database info: %s", exc)
        return {"error": str(exc)}


def check_database_health() -> Dict[str, Any]:
    """Return a structured health snapshot for operational monitoring."""
    health_info: Dict[str, Any] = {
        "status": "unknown",
        "connection_test": False,
        "response_time_ms": 0,
        "pool_info": {},
        "connection_stats": {},
        "cache_info": {},
    }

    try:
        start_time = time.time()
        connection_test = test_connection()
        response_time_ms = (time.time() - start_time) * 1000

        health_info.update(
            {
                "status": "healthy" if connection_test else "unhealthy",
                "connection_test": connection_test,
                "response_time_ms": response_time_ms,
                "pool_info": get_database_info(),
                "connection_stats": get_connection_stats(),
            }
        )

        if cache_manager:
            try:
                cache_stats = cache_manager.get_cache_stats()
                health_info["cache_info"] = {
                    "status": "available",
                    "stats": cache_stats,
                }
            except Exception as exc:  # pragma: no cover - cache optional
                health_info["cache_info"] = {
                    "status": "error",
                    "error": str(exc),
                }
        else:
            health_info["cache_info"] = {"status": "disabled"}

    except Exception as exc:
        health_info.update({"status": "error", "error": str(exc)})
        logger.error("Database health check failed: %s", exc)

    return health_info


def get_database_engine() -> Engine:
    """Legacy helper exposing the SQLAlchemy engine instance."""
    return engine


def get_session_factory() -> sessionmaker[Session]:
    """Return the configured session factory for backward compatibility."""
    return SessionLocal


def get_db_session() -> Generator[Session, None, None]:
    """Provide a session generator compatible with old dependency usage."""
    yield from get_db()