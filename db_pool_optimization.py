#!/usr/bin/env python3
"""
数据库连接池优化建议
"""

import time
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db_context, engine

def analyze_connection_pool():
    """分析当前连接池配置和使用情况"""
    print("=" * 60)
    print("Database Connection Pool Analysis")
    print("=" * 60)
    print(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    print("\n[Current SQLAlchemy Pool Configuration]")

    # 从connection.py获取当前配置
    pool = engine.pool
    print(f"Pool Class: {type(pool).__name__}")
    print(f"Pool Size: {pool.size()}")
    print(f"Max Overflow: {engine.pool._max_overflow}")
    print(f"Pool Pre-ping: {engine.pool._pre_ping}")
    print(f"Pool Recycle: {engine.pool._recycle} seconds")

    # 当前连接池状态
    print(f"\n[Current Pool Status]")
    print(f"Checked In: {pool.checkedin()}")
    print(f"Checked Out: {pool.checkedout()}")
    print(f"Overflow: {pool.overflow()}")
    print(f"Total Pool Connections: {pool.size()}")

    # 计算利用率
    total_capacity = pool.size() + engine.pool._max_overflow
    current_usage = pool.checkedout() + pool.overflow()
    utilization = (current_usage / total_capacity * 100) if total_capacity > 0 else 0

    print(f"Pool Utilization: {utilization:.1f}%")

    try:
        with get_db_context() as db:
            # 数据库层面的连接统计
            print(f"\n[Database Server Connections]")

            total_connections = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist")).fetchone()[0]
            active_connections = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist WHERE command != 'Sleep'")).fetchone()[0]

            print(f"Total DB Connections: {total_connections}")
            print(f"Active DB Connections: {active_connections}")
            print(f"Sleeping DB Connections: {total_connections - active_connections}")

            # 数据库连接配置
            max_connections = db.execute(text("SHOW VARIABLES LIKE 'max_connections'")).fetchone()[1]
            wait_timeout = db.execute(text("SHOW VARIABLES LIKE 'wait_timeout'")).fetchone()[1]
            interactive_timeout = db.execute(text("SHOW VARIABLES LIKE 'interactive_timeout'")).fetchone()[1]

            print(f"\n[Database Server Configuration]")
            print(f"Max Connections: {max_connections}")
            print(f"Wait Timeout: {wait_timeout}s")
            print(f"Interactive Timeout: {interactive_timeout}s")

            # 计算数据库连接使用率
            db_utilization = (total_connections / int(max_connections) * 100)
            print(f"DB Connection Usage: {db_utilization:.1f}%")

    except Exception as e:
        print(f"Database analysis error: {e}")

    return {
        'pool_size': pool.size(),
        'max_overflow': engine.pool._max_overflow,
        'checked_out': pool.checkedout(),
        'pool_utilization': utilization,
        'total_db_connections': total_connections if 'total_connections' in locals() else 0,
        'db_utilization': db_utilization if 'db_utilization' in locals() else 0
    }

def generate_optimization_recommendations(analysis_data):
    """生成连接池优化建议"""
    print("\n" + "=" * 60)
    print("OPTIMIZATION RECOMMENDATIONS")
    print("=" * 60)

    recommendations = []

    # 1. 连接池大小建议
    pool_utilization = analysis_data['pool_utilization']
    if pool_utilization > 80:
        recommendations.append({
            'priority': 'HIGH',
            'category': 'Pool Size',
            'issue': f'Pool utilization is high ({pool_utilization:.1f}%)',
            'recommendation': 'Consider increasing pool_size from 25 to 35-40',
            'config_change': 'pool_size=35'
        })
    elif pool_utilization < 20:
        recommendations.append({
            'priority': 'LOW',
            'category': 'Pool Size',
            'issue': f'Pool utilization is low ({pool_utilization:.1f}%)',
            'recommendation': 'Consider reducing pool_size to save resources',
            'config_change': 'pool_size=15'
        })

    # 2. 溢出连接建议
    max_overflow = analysis_data['max_overflow']
    if max_overflow > 50:
        recommendations.append({
            'priority': 'MEDIUM',
            'category': 'Overflow',
            'issue': f'Max overflow is high ({max_overflow})',
            'recommendation': 'Consider reducing max_overflow to 25-30',
            'config_change': 'max_overflow=25'
        })

    # 3. 数据库连接数建议
    total_db_connections = analysis_data['total_db_connections']
    if total_db_connections > 100:
        recommendations.append({
            'priority': 'HIGH',
            'category': 'DB Connections',
            'issue': f'High database connection count ({total_db_connections})',
            'recommendation': 'Review application connection usage patterns',
            'config_change': 'Check for connection leaks'
        })

    # 4. 超时配置建议
    recommendations.append({
        'priority': 'MEDIUM',
        'category': 'Timeouts',
        'issue': 'Optimize timeout settings',
        'recommendation': 'Consider adjusting pool_recycle and connect timeouts',
        'config_change': 'pool_recycle=3600, connect_timeout=30'
    })

    # 5. 监控建议
    recommendations.append({
        'priority': 'LOW',
        'category': 'Monitoring',
        'issue': 'Need better connection monitoring',
        'recommendation': 'Implement connection pool metrics collection',
        'config_change': 'Add pool monitoring dashboard'
    })

    # 打印建议
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. [{rec['priority']}] {rec['category']}")
        print(f"   Issue: {rec['issue']}")
        print(f"   Recommendation: {rec['recommendation']}")
        print(f"   Config: {rec['config_change']}")

    return recommendations

def generate_optimized_config():
    """生成优化后的连接配置"""
    print("\n" + "=" * 60)
    print("OPTIMIZED CONNECTION CONFIGURATION")
    print("=" * 60)

    optimized_config = """
# Optimized SQLAlchemy Engine Configuration
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=30,                    # Increased from 25
    max_overflow=25,                 # Reduced from 35
    pool_pre_ping=True,              # Keep existing
    pool_recycle=3600,               # Increased from 1800 (1 hour)
    echo=False,                      # Keep existing
    future=True,                     # Keep existing
    connect_args={
        "charset": "utf8mb4",
        "connect_timeout": 30,       # Keep existing
        "read_timeout": 900,         # Keep existing
        "write_timeout": 900,        # Keep existing
        "autocommit": False,         # Keep existing
    },
)

# Additional recommended settings
SessionLocal = sessionmaker(
    autocommit=False,
    expire_on_commit=False,
    autoflush=False,
    bind=engine,
    future=True,
)

# Connection monitoring
def monitor_pool_status():
    pool = engine.pool
    return {
        "size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "utilization": (pool.checkedout() + pool.overflow()) / (pool.size() + engine.pool._max_overflow) * 100
    }
"""

    print(optimized_config)

def create_monitoring_script():
    """创建连接池监控脚本"""
    monitoring_script = '''
#!/usr/bin/env python3
"""
数据库连接池监控脚本 - 定期运行检查
"""

import time
from datetime import datetime
from app.database.connection import engine, get_db_context
from sqlalchemy import text

def monitor_connections():
    """监控连接状态"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 连接池状态
    pool = engine.pool
    pool_stats = {
        "timestamp": timestamp,
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "utilization": (pool.checkedout() + pool.overflow()) / (pool.size() + engine.pool._max_overflow) * 100
    }

    # 数据库连接状态
    try:
        with get_db_context() as db:
            total_conn = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist")).fetchone()[0]
            active_conn = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist WHERE command != 'Sleep'")).fetchone()[0]

        db_stats = {
            "total_connections": total_conn,
            "active_connections": active_conn,
            "sleeping_connections": total_conn - active_conn
        }
    except Exception as e:
        db_stats = {"error": str(e)}

    # 输出监控信息
    print(f"[{timestamp}] Pool: {pool_stats['utilization']:.1f}% | "
          f"DB Connections: {db_stats.get('total_connections', 'N/A')} | "
          f"Active: {db_stats.get('active_connections', 'N/A')}")

    # 警告检查
    if pool_stats['utilization'] > 80:
        print(f"WARNING: High pool utilization ({pool_stats['utilization']:.1f}%)")

    if db_stats.get('total_connections', 0) > 50:
        print(f"WARNING: High database connection count ({db_stats['total_connections']})")

    return pool_stats, db_stats

if __name__ == "__main__":
    # 可以作为定期任务运行
    # 或者添加到 crontab: */5 * * * * python db_monitor.py
    monitor_connections()
'''

    with open('db_monitor.py', 'w', encoding='utf-8') as f:
        f.write(monitoring_script)

    print("\n" + "=" * 60)
    print("MONITORING SCRIPT CREATED")
    print("=" * 60)
    print("Created: db_monitor.py")
    print("Usage: python db_monitor.py")
    print("Recommendation: Run every 5 minutes via cron job")

def main():
    """主函数"""
    # 1. 分析当前连接池
    analysis_data = analyze_connection_pool()

    # 2. 生成优化建议
    recommendations = generate_optimization_recommendations(analysis_data)

    # 3. 生成优化配置
    generate_optimized_config()

    # 4. 创建监控脚本
    create_monitoring_script()

    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    print(f"Total Recommendations: {len(recommendations)}")
    print("Next Steps:")
    print("1. Review and implement high priority recommendations")
    print("2. Update connection.py with optimized configuration")
    print("3. Deploy monitoring script for ongoing observation")
    print("4. Monitor performance after changes")

if __name__ == "__main__":
    main()