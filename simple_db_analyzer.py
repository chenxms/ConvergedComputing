#!/usr/bin/env python3
"""
简化数据库连接分析器 - 兼容性更好的版本
"""

import time
import logging
from datetime import datetime
from typing import Dict, List, Any
from sqlalchemy import text
from app.database.connection import get_db_context

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def analyze_database_connections():
    """分析数据库连接状态"""
    print("正在分析数据库连接状态...")

    try:
        with get_db_context() as db:
            # 1. 检查基本连接信息
            print("\n=== 基本连接信息 ===")

            # 获取总连接数
            result = db.execute(text("""
                SELECT COUNT(*) as total_connections FROM information_schema.processlist
            """)).fetchone()
            total_connections = result[0] if result else 0
            print(f"总连接数: {total_connections}")

            # 获取活跃连接数
            result = db.execute(text("""
                SELECT COUNT(*) as active_connections
                FROM information_schema.processlist
                WHERE command != 'Sleep'
            """)).fetchone()
            active_connections = result[0] if result else 0
            print(f"活跃连接数: {active_connections}")

            # 获取睡眠连接数
            sleeping_connections = total_connections - active_connections
            print(f"睡眠连接数: {sleeping_connections}")

            # 2. 检查长时间运行的查询
            print("\n=== 长时间运行的查询 ===")

            long_queries = db.execute(text("""
                SELECT
                    id, user, host, db, command, time, state,
                    SUBSTRING(COALESCE(info, ''), 1, 100) as query_preview
                FROM information_schema.processlist
                WHERE command != 'Sleep' AND time > 10
                ORDER BY time DESC
                LIMIT 10
            """)).fetchall()

            if long_queries:
                print(f"发现 {len(long_queries)} 个长时间运行的查询:")
                for query in long_queries:
                    print(f"  ID: {query[0]}, 用户: {query[1]}, 运行时间: {query[5]}秒")
                    print(f"  状态: {query[6]}, 查询: {query[7]}")
                    print()
            else:
                print("没有发现长时间运行的查询")

            # 3. 检查连接状态分布
            print("\n=== 连接状态分布 ===")

            states = db.execute(text("""
                SELECT
                    COALESCE(state, 'NULL') as state,
                    command,
                    COUNT(*) as count
                FROM information_schema.processlist
                GROUP BY state, command
                ORDER BY count DESC
            """)).fetchall()

            for state in states:
                print(f"状态: {state[0]}, 命令: {state[1]}, 数量: {state[2]}")

            # 4. 检查数据库版本和基本配置
            print("\n=== 数据库配置信息 ===")

            version = db.execute(text("SELECT VERSION()")).fetchone()
            print(f"数据库版本: {version[0] if version else 'Unknown'}")

            max_connections = db.execute(text("""
                SHOW VARIABLES LIKE 'max_connections'
            """)).fetchone()
            if max_connections:
                print(f"最大连接数: {max_connections[1]}")
                utilization = (total_connections / int(max_connections[1]) * 100)
                print(f"连接使用率: {utilization:.1f}%")

            # 5. 检查慢查询配置
            print("\n=== 慢查询配置 ===")

            slow_query_log = db.execute(text("""
                SHOW VARIABLES LIKE 'slow_query_log'
            """)).fetchone()

            long_query_time = db.execute(text("""
                SHOW VARIABLES LIKE 'long_query_time'
            """)).fetchone()

            if slow_query_log:
                print(f"慢查询日志: {slow_query_log[1]}")
            if long_query_time:
                print(f"慢查询阈值: {long_query_time[1]}秒")

            # 6. 生成清理建议
            print("\n=== 清理建议 ===")

            # 检查长时间睡眠的连接
            long_sleep = db.execute(text("""
                SELECT COUNT(*) as count
                FROM information_schema.processlist
                WHERE command = 'Sleep' AND time > 300
            """)).fetchone()

            long_sleep_count = long_sleep[0] if long_sleep else 0

            if long_sleep_count > 0:
                print(f"发现 {long_sleep_count} 个长时间睡眠连接 (>5分钟)")
                print("建议: 可以考虑清理这些连接")

            if len(long_queries) > 3:
                print(f"发现 {len(long_queries)} 个长时间运行查询")
                print("建议: 检查并优化这些查询")

            if total_connections > 50:
                print(f"总连接数较高: {total_connections}")
                print("建议: 检查应用程序连接池配置")

            return {
                'total_connections': total_connections,
                'active_connections': active_connections,
                'sleeping_connections': sleeping_connections,
                'long_queries': len(long_queries),
                'long_sleep_connections': long_sleep_count
            }

    except Exception as e:
        print(f"分析过程中出错: {e}")
        return None


def kill_long_sleeping_connections():
    """清理长时间睡眠的连接"""
    print("\n正在清理长时间睡眠的连接...")

    try:
        with get_db_context() as db:
            # 获取长时间睡眠的连接
            long_sleep_connections = db.execute(text("""
                SELECT id, user, host, time
                FROM information_schema.processlist
                WHERE command = 'Sleep'
                  AND time > 600  -- 10分钟
                  AND user != 'system user'
                  AND id != CONNECTION_ID()
            """)).fetchall()

            killed_count = 0
            for conn in long_sleep_connections:
                try:
                    db.execute(text(f"KILL {conn[0]}"))
                    print(f"已终止连接: ID={conn[0]}, 用户={conn[1]}, 睡眠时间={conn[3]}秒")
                    killed_count += 1
                except Exception as e:
                    print(f"终止连接 {conn[0]} 失败: {e}")

            db.commit()
            print(f"总共终止了 {killed_count} 个长时间睡眠连接")
            return killed_count

    except Exception as e:
        print(f"清理连接时出错: {e}")
        return 0


def kill_long_running_queries(time_threshold=300):
    """终止长时间运行的查询"""
    print(f"\n正在终止运行时间超过{time_threshold}秒的查询...")

    try:
        with get_db_context() as db:
            # 获取长时间运行的查询
            long_queries = db.execute(text("""
                SELECT id, user, host, db, command, time, state, info
                FROM information_schema.processlist
                WHERE command != 'Sleep'
                  AND time > :time_threshold
                  AND user != 'system user'
                  AND id != CONNECTION_ID()
            """), {"time_threshold": time_threshold}).fetchall()

            killed_count = 0
            for query in long_queries:
                try:
                    db.execute(text(f"KILL {query[0]}"))
                    print(f"已终止查询: ID={query[0]}, 用户={query[1]}, 运行时间={query[5]}秒")
                    if query[7]:
                        print(f"  查询内容: {query[7][:100]}...")
                    killed_count += 1
                except Exception as e:
                    print(f"终止查询 {query[0]} 失败: {e}")

            db.commit()
            print(f"总共终止了 {killed_count} 个长时间运行查询")
            return killed_count

    except Exception as e:
        print(f"终止查询时出错: {e}")
        return 0


def optimize_database():
    """执行数据库优化"""
    print("\n正在执行数据库优化...")

    try:
        with get_db_context() as db:
            optimizations = []

            # 1. 刷新查询缓存
            try:
                db.execute(text("FLUSH QUERY CACHE"))
                optimizations.append("已刷新查询缓存")
            except Exception:
                pass  # 查询缓存可能未启用

            # 2. 刷新表缓存
            try:
                db.execute(text("FLUSH TABLES"))
                optimizations.append("已刷新表缓存")
            except Exception as e:
                print(f"刷新表缓存失败: {e}")

            # 3. 分析表（如果需要）
            # 这里可以添加特定表的分析，但要谨慎使用

            db.commit()

            for opt in optimizations:
                print(f"  - {opt}")

            return len(optimizations)

    except Exception as e:
        print(f"优化过程中出错: {e}")
        return 0


def main():
    """主函数"""

    print("=" * 50)
    print("数据库连接分析器")
    print("=" * 50)

    # 执行分析
    analysis_result = analyze_database_connections()

    if not analysis_result:
        print("分析失败，请检查数据库连接")
        return

    # 询问是否执行清理
    print("\n" + "=" * 50)

    # 清理长时间睡眠连接
    if analysis_result['long_sleep_connections'] > 0:
        choice = input(f"发现 {analysis_result['long_sleep_connections']} 个长时间睡眠连接，是否清理? (y/n): ")
        if choice.lower() == 'y':
            kill_long_sleeping_connections()

    # 清理长时间运行查询
    if analysis_result['long_queries'] > 0:
        choice = input(f"发现 {analysis_result['long_queries']} 个长时间运行查询，是否终止? (y/n): ")
        if choice.lower() == 'y':
            kill_long_running_queries(300)  # 5分钟阈值

    # 执行优化
    choice = input("是否执行数据库优化操作? (y/n): ")
    if choice.lower() == 'y':
        optimize_database()

    print("\n分析完成！建议定期运行此脚本监控数据库状态。")


if __name__ == "__main__":
    main()