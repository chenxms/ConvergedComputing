#!/usr/bin/env python3
"""
数据库状态检查 - 非交互式版本
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


def check_database_status():
    """检查数据库状态并提供清理建议"""
    print("=" * 60)
    print("数据库连接状态检查报告")
    print("=" * 60)
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        with get_db_context() as db:
            # 1. 基本连接统计
            print("\n【连接统计】")

            total_result = db.execute(text("""
                SELECT COUNT(*) FROM information_schema.processlist
            """)).fetchone()
            total_connections = total_result[0] if total_result else 0

            active_result = db.execute(text("""
                SELECT COUNT(*) FROM information_schema.processlist WHERE command != 'Sleep'
            """)).fetchone()
            active_connections = active_result[0] if active_result else 0

            sleeping_connections = total_connections - active_connections

            print(f"总连接数: {total_connections}")
            print(f"活跃连接: {active_connections}")
            print(f"睡眠连接: {sleeping_connections}")

            # 2. 长时间运行的查询
            print("\n【长时间查询】")

            long_queries = db.execute(text("""
                SELECT id, user, host, time, command, state,
                       SUBSTRING(COALESCE(info, ''), 1, 80) as query_preview
                FROM information_schema.processlist
                WHERE command != 'Sleep' AND time > 10 AND user != 'event_scheduler'
                ORDER BY time DESC
                LIMIT 10
            """)).fetchall()

            if long_queries:
                print(f"发现 {len(long_queries)} 个长时间运行查询:")
                for i, query in enumerate(long_queries, 1):
                    print(f"  {i}. ID:{query[0]} 用户:{query[1]} 时间:{query[3]}秒")
                    print(f"     状态:{query[5]} 命令:{query[4]}")
                    if query[6]:
                        print(f"     查询:{query[6]}")
                    print()
            else:
                print("✓ 没有发现长时间运行的查询")

            # 3. 长时间睡眠连接
            print("【睡眠连接分析】")

            sleep_analysis = db.execute(text("""
                SELECT
                    CASE
                        WHEN time < 60 THEN '< 1分钟'
                        WHEN time < 300 THEN '1-5分钟'
                        WHEN time < 600 THEN '5-10分钟'
                        WHEN time < 1800 THEN '10-30分钟'
                        ELSE '> 30分钟'
                    END as time_range,
                    COUNT(*) as count
                FROM information_schema.processlist
                WHERE command = 'Sleep'
                GROUP BY
                    CASE
                        WHEN time < 60 THEN '< 1分钟'
                        WHEN time < 300 THEN '1-5分钟'
                        WHEN time < 600 THEN '5-10分钟'
                        WHEN time < 1800 THEN '10-30分钟'
                        ELSE '> 30分钟'
                    END
                ORDER BY MIN(time)
            """)).fetchall()

            for range_info in sleep_analysis:
                print(f"  {range_info[0]}: {range_info[1]} 个连接")

            # 检查超长时间睡眠连接
            long_sleep = db.execute(text("""
                SELECT id, user, host, time
                FROM information_schema.processlist
                WHERE command = 'Sleep' AND time > 600 AND user != 'system user'
                ORDER BY time DESC
                LIMIT 5
            """)).fetchall()

            if long_sleep:
                print(f"\n⚠️ 发现 {len(long_sleep)} 个超长时间睡眠连接 (>10分钟):")
                for conn in long_sleep:
                    minutes = conn[3] // 60
                    print(f"  ID:{conn[0]} 用户:{conn[1]} 主机:{conn[2]} 睡眠:{minutes}分钟")

            # 4. 数据库配置检查
            print("\n【数据库配置】")

            version = db.execute(text("SELECT VERSION()")).fetchone()
            print(f"数据库版本: {version[0] if version else 'Unknown'}")

            max_conn = db.execute(text("SHOW VARIABLES LIKE 'max_connections'")).fetchone()
            if max_conn:
                max_connections = int(max_conn[1])
                utilization = (total_connections / max_connections * 100)
                print(f"最大连接数: {max_connections}")
                print(f"连接使用率: {utilization:.1f}%")

                if utilization > 80:
                    print("⚠️ 警告: 连接使用率过高!")
                elif utilization > 60:
                    print("⚠️ 注意: 连接使用率较高")

            # 5. 慢查询配置
            print("\n【慢查询配置】")

            slow_log = db.execute(text("SHOW VARIABLES LIKE 'slow_query_log'")).fetchone()
            long_time = db.execute(text("SHOW VARIABLES LIKE 'long_query_time'")).fetchone()

            print(f"慢查询日志: {slow_log[1] if slow_log else 'Unknown'}")
            print(f"慢查询阈值: {long_time[1] if long_time else 'Unknown'}秒")

            # 6. 生成清理建议
            print("\n" + "=" * 60)
            print("【清理建议】")

            recommendations = []

            # 长时间睡眠连接建议
            if len(long_sleep) > 0:
                recommendations.append(f"🔧 清理 {len(long_sleep)} 个长时间睡眠连接")
                recommendations.append("   命令: 可以执行 KILL <connection_id> 来终止")

            # 长时间查询建议
            if len(long_queries) > 0:
                recommendations.append(f"🔧 检查 {len(long_queries)} 个长时间运行查询")
                recommendations.append("   建议: 分析查询性能，添加索引或优化SQL")

            # 连接数建议
            if total_connections > 50:
                recommendations.append(f"🔧 总连接数较高 ({total_connections})")
                recommendations.append("   建议: 检查应用程序连接池配置")

            # 慢查询建议
            if slow_log and slow_log[1] == 'OFF':
                recommendations.append("🔧 建议开启慢查询日志")
                recommendations.append("   命令: SET GLOBAL slow_query_log = 'ON'")

            if recommendations:
                for rec in recommendations:
                    print(rec)
            else:
                print("✓ 数据库连接状态良好，暂无需要清理的项目")

            # 7. 可执行的清理命令
            if long_sleep:
                print("\n【可执行的清理命令】")
                print("# 清理长时间睡眠连接:")
                for conn in long_sleep[:3]:  # 只显示前3个
                    print(f"KILL {conn[0]};  -- 用户:{conn[1]}, 睡眠:{conn[3]//60}分钟")

            print("\n" + "=" * 60)
            print("检查完成!")
            print("建议: 定期运行此脚本监控数据库连接状态")
            print("=" * 60)

            return {
                'total_connections': total_connections,
                'active_connections': active_connections,
                'sleeping_connections': sleeping_connections,
                'long_queries_count': len(long_queries),
                'long_sleep_count': len(long_sleep),
                'recommendations': len(recommendations)
            }

    except Exception as e:
        print(f"❌ 检查过程中出错: {e}")
        return None


def cleanup_database_connections():
    """清理数据库连接 - 仅显示命令，不实际执行"""
    print("\n" + "=" * 60)
    print("数据库连接清理脚本")
    print("=" * 60)

    try:
        with get_db_context() as db:
            # 获取需要清理的连接
            long_sleep = db.execute(text("""
                SELECT id, user, host, time
                FROM information_schema.processlist
                WHERE command = 'Sleep'
                  AND time > 600
                  AND user != 'system user'
                  AND id != CONNECTION_ID()
                ORDER BY time DESC
            """)).fetchall()

            long_queries = db.execute(text("""
                SELECT id, user, host, time, SUBSTRING(COALESCE(info, ''), 1, 50) as query
                FROM information_schema.processlist
                WHERE command != 'Sleep'
                  AND time > 300
                  AND user != 'system user'
                  AND user != 'event_scheduler'
                  AND id != CONNECTION_ID()
                ORDER BY time DESC
            """)).fetchall()

            if long_sleep:
                print(f"发现 {len(long_sleep)} 个长时间睡眠连接:")
                print("-- 清理长时间睡眠连接的SQL命令:")
                for conn in long_sleep:
                    minutes = conn[3] // 60
                    print(f"KILL {conn[0]};  -- 用户:{conn[1]}, 睡眠:{minutes}分钟")

            if long_queries:
                print(f"\n发现 {len(long_queries)} 个长时间运行查询:")
                print("-- 终止长时间查询的SQL命令:")
                for query in long_queries:
                    minutes = query[3] // 60
                    print(f"KILL {query[0]};  -- 用户:{query[1]}, 运行:{minutes}分钟, 查询:{query[4]}")

            if not long_sleep and not long_queries:
                print("✓ 没有发现需要清理的连接")

            # 生成优化命令
            print("\n-- 数据库优化命令:")
            print("FLUSH QUERY CACHE;  -- 清理查询缓存")
            print("FLUSH TABLES;       -- 刷新表缓存")

    except Exception as e:
        print(f"❌ 生成清理命令时出错: {e}")


if __name__ == "__main__":
    # 执行状态检查
    result = check_database_status()

    # 如果发现问题，显示清理命令
    if result and (result['long_sleep_count'] > 0 or result['long_queries_count'] > 0):
        cleanup_database_connections()