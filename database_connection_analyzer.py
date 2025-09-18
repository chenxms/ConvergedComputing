#!/usr/bin/env python3
"""
数据库连接分析器 - 检查活跃连接、锁定和性能问题
"""

import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from app.database.connection import get_db_context, get_database_info, check_database_health

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseConnectionAnalyzer:
    """数据库连接和性能分析器"""

    def __init__(self):
        self.analysis_time = datetime.now()

    def analyze_all(self) -> Dict[str, Any]:
        """执行完整的数据库分析"""
        logger.info("开始数据库连接分析...")

        analysis_result = {
            "analysis_time": self.analysis_time.isoformat(),
            "database_health": {},
            "active_connections": {},
            "process_list": [],
            "lock_analysis": {},
            "slow_queries": [],
            "performance_metrics": {},
            "recommendations": []
        }

        try:
            # 1. 检查数据库健康状态
            analysis_result["database_health"] = self._check_database_health()

            # 2. 分析活跃连接
            analysis_result["active_connections"] = self._analyze_active_connections()

            # 3. 获取进程列表
            analysis_result["process_list"] = self._get_process_list()

            # 4. 分析锁定情况
            analysis_result["lock_analysis"] = self._analyze_locks()

            # 5. 检查慢查询
            analysis_result["slow_queries"] = self._get_slow_queries()

            # 6. 获取性能指标
            analysis_result["performance_metrics"] = self._get_performance_metrics()

            # 7. 生成建议
            analysis_result["recommendations"] = self._generate_recommendations(analysis_result)

        except Exception as e:
            logger.error(f"分析过程中出错: {e}")
            analysis_result["error"] = str(e)

        return analysis_result

    def _check_database_health(self) -> Dict[str, Any]:
        """检查数据库健康状态"""
        logger.info("检查数据库健康状态...")

        try:
            health_info = check_database_health()
            db_info = get_database_info()

            return {
                "health_status": health_info,
                "pool_info": db_info,
                "connection_pool_utilization": self._calculate_pool_utilization(db_info)
            }
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return {"error": str(e)}

    def _calculate_pool_utilization(self, db_info: Dict[str, Any]) -> Dict[str, Any]:
        """计算连接池利用率"""
        try:
            pool_size = db_info.get("pool_size", 0)
            checked_out = db_info.get("checked_out", 0)
            overflow = db_info.get("overflow", 0)

            utilization_rate = (checked_out / pool_size * 100) if pool_size > 0 else 0

            return {
                "pool_size": pool_size,
                "checked_out": checked_out,
                "overflow": overflow,
                "utilization_rate": round(utilization_rate, 2),
                "available_connections": pool_size - checked_out,
                "status": "high" if utilization_rate > 80 else "normal" if utilization_rate > 50 else "low"
            }
        except Exception as e:
            return {"error": str(e)}

    def _analyze_active_connections(self) -> Dict[str, Any]:
        """分析活跃连接"""
        logger.info("分析活跃数据库连接...")

        try:
            with get_db_context() as db:
                # 获取连接数统计
                result = db.execute(text("""
                    SELECT
                        COUNT(*) as total_connections,
                        SUM(CASE WHEN command != 'Sleep' THEN 1 ELSE 0 END) as active_connections,
                        SUM(CASE WHEN command = 'Sleep' THEN 1 ELSE 0 END) as sleeping_connections,
                        SUM(CASE WHEN state LIKE '%lock%' THEN 1 ELSE 0 END) as locked_connections
                    FROM information_schema.processlist
                """)).fetchone()

                connection_stats = {
                    "total_connections": result[0] if result else 0,
                    "active_connections": result[1] if result else 0,
                    "sleeping_connections": result[2] if result else 0,
                    "locked_connections": result[3] if result else 0
                }

                # 获取按状态分组的连接
                state_result = db.execute(text("""
                    SELECT
                        COALESCE(state, 'NULL') as state,
                        command,
                        COUNT(*) as count
                    FROM information_schema.processlist
                    GROUP BY state, command
                    ORDER BY count DESC
                """)).fetchall()

                connection_by_state = [
                    {"state": row[0], "command": row[1], "count": row[2]}
                    for row in state_result
                ]

                return {
                    "summary": connection_stats,
                    "by_state": connection_by_state,
                    "analysis_time": datetime.now().isoformat()
                }
        except Exception as e:
            logger.error(f"连接分析失败: {e}")
            return {"error": str(e)}

    def _get_process_list(self) -> List[Dict[str, Any]]:
        """获取进程列表（长时间运行的查询）"""
        logger.info("获取数据库进程列表...")

        try:
            with get_db_context() as db:
                result = db.execute(text("""
                    SELECT
                        id,
                        user,
                        host,
                        db,
                        command,
                        time,
                        state,
                        LEFT(info, 200) as query_preview
                    FROM information_schema.processlist
                    WHERE command != 'Sleep'
                        AND time > 5  -- 运行超过5秒的查询
                    ORDER BY time DESC
                    LIMIT 50
                """)).fetchall()

                processes = []
                for row in result:
                    processes.append({
                        "id": row[0],
                        "user": row[1],
                        "host": row[2],
                        "database": row[3],
                        "command": row[4],
                        "time_seconds": row[5],
                        "state": row[6],
                        "query_preview": row[7],
                        "is_long_running": row[5] > 30  # 超过30秒认为是长时间运行
                    })

                return processes
        except Exception as e:
            logger.error(f"获取进程列表失败: {e}")
            return [{"error": str(e)}]

    def _analyze_locks(self) -> Dict[str, Any]:
        """分析数据库锁定情况"""
        logger.info("分析数据库锁定情况...")

        try:
            with get_db_context() as db:
                # 检查表锁
                table_locks = db.execute(text("""
                    SELECT
                        table_schema,
                        table_name,
                        lock_type,
                        lock_duration,
                        COUNT(*) as lock_count
                    FROM performance_schema.table_locks
                    WHERE lock_duration != 'STATEMENT'
                    GROUP BY table_schema, table_name, lock_type, lock_duration
                    ORDER BY lock_count DESC
                    LIMIT 20
                """)).fetchall()

                # 检查元数据锁
                metadata_locks = db.execute(text("""
                    SELECT
                        object_schema,
                        object_name,
                        lock_type,
                        lock_status,
                        COUNT(*) as count
                    FROM performance_schema.metadata_locks
                    WHERE lock_status = 'PENDING'
                    GROUP BY object_schema, object_name, lock_type, lock_status
                    ORDER BY count DESC
                    LIMIT 20
                """)).fetchall()

                # 检查等待的事务
                waiting_transactions = db.execute(text("""
                    SELECT
                        r.trx_id as requesting_trx_id,
                        r.trx_mysql_thread_id as requesting_thread,
                        b.trx_id as blocking_trx_id,
                        b.trx_mysql_thread_id as blocking_thread,
                        r.trx_query as requesting_query,
                        b.trx_query as blocking_query
                    FROM information_schema.innodb_lock_waits w
                    INNER JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id
                    INNER JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
                    LIMIT 10
                """)).fetchall()

                return {
                    "table_locks": [
                        {
                            "schema": row[0],
                            "table": row[1],
                            "lock_type": row[2],
                            "duration": row[3],
                            "count": row[4]
                        }
                        for row in table_locks
                    ],
                    "metadata_locks": [
                        {
                            "schema": row[0],
                            "object": row[1],
                            "lock_type": row[2],
                            "status": row[3],
                            "count": row[4]
                        }
                        for row in metadata_locks
                    ],
                    "lock_waits": [
                        {
                            "requesting_trx_id": row[0],
                            "requesting_thread": row[1],
                            "blocking_trx_id": row[2],
                            "blocking_thread": row[3],
                            "requesting_query": row[4],
                            "blocking_query": row[5]
                        }
                        for row in waiting_transactions
                    ],
                    "lock_summary": {
                        "table_locks_count": len(table_locks),
                        "metadata_locks_count": len(metadata_locks),
                        "waiting_transactions_count": len(waiting_transactions)
                    }
                }
        except Exception as e:
            logger.error(f"锁分析失败: {e}")
            return {"error": str(e)}

    def _get_slow_queries(self) -> List[Dict[str, Any]]:
        """获取慢查询信息"""
        logger.info("检查慢查询...")

        try:
            with get_db_context() as db:
                # 检查慢查询日志是否开启
                slow_query_status = db.execute(text("""
                    SHOW VARIABLES LIKE 'slow_query_log'
                """)).fetchone()

                slow_query_file = db.execute(text("""
                    SHOW VARIABLES LIKE 'slow_query_log_file'
                """)).fetchone()

                long_query_time = db.execute(text("""
                    SHOW VARIABLES LIKE 'long_query_time'
                """)).fetchone()

                # 从performance_schema获取最近的慢查询
                recent_slow_queries = db.execute(text("""
                    SELECT
                        digest_text,
                        count_star as exec_count,
                        avg_timer_wait/1000000000 as avg_time_seconds,
                        max_timer_wait/1000000000 as max_time_seconds,
                        sum_lock_time/1000000000 as total_lock_time_seconds,
                        sum_rows_examined as total_rows_examined,
                        sum_rows_sent as total_rows_sent
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE avg_timer_wait/1000000000 > 1.0  -- 平均执行时间超过1秒
                    ORDER BY avg_timer_wait DESC
                    LIMIT 20
                """)).fetchall()

                return {
                    "slow_query_config": {
                        "slow_query_log": slow_query_status[1] if slow_query_status else "Unknown",
                        "slow_query_log_file": slow_query_file[1] if slow_query_file else "Unknown",
                        "long_query_time": float(long_query_time[1]) if long_query_time else 0.0
                    },
                    "recent_slow_queries": [
                        {
                            "query": row[0][:200] + "..." if len(row[0]) > 200 else row[0],
                            "exec_count": row[1],
                            "avg_time_seconds": round(row[2], 3),
                            "max_time_seconds": round(row[3], 3),
                            "total_lock_time": round(row[4], 3),
                            "total_rows_examined": row[5],
                            "total_rows_sent": row[6]
                        }
                        for row in recent_slow_queries
                    ]
                }
        except Exception as e:
            logger.error(f"慢查询检查失败: {e}")
            return {"error": str(e)}

    def _get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        logger.info("收集性能指标...")

        try:
            with get_db_context() as db:
                # 获取关键性能指标
                metrics_queries = {
                    "connections": "SHOW STATUS LIKE 'Connections'",
                    "aborted_connects": "SHOW STATUS LIKE 'Aborted_connects'",
                    "max_connections": "SHOW VARIABLES LIKE 'max_connections'",
                    "threads_connected": "SHOW STATUS LIKE 'Threads_connected'",
                    "threads_running": "SHOW STATUS LIKE 'Threads_running'",
                    "innodb_buffer_pool_size": "SHOW VARIABLES LIKE 'innodb_buffer_pool_size'",
                    "innodb_buffer_pool_pages_total": "SHOW STATUS LIKE 'Innodb_buffer_pool_pages_total'",
                    "innodb_buffer_pool_pages_free": "SHOW STATUS LIKE 'Innodb_buffer_pool_pages_free'",
                    "queries": "SHOW STATUS LIKE 'Queries'",
                    "uptime": "SHOW STATUS LIKE 'Uptime'"
                }

                metrics = {}
                for metric_name, query in metrics_queries.items():
                    try:
                        result = db.execute(text(query)).fetchone()
                        metrics[metric_name] = result[1] if result else "Unknown"
                    except Exception as e:
                        metrics[metric_name] = f"Error: {str(e)}"

                # 计算一些衍生指标
                try:
                    threads_connected = int(metrics.get("threads_connected", 0))
                    max_connections = int(metrics.get("max_connections", 1))
                    connection_usage = (threads_connected / max_connections * 100) if max_connections > 0 else 0

                    buffer_pool_total = int(metrics.get("innodb_buffer_pool_pages_total", 0))
                    buffer_pool_free = int(metrics.get("innodb_buffer_pool_pages_free", 0))
                    buffer_pool_usage = ((buffer_pool_total - buffer_pool_free) / buffer_pool_total * 100) if buffer_pool_total > 0 else 0

                    metrics["derived"] = {
                        "connection_usage_percent": round(connection_usage, 2),
                        "buffer_pool_usage_percent": round(buffer_pool_usage, 2)
                    }
                except (ValueError, TypeError) as e:
                    metrics["derived"] = {"error": str(e)}

                return metrics
        except Exception as e:
            logger.error(f"性能指标收集失败: {e}")
            return {"error": str(e)}

    def _generate_recommendations(self, analysis_result: Dict[str, Any]) -> List[Dict[str, str]]:
        """基于分析结果生成优化建议"""
        recommendations = []

        try:
            # 检查连接池利用率
            pool_util = analysis_result.get("database_health", {}).get("pool_info", {})
            utilization = pool_util.get("utilization_rate", 0)

            if utilization > 80:
                recommendations.append({
                    "type": "connection_pool",
                    "priority": "high",
                    "issue": "连接池使用率过高",
                    "recommendation": "考虑增加连接池大小或优化长时间连接的使用",
                    "current_value": f"{utilization}%"
                })

            # 检查长时间运行的查询
            long_running_processes = [p for p in analysis_result.get("process_list", [])
                                    if isinstance(p, dict) and p.get("is_long_running", False)]

            if long_running_processes:
                recommendations.append({
                    "type": "long_running_queries",
                    "priority": "high",
                    "issue": f"发现{len(long_running_processes)}个长时间运行的查询",
                    "recommendation": "检查并优化长时间运行的查询，考虑添加索引或重写查询",
                    "current_value": f"{len(long_running_processes)} queries"
                })

            # 检查锁等待
            lock_waits = analysis_result.get("lock_analysis", {}).get("lock_waits", [])
            if lock_waits:
                recommendations.append({
                    "type": "lock_contention",
                    "priority": "critical",
                    "issue": f"发现{len(lock_waits)}个锁等待情况",
                    "recommendation": "立即检查阻塞的事务，考虑终止长时间运行的事务",
                    "current_value": f"{len(lock_waits)} lock waits"
                })

            # 检查慢查询
            slow_queries = analysis_result.get("slow_queries", {}).get("recent_slow_queries", [])
            if len(slow_queries) > 5:
                recommendations.append({
                    "type": "slow_queries",
                    "priority": "medium",
                    "issue": f"发现{len(slow_queries)}个慢查询",
                    "recommendation": "分析慢查询日志，优化查询性能，添加适当的索引",
                    "current_value": f"{len(slow_queries)} slow queries"
                })

            # 检查性能指标
            performance = analysis_result.get("performance_metrics", {})
            derived = performance.get("derived", {})

            connection_usage = derived.get("connection_usage_percent", 0)
            if connection_usage > 70:
                recommendations.append({
                    "type": "connection_usage",
                    "priority": "medium",
                    "issue": "数据库连接使用率较高",
                    "recommendation": "监控连接使用情况，考虑优化应用程序的连接管理",
                    "current_value": f"{connection_usage}%"
                })

            buffer_pool_usage = derived.get("buffer_pool_usage_percent", 0)
            if buffer_pool_usage > 90:
                recommendations.append({
                    "type": "buffer_pool",
                    "priority": "medium",
                    "issue": "InnoDB缓冲池使用率过高",
                    "recommendation": "考虑增加innodb_buffer_pool_size的大小",
                    "current_value": f"{buffer_pool_usage}%"
                })

        except Exception as e:
            logger.error(f"生成建议时出错: {e}")
            recommendations.append({
                "type": "analysis_error",
                "priority": "low",
                "issue": "建议生成过程中出现错误",
                "recommendation": "请检查数据库连接和权限设置",
                "current_value": str(e)
            })

        return recommendations

    def kill_long_running_queries(self, time_threshold: int = 300) -> Dict[str, Any]:
        """终止长时间运行的查询（超过指定秒数）"""
        logger.warning(f"准备终止运行时间超过{time_threshold}秒的查询...")

        killed_processes = []
        errors = []

        try:
            with get_db_context() as db:
                # 获取长时间运行的查询
                long_running = db.execute(text("""
                    SELECT id, user, host, db, command, time, state, info
                    FROM information_schema.processlist
                    WHERE command != 'Sleep'
                        AND time > :time_threshold
                        AND user != 'system user'  -- 不终止系统进程
                        AND id != CONNECTION_ID()  -- 不终止当前连接
                """), {"time_threshold": time_threshold}).fetchall()

                for process in long_running:
                    process_id = process[0]
                    try:
                        db.execute(text(f"KILL {process_id}"))
                        killed_processes.append({
                            "id": process_id,
                            "user": process[1],
                            "host": process[2],
                            "database": process[3],
                            "command": process[4],
                            "time_seconds": process[5],
                            "state": process[6],
                            "query": process[7][:100] if process[7] else "NULL"
                        })
                        logger.info(f"已终止进程 {process_id} (运行时间: {process[5]}秒)")
                    except Exception as e:
                        error_msg = f"终止进程 {process_id} 失败: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)

                db.commit()

        except Exception as e:
            error_msg = f"终止长时间查询时出错: {str(e)}"
            errors.append(error_msg)
            logger.error(error_msg)

        return {
            "killed_count": len(killed_processes),
            "killed_processes": killed_processes,
            "errors": errors,
            "time_threshold": time_threshold
        }

    def optimize_connections(self) -> Dict[str, Any]:
        """优化数据库连接"""
        logger.info("执行数据库连接优化...")

        optimizations = []

        try:
            with get_db_context() as db:
                # 1. 清理睡眠连接
                sleep_connections = db.execute(text("""
                    SELECT id, user, host, time
                    FROM information_schema.processlist
                    WHERE command = 'Sleep'
                        AND time > 600  -- 睡眠超过10分钟
                        AND user != 'system user'
                """)).fetchall()

                killed_sleep = 0
                for conn in sleep_connections:
                    try:
                        db.execute(text(f"KILL {conn[0]}"))
                        killed_sleep += 1
                    except Exception as e:
                        logger.error(f"终止睡眠连接 {conn[0]} 失败: {e}")

                if killed_sleep > 0:
                    optimizations.append(f"清理了{killed_sleep}个长时间睡眠的连接")

                # 2. 刷新查询缓存（如果启用）
                try:
                    db.execute(text("FLUSH QUERY CACHE"))
                    optimizations.append("刷新了查询缓存")
                except Exception:
                    pass  # 查询缓存可能未启用

                # 3. 刷新表缓存
                try:
                    db.execute(text("FLUSH TABLES"))
                    optimizations.append("刷新了表缓存")
                except Exception as e:
                    logger.error(f"刷新表缓存失败: {e}")

                db.commit()

        except Exception as e:
            logger.error(f"连接优化失败: {e}")
            return {"error": str(e)}

        return {
            "optimizations_applied": optimizations,
            "timestamp": datetime.now().isoformat()
        }


def main():
    """主函数 - 执行数据库分析"""
    analyzer = DatabaseConnectionAnalyzer()

    print("=" * 60)
    print("数据库连接分析器")
    print("=" * 60)

    # 执行完整分析
    analysis = analyzer.analyze_all()

    # 打印分析结果摘要
    print(f"\n📊 分析时间: {analysis['analysis_time']}")

    # 数据库健康状态
    health = analysis.get("database_health", {})
    if "health_status" in health:
        status = health["health_status"].get("status", "unknown")
        print(f"🏥 数据库状态: {status}")

    # 连接统计
    conn_summary = analysis.get("active_connections", {}).get("summary", {})
    if conn_summary:
        print(f"🔗 总连接数: {conn_summary.get('total_connections', 0)}")
        print(f"⚡ 活跃连接: {conn_summary.get('active_connections', 0)}")
        print(f"😴 睡眠连接: {conn_summary.get('sleeping_connections', 0)}")
        print(f"🔒 锁定连接: {conn_summary.get('locked_connections', 0)}")

    # 长时间运行的查询
    long_running = [p for p in analysis.get("process_list", [])
                   if isinstance(p, dict) and p.get("is_long_running", False)]
    if long_running:
        print(f"⚠️  长时间运行查询: {len(long_running)}个")

    # 锁等待
    lock_waits = analysis.get("lock_analysis", {}).get("lock_waits", [])
    if lock_waits:
        print(f"🚫 锁等待: {len(lock_waits)}个")

    # 慢查询
    slow_queries = analysis.get("slow_queries", {}).get("recent_slow_queries", [])
    if slow_queries:
        print(f"🐌 慢查询: {len(slow_queries)}个")

    # 建议
    recommendations = analysis.get("recommendations", [])
    if recommendations:
        print(f"\n💡 优化建议 ({len(recommendations)}条):")
        for i, rec in enumerate(recommendations, 1):
            priority = rec.get("priority", "unknown")
            issue = rec.get("issue", "未知问题")
            recommendation = rec.get("recommendation", "无建议")
            print(f"  {i}. [{priority.upper()}] {issue}")
            print(f"     建议: {recommendation}")

    # 询问是否执行优化
    print("\n" + "=" * 60)
    user_input = input("是否要执行自动优化? (y/N): ").strip().lower()

    if user_input == 'y':
        print("\n🔧 执行优化...")

        # 终止长时间运行的查询
        if long_running:
            kill_input = input("是否终止长时间运行的查询? (y/N): ").strip().lower()
            if kill_input == 'y':
                kill_result = analyzer.kill_long_running_queries(300)  # 5分钟阈值
                print(f"✅ 已终止 {kill_result['killed_count']} 个长时间查询")

        # 优化连接
        opt_result = analyzer.optimize_connections()
        if "optimizations_applied" in opt_result:
            print("✅ 连接优化完成:")
            for opt in opt_result["optimizations_applied"]:
                print(f"   - {opt}")

    print("\n📝 完整分析报告已保存到分析结果中")
    print("建议定期运行此脚本来监控数据库性能")


if __name__ == "__main__":
    main()