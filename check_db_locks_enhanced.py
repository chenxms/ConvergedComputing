#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 汇聚重启 - 数据库锁状态检查脚本（增强版）

检查内容：
1. 数据库连接数统计
2. 锁等待情况分析
3. 长时间运行事务检测
4. statistical_aggregations表操作监控
5. 表锁状态检查
6. G7相关触发器状态
7. 性能指标监控

用法：
    python check_db_locks_enhanced.py                    # 标准检查
    python check_db_locks_enhanced.py --auto-kill        # 自动清理阻塞进程
    python check_db_locks_enhanced.py --continuous       # 持续监控模式
    python check_db_locks_enhanced.py --g7-focus         # 专注G7相关检查
"""

import sys
import os
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context


def get_timestamp() -> str:
    """获取当前时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_message(message: str, level: str = "INFO"):
    """记录日志消息"""
    timestamp = get_timestamp()
    prefix = f"[{timestamp}] [{level}]"
    print(f"{prefix} {message}")


def check_g7_triggers() -> Dict[str, Any]:
    """检查G7相关触发器状态"""
    log_message("检查G7相关触发器状态...")

    trigger_info = {
        'triggers_found': [],
        'total_triggers': 0,
        'g7_specific': 0
    }

    try:
        with get_db_context() as db:
            # 查询所有触发器
            triggers = db.execute(text("""
                SELECT
                    trigger_name,
                    event_manipulation,
                    event_object_table,
                    trigger_body
                FROM information_schema.triggers
                WHERE trigger_schema = DATABASE()
                ORDER BY trigger_name
            """)).fetchall()

            trigger_info['total_triggers'] = len(triggers)

            for trigger in triggers:
                trigger_data = {
                    'name': trigger[0],
                    'event': trigger[1],
                    'table': trigger[2],
                    'body': trigger[3][:200] + '...' if len(trigger[3]) > 200 else trigger[3],
                    'is_g7_related': False
                }

                # 检查是否为G7相关触发器
                trigger_body = trigger[3].lower()
                if any(keyword in trigger_body for keyword in [
                    'g7-2025', 'g7_2025', 'prevent_g7', 'block_g7', 'g7_guard'
                ]):
                    trigger_data['is_g7_related'] = True
                    trigger_info['g7_specific'] += 1
                    log_message(f"  G7触发器: {trigger[0]} ({trigger[1]} on {trigger[2]})")

                trigger_info['triggers_found'].append(trigger_data)

            if trigger_info['g7_specific'] > 0:
                log_message(f"发现 {trigger_info['g7_specific']} 个G7相关触发器", "WARN")
            else:
                log_message("未发现G7相关触发器")

    except Exception as e:
        log_message(f"检查触发器失败: {e}", "ERROR")
        trigger_info['error'] = str(e)

    return trigger_info


def check_performance_metrics() -> Dict[str, Any]:
    """检查数据库性能指标"""
    log_message("检查数据库性能指标...")

    metrics = {
        'qps': 0,
        'tps': 0,
        'slow_queries': 0,
        'connections_usage': 0,
        'innodb_buffer_usage': 0,
        'table_locks_waited': 0
    }

    try:
        with get_db_context() as db:
            # 获取状态变量
            status_vars = db.execute(text("""
                SHOW STATUS WHERE Variable_name IN (
                    'Questions', 'Com_commit', 'Com_rollback',
                    'Slow_queries', 'Threads_connected', 'Max_connections',
                    'Innodb_buffer_pool_pages_total', 'Innodb_buffer_pool_pages_free',
                    'Table_locks_waited'
                )
            """)).fetchall()

            status_dict = {var[0]: var[1] for var in status_vars}

            # 计算连接使用率
            if 'Threads_connected' in status_dict and 'Max_connections' in status_dict:
                metrics['connections_usage'] = (
                    int(status_dict['Threads_connected']) / int(status_dict['Max_connections'])
                ) * 100

            # 计算InnoDB缓冲池使用率
            if 'Innodb_buffer_pool_pages_total' in status_dict and 'Innodb_buffer_pool_pages_free' in status_dict:
                total_pages = int(status_dict['Innodb_buffer_pool_pages_total'])
                free_pages = int(status_dict['Innodb_buffer_pool_pages_free'])
                if total_pages > 0:
                    metrics['innodb_buffer_usage'] = ((total_pages - free_pages) / total_pages) * 100

            # 其他指标
            metrics['slow_queries'] = int(status_dict.get('Slow_queries', 0))
            metrics['table_locks_waited'] = int(status_dict.get('Table_locks_waited', 0))

            log_message(f"连接使用率: {metrics['connections_usage']:.1f}%")
            log_message(f"InnoDB缓冲池使用率: {metrics['innodb_buffer_usage']:.1f}%")
            log_message(f"慢查询数: {metrics['slow_queries']}")
            log_message(f"表锁等待数: {metrics['table_locks_waited']}")

    except Exception as e:
        log_message(f"检查性能指标失败: {e}", "ERROR")
        metrics['error'] = str(e)

    return metrics


def check_database_locks(focus_g7: bool = False) -> Dict[str, Any]:
    """检查数据库锁情况"""
    log_message("开始数据库锁情况检查...")

    lock_status = {
        'total_connections': 0,
        'g7_connections': 0,
        'lock_waits': 0,
        'long_transactions': 0,
        'agg_table_operations': 0,
        'table_locks': 0,
        'critical_issues': []
    }

    try:
        with get_db_context() as db:
            # 1. 检查当前连接数
            log_message("检查数据库连接状态...")
            connections = db.execute(text("SHOW PROCESSLIST")).fetchall()
            lock_status['total_connections'] = len(connections)

            log_message(f"总连接数: {lock_status['total_connections']}")

            # 按状态统计连接
            status_count = {}
            g7_connections = []

            for conn in connections:
                status = conn.State or 'NULL'
                status_count[status] = status_count.get(status, 0) + 1

                # 检查G7相关连接
                if conn.Info and any(keyword in str(conn.Info).lower() for keyword in [
                    'g7-2025', 'g7_2025', 'statistical_aggregations'
                ]):
                    g7_connections.append({
                        'id': conn.Id,
                        'user': conn.User,
                        'host': conn.Host,
                        'time': conn.Time,
                        'state': conn.State,
                        'info': str(conn.Info)[:100] + '...' if len(str(conn.Info)) > 100 else str(conn.Info)
                    })

            lock_status['g7_connections'] = len(g7_connections)

            for status, count in status_count.items():
                log_message(f"  {status}: {count}")

            if g7_connections:
                log_message(f"发现 {len(g7_connections)} 个G7相关连接:", "WARN")
                for conn in g7_connections:
                    log_message(f"  连接{conn['id']}: {conn['user']}@{conn['host']} ({conn['time']}s)")
                    if focus_g7:
                        log_message(f"    查询: {conn['info']}")

            # 2. 检查锁等待
            log_message("检查锁等待情况...")
            try:
                locks = db.execute(text("""
                    SELECT
                        r.trx_id waiting_trx_id,
                        r.trx_mysql_thread_id waiting_thread,
                        r.trx_query waiting_query,
                        b.trx_id blocking_trx_id,
                        b.trx_mysql_thread_id blocking_thread,
                        b.trx_query blocking_query
                    FROM information_schema.innodb_lock_waits w
                    INNER JOIN information_schema.innodb_trx b
                      ON b.trx_id = w.blocking_trx_id
                    INNER JOIN information_schema.innodb_trx r
                      ON r.trx_id = w.requesting_trx_id
                """)).fetchall()

                lock_status['lock_waits'] = len(locks)

                if locks:
                    log_message(f"发现 {len(locks)} 个锁等待:", "WARN")
                    for lock in locks:
                        log_message(f"  等待事务 {lock.waiting_trx_id} (线程{lock.waiting_thread})")
                        log_message(f"  被阻塞by 事务 {lock.blocking_trx_id} (线程{lock.blocking_thread})")
                        log_message(f"  等待查询: {lock.waiting_query}")
                        log_message(f"  阻塞查询: {lock.blocking_query}")

                        # 检查是否为G7相关锁等待
                        if any(query and 'g7' in str(query).lower() for query in [lock.waiting_query, lock.blocking_query]):
                            lock_status['critical_issues'].append(f"G7相关锁等待: 事务{lock.waiting_trx_id}")
                else:
                    log_message("当前没有锁等待")

            except Exception as e:
                log_message(f"检查锁等待失败: {e}", "ERROR")

            # 3. 检查长时间运行的事务
            log_message("检查长时间运行的事务...")
            try:
                long_trx = db.execute(text("""
                    SELECT
                        trx_id,
                        trx_mysql_thread_id,
                        trx_state,
                        trx_started,
                        TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds,
                        trx_query
                    FROM information_schema.innodb_trx
                    WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 30
                    ORDER BY duration_seconds DESC
                """)).fetchall()

                lock_status['long_transactions'] = len(long_trx)

                if long_trx:
                    log_message(f"发现 {len(long_trx)} 个长时间事务:", "WARN")
                    for trx in long_trx:
                        log_message(f"  事务 {trx.trx_id} (线程{trx.trx_mysql_thread_id}):")
                        log_message(f"    状态: {trx.trx_state}")
                        log_message(f"    开始时间: {trx.trx_started}")
                        log_message(f"    运行时长: {trx.duration_seconds}秒")
                        log_message(f"    查询: {trx.trx_query}")

                        # 检查是否为G7相关长事务
                        if trx.trx_query and 'g7' in str(trx.trx_query).lower():
                            lock_status['critical_issues'].append(f"G7相关长事务: {trx.trx_id} ({trx.duration_seconds}s)")

                        # 超过5分钟的事务标记为严重问题
                        if trx.duration_seconds > 300:
                            lock_status['critical_issues'].append(f"超长事务: {trx.trx_id} ({trx.duration_seconds}s)")
                else:
                    log_message("没有发现长时间运行的事务")

            except Exception as e:
                log_message(f"检查长事务失败: {e}", "ERROR")

            # 4. 检查正在处理statistical_aggregations表的连接
            log_message("检查statistical_aggregations表操作...")
            agg_connections = [conn for conn in connections
                              if conn.Info and 'statistical_aggregations' in str(conn.Info)]

            lock_status['agg_table_operations'] = len(agg_connections)

            if agg_connections:
                log_message(f"发现 {len(agg_connections)} 个相关连接:", "WARN")
                for conn in agg_connections:
                    log_message(f"  线程 {conn.Id}: {conn.Command} | {conn.State}")
                    log_message(f"    查询: {conn.Info}")

                    # 检查是否为G7-2025相关操作
                    if 'g7-2025' in str(conn.Info).lower():
                        lock_status['critical_issues'].append(f"G7-2025表操作: 线程{conn.Id}")
            else:
                log_message("没有发现操作statistical_aggregations表的连接")

            # 5. 检查表锁
            log_message("检查表锁情况...")
            try:
                table_locks = db.execute(text("SHOW OPEN TABLES WHERE In_use > 0")).fetchall()
                lock_status['table_locks'] = len(table_locks)

                if table_locks:
                    log_message(f"发现 {len(table_locks)} 个被锁的表:", "WARN")
                    for lock in table_locks:
                        log_message(f"  {lock.Database}.{lock.Table}: In_use={lock.In_use}")

                        # 检查是否为关键表锁定
                        if lock.Table == 'statistical_aggregations':
                            lock_status['critical_issues'].append(f"统计汇总表被锁定: In_use={lock.In_use}")
                else:
                    log_message("没有发现被锁的表")
            except Exception as e:
                log_message(f"检查表锁失败: {e}", "ERROR")

    except Exception as e:
        log_message(f"数据库锁检查失败: {e}", "ERROR")
        lock_status['error'] = str(e)

    return lock_status


def kill_blocking_processes(auto_kill: bool = False) -> Dict[str, Any]:
    """杀死可能的阻塞进程"""
    log_message("开始清理可能的阻塞进程...")

    cleanup_result = {
        'candidates_found': 0,
        'processes_killed': 0,
        'failed_kills': 0,
        'errors': []
    }

    try:
        with get_db_context() as db:
            # 查找可能的阻塞进程
            connections = db.execute(text("SHOW PROCESSLIST")).fetchall()

            # 找出长时间运行且可能阻塞的连接
            blocking_candidates = []
            current_thread_id = None

            try:
                # 获取当前连接ID
                current_thread_id = db.execute(text("SELECT CONNECTION_ID()")).scalar()
            except Exception:
                pass

            for conn in connections:
                # 跳过当前连接
                if current_thread_id and conn.Id == current_thread_id:
                    continue

                # 查找长时间运行的连接（超过5分钟）
                if conn.Time and conn.Time > 300:
                    blocking_candidates.append({
                        'id': conn.Id,
                        'user': conn.User,
                        'host': conn.Host,
                        'time': conn.Time,
                        'state': conn.State,
                        'info': conn.Info,
                        'is_g7_related': conn.Info and 'g7' in str(conn.Info).lower()
                    })

            cleanup_result['candidates_found'] = len(blocking_candidates)

            if blocking_candidates:
                log_message(f"发现 {len(blocking_candidates)} 个可能的阻塞连接:")

                for conn in blocking_candidates:
                    log_message(f"  线程 {conn['id']}: {conn['user']}@{conn['host']}")
                    log_message(f"    运行时间: {conn['time']}秒, 状态: {conn['state']}")
                    log_message(f"    查询: {conn['info']}")

                    # 决定是否杀死进程
                    should_kill = auto_kill

                    if not auto_kill:
                        # 交互式确认
                        response = input(f"是否杀死线程 {conn['id']}? (y/n): ").strip().lower()
                        should_kill = response == 'y'

                    if should_kill:
                        try:
                            db.execute(text(f"KILL {conn['id']}"))
                            log_message(f"    已杀死线程 {conn['id']}")
                            cleanup_result['processes_killed'] += 1
                        except Exception as e:
                            error_msg = f"杀死线程 {conn['id']} 失败: {e}"
                            log_message(f"    {error_msg}", "ERROR")
                            cleanup_result['failed_kills'] += 1
                            cleanup_result['errors'].append(error_msg)
                    else:
                        log_message(f"    跳过线程 {conn['id']}")
            else:
                log_message("没有发现需要清理的阻塞连接")

    except Exception as e:
        error_msg = f"清理进程失败: {e}"
        log_message(error_msg, "ERROR")
        cleanup_result['errors'].append(error_msg)

    return cleanup_result


def continuous_monitoring(interval: int = 30, duration: int = 3600):
    """持续监控模式"""
    log_message(f"开始持续监控，间隔{interval}秒，持续{duration}秒...")

    start_time = datetime.now()
    cycle_count = 0

    try:
        while True:
            cycle_count += 1
            current_time = datetime.now()
            elapsed = (current_time - start_time).total_seconds()

            if elapsed >= duration:
                log_message(f"达到监控时限 {duration} 秒，停止监控")
                break

            log_message(f"监控周期 #{cycle_count} (已运行 {elapsed:.0f}s)")

            # 执行检查
            lock_status = check_database_locks(focus_g7=True)

            # 检查是否有严重问题
            if lock_status.get('critical_issues'):
                log_message("发现严重问题:", "WARN")
                for issue in lock_status['critical_issues']:
                    log_message(f"  - {issue}", "WARN")

            # 检查触发器
            trigger_info = check_g7_triggers()
            if trigger_info.get('g7_specific', 0) > 0:
                log_message(f"G7触发器仍然活跃: {trigger_info['g7_specific']} 个")

            time.sleep(interval)

    except KeyboardInterrupt:
        log_message("监控被用户中断")
    except Exception as e:
        log_message(f"监控过程中发生错误: {e}", "ERROR")

    log_message(f"监控结束，共执行 {cycle_count} 个周期")


def generate_summary_report(lock_status: Dict[str, Any], trigger_info: Dict[str, Any], metrics: Dict[str, Any]) -> str:
    """生成汇总报告"""
    timestamp = get_timestamp()

    report = f"""
G7-2025 数据库状态检查报告
=====================================
检查时间: {timestamp}

连接状态:
- 总连接数: {lock_status.get('total_connections', 0)}
- G7相关连接: {lock_status.get('g7_connections', 0)}
- 锁等待数: {lock_status.get('lock_waits', 0)}
- 长事务数: {lock_status.get('long_transactions', 0)}
- 汇总表操作: {lock_status.get('agg_table_operations', 0)}
- 表锁数: {lock_status.get('table_locks', 0)}

触发器状态:
- 总触发器数: {trigger_info.get('total_triggers', 0)}
- G7相关触发器: {trigger_info.get('g7_specific', 0)}

性能指标:
- 连接使用率: {metrics.get('connections_usage', 0):.1f}%
- InnoDB缓冲池使用率: {metrics.get('innodb_buffer_usage', 0):.1f}%
- 慢查询数: {metrics.get('slow_queries', 0)}
- 表锁等待数: {metrics.get('table_locks_waited', 0)}
"""

    # 添加严重问题
    critical_issues = lock_status.get('critical_issues', [])
    if critical_issues:
        report += f"\n严重问题 ({len(critical_issues)} 个):\n"
        for issue in critical_issues:
            report += f"- {issue}\n"
    else:
        report += "\n✅ 未发现严重问题\n"

    return report


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='G7-2025 数据库锁状态检查工具')
    parser.add_argument('--auto-kill', action='store_true', help='自动清理阻塞进程')
    parser.add_argument('--continuous', action='store_true', help='持续监控模式')
    parser.add_argument('--g7-focus', action='store_true', help='专注G7相关检查')
    parser.add_argument('--interval', type=int, default=30, help='监控间隔（秒），仅持续模式')
    parser.add_argument('--duration', type=int, default=3600, help='监控持续时间（秒），仅持续模式')
    parser.add_argument('--report-file', help='保存报告到文件')

    args = parser.parse_args()

    try:
        log_message("开始G7-2025数据库状态检查...")

        if args.continuous:
            # 持续监控模式
            continuous_monitoring(args.interval, args.duration)
        else:
            # 单次检查模式
            log_message("执行单次数据库状态检查")

            # 1. 检查数据库锁状态
            lock_status = check_database_locks(args.g7_focus)

            # 2. 检查G7触发器状态
            trigger_info = check_g7_triggers()

            # 3. 检查性能指标
            metrics = check_performance_metrics()

            # 4. 生成报告
            report = generate_summary_report(lock_status, trigger_info, metrics)
            print("\n" + "="*50)
            print(report)

            # 保存报告到文件
            if args.report_file:
                with open(args.report_file, 'w', encoding='utf-8') as f:
                    f.write(report)
                log_message(f"报告已保存到: {args.report_file}")

            # 5. 询问是否需要清理阻塞进程
            if lock_status.get('critical_issues') or lock_status.get('lock_waits', 0) > 0:
                if args.auto_kill:
                    log_message("自动清理模式，开始清理阻塞进程...")
                    cleanup_result = kill_blocking_processes(auto_kill=True)
                    log_message(f"清理完成: 发现{cleanup_result['candidates_found']}个候选，" +
                              f"成功清理{cleanup_result['processes_killed']}个，" +
                              f"失败{cleanup_result['failed_kills']}个")
                else:
                    response = input("\n是否需要清理可能的阻塞进程? (y/n): ").strip().lower()
                    if response == 'y':
                        cleanup_result = kill_blocking_processes(auto_kill=False)
                        log_message(f"清理完成: 成功{cleanup_result['processes_killed']}个，" +
                                  f"失败{cleanup_result['failed_kills']}个")
                    else:
                        log_message("跳过清理操作")

            # 6. 返回状态码
            critical_count = len(lock_status.get('critical_issues', []))
            if critical_count > 0:
                log_message(f"检查完成，发现 {critical_count} 个严重问题", "WARN")
                return 1
            else:
                log_message("检查完成，数据库状态正常")
                return 0

    except KeyboardInterrupt:
        log_message("检查被用户中断")
        return 2
    except Exception as e:
        log_message(f"检查过程中发生错误: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return 3


if __name__ == "__main__":
    sys.exit(main())