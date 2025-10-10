#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 汇聚流水线实时监控脚本

监控内容：
1. 数据库写入活动
2. 应用日志监控
3. 错误率统计
4. 内存和CPU占用
5. 异常写入检测
6. 流水线进度跟踪

用法：
    python monitor_g7_pipeline.py                    # 标准监控
    python monitor_g7_pipeline.py --alert-only       # 仅显示告警
    python monitor_g7_pipeline.py --duration 3600    # 监控1小时
    python monitor_g7_pipeline.py --emergency-stop   # 启用紧急停止检测
"""

import os
import sys
import time
import argparse
import threading
import subprocess
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
import re

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)


class G7PipelineMonitor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.start_time = datetime.now()
        self.monitoring = True
        self.stats = {
            'db_writes': 0,
            'db_reads': 0,
            'errors': 0,
            'warnings': 0,
            'processed_schools': set(),
            'last_activity': None,
            'peak_memory': 0,
            'peak_cpu': 0
        }
        self.alerts = []
        self.emergency_triggers = 0

    def get_timestamp(self) -> str:
        """获取时间戳"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def log(self, message: str, level: str = "INFO"):
        """记录日志"""
        timestamp = self.get_timestamp()
        if not self.config.get('alert_only') or level in ['WARN', 'ERROR', 'CRITICAL']:
            print(f"[{timestamp}] [{level}] {message}")

        # 记录到文件
        if self.config.get('log_file'):
            with open(self.config['log_file'], 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] [{level}] {message}\n")

    def check_database_activity(self) -> Dict[str, Any]:
        """检查数据库活动"""
        try:
            from app.database.connection import get_db_context
            from sqlalchemy import text

            activity = {
                'active_connections': 0,
                'g7_related_queries': 0,
                'long_running_queries': 0,
                'lock_waits': 0,
                'writes_per_minute': 0,
                'current_queries': []
            }

            with get_db_context() as db:
                # 1. 检查活跃连接
                connections = db.execute(text("SHOW PROCESSLIST")).fetchall()
                activity['active_connections'] = len(connections)

                g7_queries = []
                long_running = []

                for conn in connections:
                    # 检查G7相关查询
                    if conn.Info and any(keyword in str(conn.Info).lower() for keyword in [
                        'g7-2025', 'statistical_aggregations', 'batch_code'
                    ]):
                        g7_queries.append({
                            'id': conn.Id,
                            'time': conn.Time,
                            'state': conn.State,
                            'query': str(conn.Info)[:100] + '...' if len(str(conn.Info)) > 100 else str(conn.Info)
                        })
                        activity['g7_related_queries'] += 1

                    # 检查长时间运行的查询
                    if conn.Time and conn.Time > 60:  # 超过1分钟
                        long_running.append({
                            'id': conn.Id,
                            'time': conn.Time,
                            'query': str(conn.Info)[:50] + '...' if conn.Info else 'NULL'
                        })
                        activity['long_running_queries'] += 1

                activity['current_queries'] = g7_queries

                # 2. 检查锁等待
                try:
                    lock_waits = db.execute(text("""
                        SELECT COUNT(*)
                        FROM information_schema.innodb_lock_waits w
                        INNER JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
                        INNER JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id
                    """)).scalar()
                    activity['lock_waits'] = lock_waits or 0
                except Exception:
                    activity['lock_waits'] = 0

                # 3. 统计G7数据变化
                try:
                    current_count = db.execute(text("""
                        SELECT COUNT(*) FROM statistical_aggregations
                        WHERE batch_code = 'G7-2025'
                        AND updated_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
                    """)).scalar()
                    activity['writes_per_minute'] = current_count or 0
                    self.stats['db_writes'] += activity['writes_per_minute']
                except Exception:
                    activity['writes_per_minute'] = 0

                # 更新统计
                if activity['g7_related_queries'] > 0:
                    self.stats['last_activity'] = datetime.now()

            return activity

        except Exception as e:
            self.log(f"检查数据库活动失败: {e}", "ERROR")
            return {'error': str(e)}

    def check_system_resources(self) -> Dict[str, Any]:
        """检查系统资源使用情况"""
        try:
            resources = {
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'memory_available_gb': psutil.virtual_memory().available / (1024**3),
                'disk_usage_percent': psutil.disk_usage('/').percent,
                'load_average': os.getloadavg() if hasattr(os, 'getloadavg') else [0, 0, 0],
                'python_processes': 0,
                'suspicious_processes': []
            }

            # 统计Python进程
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_percent', 'cpu_percent']):
                try:
                    if proc.info['name'] and 'python' in proc.info['name'].lower():
                        resources['python_processes'] += 1

                        # 检查可疑进程
                        cmdline = ' '.join(proc.info['cmdline'] or [])
                        if any(keyword in cmdline.lower() for keyword in [
                            'g7-2025', 'statistical_aggregations', 'batch_processing'
                        ]):
                            resources['suspicious_processes'].append({
                                'pid': proc.info['pid'],
                                'cmdline': cmdline[:100] + '...' if len(cmdline) > 100 else cmdline,
                                'memory_percent': proc.info['memory_percent'],
                                'cpu_percent': proc.info['cpu_percent']
                            })

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            # 更新峰值统计
            if resources['memory_percent'] > self.stats['peak_memory']:
                self.stats['peak_memory'] = resources['memory_percent']
            if resources['cpu_percent'] > self.stats['peak_cpu']:
                self.stats['peak_cpu'] = resources['cpu_percent']

            return resources

        except Exception as e:
            self.log(f"检查系统资源失败: {e}", "ERROR")
            return {'error': str(e)}

    def check_application_logs(self) -> Dict[str, Any]:
        """检查应用日志"""
        log_analysis = {
            'recent_errors': [],
            'recent_warnings': [],
            'g7_activities': [],
            'error_count': 0,
            'warning_count': 0
        }

        # 检查可能的日志文件
        log_files = [
            '/var/log/app.log',
            '/var/log/uvicorn.log',
            'app.log',
            'logs/app.log',
            '/tmp/g7_pipeline.log'
        ]

        for log_file in log_files:
            if os.path.exists(log_file):
                try:
                    # 读取最近的日志
                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()

                    # 分析最近10分钟的日志
                    recent_time = datetime.now() - timedelta(minutes=10)

                    for line in lines[-1000:]:  # 只看最后1000行
                        line = line.strip()
                        if not line:
                            continue

                        # 检查错误
                        if any(keyword in line.lower() for keyword in ['error', 'exception', 'failed']):
                            if 'g7' in line.lower() or '2025' in line:
                                log_analysis['recent_errors'].append(line[-200:])  # 最后200字符
                                log_analysis['error_count'] += 1

                        # 检查警告
                        elif any(keyword in line.lower() for keyword in ['warning', 'warn']):
                            if 'g7' in line.lower() or '2025' in line:
                                log_analysis['recent_warnings'].append(line[-200:])
                                log_analysis['warning_count'] += 1

                        # 检查G7活动
                        elif any(keyword in line.lower() for keyword in [
                            'g7-2025', 'statistical_aggregations', 'materialize'
                        ]):
                            log_analysis['g7_activities'].append(line[-150:])

                except Exception as e:
                    self.log(f"读取日志文件 {log_file} 失败: {e}", "WARN")

        # 更新统计
        self.stats['errors'] += log_analysis['error_count']
        self.stats['warnings'] += log_analysis['warning_count']

        return log_analysis

    def check_pipeline_progress(self) -> Dict[str, Any]:
        """检查流水线进度"""
        try:
            from app.database.connection import get_db_context
            from sqlalchemy import text

            progress = {
                'total_schools': 0,
                'completed_schools': 0,
                'failed_schools': 0,
                'in_progress_schools': 0,
                'completion_rate': 0.0,
                'recent_completions': []
            }

            with get_db_context() as db:
                # 统计学校数量
                school_stats = db.execute(text("""
                    SELECT
                        calculation_status,
                        COUNT(DISTINCT school_id) as school_count
                    FROM statistical_aggregations
                    WHERE batch_code = 'G7-2025'
                    GROUP BY calculation_status
                """)).fetchall()

                for stat in school_stats:
                    status = stat[0]
                    count = stat[1]

                    if status == 'COMPLETED':
                        progress['completed_schools'] = count
                    elif status == 'FAILED':
                        progress['failed_schools'] = count
                    elif status in ['PROCESSING', 'PENDING']:
                        progress['in_progress_schools'] = count

                progress['total_schools'] = (progress['completed_schools'] +
                                           progress['failed_schools'] +
                                           progress['in_progress_schools'])

                if progress['total_schools'] > 0:
                    progress['completion_rate'] = (progress['completed_schools'] /
                                                 progress['total_schools']) * 100

                # 获取最近完成的学校
                recent = db.execute(text("""
                    SELECT school_id, school_name, updated_at
                    FROM statistical_aggregations
                    WHERE batch_code = 'G7-2025'
                    AND calculation_status = 'COMPLETED'
                    AND updated_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
                    ORDER BY updated_at DESC
                    LIMIT 10
                """)).fetchall()

                progress['recent_completions'] = [
                    {
                        'school_id': r[0],
                        'school_name': r[1],
                        'completed_at': r[2].strftime('%H:%M:%S') if r[2] else 'N/A'
                    }
                    for r in recent
                ]

                # 更新已处理学校集合
                self.stats['processed_schools'].update([r[0] for r in recent])

            return progress

        except Exception as e:
            self.log(f"检查流水线进度失败: {e}", "ERROR")
            return {'error': str(e)}

    def detect_anomalies(self, db_activity: Dict, resources: Dict, progress: Dict) -> List[str]:
        """检测异常情况"""
        anomalies = []

        # 1. 数据库异常检测
        if db_activity.get('lock_waits', 0) > 0:
            anomalies.append(f"检测到 {db_activity['lock_waits']} 个数据库锁等待")

        if db_activity.get('long_running_queries', 0) > 0:
            anomalies.append(f"检测到 {db_activity['long_running_queries']} 个长时间运行的查询")

        if db_activity.get('g7_related_queries', 0) > 10:
            anomalies.append(f"G7相关查询数量异常: {db_activity['g7_related_queries']}")

        # 2. 资源异常检测
        if resources.get('memory_percent', 0) > 90:
            anomalies.append(f"内存使用率过高: {resources['memory_percent']:.1f}%")
            self.emergency_triggers += 1

        if resources.get('cpu_percent', 0) > 95:
            anomalies.append(f"CPU使用率过高: {resources['cpu_percent']:.1f}%")
            self.emergency_triggers += 1

        if resources.get('disk_usage_percent', 0) > 95:
            anomalies.append(f"磁盘使用率过高: {resources['disk_usage_percent']:.1f}%")
            self.emergency_triggers += 1

        # 3. 进度异常检测
        if progress.get('failed_schools', 0) > progress.get('completed_schools', 0):
            anomalies.append(f"失败学校数 ({progress['failed_schools']}) 超过完成数 ({progress['completed_schools']})")

        # 4. 活动异常检测
        if self.stats['last_activity']:
            inactive_time = datetime.now() - self.stats['last_activity']
            if inactive_time.total_seconds() > 300:  # 5分钟无活动
                anomalies.append(f"流水线已无活动 {inactive_time.total_seconds():.0f} 秒")

        # 5. 错误率检测
        if self.stats['errors'] > 50:
            anomalies.append(f"错误数量过多: {self.stats['errors']}")
            self.emergency_triggers += 1

        return anomalies

    def emergency_stop_check(self) -> bool:
        """紧急停止检查"""
        if not self.config.get('emergency_stop'):
            return False

        # 紧急停止条件
        if self.emergency_triggers >= 3:
            self.log("触发紧急停止条件!", "CRITICAL")
            self.log(f"紧急触发次数: {self.emergency_triggers}", "CRITICAL")

            if self.config.get('auto_emergency_stop'):
                self.log("执行自动紧急停止...", "CRITICAL")
                try:
                    # 执行紧急停止脚本
                    subprocess.run([sys.executable, 'ultimate_stop.py'], timeout=30)
                    return True
                except Exception as e:
                    self.log(f"自动紧急停止失败: {e}", "ERROR")

            return True

        return False

    def generate_report(self) -> str:
        """生成监控报告"""
        running_time = datetime.now() - self.start_time

        report = f"""
G7-2025 汇聚流水线监控报告
=====================================
监控时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')} ~ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
运行时长: {str(running_time).split('.')[0]}

累计统计:
- 数据库写入: {self.stats['db_writes']}
- 错误数量: {self.stats['errors']}
- 警告数量: {self.stats['warnings']}
- 已处理学校: {len(self.stats['processed_schools'])}
- 峰值内存: {self.stats['peak_memory']:.1f}%
- 峰值CPU: {self.stats['peak_cpu']:.1f}%

最后活动时间: {self.stats['last_activity'].strftime('%H:%M:%S') if self.stats['last_activity'] else '无'}

告警记录:
"""

        for alert in self.alerts[-10:]:  # 最后10条告警
            report += f"- {alert}\n"

        return report

    def run_monitoring_cycle(self):
        """运行一次监控周期"""
        cycle_start = datetime.now()

        # 1. 检查数据库活动
        db_activity = self.check_database_activity()

        # 2. 检查系统资源
        resources = self.check_system_resources()

        # 3. 检查应用日志
        logs = self.check_application_logs()

        # 4. 检查流水线进度
        progress = self.check_pipeline_progress()

        # 5. 异常检测
        anomalies = self.detect_anomalies(db_activity, resources, progress)

        # 6. 输出监控信息
        if not self.config.get('alert_only'):
            self.log("=" * 50)
            self.log(f"监控周期 #{len(self.alerts) + 1}")

            # 数据库活动
            if 'error' not in db_activity:
                self.log(f"数据库: {db_activity['active_connections']} 连接, "
                        f"{db_activity['g7_related_queries']} G7查询, "
                        f"{db_activity['writes_per_minute']} 写入/分钟")

            # 系统资源
            if 'error' not in resources:
                self.log(f"系统: CPU {resources['cpu_percent']:.1f}%, "
                        f"内存 {resources['memory_percent']:.1f}%, "
                        f"磁盘 {resources['disk_usage_percent']:.1f}%")

            # 流水线进度
            if 'error' not in progress:
                self.log(f"进度: {progress['completed_schools']}/{progress['total_schools']} 学校 "
                        f"({progress['completion_rate']:.1f}%), "
                        f"{len(progress['recent_completions'])} 最近完成")

            # 日志分析
            if logs['error_count'] > 0 or logs['warning_count'] > 0:
                self.log(f"日志: {logs['error_count']} 错误, {logs['warning_count']} 警告")

        # 7. 处理异常
        for anomaly in anomalies:
            self.log(f"异常: {anomaly}", "WARN")
            self.alerts.append(f"[{self.get_timestamp()}] {anomaly}")

        # 8. 紧急停止检查
        if self.emergency_stop_check():
            self.monitoring = False
            return False

        return True

    def run(self):
        """运行监控"""
        self.log("开始G7-2025汇聚流水线监控...")
        self.log(f"监控配置: {json.dumps(self.config, indent=2)}")

        try:
            cycle_count = 0
            while self.monitoring:
                cycle_count += 1

                # 运行监控周期
                if not self.run_monitoring_cycle():
                    break

                # 检查运行时间限制
                if self.config.get('duration'):
                    running_time = (datetime.now() - self.start_time).total_seconds()
                    if running_time >= self.config['duration']:
                        self.log(f"达到运行时间限制 {self.config['duration']} 秒，停止监控")
                        break

                # 等待下一个周期
                time.sleep(self.config.get('interval', 30))

        except KeyboardInterrupt:
            self.log("监控被用户中断", "INFO")
        except Exception as e:
            self.log(f"监控过程中发生错误: {e}", "ERROR")
            import traceback
            traceback.print_exc()
        finally:
            # 生成最终报告
            report = self.generate_report()
            self.log("监控结束，生成最终报告:")
            print(report)

            # 保存报告到文件
            report_file = f"g7_monitor_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            self.log(f"报告已保存到: {report_file}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='G7-2025 汇聚流水线监控器')
    parser.add_argument('--alert-only', action='store_true', help='仅显示告警信息')
    parser.add_argument('--duration', type=int, help='监控持续时间（秒）')
    parser.add_argument('--interval', type=int, default=30, help='监控间隔（秒）')
    parser.add_argument('--emergency-stop', action='store_true', help='启用紧急停止检测')
    parser.add_argument('--auto-emergency-stop', action='store_true', help='自动执行紧急停止')
    parser.add_argument('--log-file', help='日志文件路径')

    args = parser.parse_args()

    config = {
        'alert_only': args.alert_only,
        'duration': args.duration,
        'interval': args.interval,
        'emergency_stop': args.emergency_stop,
        'auto_emergency_stop': args.auto_emergency_stop,
        'log_file': args.log_file
    }

    monitor = G7PipelineMonitor(config)
    monitor.run()


if __name__ == "__main__":
    main()