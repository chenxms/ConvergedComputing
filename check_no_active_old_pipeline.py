#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
检查是否有旧的G7-2025汇聚流水线进程正在运行

此脚本用于G7-2025汇聚重启前的预检查，确保没有旧版本的汇聚流程在后台运行
避免在新流程启动时与旧流程产生竞争和冲突
"""

import os
import sys
import time
import psutil
import subprocess
from datetime import datetime
from typing import List, Dict, Any

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)


def get_timestamp() -> str:
    """获取当前时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def check_python_processes() -> List[Dict[str, Any]]:
    """检查可能的G7汇聚相关Python进程"""
    print(f"[{get_timestamp()}] 检查Python进程中的G7汇聚流水线...")

    suspicious_processes = []
    g7_keywords = [
        'G7-2025', 'g7_2025', 'G7_2025',
        'materialize_g7', 'batch_aggregation', 'run_full_batch',
        'statistical_aggregations', 'batch_processing'
    ]

    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time', 'cpu_percent']):
        try:
            # 只检查Python进程
            if proc.info['name'] and 'python' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                if cmdline:
                    cmd_str = ' '.join(cmdline)

                    # 检查是否包含G7相关关键词
                    for keyword in g7_keywords:
                        if keyword.lower() in cmd_str.lower():
                            # 获取进程运行时间
                            create_time = datetime.fromtimestamp(proc.info['create_time'])
                            running_time = datetime.now() - create_time

                            suspicious_processes.append({
                                'pid': proc.info['pid'],
                                'name': proc.info['name'],
                                'cmdline': cmd_str,
                                'create_time': create_time,
                                'running_time': str(running_time).split('.')[0],  # 去掉微秒
                                'cpu_percent': proc.info['cpu_percent'],
                                'keyword_matched': keyword
                            })
                            break  # 匹配到一个关键词就够了

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    return suspicious_processes


def check_container_processes() -> List[Dict[str, Any]]:
    """检查Docker容器中的G7相关进程"""
    print(f"[{get_timestamp()}] 检查Docker容器中的G7流水线...")

    container_processes = []

    try:
        # 获取所有运行中的容器
        result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'],
                              capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            containers = result.stdout.strip().split('\n')
            containers = [c for c in containers if c]  # 过滤空行

            for container in containers:
                # 在每个容器中检查G7相关进程
                try:
                    ps_result = subprocess.run([
                        'docker', 'exec', container, 'ps', 'aux'
                    ], capture_output=True, text=True, timeout=10)

                    if ps_result.returncode == 0:
                        lines = ps_result.stdout.split('\n')
                        for line in lines:
                            if any(keyword.lower() in line.lower() for keyword in [
                                'G7-2025', 'g7_2025', 'materialize_g7', 'batch_aggregation'
                            ]):
                                container_processes.append({
                                    'container': container,
                                    'process_line': line.strip()
                                })

                except subprocess.TimeoutExpired:
                    print(f"  警告: 检查容器 {container} 超时")
                except subprocess.SubprocessError as e:
                    print(f"  警告: 检查容器 {container} 失败: {e}")

    except subprocess.TimeoutExpired:
        print("  警告: 获取Docker容器列表超时")
    except subprocess.SubprocessError as e:
        print(f"  警告: Docker命令执行失败: {e}")
    except FileNotFoundError:
        print("  提示: 未找到Docker命令，跳过容器检查")

    return container_processes


def check_cron_jobs() -> List[str]:
    """检查可能的定时任务"""
    print(f"[{get_timestamp()}] 检查cron定时任务...")

    cron_entries = []

    try:
        # 检查当前用户的crontab
        result = subprocess.run(['crontab', '-l'],
                              capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            lines = result.stdout.split('\n')
            for line in lines:
                if line.strip() and not line.strip().startswith('#'):
                    if any(keyword.lower() in line.lower() for keyword in [
                        'G7-2025', 'g7_2025', 'materialize_g7', 'batch_aggregation'
                    ]):
                        cron_entries.append(line.strip())

    except subprocess.TimeoutExpired:
        print("  警告: 检查crontab超时")
    except subprocess.SubprocessError:
        print("  提示: 无法访问crontab或没有设置定时任务")
    except FileNotFoundError:
        print("  提示: 系统中没有crontab命令")

    return cron_entries


def check_database_connections() -> List[Dict[str, Any]]:
    """检查数据库中可能的G7汇聚连接"""
    print(f"[{get_timestamp()}] 检查数据库连接...")

    try:
        from app.database.connection import get_db_context
        from sqlalchemy import text

        active_connections = []

        with get_db_context() as db:
            # 检查当前活跃连接
            connections = db.execute(text("SHOW PROCESSLIST")).fetchall()

            for conn in connections:
                if conn.Info:
                    info_lower = str(conn.Info).lower()
                    if any(keyword.lower() in info_lower for keyword in [
                        'statistical_aggregations', 'G7-2025', 'g7_2025',
                        'batch_code', 'materialize'
                    ]):
                        active_connections.append({
                            'id': conn.Id,
                            'user': conn.User,
                            'host': conn.Host,
                            'db': conn.db,
                            'command': conn.Command,
                            'time': conn.Time,
                            'state': conn.State,
                            'info': conn.Info
                        })

        return active_connections

    except Exception as e:
        print(f"  警告: 检查数据库连接失败: {e}")
        return []


def kill_process_interactive(processes: List[Dict[str, Any]]) -> bool:
    """交互式杀死进程"""
    if not processes:
        return True

    print(f"\n发现 {len(processes)} 个可疑进程，是否需要停止？")
    for i, proc in enumerate(processes):
        print(f"  {i+1}. PID {proc['pid']}: {proc['cmdline'][:100]}...")
        print(f"     运行时间: {proc['running_time']}, CPU: {proc.get('cpu_percent', 'N/A')}%")

    print(f"\n选择操作:")
    print(f"  1. 全部停止")
    print(f"  2. 逐个确认")
    print(f"  3. 跳过")

    choice = input("请输入选择 (1/2/3): ").strip()

    if choice == '1':
        # 全部停止
        killed_count = 0
        for proc in processes:
            try:
                psutil.Process(proc['pid']).terminate()
                print(f"  已停止进程 {proc['pid']}")
                killed_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                print(f"  停止进程 {proc['pid']} 失败: {e}")

        print(f"成功停止 {killed_count}/{len(processes)} 个进程")
        return killed_count == len(processes)

    elif choice == '2':
        # 逐个确认
        killed_count = 0
        for proc in processes:
            confirm = input(f"是否停止进程 {proc['pid']}? (y/n): ").strip().lower()
            if confirm == 'y':
                try:
                    psutil.Process(proc['pid']).terminate()
                    print(f"  已停止进程 {proc['pid']}")
                    killed_count += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                    print(f"  停止进程 {proc['pid']} 失败: {e}")

        print(f"成功停止 {killed_count}/{len(processes)} 个进程")
        return True  # 允许用户选择性停止

    else:
        print("跳过进程停止")
        return False


def main() -> bool:
    """主检查流程"""
    print("=" * 60)
    print("G7-2025 汇聚流水线预检查")
    print("=" * 60)
    print(f"检查时间: {get_timestamp()}")
    print()

    all_clear = True

    # 1. 检查Python进程
    python_processes = check_python_processes()
    if python_processes:
        print(f"❌ 发现 {len(python_processes)} 个可疑的Python进程:")
        for proc in python_processes:
            print(f"  - PID {proc['pid']}: {proc['name']}")
            print(f"    命令: {proc['cmdline'][:100]}...")
            print(f"    匹配关键词: {proc['keyword_matched']}")
            print(f"    运行时间: {proc['running_time']}")
            print()
        all_clear = False
    else:
        print("✅ 没有发现可疑的Python进程")

    # 2. 检查容器进程
    container_processes = check_container_processes()
    if container_processes:
        print(f"❌ 发现 {len(container_processes)} 个容器中的可疑进程:")
        for proc in container_processes:
            print(f"  - 容器 {proc['container']}: {proc['process_line']}")
        all_clear = False
    else:
        print("✅ 容器中没有发现可疑进程")

    # 3. 检查定时任务
    cron_jobs = check_cron_jobs()
    if cron_jobs:
        print(f"❌ 发现 {len(cron_jobs)} 个可疑的定时任务:")
        for job in cron_jobs:
            print(f"  - {job}")
        all_clear = False
    else:
        print("✅ 没有发现可疑的定时任务")

    # 4. 检查数据库连接
    db_connections = check_database_connections()
    if db_connections:
        print(f"❌ 发现 {len(db_connections)} 个可疑的数据库连接:")
        for conn in db_connections:
            print(f"  - 连接ID {conn['id']}: {conn['user']}@{conn['host']}")
            print(f"    命令: {conn['command']}, 运行时间: {conn['time']}秒")
            print(f"    状态: {conn['state']}")
            print(f"    查询: {str(conn['info'])[:100]}...")
            print()
        all_clear = False
    else:
        print("✅ 没有发现可疑的数据库连接")

    print("\n" + "=" * 60)

    if all_clear:
        print("✅ 预检查通过！没有发现活跃的旧版G7汇聚流水线")
        print("可以安全启动新版汇聚流程")
        return True
    else:
        print("❌ 预检查发现问题！存在活跃的旧版流水线组件")
        print("建议在启动新流程前先停止这些组件")

        # 提供交互式停止选项
        if python_processes:
            print(f"\n处理Python进程...")
            kill_process_interactive(python_processes)

        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n\n检查被用户中断")
        sys.exit(2)
    except Exception as e:
        print(f"\n\n检查过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(3)