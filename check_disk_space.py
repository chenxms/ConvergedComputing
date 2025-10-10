#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
检查磁盘空间是否足够进行G7-2025汇聚操作

此脚本检查：
1. 系统磁盘剩余空间
2. 数据库目录空间
3. 日志目录空间
4. 临时目录空间
5. 估算G7-2025汇聚所需空间
"""

import os
import sys
import shutil
from datetime import datetime
from typing import Dict, List, Tuple
import subprocess

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)


def get_timestamp() -> str:
    """获取当前时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def bytes_to_human(bytes_size: int) -> str:
    """将字节数转换为人类可读格式"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def get_disk_usage(path: str) -> Tuple[int, int, int]:
    """获取指定路径的磁盘使用情况

    Returns:
        Tuple[total, used, free] in bytes
    """
    try:
        if os.path.exists(path):
            usage = shutil.disk_usage(path)
            return usage.total, usage.used, usage.free
        else:
            return 0, 0, 0
    except Exception:
        return 0, 0, 0


def get_directory_size(path: str) -> int:
    """获取目录大小（递归计算）"""
    total_size = 0
    try:
        if os.path.exists(path):
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(file_path)
                    except (OSError, FileNotFoundError):
                        continue
    except Exception:
        pass
    return total_size


def check_mysql_datadir() -> Tuple[str, int]:
    """检查MySQL数据目录位置和大小"""
    mysql_datadir = "/var/lib/mysql"  # 默认位置

    try:
        # 尝试从MySQL配置中获取实际的datadir
        from app.database.connection import get_db_context
        from sqlalchemy import text

        with get_db_context() as db:
            result = db.execute(text("SHOW VARIABLES LIKE 'datadir'")).fetchone()
            if result:
                mysql_datadir = result[1]
    except Exception:
        pass

    # 计算数据目录大小
    datadir_size = get_directory_size(mysql_datadir)

    return mysql_datadir, datadir_size


def estimate_g7_space_requirements() -> Dict[str, int]:
    """估算G7-2025汇聚所需的空间"""
    try:
        from app.database.connection import get_db_context
        from sqlalchemy import text

        requirements = {
            'backup_space': 0,
            'temp_space': 0,
            'log_space': 0,
            'result_space': 0,
            'total_required': 0
        }

        with get_db_context() as db:
            # 1. 估算备份空间 - 基于现有G7数据大小
            backup_size_result = db.execute(text("""
                SELECT
                    COUNT(*) as record_count,
                    AVG(LENGTH(statistics_data)) as avg_data_size
                FROM statistical_aggregations
                WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            """)).fetchone()

            if backup_size_result and backup_size_result[0] > 0:
                # 估算备份文件大小 (记录数 * 平均数据大小 * 压缩比0.3)
                estimated_backup = int(backup_size_result[0] * (backup_size_result[1] or 1000) * 0.3)
                requirements['backup_space'] = max(estimated_backup, 100 * 1024 * 1024)  # 至少100MB
            else:
                requirements['backup_space'] = 100 * 1024 * 1024  # 100MB默认

            # 2. 估算临时空间 - 基于学校数量
            school_count_result = db.execute(text("""
                SELECT COUNT(DISTINCT school_id) as school_count
                FROM student_score_detail
                WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            """)).fetchone()

            school_count = school_count_result[0] if school_count_result else 100
            # 每个学校大约需要500KB临时空间
            requirements['temp_space'] = max(school_count * 500 * 1024, 200 * 1024 * 1024)  # 至少200MB

            # 3. 日志空间 - 汇聚过程产生的日志
            requirements['log_space'] = 50 * 1024 * 1024  # 50MB

            # 4. 结果空间 - 新生成的汇聚数据
            requirements['result_space'] = requirements['backup_space'] * 2  # 是备份的2倍

            # 5. 总计 (添加20%缓冲)
            total = sum(requirements.values())
            requirements['total_required'] = int(total * 1.2)

    except Exception as e:
        print(f"  警告: 估算空间需求时出错: {e}")
        # 使用默认值
        requirements = {
            'backup_space': 100 * 1024 * 1024,   # 100MB
            'temp_space': 200 * 1024 * 1024,     # 200MB
            'log_space': 50 * 1024 * 1024,       # 50MB
            'result_space': 200 * 1024 * 1024,   # 200MB
            'total_required': 650 * 1024 * 1024  # 650MB total with buffer
        }

    return requirements


def check_docker_space() -> Dict[str, any]:
    """检查Docker相关空间使用情况"""
    docker_info = {
        'available': False,
        'total_size': 0,
        'containers_size': 0,
        'images_size': 0,
        'volumes_size': 0
    }

    try:
        # 检查Docker系统信息
        result = subprocess.run(['docker', 'system', 'df', '--format', 'json'],
                              capture_output=True, text=True, timeout=15)

        if result.returncode == 0:
            import json
            data = json.loads(result.stdout)

            docker_info['available'] = True
            for item in data:
                if item['Type'] == 'Images':
                    docker_info['images_size'] = int(item['Size'])
                elif item['Type'] == 'Containers':
                    docker_info['containers_size'] = int(item['Size'])
                elif item['Type'] == 'Local Volumes':
                    docker_info['volumes_size'] = int(item['Size'])

            docker_info['total_size'] = (docker_info['containers_size'] +
                                       docker_info['images_size'] +
                                       docker_info['volumes_size'])

    except (subprocess.SubprocessError, FileNotFoundError, json.JSONDecodeError, subprocess.TimeoutExpired):
        docker_info['available'] = False

    return docker_info


def main() -> bool:
    """主检查流程"""
    print("=" * 60)
    print("G7-2025 汇聚磁盘空间检查")
    print("=" * 60)
    print(f"检查时间: {get_timestamp()}")
    print()

    overall_status = True

    # 1. 检查系统根目录空间
    print("1. 系统磁盘空间检查:")
    root_total, root_used, root_free = get_disk_usage("/")
    if root_total > 0:
        root_usage_percent = (root_used / root_total) * 100
        print(f"   根目录 (/)")
        print(f"   总空间: {bytes_to_human(root_total)}")
        print(f"   已使用: {bytes_to_human(root_used)} ({root_usage_percent:.1f}%)")
        print(f"   可用空间: {bytes_to_human(root_free)}")

        if root_usage_percent > 90:
            print("   ❌ 警告: 根目录使用率超过90%")
            overall_status = False
        elif root_usage_percent > 80:
            print("   ⚠️  注意: 根目录使用率超过80%")
        else:
            print("   ✅ 根目录空间充足")
    else:
        print("   ❌ 无法获取根目录信息")
        overall_status = False

    # 2. 检查MySQL数据目录
    print(f"\n2. 数据库存储空间检查:")
    mysql_datadir, datadir_size = check_mysql_datadir()
    datadir_total, datadir_used, datadir_free = get_disk_usage(mysql_datadir)

    print(f"   MySQL数据目录: {mysql_datadir}")
    print(f"   目录大小: {bytes_to_human(datadir_size)}")

    if datadir_total > 0:
        datadir_usage_percent = (datadir_used / datadir_total) * 100
        print(f"   分区总空间: {bytes_to_human(datadir_total)}")
        print(f"   分区已使用: {bytes_to_human(datadir_used)} ({datadir_usage_percent:.1f}%)")
        print(f"   分区可用: {bytes_to_human(datadir_free)}")

        if datadir_usage_percent > 85:
            print("   ❌ 警告: 数据库分区使用率过高")
            overall_status = False
        else:
            print("   ✅ 数据库存储空间充足")
    else:
        print("   ❌ 无法获取数据库分区信息")
        overall_status = False

    # 3. 检查临时目录空间
    print(f"\n3. 临时目录空间检查:")
    temp_dirs = ["/tmp", "/var/tmp", os.environ.get('TMPDIR', '/tmp')]
    for temp_dir in set(temp_dirs):  # 去重
        if os.path.exists(temp_dir):
            temp_total, temp_used, temp_free = get_disk_usage(temp_dir)
            if temp_total > 0:
                temp_usage_percent = (temp_used / temp_total) * 100
                print(f"   {temp_dir}: {bytes_to_human(temp_free)} 可用 ({100-temp_usage_percent:.1f}% 空闲)")

                if temp_usage_percent > 90:
                    print(f"   ❌ 警告: {temp_dir} 使用率过高")
                    overall_status = False

    # 4. 检查项目目录空间
    print(f"\n4. 项目目录空间检查:")
    project_dir = CURR_DIR
    project_size = get_directory_size(project_dir)
    project_total, project_used, project_free = get_disk_usage(project_dir)

    print(f"   项目目录: {project_dir}")
    print(f"   项目大小: {bytes_to_human(project_size)}")
    if project_total > 0:
        print(f"   可用空间: {bytes_to_human(project_free)}")

    # 5. 估算G7汇聚空间需求
    print(f"\n5. G7-2025汇聚空间需求估算:")
    space_requirements = estimate_g7_space_requirements()

    print(f"   备份空间需求: {bytes_to_human(space_requirements['backup_space'])}")
    print(f"   临时空间需求: {bytes_to_human(space_requirements['temp_space'])}")
    print(f"   日志空间需求: {bytes_to_human(space_requirements['log_space'])}")
    print(f"   结果空间需求: {bytes_to_human(space_requirements['result_space'])}")
    print(f"   总计需求(含缓冲): {bytes_to_human(space_requirements['total_required'])}")

    # 6. 检查Docker空间（如果可用）
    print(f"\n6. Docker空间使用检查:")
    docker_info = check_docker_space()
    if docker_info['available']:
        print(f"   Docker总使用: {bytes_to_human(docker_info['total_size'])}")
        print(f"   镜像大小: {bytes_to_human(docker_info['images_size'])}")
        print(f"   容器大小: {bytes_to_human(docker_info['containers_size'])}")
        print(f"   卷大小: {bytes_to_human(docker_info['volumes_size'])}")
    else:
        print("   Docker不可用或检查失败")

    # 7. 综合评估
    print(f"\n" + "=" * 60)
    print("综合评估:")

    # 检查是否有足够空间进行汇聚
    min_required_space = space_requirements['total_required']

    # 使用数据库分区的可用空间进行评估
    available_space = datadir_free if datadir_free > 0 else root_free

    if available_space >= min_required_space:
        print(f"✅ 空间检查通过!")
        print(f"   可用空间: {bytes_to_human(available_space)}")
        print(f"   所需空间: {bytes_to_human(min_required_space)}")
        print(f"   剩余缓冲: {bytes_to_human(available_space - min_required_space)}")
    else:
        print(f"❌ 空间不足!")
        print(f"   可用空间: {bytes_to_human(available_space)}")
        print(f"   所需空间: {bytes_to_human(min_required_space)}")
        print(f"   缺少空间: {bytes_to_human(min_required_space - available_space)}")
        overall_status = False

    # 空间清理建议
    if not overall_status or available_space < min_required_space * 1.5:
        print(f"\n建议的空间清理操作:")
        print(f"   1. 清理临时文件: sudo find /tmp -type f -atime +7 -delete")
        print(f"   2. 清理日志文件: sudo journalctl --vacuum-time=7d")
        if docker_info['available']:
            print(f"   3. 清理Docker: docker system prune -a")
        print(f"   4. 清理项目缓存: find {project_dir} -name '__pycache__' -exec rm -rf {{}} +")
        print(f"   5. 备份并压缩旧日志文件")

    return overall_status


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