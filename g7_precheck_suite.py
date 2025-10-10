#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 汇聚重启预检查套件

统一执行所有预检查脚本：
1. check_no_active_old_pipeline.py  - 检查旧流水线进程
2. check_disk_space.py              - 检查磁盘空间
3. check_db_locks_enhanced.py       - 检查数据库锁状态
4. backup_G7_2025_stats.sql         - 执行数据备份
5. validate_g7_data.py              - 数据验证（可选）

用法：
    python g7_precheck_suite.py                    # 完整预检查
    python g7_precheck_suite.py --quick            # 快速检查（跳过备份和验证）
    python g7_precheck_suite.py --backup-only      # 仅执行备份
    python g7_precheck_suite.py --validation-only  # 仅执行验证
"""

import os
import sys
import subprocess
import argparse
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)


class G7PrecheckSuite:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.start_time = datetime.now()
        self.results = {}
        self.errors = []
        self.warnings = []

    def get_timestamp(self) -> str:
        """获取时间戳"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def log(self, message: str, level: str = "INFO"):
        """记录日志"""
        timestamp = self.get_timestamp()
        prefix = f"[{timestamp}] [{level}]"
        print(f"{prefix} {message}")

        if level == "ERROR":
            self.errors.append(message)
        elif level == "WARN":
            self.warnings.append(message)

    def run_script(self, script_name: str, args: List[str] = None, timeout: int = 300) -> Dict[str, Any]:
        """运行单个脚本"""
        self.log(f"运行脚本: {script_name}")

        script_path = os.path.join(CURR_DIR, script_name)
        if not os.path.exists(script_path):
            error = f"脚本文件不存在: {script_path}"
            self.log(error, "ERROR")
            return {'success': False, 'error': error}

        cmd = [sys.executable, script_path]
        if args:
            cmd.extend(args)

        try:
            start_time = datetime.now()

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=CURR_DIR
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            script_result = {
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'duration': duration,
                'stdout': result.stdout,
                'stderr': result.stderr
            }

            if script_result['success']:
                self.log(f"  ✅ {script_name} 执行成功 ({duration:.1f}s)")
            else:
                self.log(f"  ❌ {script_name} 执行失败 (返回码: {result.returncode})", "ERROR")
                if result.stderr:
                    self.log(f"  错误输出: {result.stderr}", "ERROR")

            return script_result

        except subprocess.TimeoutExpired:
            error = f"脚本 {script_name} 执行超时 ({timeout}秒)"
            self.log(error, "ERROR")
            return {'success': False, 'error': error, 'timeout': True}

        except Exception as e:
            error = f"执行脚本 {script_name} 时发生异常: {e}"
            self.log(error, "ERROR")
            return {'success': False, 'error': error}

    def run_sql_backup(self) -> Dict[str, Any]:
        """执行SQL备份"""
        self.log("执行G7-2025数据备份...")

        # 使用无BOM且已校验的备份脚本
        backup_script = "backup_G7_2025_stats_clean.sql"
        backup_path = os.path.join(CURR_DIR, backup_script)

        if not os.path.exists(backup_path):
            error = f"备份脚本不存在: {backup_path}"
            self.log(error, "ERROR")
            return {'success': False, 'error': error}

        try:
            from app.database.connection import get_db_context

            start_time = datetime.now()

            # 读取SQL文件内容
            with open(backup_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # 执行SQL备份
            with get_db_context() as db:
                # 分割SQL语句并执行
                statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

                for i, stmt in enumerate(statements):
                    if stmt.upper().startswith('SELECT') and ('status' in stmt.lower() or 'note' in stmt.lower()):
                        # 这是输出语句，显示结果
                        try:
                            result = db.execute(stmt).fetchall()
                            for row in result:
                                self.log(f"  {row[0] if len(row) == 1 else ' | '.join(map(str, row))}")
                        except Exception as e:
                            self.log(f"  执行输出语句失败: {e}", "WARN")
                    else:
                        # 普通SQL语句
                        try:
                            db.execute(stmt)
                        except Exception as e:
                            self.log(f"  执行SQL语句失败: {e}", "WARN")

                db.commit()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.log(f"  ✅ 数据备份完成 ({duration:.1f}s)")
            return {'success': True, 'duration': duration}

        except Exception as e:
            error = f"执行数据备份失败: {e}"
            self.log(error, "ERROR")
            return {'success': False, 'error': error}

    def check_prerequisites(self) -> bool:
        """检查预检查的前提条件"""
        self.log("检查预检查前提条件...")

        # 1. 检查Python环境
        try:
            import psutil
            import requests
            import pandas as pd
            from sqlalchemy import text
            self.log("  ✅ Python依赖库检查通过")
        except ImportError as e:
            self.log(f"  ❌ 缺少必要的Python库: {e}", "ERROR")
            return False

        # 2. 检查数据库连接
        try:
            from app.database.connection import get_db_context
            with get_db_context() as db:
                db.execute(text("SELECT 1")).scalar()
            self.log("  ✅ 数据库连接检查通过")
        except Exception as e:
            self.log(f"  ❌ 数据库连接失败: {e}", "ERROR")
            return False

        # 3. 检查脚本文件存在性
        required_scripts = [
            'check_no_active_old_pipeline.py',
            'check_disk_space.py',
            'check_db_locks_enhanced.py',
            'backup_G7_2025_stats.sql'
        ]

        if not self.config.get('quick'):
            required_scripts.append('validate_g7_data.py')

        missing_scripts = []
        for script in required_scripts:
            script_path = os.path.join(CURR_DIR, script)
            if not os.path.exists(script_path):
                missing_scripts.append(script)

        if missing_scripts:
            self.log(f"  ❌ 缺少必要的脚本文件: {missing_scripts}", "ERROR")
            return False

        self.log("  ✅ 脚本文件检查通过")
        return True

    def run_precheck_suite(self) -> bool:
        """运行完整的预检查套件"""
        self.log("开始G7-2025汇聚重启预检查套件...")
        self.log(f"配置: {json.dumps(self.config, indent=2)}")

        # 检查前提条件
        if not self.check_prerequisites():
            self.log("前提条件检查失败，终止预检查", "ERROR")
            return False

        success_count = 0
        total_checks = 0

        try:
            # 1. 检查旧流水线进程
            if not self.config.get('backup_only') and not self.config.get('validation_only'):
                total_checks += 1
                self.log("\n" + "="*50)
                self.log("1. 检查旧流水线进程")
                result = self.run_script('check_no_active_old_pipeline.py')
                self.results['old_pipeline_check'] = result
                if result['success']:
                    success_count += 1

            # 2. 检查磁盘空间
            if not self.config.get('backup_only') and not self.config.get('validation_only'):
                total_checks += 1
                self.log("\n" + "="*50)
                self.log("2. 检查磁盘空间")
                result = self.run_script('check_disk_space.py')
                self.results['disk_space_check'] = result
                if result['success']:
                    success_count += 1

            # 3. 检查数据库锁状态
            if not self.config.get('backup_only') and not self.config.get('validation_only'):
                total_checks += 1
                self.log("\n" + "="*50)
                self.log("3. 检查数据库锁状态")
                args = ['--g7-focus']
                if self.config.get('auto_kill_db_locks'):
                    args.append('--auto-kill')
                result = self.run_script('check_db_locks_enhanced.py', args)
                self.results['db_locks_check'] = result
                if result['success']:
                    success_count += 1

            # 4. 执行数据备份
            if not self.config.get('quick') and not self.config.get('validation_only'):
                total_checks += 1
                self.log("\n" + "="*50)
                self.log("4. 执行G7-2025数据备份")
                result = self.run_sql_backup()
                self.results['backup'] = result
                if result['success']:
                    success_count += 1

            # 5. 数据验证（可选）
            if not self.config.get('quick') and not self.config.get('backup_only'):
                if self.config.get('validation_only') or self.config.get('with_validation'):
                    total_checks += 1
                    self.log("\n" + "="*50)
                    self.log("5. 执行数据验证")
                    args = ['--quick'] if self.config.get('quick_validation') else []
                    result = self.run_script('validate_g7_data.py', args, timeout=600)
                    self.results['validation'] = result
                    if result['success']:
                        success_count += 1

        except Exception as e:
            self.log(f"预检查过程中发生严重错误: {e}", "ERROR")
            return False

        # 生成总结报告
        self.generate_summary_report(success_count, total_checks)

        return success_count == total_checks

    def generate_summary_report(self, success_count: int, total_checks: int):
        """生成预检查总结报告"""
        end_time = datetime.now()
        duration = end_time - self.start_time

        report = f"""
G7-2025 汇聚重启预检查总结报告
=====================================
开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}
结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}
总耗时: {str(duration).split('.')[0]}

检查结果概览:
- 总检查项: {total_checks}
- 成功项目: {success_count}
- 失败项目: {total_checks - success_count}
- 成功率: {(success_count/total_checks*100) if total_checks > 0 else 0:.1f}%

详细结果:
"""

        # 添加各检查项的详细结果
        for check_name, result in self.results.items():
            status = "✅ 通过" if result.get('success') else "❌ 失败"
            duration = result.get('duration', 0)
            report += f"- {check_name}: {status} ({duration:.1f}s)\n"

            if not result.get('success') and 'error' in result:
                report += f"  错误: {result['error']}\n"

        # 添加错误和警告汇总
        if self.errors:
            report += f"\n错误汇总 ({len(self.errors)} 个):\n"
            for error in self.errors:
                report += f"- {error}\n"

        if self.warnings:
            report += f"\n警告汇总 ({len(self.warnings)} 个):\n"
            for warning in self.warnings:
                report += f"- {warning}\n"

        # 总体结论
        if success_count == total_checks:
            report += f"\n总体结论: ✅ 所有预检查通过\n"
            report += f"系统已就绪，可以执行G7-2025汇聚重启操作。\n"
        else:
            failed_count = total_checks - success_count
            report += f"\n总体结论: ❌ 预检查未完全通过\n"
            report += f"发现 {failed_count} 个失败项目，建议修复后重新执行预检查。\n"

        # 输出报告
        print("\n" + "="*60)
        print(report)

        # 保存报告到文件
        report_file = f"g7_precheck_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            self.log(f"预检查报告已保存到: {report_file}")
        except Exception as e:
            self.log(f"保存报告失败: {e}", "WARN")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='G7-2025 汇聚重启预检查套件')
    parser.add_argument('--quick', action='store_true', help='快速检查（跳过备份和验证）')
    parser.add_argument('--backup-only', action='store_true', help='仅执行备份')
    parser.add_argument('--validation-only', action='store_true', help='仅执行验证')
    parser.add_argument('--with-validation', action='store_true', help='包含数据验证')
    parser.add_argument('--quick-validation', action='store_true', help='快速验证模式')
    parser.add_argument('--auto-kill-db-locks', action='store_true', help='自动清理数据库锁')

    args = parser.parse_args()

    config = {
        'quick': args.quick,
        'backup_only': args.backup_only,
        'validation_only': args.validation_only,
        'with_validation': args.with_validation,
        'quick_validation': args.quick_validation,
        'auto_kill_db_locks': args.auto_kill_db_locks
    }

    # 检查互斥选项
    exclusive_options = [args.quick, args.backup_only, args.validation_only]
    if sum(exclusive_options) > 1:
        print("错误: --quick, --backup-only, --validation-only 选项互斥")
        return 1

    suite = G7PrecheckSuite(config)
    success = suite.run_precheck_suite()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
