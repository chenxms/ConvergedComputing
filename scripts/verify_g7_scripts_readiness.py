#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 重启脚本就绪状态验证工具
验证所有关键脚本是否存在且配置正确
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

def get_project_root():
    """获取项目根目录"""
    current = Path(__file__).parent.parent
    return current

def check_file_exists(file_path: Path, description: str) -> dict:
    """检查文件是否存在"""
    exists = file_path.exists()
    result = {
        'path': str(file_path),
        'description': description,
        'exists': exists,
        'size': file_path.stat().st_size if exists else 0,
        'modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat() if exists else None
    }
    return result

def check_script_syntax(file_path: Path) -> dict:
    """检查Python脚本语法"""
    if not file_path.exists() or file_path.suffix != '.py':
        return {'valid': False, 'error': 'File not found or not a Python file'}

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            compile(f.read(), str(file_path), 'exec')
        return {'valid': True, 'error': None}
    except SyntaxError as e:
        return {'valid': False, 'error': f'Syntax error: {e}'}
    except Exception as e:
        return {'valid': False, 'error': f'Error: {e}'}

def verify_g7_scripts():
    """验证G7重启脚本就绪状态"""

    project_root = get_project_root()

    print("=" * 60)
    print("G7-2025 重启脚本就绪状态验证")
    print("=" * 60)
    print(f"项目根目录: {project_root}")
    print(f"验证时间: {datetime.now().isoformat()}")
    print()

    # 定义要检查的关键脚本
    scripts_to_check = [
        # 核心脚本
        {
            'path': project_root / 'scripts' / 'g7_guard_switch.py',
            'description': 'G7守卫模式切换工具',
            'required': True,
            'check_syntax': True
        },
        {
            'path': project_root / 'scripts' / 'run_g7_relaunch.sh',
            'description': 'Bash一键执行脚本',
            'required': True,
            'check_syntax': False
        },
        {
            'path': project_root / 'scripts' / 'run_g7_relaunch.ps1',
            'description': 'PowerShell一键执行脚本',
            'required': True,
            'check_syntax': False
        },
        {
            'path': project_root / 'backup_G7_2025_stats_clean.sql',
            'description': '干净版SQL备份脚本',
            'required': True,
            'check_syntax': False
        },

        # 支持脚本
        {
            'path': project_root / 'g7_precheck_suite.py',
            'description': '预检查套件',
            'required': True,
            'check_syntax': True
        },
        {
            'path': project_root / 'run_g7_pipeline_wrapper.py',
            'description': '流水线包装器',
            'required': True,
            'check_syntax': True
        },
        {
            'path': project_root / 'validate_g7_data.py',
            'description': '数据验证工具',
            'required': True,
            'check_syntax': True
        },
        {
            'path': project_root / 'scripts' / 'enhanced_g7_guard.py',
            'description': '增强守卫核心',
            'required': True,
            'check_syntax': True
        },
        {
            'path': project_root / 'scripts' / 'validate_g7_triggers.py',
            'description': '触发器验证工具',
            'required': True,
            'check_syntax': True
        },

        # 预检查依赖脚本
        {
            'path': project_root / 'check_no_active_old_pipeline.py',
            'description': '旧流水线进程检查',
            'required': True,
            'check_syntax': True
        },
        {
            'path': project_root / 'check_disk_space.py',
            'description': '磁盘空间检查',
            'required': True,
            'check_syntax': True
        },
        {
            'path': project_root / 'check_db_locks_enhanced.py',
            'description': '数据库锁状态检查',
            'required': True,
            'check_syntax': True
        }
    ]

    # 执行检查
    results = {'passed': 0, 'failed': 0, 'warnings': 0, 'details': []}

    print("检查关键脚本:")
    print("-" * 60)

    for script_info in scripts_to_check:
        file_path = script_info['path']
        description = script_info['description']
        required = script_info['required']
        check_syntax = script_info['check_syntax']

        # 检查文件存在性
        file_check = check_file_exists(file_path, description)

        # 检查语法（如果是Python文件）
        syntax_check = None
        if check_syntax and file_check['exists']:
            syntax_check = check_script_syntax(file_path)

        # 生成结果
        status = "[PASS]"
        details = {}

        if not file_check['exists']:
            if required:
                status = "[FAIL]"
                results['failed'] += 1
                details['error'] = "必需文件不存在"
            else:
                status = "[WARN]"
                results['warnings'] += 1
                details['warning'] = "可选文件不存在"
        elif syntax_check and not syntax_check['valid']:
            status = "[FAIL]"
            results['failed'] += 1
            details['error'] = f"语法错误: {syntax_check['error']}"
        else:
            results['passed'] += 1
            details['info'] = f"文件大小: {file_check['size']} bytes"

        print(f"{status} {description}")
        print(f"     路径: {file_path}")
        if details.get('error'):
            print(f"     错误: {details['error']}")
        elif details.get('warning'):
            print(f"     警告: {details['warning']}")
        elif details.get('info'):
            print(f"     信息: {details['info']}")
        print()

        # 保存详细结果
        result_detail = {
            'script': description,
            'path': str(file_path),
            'required': required,
            'status': status,
            'file_check': file_check,
            'syntax_check': syntax_check,
            'details': details
        }
        results['details'].append(result_detail)

    # 检查环境变量支持
    print("检查环境变量支持:")
    print("-" * 60)

    env_vars = ['G7_DB_HOST', 'G7_DB_PORT', 'G7_DB_NAME', 'G7_DB_PASSWORD']
    env_status = {}

    for var in env_vars:
        value = os.getenv(var)
        if value:
            print(f"[SET] {var}: 已设置")
            env_status[var] = 'set'
        else:
            print(f"[INFO] {var}: 未设置（运行时需要）")
            env_status[var] = 'not_set'

    results['environment'] = env_status
    print()

    # 生成总结
    print("验证总结:")
    print("-" * 60)
    print(f"[PASS] 通过: {results['passed']}")
    print(f"[FAIL] 失败: {results['failed']}")
    print(f"[WARN] 警告: {results['warnings']}")
    print()

    if results['failed'] == 0:
        print("[SUCCESS] 所有关键脚本已就绪！可以执行G7-2025重启流程。")
        overall_status = "READY"
    elif results['failed'] <= 2:
        print("[CAUTION] 发现少量问题，建议修复后执行。")
        overall_status = "CAUTION"
    else:
        print("[ERROR] 发现重大问题，必须修复后才能执行。")
        overall_status = "NOT_READY"

    # 保存详细报告
    report = {
        'timestamp': datetime.now().isoformat(),
        'overall_status': overall_status,
        'summary': {
            'passed': results['passed'],
            'failed': results['failed'],
            'warnings': results['warnings']
        },
        'environment': results['environment'],
        'details': results['details']
    }

    report_file = project_root / 'reports' / f'g7_scripts_readiness_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    report_file.parent.mkdir(exist_ok=True)

    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\n[REPORT] 详细报告已保存: {report_file}")

    return overall_status == "READY"

def main():
    """主函数"""
    try:
        is_ready = verify_g7_scripts()
        sys.exit(0 if is_ready else 1)
    except Exception as e:
        print(f"[ERROR] 验证过程中发生错误: {e}")
        sys.exit(2)

if __name__ == '__main__':
    main()