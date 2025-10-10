#!/usr/bin/env python3
"""
部署包完整性验证脚本 - 简化版
用于验证修复后的部署包是否包含所有必需文件
"""

import os
import sys
from pathlib import Path

def verify_deployment_package(package_dir: str) -> bool:
    """验证部署包完整性"""
    package_path = Path(package_dir)

    if not package_path.exists():
        print(f"ERROR: 部署包目录不存在: {package_dir}")
        return False

    print(f"验证部署包: {package_dir}")
    print("=" * 60)

    # 必需的文件和目录
    required_items = [
        # 核心目录
        ("app", "dir", "核心应用代码"),
        ("scripts", "dir", "运维脚本工具链"),
        ("docs", "dir", "部署和API文档"),
        ("ops", "dir", "运维手册"),
        ("tests", "dir", "测试文件目录"),
        ("config", "dir", "配置文件目录"),

        # 核心文件
        ("Dockerfile", "file", "Docker镜像定义"),
        ("docker-compose.yml", "file", "Docker编排配置"),
        ("deploy.sh", "file", "一键部署脚本"),
        ("health_check.py", "file", "健康检查工具"),
        ("requirements.txt", "file", "Python依赖"),
        ("run_full_batch_pipeline.py", "file", "完整批次管道脚本"),
        ("data_cleaning_service.py", "file", "数据清洗服务"),

        # 新增的脚本文件（修复缺失）
        ("fast_materialize_subjects_v12.py", "file", "V12快速物化脚本"),
        ("fast_materialize_all_batches_v12.py", "file", "批量V12物化脚本"),
        ("enhanced_questionnaire_clean.py", "file", "增强问卷清洗脚本"),
        ("fixed_questionnaire_clean.py", "file", "修复问卷清洗脚本"),

        # 文档文件
        ("MANIFEST.json", "file", "部署包清单"),
        ("VERSION_NOTES.md", "file", "版本说明"),
    ]

    success_count = 0
    total_count = len(required_items)

    print("文件检查结果:")
    print("-" * 60)

    for item_name, item_type, description in required_items:
        item_path = package_path / item_name

        if item_type == "dir":
            exists = item_path.is_dir()
            type_symbol = "[DIR]"
        else:
            exists = item_path.is_file()
            type_symbol = "[FILE]"

        if exists:
            status = "[OK]"
            success_count += 1
        else:
            status = "[MISSING]"

        print(f"{status} {type_symbol} {item_name:<35} - {description}")

    print("-" * 60)
    print(f"检查结果: {success_count}/{total_count} 项通过")

    # 额外检查：关键子目录
    print("\n关键子目录检查:")
    key_subdirs = [
        ("app/api", "API路由层"),
        ("app/services", "业务逻辑层"),
        ("app/database", "数据访问层"),
        ("docs/ops", "运维文档"),
    ]

    for subdir, desc in key_subdirs:
        subdir_path = package_path / subdir
        if subdir_path.exists():
            print(f"[OK] [DIR] {subdir:<25} - {desc}")
        else:
            print(f"[WARN] [DIR] {subdir:<25} - {desc} (可选)")

    # Docker相关验证
    print("\nDocker配置验证:")
    dockerfile_path = package_path / "Dockerfile"
    if dockerfile_path.exists():
        try:
            with open(dockerfile_path, 'r', encoding='utf-8') as f:
                dockerfile_content = f.read()

            # 检查Dockerfile中的COPY指令
            copy_checks = [
                "./tests ./tests",
                "./config ./config",
                "./fast_materialize_subjects_v12.py",
                "./fast_materialize_all_batches_v12.py",
                "./enhanced_questionnaire_clean.py",
                "./fixed_questionnaire_clean.py",
            ]

            for copy_cmd in copy_checks:
                if copy_cmd in dockerfile_content:
                    print(f"[OK] COPY {copy_cmd}")
                else:
                    print(f"[MISSING] COPY {copy_cmd}")
        except Exception as e:
            print(f"[ERROR] 读取Dockerfile失败: {e}")

    print("\n" + "=" * 60)

    if success_count == total_count:
        print("SUCCESS: 部署包验证通过！所有必需文件都存在。")
        print("可以安全地进行Docker构建和部署。")
        return True
    else:
        missing_count = total_count - success_count
        print(f"FAILED: 部署包验证失败！缺少 {missing_count} 个必需文件。")
        print("请补充缺失文件后重新验证。")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("使用方法: python verify_package_simple.py <部署包目录>")
        print("示例: python verify_package_simple.py deployment_package_v1.2_production_20250919")
        sys.exit(1)

    package_dir = sys.argv[1]
    success = verify_deployment_package(package_dir)
    sys.exit(0 if success else 1)