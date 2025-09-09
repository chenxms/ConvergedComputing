#!/usr/bin/env python3
"""
开发工具脚本 - 自动化容器同步和测试
"""
import subprocess
import sys
import os
import time
from pathlib import Path

def run_command(cmd, description):
    """运行命令并显示结果"""
    print(f"\n[TOOL] {description}")
    print(f"执行命令: {cmd}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, 
                              capture_output=True, text=True, encoding='utf-8')
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] 命令执行失败: {e}")
        if e.stdout:
            print("标准输出:", e.stdout)
        if e.stderr:
            print("错误输出:", e.stderr)
        return False

def build_and_test():
    """构建并测试Docker容器"""
    print("[BUILD] 开始自动化Docker构建和测试流程")
    
    # 1. 构建Docker镜像
    if not run_command("docker-compose build", "重新构建Docker镜像"):
        return False
    
    # 2. 测试容器文件结构
    if not run_command("docker-compose run --rm app ls -la", "检查容器文件结构"):
        return False
    
    # 3. 测试Python模块导入
    test_imports = [
        "clean_batch",
        "data_cleaning_service", 
        "statistics_calculator",
        "aggregation_engine",
        "exam_aggregator",
        "questionnaire_aggregator",
        "multi_layer_aggregator",
        "aggregation_api"
    ]
    
    for module in test_imports:
        cmd = f'docker-compose run --rm app python -c "import {module}; print(\\"{module} 导入成功\\")"'
        if not run_command(cmd, f"测试 {module} 模块导入"):
            print(f"⚠️  {module} 模块导入失败，请检查文件是否存在")
            return False
    
    print("\n✅ 所有测试通过！Docker容器与本地开发环境同步成功")
    return True

def check_sync_status():
    """检查同步状态"""
    print("🔍 检查本地文件与容器同步状态")
    
    # 获取本地Python文件列表
    local_py_files = list(Path(".").glob("*.py"))
    local_py_files = [f.name for f in local_py_files if not f.name.startswith("test_")]
    
    print(f"本地Python文件数量: {len(local_py_files)}")
    print("主要文件:", local_py_files[:10])  # 显示前10个
    
    # 检查容器中的文件
    cmd = "docker-compose run --rm app ls *.py"
    print(f"\n执行命令: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, 
                              capture_output=True, text=True, encoding='utf-8')
        container_files = result.stdout.strip().split('\n')
        container_files = [f.strip() for f in container_files if f.strip()]
        
        print(f"容器Python文件数量: {len(container_files)}")
        print("容器文件:", container_files[:10])  # 显示前10个
        
        # 检查差异
        local_set = set(local_py_files)
        container_set = set(container_files)
        
        missing_in_container = local_set - container_set
        extra_in_container = container_set - local_set
        
        if missing_in_container:
            print(f"\n⚠️  容器中缺失的文件: {missing_in_container}")
            return False
            
        if extra_in_container:
            print(f"\n⚠️  容器中多余的文件: {extra_in_container}")
        
        print("\n✅ 文件同步状态良好")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ 检查失败: {e}")
        return False

def quick_verify(batch_code="G4-2025"):
    """快速验证指定批次"""
    print(f"🧪 快速验证批次 {batch_code}")
    
    cmd = f'docker-compose run --rm app python -c "from verify_g4_cleaning import verify_g4_cleaning; import asyncio; asyncio.run(verify_g4_cleaning())"'
    
    if run_command(cmd, f"验证 {batch_code} 批次清洗结果"):
        print(f"✅ {batch_code} 验证成功")
        return True
    else:
        print(f"❌ {batch_code} 验证失败")
        return False

def show_help():
    """显示帮助信息"""
    print("""
🛠️  开发工具使用说明:

python dev-tools.py build     - 构建并测试Docker容器
python dev-tools.py check     - 检查文件同步状态  
python dev-tools.py verify    - 快速验证G4-2025批次
python dev-tools.py all       - 执行完整流程 (build + check + verify)
python dev-tools.py help      - 显示此帮助

💡 建议工作流程:
1. 开发新功能后运行: python dev-tools.py all
2. 提交代码前运行: python dev-tools.py build
3. 发现问题时运行: python dev-tools.py check

这样可以确保Docker容器始终与本地开发环境同步！
    """)

def main():
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == "build":
        build_and_test()
    elif command == "check":
        check_sync_status()
    elif command == "verify":
        quick_verify()
    elif command == "all":
        print("🚀 执行完整同步验证流程\n")
        success = True
        success &= build_and_test()
        success &= check_sync_status()
        success &= quick_verify()
        
        if success:
            print("\n🎉 所有步骤完成！Docker容器与开发环境完全同步")
        else:
            print("\n❌ 部分步骤失败，请检查上述错误信息")
    elif command == "help":
        show_help()
    else:
        print(f"❌ 未知命令: {command}")
        show_help()

if __name__ == "__main__":
    main()