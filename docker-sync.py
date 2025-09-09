#!/usr/bin/env python3
"""
简化版Docker同步工具
"""
import subprocess
import sys

def run_cmd(cmd):
    """运行命令"""
    print(f"[CMD] {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {e}")
        return False

def build_docker():
    """构建Docker镜像"""
    print("[BUILD] 构建Docker镜像...")
    return run_cmd("docker-compose build")

def test_imports():
    """测试关键模块导入"""
    print("[TEST] 测试模块导入...")
    modules = ["clean_batch", "aggregation_api", "statistics_calculator"]
    
    for module in modules:
        cmd = f'docker-compose run --rm app python -c "import {module}; print(\\"{module} OK\\")"'
        if not run_cmd(cmd):
            return False
    return True

def check_files():
    """检查容器文件"""
    print("[CHECK] 检查容器文件...")
    return run_cmd("docker-compose run --rm app ls -la *.py")

def verify_batch():
    """验证批次"""
    print("[VERIFY] 验证G4-2025批次...")
    cmd = 'docker-compose run --rm app python -c "print(\\\"验证命令已准备就绪\\\")"'
    return run_cmd(cmd)

def main():
    if len(sys.argv) < 2:
        print("用法: python docker-sync.py [build|test|check|verify|all]")
        return
    
    action = sys.argv[1]
    
    if action == "build":
        build_docker()
    elif action == "test":
        test_imports()
    elif action == "check":
        check_files()
    elif action == "verify":
        verify_batch()
    elif action == "all":
        print("[SYNC] 执行完整同步流程")
        success = True
        success &= build_docker()
        success &= test_imports()
        success &= check_files()
        
        if success:
            print("[SUCCESS] Docker同步完成!")
        else:
            print("[FAILED] 同步过程中出现错误")
    else:
        print(f"未知操作: {action}")

if __name__ == "__main__":
    main()