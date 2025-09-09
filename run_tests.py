#!/usr/bin/env python3
"""
一键运行所有测试
适用于Docker环境已就绪的情况
"""

import sys
import os
import subprocess
from datetime import datetime


def run_command(description, command, required=True):
    """运行命令并处理结果"""
    print(f"\n{'='*60}")
    print(f"🔍 {description}")
    print(f"{'='*60}")
    
    try:
        # 使用subprocess运行Python脚本
        result = subprocess.run(
            [sys.executable] + command.split()[1:],  # 去掉python，使用当前解释器
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=300  # 5分钟超时
        )
        
        # 输出结果
        if result.stdout:
            print(result.stdout)
        if result.stderr and result.returncode != 0:
            print("ERROR OUTPUT:", result.stderr)
        
        success = result.returncode == 0
        
        if success:
            print(f"✅ {description} - 通过")
        else:
            print(f"❌ {description} - 失败")
            if required:
                print("关键测试失败，停止后续测试")
                return False
        
        return success
        
    except subprocess.TimeoutExpired:
        print(f"⏰ {description} - 超时")
        return False
    except Exception as e:
        print(f"❌ {description} - 异常: {str(e)}")
        return False


def main():
    print("=" * 70)
    print("🚀 Data-Calculation 一键测试启动器")
    print("=" * 70)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n📋 测试环境检查:")
    print("   - Docker容器运行: ✅ 已确认")
    print("   - API服务运行: ✅ http://localhost:8000")
    print("   - 数据库连接: ✅ 117.72.14.166:23506")
    
    # 定义测试序列
    tests = [
        {
            "name": "统计计算引擎测试",
            "command": "python scripts/test_engine_simple.py",
            "required": True,
            "description": "验证所有计算策略和算法"
        },
        {
            "name": "API接口功能测试", 
            "command": "python scripts/test_api_endpoints.py",
            "required": True,
            "description": "验证所有API端点和响应"
        },
        {
            "name": "端到端业务流程测试",
            "command": "python scripts/end_to_end_test.py", 
            "required": True,
            "description": "验证完整数据处理流程"
        }
    ]
    
    # 运行测试
    results = []
    total_start_time = datetime.now()
    
    for i, test in enumerate(tests, 1):
        print(f"\n\n📋 第{i}步：{test['name']}")
        print(f"📝 {test['description']}")
        
        success = run_command(test['name'], test['command'], test['required'])
        results.append((test['name'], success))
        
        if not success and test['required']:
            print(f"\n❌ 关键测试失败，中止后续测试")
            break
    
    # 计算总耗时
    total_time = datetime.now() - total_start_time
    
    # 生成测试报告
    print("\n" + "=" * 70)
    print("📊 测试结果汇总报告")
    print("=" * 70)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"   {test_name}: {status}")
        if success:
            passed += 1
    
    # 总体统计
    success_rate = (passed / total * 100) if total > 0 else 0
    print(f"\n📈 测试统计:")
    print(f"   通过率: {passed}/{total} ({success_rate:.1f}%)")
    print(f"   总耗时: {total_time.total_seconds():.1f}秒")
    
    # 结果判定和建议
    print(f"\n🎯 测试评估:")
    
    if success_rate == 100:
        print("   🎉 所有测试完美通过！系统运行优秀！")
        print("\n🚀 系统已准备就绪，建议后续操作：")
        print("   1. 导入真实业务数据进行验证")
        print("   2. 进行负载测试验证高并发性能")  
        print("   3. 集成前端系统进行UI测试")
        print("   4. 配置生产环境监控告警")
        return_code = 0
        
    elif success_rate >= 80:
        print("   ✅ 大部分测试通过，系统基本正常")
        print("   ⚠️ 建议解决失败项后部署生产环境")
        print("\n🔧 后续建议：")
        print("   1. 查看失败测试的详细日志")
        print("   2. 修复失败的功能模块")
        print("   3. 重新运行完整测试验证")
        return_code = 1
        
    elif success_rate >= 60:
        print("   ⚠️ 部分核心功能存在问题")
        print("   🔧 需要重点排查和修复")
        print("\n🛠️ 紧急建议：")
        print("   1. 检查Docker容器资源和网络")
        print("   2. 验证数据库连接和权限") 
        print("   3. 查看应用日志排查错误")
        print("   4. 逐个修复失败的测试项")
        return_code = 1
        
    else:
        print("   ❌ 多个关键功能测试失败")
        print("   🚨 系统存在严重问题，不建议继续使用")
        print("\n🆘 应急处理：")
        print("   1. 检查Docker环境是否正常")
        print("   2. 重启所有服务：docker-compose restart")
        print("   3. 查看系统资源使用情况")
        print("   4. 联系技术支持获取帮助")
        return_code = 2
    
    # 快速问题排查提示
    if success_rate < 100:
        print(f"\n🔍 快速排查命令：")
        print("   docker-compose ps              # 检查容器状态")
        print("   docker-compose logs web        # 查看应用日志")
        print("   curl http://localhost:8000/health  # 测试API连接")
    
    print("\n" + "=" * 70)
    sys.exit(return_code)


if __name__ == "__main__":
    main()