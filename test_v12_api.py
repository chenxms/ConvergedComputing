"""
测试V1.2 API端点
"""
import time
import sys
import os

# 设置项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ['PYTHONPATH'] = os.path.dirname(os.path.abspath(__file__))

def test_v12_endpoint():
    """测试V1.2端点是否正常工作"""
    from app.database.connection import SessionLocal
    from app.api.subjects_v12_api import _fetch_v12_regional

    print("测试V1.2 API端点...")

    db = SessionLocal()
    try:
        batch_code = "G4-2025"
        print(f"获取批次 {batch_code} 的区域级数据...")

        start_time = time.time()
        result = _fetch_v12_regional(db, batch_code)
        elapsed_time = time.time() - start_time

        print(f"✅ 成功获取数据！耗时: {elapsed_time:.2f}秒")

        # 输出数据概要
        if result:
            print(f"数据版本: {result.get('data_version', 'N/A')}")
            print(f"批次代码: {result.get('batch_code', 'N/A')}")
            print(f"聚合级别: {result.get('aggregation_level', 'N/A')}")
            subjects = result.get('subjects', [])
            print(f"科目数量: {len(subjects)}")

            # 显示科目列表
            for subj in subjects[:3]:  # 只显示前3个
                print(f"  - {subj.get('name', 'N/A')} (类型: {subj.get('type', 'N/A')})")

            return True
        else:
            print("❌ 未获取到数据")
            return False

    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    success = test_v12_endpoint()
    sys.exit(0 if success else 1)