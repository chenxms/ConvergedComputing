#!/usr/bin/env python3
"""
SubjectsBuilder 调试脚本
逐步测试每个方法，找出阻塞点
"""
import sys
import os

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

def test_step(step_name, test_func):
    """执行测试步骤并记录结果"""
    try:
        print(f"[开始] {step_name}...", flush=True)
        result = test_func()
        print(f"[成功] {step_name}: {result}", flush=True)
        return True
    except Exception as e:
        print(f"[失败] {step_name}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return False

def test_database_connection():
    """测试数据库连接"""
    from app.database.connection import get_db_context
    from sqlalchemy import text
    
    with get_db_context() as db:
        result = db.execute(text("SELECT COUNT(*) FROM school_master_data WHERE batch_code='G4-2025' AND status='ACTIVE'")).fetchone()
        return f"G4-2025有{result[0]}所活跃学校"

def test_subjects_builder_init():
    """测试SubjectsBuilder初始化"""
    from app.services.subjects_builder import SubjectsBuilder
    sb = SubjectsBuilder()
    return "SubjectsBuilder初始化成功"

def test_list_subjects():
    """测试list_subjects方法"""
    from app.services.subjects_builder import SubjectsBuilder
    sb = SubjectsBuilder()
    subjects = sb.list_subjects('G4-2025')
    return f"发现{len(subjects)}个科目: {[s.name for s in subjects]}"

def test_discover_dimension_codes():
    """测试维度发现功能"""
    from app.services.subjects_builder import SubjectsBuilder
    sb = SubjectsBuilder()
    dims = sb._discover_dimension_codes('G4-2025', '数学')
    return f"数学科目维度: {dims[:3]}..." if dims else "无维度"

def test_compute_subject_metrics():
    """测试科目指标计算"""
    from app.services.subjects_builder import SubjectsBuilder
    sb = SubjectsBuilder()
    metrics = sb._compute_subject_metrics('G4-2025', '数学')
    return f"数学指标: avg={metrics.get('avg', 'N/A')}, stddev={metrics.get('stddev', 'N/A')}"

def main():
    print("=== SubjectsBuilder 诊断开始 ===", flush=True)
    
    tests = [
        ("数据库连接测试", test_database_connection),
        ("SubjectsBuilder初始化", test_subjects_builder_init),
        ("科目列表获取", test_list_subjects),
        ("维度代码发现", test_discover_dimension_codes),
        ("科目指标计算", test_compute_subject_metrics),
    ]
    
    for step_name, test_func in tests:
        success = test_step(step_name, test_func)
        if not success:
            print(f"=== 诊断中止于: {step_name} ===", flush=True)
            return 1
        print("", flush=True)  # 空行分隔
    
    print("=== 所有测试通过 ===", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())