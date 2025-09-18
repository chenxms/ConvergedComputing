#!/usr/bin/env python3
"""
检查G7-2025物化状态和问题
"""
import subprocess
import json

def run_docker_command(cmd, description=""):
    """执行docker命令"""
    if description:
        print(f"\n{description}")

    full_cmd = f'docker exec converged-computing-app {cmd}'
    print(f"执行: {full_cmd}")

    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True, encoding='utf-8')

    if result.returncode == 0:
        return result.stdout
    else:
        print(f"错误: {result.stderr if result.stderr else '命令执行失败'}")
        return None

def main():
    print("="*60)
    print("G7-2025物化状态检查")
    print("="*60)

    # 1. 检查数据库中是否有G7-2025的数据
    print("\n[1] 检查数据库中G7-2025数据...")

    check_sql = """python -c "
import sys
sys.path.insert(0, '/app')
from app.database.connection import get_db_context
from sqlalchemy import text

with get_db_context() as db:
    # 检查statistical_aggregations表
    result = db.execute(text('''
        SELECT
            batch_code,
            aggregation_level,
            COUNT(*) as count,
            MAX(created_at) as latest,
            MIN(created_at) as earliest
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        GROUP BY batch_code, aggregation_level
    ''')).fetchall()

    if result:
        print('找到G7-2025数据:')
        for row in result:
            print(f'  级别: {row[1]}, 数量: {row[2]}, 最新: {row[3]}, 最早: {row[4]}')
    else:
        print('❌ 没有找到G7-2025的汇总数据')

    # 检查原始数据
    raw_count = db.execute(text('''
        SELECT COUNT(DISTINCT school_id) as schools, COUNT(*) as total
        FROM student_score_detail
        WHERE batch_code = 'G7-2025'
    ''')).fetchone()

    if raw_count:
        print(f'\\n原始数据: {raw_count[0]}个学校, {raw_count[1]}条记录')
"
"""

    run_docker_command(check_sql)

    # 2. 查看最近的日志
    print("\n[2] 查看容器最近日志（查找G7-2025相关）...")
    log_cmd = "tail -n 100 /app/logs/app.log 2>/dev/null | grep -i g7-2025 || echo '未找到相关日志'"
    run_docker_command(log_cmd)

    # 3. 直接测试物化脚本
    print("\n[3] 测试物化脚本（带详细输出）...")

    test_materialize = """python -c "
import sys
import os
sys.path.insert(0, '/app')

# 临时解锁
print('清除锁定...')
os.environ['DISABLE_WRITES_FOR_BATCHES'] = ''
print(f'当前DISABLE_WRITES_FOR_BATCHES: {os.environ.get(\"DISABLE_WRITES_FOR_BATCHES\", \"未设置\")}')

try:
    from scripts.rewrite_subjects_v12 import rewrite_batch
    from app.database.connection import get_db_context
    from sqlalchemy import text

    # 先检查是否已有数据
    with get_db_context() as db:
        existing = db.execute(text(
            'SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = :b'
        ), {'b': 'G7-2025'}).scalar()
        print(f'现有G7-2025记录数: {existing}')

        if existing > 0:
            print('⚠️ 已存在数据，清理中...')
            db.execute(text('DELETE FROM statistical_aggregations WHERE batch_code = :b'), {'b': 'G7-2025'})
            db.commit()
            print('已清理旧数据')

    print('\\n开始物化G7-2025...')
    print('这应该需要较长时间，如果快速结束说明有问题')

    # 执行物化
    rewrite_batch('G7-2025')

    print('\\n物化脚本执行完成')

    # 验证结果
    with get_db_context() as db:
        new_count = db.execute(text(
            'SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = :b'
        ), {'b': 'G7-2025'}).scalar()
        print(f'新增记录数: {new_count}')

except Exception as e:
    import traceback
    print(f'❌ 错误: {e}')
    traceback.print_exc()
"
"""

    run_docker_command(test_materialize)

    # 4. 检查锁定是否真的影响了写入
    print("\n[4] 测试锁定机制...")

    test_lock = """python -c "
import sys
import os
sys.path.insert(0, '/app')

# 测试有锁和无锁的情况
for disable in ['G7-2025', '']:
    os.environ['DISABLE_WRITES_FOR_BATCHES'] = disable
    print(f'\\n测试 DISABLE_WRITES_FOR_BATCHES={disable}')

    from app.database.repositories import StatisticalAggregationRepository
    from app.database.connection import get_db_context

    with get_db_context() as db:
        repo = StatisticalAggregationRepository(db)

        # 尝试写入测试数据
        test_data = {
            'batch_code': 'G7-2025',
            'aggregation_level': 'REGIONAL',
            'statistics_data': {'test': True},
            'school_id': None
        }

        try:
            result = repo.upsert_statistics(test_data)
            if result:
                print('  ✅ 写入成功')
            else:
                print('  ❌ 写入返回None（被阻止）')
        except Exception as e:
            print(f'  ❌ 写入失败: {e}')
"
"""

    run_docker_command(test_lock)

    print("\n" + "="*60)
    print("检查完成！")
    print("="*60)

    print("\n建议：")
    print("1. 如果没有原始数据，需要先导入student_score_detail表的G7-2025数据")
    print("2. 如果锁定机制阻止写入，需要在容器内临时解锁")
    print("3. 物化应该显示详细进度，如果快速结束说明有问题")

if __name__ == "__main__":
    main()