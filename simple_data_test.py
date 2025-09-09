#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单数据测试脚本
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.connection import engine

def main():
    print("数据库连接已更新到appraisal_test")
    print("正在检查可用的批次数据...")
    
    try:
        with engine.connect() as connection:
            # 检查student_score_detail中的批次
            result = connection.execute(text("""
                SELECT batch_code, COUNT(*) as records, 
                       COUNT(DISTINCT student_id) as students
                FROM student_score_detail 
                GROUP BY batch_code
                ORDER BY records DESC
            """))
            
            batches = result.fetchall()
            
            if batches:
                print(f"发现{len(batches)}个批次:")
                for batch_code, records, students in batches:
                    print(f"  {batch_code}: {records}条记录, {students}个学生")
                
                # 测试第一个批次
                test_batch = batches[0][0]
                print(f"\n测试批次: {test_batch}")
                
                # 检查学校数据
                result = connection.execute(text("""
                    SELECT COUNT(DISTINCT school_id) as school_count
                    FROM student_score_detail 
                    WHERE batch_code = :batch_code AND school_id IS NOT NULL
                """), {"batch_code": test_batch})
                
                school_count = result.fetchone()[0]
                print(f"包含学校数: {school_count}")
                
                print("\n数据汇聚计算可以开始测试!")
                print("下一步: 启动FastAPI服务器进行API测试")
                return True
            else:
                print("未找到批次数据")
                return False
                
    except Exception as e:
        print(f"检查失败: {e}")
        return False

if __name__ == "__main__":
    main()