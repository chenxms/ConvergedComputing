#!/usr/bin/env python3
"""
调试维度统计问题
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

async def debug_dimensions():
    """调试维度统计问题"""
    print("=== 调试维度统计问题 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        batch_code = 'G4-2025'
        subject_name = '数学'
        
        # 1. 检查batch_dimension_definition表结构和数据
        print("1. 检查batch_dimension_definition表:")
        query1 = text("DESCRIBE batch_dimension_definition")
        result1 = session.execute(query1)
        print("表结构:")
        for row in result1.fetchall():
            print(f"  {row}")
        
        # 检查有哪些subject_id值
        query2 = text("""
            SELECT DISTINCT batch_code, subject_id 
            FROM batch_dimension_definition 
            WHERE batch_code = :batch_code
            LIMIT 10
        """)
        result2 = session.execute(query2, {'batch_code': batch_code})
        print(f"\n批次 {batch_code} 的subject_id值:")
        for row in result2.fetchall():
            print(f"  batch_code: {row[0]}, subject_id: {row[1]}")
        
        # 2. 检查question_dimension_mapping表
        print(f"\n2. 检查question_dimension_mapping表:")
        query3 = text("DESCRIBE question_dimension_mapping")
        result3 = session.execute(query3)
        print("表结构:")
        for row in result3.fetchall():
            print(f"  {row}")
            
        query4 = text("""
            SELECT DISTINCT batch_code, subject_id 
            FROM question_dimension_mapping 
            WHERE batch_code = :batch_code
            LIMIT 10
        """)
        result4 = session.execute(query4, {'batch_code': batch_code})
        print(f"\n批次 {batch_code} 的subject_id值:")
        for row in result4.fetchall():
            print(f"  batch_code: {row[0]}, subject_id: {row[1]}")
        
        # 3. 查找正确的subject_id对应关系
        print(f"\n3. 查找科目名称到subject_id的映射:")
        # 从student_score_detail表查看subject_name和可能的映射
        query5 = text("""
            SELECT DISTINCT subject_name
            FROM student_score_detail 
            WHERE batch_code = :batch_code
            ORDER BY subject_name
        """)
        result5 = session.execute(query5, {'batch_code': batch_code})
        print(f"student_score_detail表中的科目名称:")
        for row in result5.fetchall():
            print(f"  {row[0]}")
        
        # 4. 尝试用科目名称直接查询维度 (修复后)
        print(f"\n4. 尝试用科目名称 '{subject_name}' 查询维度 (修复后):")
        query6 = text("""
            SELECT dimension_code, dimension_name
            FROM batch_dimension_definition 
            WHERE batch_code = :batch_code 
                AND subject_name = :subject_name
        """)
        result6 = session.execute(query6, {
            'batch_code': batch_code, 
            'subject_name': subject_name
        })
        dimensions = result6.fetchall()
        print(f"找到 {len(dimensions)} 个维度:")
        for row in dimensions:
            print(f"  {row[0]} - {row[1]}")
            
        # 5. 测试题目映射
        if dimensions:
            dimension_code = dimensions[0][0]  # 取第一个维度测试
            print(f"\n5. 测试维度 '{dimension_code}' 的题目映射:")
            query7 = text("""
                SELECT DISTINCT question_id
                FROM question_dimension_mapping
                WHERE batch_code = :batch_code
                    AND subject_name = :subject_name
                    AND dimension_code = :dimension_code
                ORDER BY question_id
            """)
            result7 = session.execute(query7, {
                'batch_code': batch_code,
                'subject_name': subject_name, 
                'dimension_code': dimension_code
            })
            questions = result7.fetchall()
            print(f"找到 {len(questions)} 个题目:")
            for i, row in enumerate(questions[:5]):  # 只显示前5个
                print(f"  {row[0]}")
            if len(questions) > 5:
                print(f"  ... 还有 {len(questions) - 5} 个题目")
        
        session.close()
        
    except Exception as e:
        print(f"调试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_dimensions())