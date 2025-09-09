#!/usr/bin/env python3
"""
数据调查服务 - 分析异常数据情况
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class DataInvestigationService:
    """数据调查服务"""
    
    def __init__(self, db_session):
        self.db_session = db_session
    
    async def investigate_zero_scores(self, batch_code: str = 'G4-2025'):
        """调查数学科目分数为0的异常情况"""
        print(f"=== 调查数学科目异常情况 (批次: {batch_code}) ===\n")
        
        try:
            # 1. 检查原始数据中数学科目的情况
            await self._check_original_math_data(batch_code)
            
            # 2. 检查科目配置
            await self._check_subject_config(batch_code)
            
            # 3. 检查清洗过程的问题
            await self._check_cleaning_process(batch_code)
            
        except Exception as e:
            print(f"数据调查失败: {e}")
            import traceback
            traceback.print_exc()
    
    async def _check_original_math_data(self, batch_code: str):
        """检查原始数学数据"""
        print("1. 检查原始数学数据...")
        
        try:
            # 检查原始数据中的数学科目记录
            query = text("""
                SELECT 
                    subject_name,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT student_id) as student_count,
                    MIN(total_score) as min_score,
                    MAX(total_score) as max_score,
                    AVG(total_score) as avg_score,
                    COUNT(CASE WHEN total_score = 0 THEN 1 END) as zero_scores
                FROM student_score_detail 
                WHERE batch_code = :batch_code 
                AND subject_name LIKE '%数学%'
                GROUP BY subject_name
                ORDER BY subject_name
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            rows = result.fetchall()
            
            if rows:
                print("原始数学数据统计:")
                print("-" * 100)
                print(f"{'科目名称':<15} {'记录数':<8} {'学生数':<8} {'最低分':<8} {'最高分':<8} {'平均分':<8} {'零分数':<8}")
                print("-" * 100)
                
                for row in rows:
                    subject_name, record_count, student_count, min_score, max_score, avg_score, zero_scores = row
                    print(f"{subject_name:<15} {record_count:<8} {student_count:<8} {min_score:<8.2f} {max_score:<8.2f} {avg_score:<8.2f} {zero_scores:<8}")
                print("-" * 100)
                
                # 查看具体的零分记录样本
                await self._show_zero_score_samples(batch_code)
            else:
                print("没有找到原始数学数据")
                
        except Exception as e:
            print(f"检查原始数学数据失败: {e}")
    
    async def _show_zero_score_samples(self, batch_code: str):
        """显示零分记录样本"""
        try:
            query = text("""
                SELECT 
                    student_id, student_name, subject_name, total_score,
                    school_name, class_name
                FROM student_score_detail 
                WHERE batch_code = :batch_code 
                AND subject_name LIKE '%数学%'
                AND total_score = 0
                LIMIT 5
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            rows = result.fetchall()
            
            if rows:
                print("\n零分记录样本:")
                print("-" * 120)
                print(f"{'学生ID':<15} {'学生姓名':<10} {'科目':<10} {'分数':<8} {'学校':<20} {'班级':<15}")
                print("-" * 120)
                
                for row in rows:
                    student_id, student_name, subject_name, total_score, school_name, class_name = row
                    print(f"{student_id:<15} {str(student_name):<10} {subject_name:<10} {total_score:<8.2f} {str(school_name):<20} {str(class_name):<15}")
                print("-" * 120)
                
        except Exception as e:
            print(f"显示零分样本失败: {e}")
    
    async def _check_subject_config(self, batch_code: str):
        """检查科目配置"""
        print("\n2. 检查科目配置...")
        
        try:
            query = text("""
                SELECT 
                    subject_name,
                    COUNT(*) as question_count,
                    SUM(max_score) as total_max_score,
                    MIN(max_score) as min_question_score,
                    MAX(max_score) as max_question_score
                FROM subject_question_config 
                WHERE batch_code = :batch_code
                GROUP BY subject_name
                ORDER BY subject_name
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            rows = result.fetchall()
            
            if rows:
                print("科目配置统计:")
                print("-" * 100)
                print(f"{'科目名称':<15} {'题目数':<8} {'总满分':<10} {'最小题分':<10} {'最大题分':<10}")
                print("-" * 100)
                
                for row in rows:
                    subject_name, question_count, total_max_score, min_q_score, max_q_score = row
                    print(f"{subject_name:<15} {question_count:<8} {total_max_score:<10.1f} {min_q_score:<10.1f} {max_q_score:<10.1f}")
                print("-" * 100)
            else:
                print("没有找到科目配置")
                
        except Exception as e:
            print(f"检查科目配置失败: {e}")
    
    async def _check_cleaning_process(self, batch_code: str):
        """检查清洗过程"""
        print("\n3. 检查清洗过程...")
        
        try:
            # 检查是否有重复的科目名称导致问题
            query = text("""
                SELECT 
                    subject_name,
                    COUNT(DISTINCT subject_id) as unique_subject_ids
                FROM student_score_detail 
                WHERE batch_code = :batch_code
                GROUP BY subject_name
                HAVING COUNT(DISTINCT subject_id) > 1
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            rows = result.fetchall()
            
            if rows:
                print("发现科目名称对应多个subject_id的情况:")
                for row in rows:
                    subject_name, unique_ids = row
                    print(f"  科目 '{subject_name}' 对应 {unique_ids} 个不同的subject_id")
                    
                    # 显示具体的subject_id
                    detail_query = text("""
                        SELECT DISTINCT subject_id, COUNT(*) as record_count
                        FROM student_score_detail 
                        WHERE batch_code = :batch_code AND subject_name = :subject_name
                        GROUP BY subject_id
                        ORDER BY record_count DESC
                    """)
                    
                    detail_result = self.db_session.execute(detail_query, {
                        'batch_code': batch_code, 
                        'subject_name': subject_name
                    })
                    detail_rows = detail_result.fetchall()
                    
                    print(f"    详细分布:")
                    for detail_row in detail_rows:
                        subject_id, count = detail_row
                        print(f"      subject_id: {subject_id}, 记录数: {count}")
                    print()
            else:
                print("没有发现科目名称重复映射问题")
            
            # 检查聚合逻辑是否正确
            await self._verify_aggregation_logic(batch_code)
                
        except Exception as e:
            print(f"检查清洗过程失败: {e}")
    
    async def _verify_aggregation_logic(self, batch_code: str):
        """验证聚合逻辑"""
        print("\n4. 验证聚合逻辑...")
        
        try:
            # 选择一个数学分数为0的学生，检查其原始数据
            query = text("""
                SELECT student_id 
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code 
                AND subject_name = '数学' 
                AND total_score = 0
                LIMIT 1
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            row = result.fetchone()
            
            if row:
                student_id = row[0]
                print(f"检查学生 {student_id} 的数学原始数据:")
                
                # 查看该学生的原始数学数据
                detail_query = text("""
                    SELECT 
                        student_id, subject_name, total_score,
                        question_id, question_name
                    FROM student_score_detail 
                    WHERE batch_code = :batch_code 
                    AND student_id = :student_id
                    AND subject_name = '数学'
                    ORDER BY question_id
                """)
                
                detail_result = self.db_session.execute(detail_query, {
                    'batch_code': batch_code,
                    'student_id': student_id
                })
                detail_rows = detail_result.fetchall()
                
                if detail_rows:
                    print("-" * 80)
                    print(f"{'题目ID':<15} {'题目名称':<25} {'得分':<8}")
                    print("-" * 80)
                    
                    total = 0
                    for detail_row in detail_rows:
                        _, _, score, question_id, question_name = detail_row
                        print(f"{str(question_id):<15} {str(question_name):<25} {score:<8.2f}")
                        total += score
                    
                    print("-" * 80)
                    print(f"{'合计':<40} {total:<8.2f}")
                    print("-" * 80)
                    
                    print(f"\n该学生数学总分应为: {total:.2f}")
                else:
                    print(f"没有找到学生 {student_id} 的数学原始数据")
            else:
                print("没有找到数学分数为0的学生")
                
        except Exception as e:
            print(f"验证聚合逻辑失败: {e}")


async def main():
    """主函数"""
    print("=== 数据调查服务 ===\n")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    
    try:
        engine = create_engine(DATABASE_URL, echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建调查服务
        investigation_service = DataInvestigationService(session)
        
        # 调查零分异常
        await investigation_service.investigate_zero_scores('G4-2025')
        
        session.close()
        print("\n=== 调查完成 ===")
        
    except Exception as e:
        print(f"程序执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())