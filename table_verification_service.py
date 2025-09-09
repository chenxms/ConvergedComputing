#!/usr/bin/env python3
"""
表验证和数据清洗验证服务
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


class TableVerificationService:
    """表验证服务"""
    
    def __init__(self, db_session):
        self.db_session = db_session
        self.engine = db_session.bind
    
    async def verify_and_create_table(self) -> bool:
        """验证并创建student_cleaned_scores表"""
        print("=== 表验证和创建 ===")
        
        try:
            # 1. 检查表是否存在
            table_exists = await self._check_table_exists('student_cleaned_scores')
            
            if table_exists:
                print("[OK] student_cleaned_scores 表已存在")
                # 显示表结构
                await self._show_table_structure('student_cleaned_scores')
                return True
            else:
                print("[ERROR] student_cleaned_scores 表不存在，开始创建...")
                return await self._create_table()
                
        except Exception as e:
            print(f"表验证失败: {e}")
            return False
    
    async def _check_table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            query = text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = :table_name
            """)
            result = self.db_session.execute(query, {'table_name': table_name})
            count = result.scalar()
            return count > 0
        except Exception as e:
            print(f"检查表存在性失败: {e}")
            return False
    
    async def _show_table_structure(self, table_name: str):
        """显示表结构"""
        try:
            query = text(f"DESCRIBE {table_name}")
            result = self.db_session.execute(query)
            rows = result.fetchall()
            
            print(f"\n{table_name} 表结构:")
            print("-" * 80)
            print(f"{'字段名':<20} {'类型':<20} {'允许NULL':<10} {'键':<10} {'默认值':<15} {'备注'}")
            print("-" * 80)
            
            for row in rows:
                field, type_info, null, key, default, extra = row
                print(f"{field:<20} {type_info:<20} {null:<10} {key:<10} {str(default):<15} {extra}")
            print("-" * 80)
            
        except Exception as e:
            print(f"获取表结构失败: {e}")
    
    async def _create_table(self) -> bool:
        """创建student_cleaned_scores表"""
        try:
            # 读取SQL文件
            sql_file_path = os.path.join(os.path.dirname(__file__), 'create_cleaned_scores_table.sql')
            
            if not os.path.exists(sql_file_path):
                print(f"SQL文件不存在: {sql_file_path}")
                return False
            
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                create_sql = f.read()
            
            print("执行建表SQL...")
            self.db_session.execute(text(create_sql))
            self.db_session.commit()
            
            # 验证创建是否成功
            if await self._check_table_exists('student_cleaned_scores'):
                print("[OK] student_cleaned_scores 表创建成功!")
                await self._show_table_structure('student_cleaned_scores')
                return True
            else:
                print("[ERROR] 表创建失败")
                return False
                
        except Exception as e:
            print(f"创建表失败: {e}")
            self.db_session.rollback()
            return False
    
    async def verify_data_after_cleaning(self, batch_code: str = 'G4-2025'):
        """验证清洗后的数据"""
        print(f"\n=== 验证清洗后的数据 (批次: {batch_code}) ===")
        
        try:
            # 1. 统计总体数据
            await self._show_cleaning_summary(batch_code)
            
            # 2. 检查每个科目的数据
            await self._show_subject_details(batch_code)
            
            # 3. 检查数据完整性
            await self._check_data_integrity(batch_code)
            
        except Exception as e:
            print(f"数据验证失败: {e}")
    
    async def _show_cleaning_summary(self, batch_code: str):
        """显示清洗数据概要"""
        try:
            query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students,
                    COUNT(DISTINCT subject_name) as subjects_count,
                    MIN(total_score) as min_score,
                    MAX(total_score) as max_score,
                    AVG(total_score) as avg_score
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            row = result.fetchone()
            
            if row[0] > 0:
                print(f"清洗数据概要:")
                print(f"  总记录数: {row[0]}")
                print(f"  学生人数: {row[1]}")
                print(f"  科目数量: {row[2]}")
                print(f"  分数范围: {row[3]:.2f} - {row[4]:.2f}")
                print(f"  平均分数: {row[5]:.2f}")
            else:
                print("没有找到清洗后的数据")
                
        except Exception as e:
            print(f"获取清洗概要失败: {e}")
    
    async def _show_subject_details(self, batch_code: str):
        """显示各科目详细统计"""
        try:
            query = text("""
                SELECT 
                    subject_name,
                    max_score,
                    COUNT(*) as student_count,
                    MIN(total_score) as min_score,
                    MAX(total_score) as max_score,
                    AVG(total_score) as avg_score
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
                GROUP BY subject_name, max_score
                ORDER BY subject_name
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            rows = result.fetchall()
            
            if rows:
                print(f"\n各科目统计:")
                print("-" * 100)
                print(f"{'科目名称':<15} {'满分':<8} {'学生数':<8} {'最低分':<8} {'最高分':<8} {'平均分':<8} {'得分率':<8}")
                print("-" * 100)
                
                for row in rows:
                    subject_name, max_score, student_count, min_score, max_score_actual, avg_score = row
                    score_rate = (avg_score / max_score * 100) if max_score > 0 else 0
                    print(f"{subject_name:<15} {max_score:<8.1f} {student_count:<8} {min_score:<8.2f} {max_score_actual:<8.2f} {avg_score:<8.2f} {score_rate:<7.1f}%")
                print("-" * 100)
            else:
                print("没有找到科目数据")
                
        except Exception as e:
            print(f"获取科目统计失败: {e}")
    
    async def _check_data_integrity(self, batch_code: str):
        """检查数据完整性"""
        try:
            print(f"\n数据完整性检查:")
            
            # 1. 检查是否有重复记录
            query = text("""
                SELECT student_id, subject_name, COUNT(*) as cnt
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
                GROUP BY student_id, subject_name
                HAVING COUNT(*) > 1
                LIMIT 10
            """)
            result = self.db_session.execute(query, {'batch_code': batch_code})
            duplicates = result.fetchall()
            
            if duplicates:
                print(f"  [ERROR] 发现 {len(duplicates)} 组重复数据 (学生ID-科目组合)")
                for dup in duplicates:
                    print(f"    学生 {dup[0]}, 科目 {dup[1]}: {dup[2]} 条记录")
            else:
                print("  [OK] 无重复记录")
            
            # 2. 检查分数范围异常
            query = text("""
                SELECT subject_name, COUNT(*) as invalid_count
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code 
                AND (total_score < 0 OR total_score > max_score)
                GROUP BY subject_name
            """)
            result = self.db_session.execute(query, {'batch_code': batch_code})
            invalid_scores = result.fetchall()
            
            if invalid_scores:
                print(f"  [ERROR] 发现分数范围异常:")
                for inv in invalid_scores:
                    print(f"    科目 {inv[0]}: {inv[1]} 条异常记录")
            else:
                print("  [OK] 所有分数在有效范围内")
            
            # 3. 检查必填字段为空
            query = text("""
                SELECT 
                    SUM(CASE WHEN student_id IS NULL OR student_id = '' THEN 1 ELSE 0 END) as missing_student_id,
                    SUM(CASE WHEN subject_name IS NULL OR subject_name = '' THEN 1 ELSE 0 END) as missing_subject_name,
                    SUM(CASE WHEN total_score IS NULL THEN 1 ELSE 0 END) as missing_score
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
            """)
            result = self.db_session.execute(query, {'batch_code': batch_code})
            row = result.fetchone()
            
            issues = []
            if row[0] > 0: issues.append(f"学生ID缺失: {row[0]} 条")
            if row[1] > 0: issues.append(f"科目名称缺失: {row[1]} 条")
            if row[2] > 0: issues.append(f"分数缺失: {row[2]} 条")
            
            if issues:
                print(f"  [ERROR] 必填字段缺失:")
                for issue in issues:
                    print(f"    {issue}")
            else:
                print("  [OK] 必填字段完整")
                
        except Exception as e:
            print(f"数据完整性检查失败: {e}")


async def main():
    """主函数"""
    print("=== 表验证和数据清洗验证服务 ===\n")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    
    try:
        engine = create_engine(DATABASE_URL, echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 测试连接
        print("测试数据库连接...")
        session.execute(text("SELECT 1"))
        print("[OK] 数据库连接成功\n")
        
        # 创建验证服务
        verification_service = TableVerificationService(session)
        
        # 1. 验证并创建表
        table_created = await verification_service.verify_and_create_table()
        
        if not table_created:
            print("表创建失败，无法继续")
            return
        
        print("\n" + "="*50)
        
        # 2. 运行数据清洗服务
        print("开始运行数据清洗服务...")
        from data_cleaning_service import DataCleaningService
        
        cleaning_service = DataCleaningService(session)
        batch_code = 'G4-2025'
        
        cleaning_result = await cleaning_service.clean_batch_scores(batch_code)
        
        print("\n" + "="*50)
        
        # 3. 验证清洗后的数据
        await verification_service.verify_data_after_cleaning(batch_code)
        
        session.close()
        print("\n=== 验证完成 ===")
        
    except Exception as e:
        print(f"程序执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())