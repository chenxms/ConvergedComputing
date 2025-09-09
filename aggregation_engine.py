#!/usr/bin/env python3
"""
数据汇聚引擎基类
提供统一的汇聚计算接口和通用功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
import json
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from statistics_calculator import EducationalStatisticsCalculator

class AggregationLevel:
    """汇聚层级常量"""
    SCHOOL = "school"
    REGION = "region"

class SubjectType:
    """学科类型常量"""
    EXAM = "exam"
    QUESTIONNAIRE = "questionnaire"
    INTERACTION = "interaction"

class BaseAggregationEngine(ABC):
    """汇聚引擎基类"""
    
    def __init__(self, database_url: str = None):
        """
        初始化汇聚引擎
        
        Args:
            database_url: 数据库连接URL
        """
        self.database_url = database_url or "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        self.engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.engine)
        self.calculator = EducationalStatisticsCalculator()
        self.cache = {}  # 简单内存缓存
    
    def get_session(self):
        """获取数据库会话"""
        return self.Session()
    
    def close_session(self, session):
        """关闭数据库会话"""
        if session:
            session.close()
    
    def get_cache_key(self, *args) -> str:
        """生成缓存键"""
        return "_".join(str(arg) for arg in args)
    
    def set_cache(self, key: str, value: Any, ttl: int = 300):
        """设置缓存（简单实现，生产环境建议使用Redis）"""
        self.cache[key] = {
            'value': value,
            'expires': datetime.datetime.now() + datetime.timedelta(seconds=ttl)
        }
    
    def get_cache(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if key in self.cache:
            cache_item = self.cache[key]
            if datetime.datetime.now() < cache_item['expires']:
                return cache_item['value']
            else:
                del self.cache[key]
        return None
    
    def clear_expired_cache(self):
        """清理过期缓存"""
        now = datetime.datetime.now()
        expired_keys = [k for k, v in self.cache.items() if now >= v['expires']]
        for key in expired_keys:
            del self.cache[key]
    
    @abstractmethod
    def aggregate_subject_level(self, batch_code: str, school_code: str = None, 
                              subject_name: str = None) -> Dict[str, Any]:
        """
        学科层级汇聚（抽象方法）
        
        Args:
            batch_code: 批次代码
            school_code: 学校代码（可选，为空则进行区域层级汇聚）
            subject_name: 学科名称（可选）
            
        Returns:
            学科汇聚结果
        """
        pass
    
    @abstractmethod
    def aggregate_dimension_level(self, batch_code: str, school_code: str = None,
                                subject_name: str = None, dimension_code: str = None) -> Dict[str, Any]:
        """
        维度层级汇聚（抽象方法）
        
        Args:
            batch_code: 批次代码  
            school_code: 学校代码（可选）
            subject_name: 学科名称（可选）
            dimension_code: 维度代码（可选）
            
        Returns:
            维度汇聚结果
        """
        pass
    
    def get_school_info(self, batch_code: str, school_code: str = None) -> List[Tuple]:
        """
        获取学校信息
        
        Returns:
            (school_code, school_name, student_count) 元组列表
        """
        session = self.get_session()
        try:
            if school_code:
                query = text("""
                    SELECT DISTINCT school_code, school_name, COUNT(DISTINCT student_id) as student_count
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code AND school_code = :school_code
                    GROUP BY school_code, school_name
                """)
                result = session.execute(query, {'batch_code': batch_code, 'school_code': school_code})
            else:
                query = text("""
                    SELECT school_code, school_name, COUNT(DISTINCT student_id) as student_count
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code
                    GROUP BY school_code, school_name
                    ORDER BY student_count DESC
                """)
                result = session.execute(query, {'batch_code': batch_code})
            
            return result.fetchall()
        finally:
            self.close_session(session)
    
    def get_subject_info(self, batch_code: str, subject_type: str = None) -> List[Tuple]:
        """
        获取学科信息
        
        Returns:
            (subject_name, subject_type, student_count) 元组列表
        """
        session = self.get_session()
        try:
            if subject_type:
                query = text("""
                    SELECT subject_name, subject_type, COUNT(DISTINCT student_id) as student_count
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code AND subject_type = :subject_type
                    GROUP BY subject_name, subject_type
                    ORDER BY student_count DESC
                """)
                result = session.execute(query, {'batch_code': batch_code, 'subject_type': subject_type})
            else:
                query = text("""
                    SELECT subject_name, subject_type, COUNT(DISTINCT student_id) as student_count
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code
                    GROUP BY subject_name, subject_type
                    ORDER BY subject_type, student_count DESC
                """)
                result = session.execute(query, {'batch_code': batch_code})
                
            return result.fetchall()
        finally:
            self.close_session(session)
    
    def build_response(self, aggregation_level: str, batch_code: str, school_code: str = None,
                      subject_analysis: Dict = None, dimension_analysis: Dict = None,
                      metadata: Dict = None) -> Dict[str, Any]:
        """
        构建标准化的响应格式
        
        Args:
            aggregation_level: 汇聚层级
            batch_code: 批次代码
            school_code: 学校代码
            subject_analysis: 学科分析结果
            dimension_analysis: 维度分析结果
            metadata: 元数据
            
        Returns:
            标准化响应
        """
        response = {
            'aggregation_level': aggregation_level,
            'batch_code': batch_code,
            'generated_at': datetime.datetime.now().isoformat(),
            'data_version': '1.0'
        }
        
        if school_code:
            response['school_code'] = school_code
        
        if subject_analysis:
            response['subject_analysis'] = subject_analysis
            
        if dimension_analysis:
            response['dimension_analysis'] = dimension_analysis
        
        if metadata:
            response['metadata'] = metadata
            
        return response
    
    def validate_parameters(self, batch_code: str, school_code: str = None) -> Tuple[bool, str]:
        """
        验证输入参数
        
        Returns:
            (是否有效, 错误信息)
        """
        if not batch_code:
            return False, "批次代码不能为空"
        
        # 验证批次是否存在
        session = self.get_session()
        try:
            query = text("SELECT COUNT(*) FROM student_cleaned_scores WHERE batch_code = :batch_code")
            result = session.execute(query, {'batch_code': batch_code})
            count = result.scalar()
            
            if count == 0:
                return False, f"批次 {batch_code} 不存在"
            
            # 如果指定学校，验证学校是否存在
            if school_code:
                query = text("""
                    SELECT COUNT(*) FROM student_cleaned_scores 
                    WHERE batch_code = :batch_code AND school_code = :school_code
                """)
                result = session.execute(query, {'batch_code': batch_code, 'school_code': school_code})
                count = result.scalar()
                
                if count == 0:
                    return False, f"学校 {school_code} 在批次 {batch_code} 中不存在"
            
            return True, "参数有效"
            
        except Exception as e:
            return False, f"参数验证失败: {str(e)}"
        finally:
            self.close_session(session)
    
    def get_aggregation_metadata(self, batch_code: str, school_code: str = None) -> Dict[str, Any]:
        """
        获取汇聚元数据
        
        Returns:
            包含汇聚统计信息的元数据
        """
        session = self.get_session()
        try:
            # 基础统计
            if school_code:
                query = text("""
                    SELECT 
                        COUNT(DISTINCT student_id) as total_students,
                        COUNT(DISTINCT subject_name) as total_subjects,
                        1 as total_schools
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code AND school_code = :school_code
                """)
                result = session.execute(query, {'batch_code': batch_code, 'school_code': school_code})
            else:
                query = text("""
                    SELECT 
                        COUNT(DISTINCT student_id) as total_students,
                        COUNT(DISTINCT subject_name) as total_subjects,
                        COUNT(DISTINCT school_code) as total_schools
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code
                """)
                result = session.execute(query, {'batch_code': batch_code})
            
            row = result.fetchone()
            
            metadata = {
                'total_students': row[0] if row else 0,
                'total_subjects': row[1] if row else 0,
                'total_schools': row[2] if row else 0,
                'aggregation_time': datetime.datetime.now().isoformat(),
                'cache_enabled': True
            }
            
            return metadata
            
        finally:
            self.close_session(session)

def test_base_aggregation_engine():
    """测试基础汇聚引擎功能"""
    print("=== 测试基础汇聚引擎 ===\n")
    
    # 创建一个简单的测试实现
    class TestAggregationEngine(BaseAggregationEngine):
        def aggregate_subject_level(self, batch_code: str, school_code: str = None, 
                                  subject_name: str = None) -> Dict[str, Any]:
            return {"test": "subject_level"}
        
        def aggregate_dimension_level(self, batch_code: str, school_code: str = None,
                                    subject_name: str = None, dimension_code: str = None) -> Dict[str, Any]:
            return {"test": "dimension_level"}
    
    engine = TestAggregationEngine()
    
    # 测试参数验证
    valid, msg = engine.validate_parameters("G4-2025")
    print(f"1. 参数验证 (G4-2025): {valid} - {msg}")
    
    valid, msg = engine.validate_parameters("INVALID-BATCH")
    print(f"   参数验证 (INVALID): {valid} - {msg}")
    
    # 测试学校信息获取
    schools = engine.get_school_info("G4-2025")
    print(f"\n2. G4-2025批次学校数: {len(schools)}")
    if schools:
        print(f"   示例: {schools[0]}")
    
    # 测试学科信息获取
    subjects = engine.get_subject_info("G4-2025")
    print(f"\n3. G4-2025批次学科数: {len(subjects)}")
    if subjects:
        for subject in subjects[:3]:  # 显示前3个
            print(f"   {subject}")
    
    # 测试元数据获取
    metadata = engine.get_aggregation_metadata("G4-2025")
    print(f"\n4. 元数据:")
    print(f"   总学生数: {metadata['total_students']}")
    print(f"   总学科数: {metadata['total_subjects']}")
    print(f"   总学校数: {metadata['total_schools']}")
    
    # 测试缓存功能
    engine.set_cache("test_key", {"data": "test"}, ttl=60)
    cached_data = engine.get_cache("test_key")
    print(f"\n5. 缓存测试: {cached_data}")
    
    print("\n=== 基础功能测试完成 ===")

if __name__ == "__main__":
    test_base_aggregation_engine()