# 简化统计计算器
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Union
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..utils.precision_handler import (
    format_decimal, format_percentage, batch_format_dict,
    create_statistics_summary, validate_numeric_ranges, safe_divide
)
from ..database.enums import SubjectType

logger = logging.getLogger(__name__)


class SimplifiedStatisticsCalculator:
    """简化的统计计算器，专注于核心指标计算"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.decimal_places = 2
    
    def calculate_subject_metrics(self, batch_code: str, subject_name: str, 
                                subject_type: str = "考试类", 
                                aggregation_level: str = "regional",
                                school_id: Optional[str] = None) -> Dict[str, Any]:
        """
        计算科目级统计指标
        
        Args:
            batch_code: 批次代码
            subject_name: 科目名称
            subject_type: 科目类型（考试类/问卷类）
            aggregation_level: 聚合级别（regional/school）
            school_id: 学校ID（学校级时需要）
            
        Returns:
            科目统计指标字典
        """
        try:
            logger.info(f"开始计算科目指标: {subject_name}, 批次: {batch_code}")
            
            # 获取数据
            data = self._fetch_subject_data(batch_code, subject_name, aggregation_level, school_id)
            if data.empty:
                logger.warning(f"未找到科目数据: {subject_name}, 批次: {batch_code}")
                return self._create_empty_metrics()
            
            # 根据科目类型选择计算方法
            if subject_type == "问卷类":
                return self._calculate_survey_metrics(data, subject_name)
            else:
                return self._calculate_exam_metrics(data, subject_name)
        
        except Exception as e:
            logger.error(f"计算科目指标失败: {subject_name}, 错误: {str(e)}")
            return self._create_empty_metrics()
    
    def _fetch_subject_data(self, batch_code: str, subject_name: str, 
                           aggregation_level: str, school_id: Optional[str] = None) -> pd.DataFrame:
        """从cleaned_scores表获取科目数据"""
        try:
            # 构建基础查询
            base_query = """
                SELECT 
                    student_id,
                    school_id,
                    school_name,
                    subject_name,
                    total_score,
                    max_score,
                    dimension_scores,
                    grade_level
                FROM cleaned_scores 
                WHERE batch_code = :batch_code 
                AND subject_name = :subject_name
            """
            
            params = {
                'batch_code': batch_code,
                'subject_name': subject_name
            }
            
            # 添加学校级筛选
            if aggregation_level == "school" and school_id:
                base_query += " AND school_id = :school_id"
                params['school_id'] = school_id
            
            # 执行查询
            result = self.db.execute(text(base_query), params)
            data = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            logger.info(f"获取数据记录数: {len(data)}")
            return data
        
        except Exception as e:
            logger.error(f"获取科目数据失败: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_exam_metrics(self, data: pd.DataFrame, subject_name: str) -> Dict[str, Any]:
        """计算考试类科目指标"""
        try:
            # 基础数据准备
            scores = pd.to_numeric(data['total_score'], errors='coerce').dropna()
            max_scores = pd.to_numeric(data['max_score'], errors='coerce')
            max_score = max_scores.iloc[0] if len(max_scores) > 0 else 100
            
            if len(scores) == 0:
                return self._create_empty_metrics()
            
            # 核心统计指标
            metrics = {
                'subject_name': subject_name,
                'subject_type': '考试类',
                'total_students': len(scores),
                'avg_score': format_decimal(scores.mean()),
                'median_score': format_decimal(scores.median()),
                'std_deviation': format_decimal(scores.std(ddof=1)),
                'min_score': format_decimal(scores.min()),
                'max_score': format_decimal(scores.max()),
                'max_possible_score': format_decimal(max_score)
            }
            
            # 难度系数
            metrics['difficulty_coefficient'] = format_percentage(
                safe_divide(metrics['avg_score'], max_score)
            )
            
            # 区分度计算
            metrics['discrimination_index'] = self._calculate_discrimination_index(
                scores, max_score
            )
            
            # 百分位数
            percentiles = self._calculate_percentiles(scores, [10, 50, 90])
            metrics.update(percentiles)
            
            # 等级分布
            grade_level = data['grade_level'].iloc[0] if len(data) > 0 else '1st_grade'
            grade_distribution = self._calculate_grade_distribution(scores, max_score, grade_level)
            metrics['grade_distribution'] = grade_distribution
            
            # 格式化所有数值
            return batch_format_dict(metrics, self.decimal_places)
        
        except Exception as e:
            logger.error(f"计算考试类指标失败: {str(e)}")
            return self._create_empty_metrics()
    
    def _calculate_survey_metrics(self, data: pd.DataFrame, subject_name: str) -> Dict[str, Any]:
        """计算问卷类科目指标（简化版）"""
        try:
            scores = pd.to_numeric(data['total_score'], errors='coerce').dropna()
            max_scores = pd.to_numeric(data['max_score'], errors='coerce')
            max_score = max_scores.iloc[0] if len(max_scores) > 0 else 5  # 问卷通常5分制
            
            if len(scores) == 0:
                return self._create_empty_metrics()
            
            metrics = {
                'subject_name': subject_name,
                'subject_type': '问卷类',
                'total_students': len(scores),
                'avg_score': format_decimal(scores.mean()),
                'median_score': format_decimal(scores.median()),
                'std_deviation': format_decimal(scores.std(ddof=1)),
                'min_score': format_decimal(scores.min()),
                'max_score': format_decimal(scores.max()),
                'max_possible_score': format_decimal(max_score)
            }
            
            # 问卷类没有难度系数和区分度，但有得分率
            metrics['average_score_rate'] = format_percentage(
                safe_divide(metrics['avg_score'], max_score)
            )
            
            # 百分位数
            percentiles = self._calculate_percentiles(scores, [10, 50, 90])
            metrics.update(percentiles)
            
            # 格式化所有数值
            return batch_format_dict(metrics, self.decimal_places)
        
        except Exception as e:
            logger.error(f"计算问卷类指标失败: {str(e)}")
            return self._create_empty_metrics()
    
    def calculate_dimension_metrics(self, batch_code: str, subject_name: str,
                                  aggregation_level: str = "regional",
                                  school_id: Optional[str] = None) -> Dict[str, Any]:
        """
        计算维度级统计指标
        
        Args:
            batch_code: 批次代码
            subject_name: 科目名称
            aggregation_level: 聚合级别
            school_id: 学校ID
            
        Returns:
            维度统计指标字典
        """
        try:
            logger.info(f"开始计算维度指标: {subject_name}, 批次: {batch_code}")
            
            data = self._fetch_subject_data(batch_code, subject_name, aggregation_level, school_id)
            if data.empty:
                return {}
            
            dimension_metrics = {}
            
            # 处理每个学生的维度分数
            for _, row in data.iterrows():
                try:
                    # 解析JSON格式的维度分数
                    dimension_scores = row['dimension_scores']
                    if dimension_scores and isinstance(dimension_scores, dict):
                        for dimension_name, score in dimension_scores.items():
                            if dimension_name not in dimension_metrics:
                                dimension_metrics[dimension_name] = []
                            
                            # 添加有效分数
                            if score is not None and pd.notna(score):
                                dimension_metrics[dimension_name].append(float(score))
                
                except Exception as e:
                    logger.warning(f"处理维度分数失败，学生ID: {row.get('student_id')}, 错误: {str(e)}")
                    continue
            
            # 计算每个维度的统计指标
            result = {}
            for dimension_name, scores_list in dimension_metrics.items():
                if scores_list:
                    scores_series = pd.Series(scores_list)
                    
                    # 获取维度满分（假设从题目配置中获取，这里简化处理）
                    max_score = self._get_dimension_max_score(subject_name, dimension_name)
                    
                    dimension_stats = {
                        'dimension_name': dimension_name,
                        'student_count': len(scores_list),
                        'avg_score': format_decimal(scores_series.mean()),
                        'score_rate': format_percentage(
                            safe_divide(scores_series.mean(), max_score)
                        ),
                        'std_deviation': format_decimal(scores_series.std(ddof=1)),
                        'min_score': format_decimal(scores_series.min()),
                        'max_score': format_decimal(scores_series.max())
                    }
                    
                    result[dimension_name] = dimension_stats
            
            return result
        
        except Exception as e:
            logger.error(f"计算维度指标失败: {str(e)}")
            return {}
    
    def _calculate_discrimination_index(self, scores: pd.Series, max_score: float) -> Optional[float]:
        """计算区分度指数"""
        try:
            if len(scores) < 10:
                return None
            
            scores_sorted = scores.sort_values(ascending=False)
            n = len(scores_sorted)
            
            # 前27%和后27%
            high_group_size = max(1, int(n * 0.27))
            low_group_size = max(1, int(n * 0.27))
            
            high_group_mean = scores_sorted.iloc[:high_group_size].mean()
            low_group_mean = scores_sorted.iloc[-low_group_size:].mean()
            
            discrimination = (high_group_mean - low_group_mean) / max_score
            return format_decimal(discrimination, 4)  # 区分度保留4位小数
        
        except Exception as e:
            logger.warning(f"计算区分度失败: {str(e)}")
            return None
    
    def _calculate_percentiles(self, scores: pd.Series, percentiles: List[int]) -> Dict[str, Optional[float]]:
        """计算百分位数"""
        try:
            scores_sorted = scores.sort_values()
            n = len(scores_sorted)
            result = {}
            
            for p in percentiles:
                # 使用教育统计标准算法
                rank = int(np.floor(n * p / 100.0))
                rank = max(0, min(rank, n - 1))
                result[f'P{p}'] = format_decimal(scores_sorted.iloc[rank])
            
            return result
        
        except Exception as e:
            logger.warning(f"计算百分位数失败: {str(e)}")
            return {f'P{p}': None for p in percentiles}
    
    def _calculate_grade_distribution(self, scores: pd.Series, max_score: float, 
                                    grade_level: str) -> Dict[str, Any]:
        """计算等级分布"""
        try:
            total_count = len(scores)
            
            if self._is_primary_grade(grade_level):
                # 小学标准：优秀≥90, 良好80-89, 及格60-79, 不及格<60
                excellent_count = (scores >= max_score * 0.90).sum()
                good_count = ((scores >= max_score * 0.80) & (scores < max_score * 0.90)).sum()
                pass_count = ((scores >= max_score * 0.60) & (scores < max_score * 0.80)).sum()
                fail_count = (scores < max_score * 0.60).sum()
                
                return {
                    'excellent': {
                        'count': int(excellent_count),
                        'percentage': format_percentage(excellent_count / total_count)
                    },
                    'good': {
                        'count': int(good_count),
                        'percentage': format_percentage(good_count / total_count)
                    },
                    'pass': {
                        'count': int(pass_count),
                        'percentage': format_percentage(pass_count / total_count)
                    },
                    'fail': {
                        'count': int(fail_count),
                        'percentage': format_percentage(fail_count / total_count)
                    }
                }
            else:
                # 初中标准：A≥85, B70-84, C60-69, D<60
                a_count = (scores >= max_score * 0.85).sum()
                b_count = ((scores >= max_score * 0.70) & (scores < max_score * 0.85)).sum()
                c_count = ((scores >= max_score * 0.60) & (scores < max_score * 0.70)).sum()
                d_count = (scores < max_score * 0.60).sum()
                
                return {
                    'A': {
                        'count': int(a_count),
                        'percentage': format_percentage(a_count / total_count)
                    },
                    'B': {
                        'count': int(b_count),
                        'percentage': format_percentage(b_count / total_count)
                    },
                    'C': {
                        'count': int(c_count),
                        'percentage': format_percentage(c_count / total_count)
                    },
                    'D': {
                        'count': int(d_count),
                        'percentage': format_percentage(d_count / total_count)
                    }
                }
        
        except Exception as e:
            logger.warning(f"计算等级分布失败: {str(e)}")
            return {}
    
    def _is_primary_grade(self, grade_level: str) -> bool:
        """判断是否为小学年级"""
        primary_grades = [
            '1st_grade', '2nd_grade', '3rd_grade', 
            '4th_grade', '5th_grade', '6th_grade',
            '1th_grade', '2th_grade', '3th_grade'  # 兼容旧格式
        ]
        return grade_level in primary_grades
    
    def _get_dimension_max_score(self, subject_name: str, dimension_name: str) -> float:
        """获取维度满分（简化实现）"""
        try:
            # 从题目配置表查询维度满分
            query = text("""
                SELECT SUM(max_score) as total_max_score
                FROM question_dimension_mapping qdm
                JOIN subject_question_config sqc ON qdm.question_id = sqc.question_id
                WHERE sqc.subject_name = :subject_name 
                AND qdm.dimension_name = :dimension_name
            """)
            
            result = self.db.execute(query, {
                'subject_name': subject_name,
                'dimension_name': dimension_name
            }).fetchone()
            
            if result and result.total_max_score:
                return float(result.total_max_score)
            else:
                # 默认满分（根据科目类型）
                return 100.0 if "考试" in subject_name else 5.0
        
        except Exception as e:
            logger.warning(f"获取维度满分失败: {str(e)}")
            return 100.0  # 默认满分
    
    def _create_empty_metrics(self) -> Dict[str, Any]:
        """创建空的指标字典"""
        return {
            'subject_name': '',
            'subject_type': '',
            'total_students': 0,
            'avg_score': None,
            'median_score': None,
            'std_deviation': None,
            'min_score': None,
            'max_score': None,
            'max_possible_score': None,
            'difficulty_coefficient': None,
            'discrimination_index': None,
            'P10': None,
            'P50': None,
            'P90': None,
            'grade_distribution': {}
        }
    
    def calculate_batch_summary(self, batch_code: str, aggregation_level: str = "regional",
                               school_id: Optional[str] = None) -> Dict[str, Any]:
        """
        计算批次汇总统计
        
        Args:
            batch_code: 批次代码
            aggregation_level: 聚合级别
            school_id: 学校ID
            
        Returns:
            批次汇总统计
        """
        try:
            # 获取批次中的所有科目
            query = text("""
                SELECT DISTINCT subject_name, COUNT(DISTINCT student_id) as student_count
                FROM cleaned_scores 
                WHERE batch_code = :batch_code
            """)
            
            params = {'batch_code': batch_code}
            
            if aggregation_level == "school" and school_id:
                query = text("""
                    SELECT DISTINCT subject_name, COUNT(DISTINCT student_id) as student_count
                    FROM cleaned_scores 
                    WHERE batch_code = :batch_code AND school_id = :school_id
                    GROUP BY subject_name
                """)
                params['school_id'] = school_id
            else:
                query = text("""
                    SELECT DISTINCT subject_name, COUNT(DISTINCT student_id) as student_count
                    FROM cleaned_scores 
                    WHERE batch_code = :batch_code
                    GROUP BY subject_name
                """)
            
            result = self.db.execute(query, params)
            subjects_data = result.fetchall()
            
            summary = {
                'batch_code': batch_code,
                'aggregation_level': aggregation_level,
                'total_subjects': len(subjects_data),
                'subjects': {}
            }
            
            if school_id:
                summary['school_id'] = school_id
            
            total_students = 0
            for row in subjects_data:
                subject_name = row.subject_name
                student_count = row.student_count
                
                # 计算科目指标
                subject_metrics = self.calculate_subject_metrics(
                    batch_code, subject_name, "考试类", aggregation_level, school_id
                )
                
                summary['subjects'][subject_name] = subject_metrics
                total_students = max(total_students, student_count)
            
            summary['total_students'] = total_students
            
            return batch_format_dict(summary, self.decimal_places)
        
        except Exception as e:
            logger.error(f"计算批次汇总失败: {str(e)}")
            return {
                'batch_code': batch_code,
                'aggregation_level': aggregation_level,
                'total_subjects': 0,
                'total_students': 0,
                'subjects': {}
            }


class CalculatorFactory:
    """计算器工厂类"""
    
    @staticmethod
    def create_calculator(db_session: Session, calculator_type: str = "simplified") -> SimplifiedStatisticsCalculator:
        """
        创建计算器实例
        
        Args:
            db_session: 数据库会话
            calculator_type: 计算器类型
            
        Returns:
            计算器实例
        """
        if calculator_type == "simplified":
            return SimplifiedStatisticsCalculator(db_session)
        else:
            raise ValueError(f"不支持的计算器类型: {calculator_type}")


# 便捷函数
def calculate_subject_statistics(db_session: Session, batch_code: str, subject_name: str,
                               subject_type: str = "考试类", aggregation_level: str = "regional",
                               school_id: Optional[str] = None) -> Dict[str, Any]:
    """
    便捷函数：计算科目统计
    """
    calculator = CalculatorFactory.create_calculator(db_session, "simplified")
    return calculator.calculate_subject_metrics(
        batch_code, subject_name, subject_type, aggregation_level, school_id
    )


def calculate_dimension_statistics(db_session: Session, batch_code: str, subject_name: str,
                                 aggregation_level: str = "regional",
                                 school_id: Optional[str] = None) -> Dict[str, Any]:
    """
    便捷函数：计算维度统计
    """
    calculator = CalculatorFactory.create_calculator(db_session, "simplified")
    return calculator.calculate_dimension_metrics(
        batch_code, subject_name, aggregation_level, school_id
    )