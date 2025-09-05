"""
统计数据集成器

负责收集和整合所有统计计算结果（Tasks #2-10），
为JSON序列化提供统一的数据源。
"""

import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from datetime import datetime

from ...services.calculation_service import CalculationService
from ...calculation.calculators.survey_calculator import SurveyCalculator
from ...services.task_manager import TaskManager
from ...database.enums import AggregationLevel
# Note: These repositories need to be implemented based on existing database models
# For now we'll use mock implementations

logger = logging.getLogger(__name__)


class StatisticsDataIntegrator:
    """统计数据集成器"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.calculation_service = CalculationService(db_session)
        self.survey_calculator = SurveyCalculator(db_session)
        self.task_manager = TaskManager(db_session)
        # TODO: Replace with actual repository implementations
        self.aggregation_repo = None  # StatisticalAggregationRepository(db_session)
        self.score_repo = None        # StudentScoreRepository(db_session) 
        self.config_repo = None       # SubjectQuestionConfigRepository(db_session)
        self.dimension_repo = None    # BatchDimensionDefinitionRepository(db_session)
    
    async def collect_all_statistics(self, batch_code: str) -> Dict[str, Any]:
        """
        收集所有统计计算结果
        
        Args:
            batch_code: 批次代码
            
        Returns:
            包含所有统计数据的字典
        """
        logger.info(f"开始收集批次 {batch_code} 的所有统计数据")
        
        try:
            integrated_data = {
                'batch_code': batch_code,
                'collection_time': datetime.utcnow().isoformat()
            }
            
            # 收集基础信息
            batch_info = await self._collect_batch_info(batch_code)
            integrated_data['batch_info'] = batch_info
            
            # 收集学业科目统计 (Tasks #4-8)
            academic_subjects = await self._collect_academic_subjects(batch_code)
            integrated_data['academic_subjects'] = academic_subjects
            
            # 收集非学业科目统计 (Task #9)
            non_academic_subjects = await self._collect_non_academic_subjects(batch_code)
            integrated_data['non_academic_subjects'] = non_academic_subjects
            
            # 收集维度数据
            dimensions = await self._collect_dimensions_data(batch_code)
            integrated_data['dimensions'] = dimensions
            
            # 收集任务元数据 (Task #10)
            task_metadata = await self._collect_task_metadata(batch_code)
            integrated_data['task_metadata'] = task_metadata
            
            logger.info(f"批次 {batch_code} 统计数据收集完成")
            return integrated_data
            
        except Exception as e:
            logger.error(f"收集批次 {batch_code} 统计数据失败: {str(e)}")
            raise
    
    async def _collect_batch_info(self, batch_code: str) -> Dict[str, Any]:
        """收集批次基础信息"""
        logger.debug(f"收集批次 {batch_code} 基础信息")
        
        # 获取批次汇总数据
        regional_stats = await self.aggregation_repo.get_by_batch_code_and_level(
            batch_code, AggregationLevel.REGIONAL
        )
        
        if not regional_stats:
            # 如果没有汇总数据，从原始数据统计
            total_students = await self.score_repo.count_students_in_batch(batch_code)
            total_schools = await self.score_repo.count_schools_in_batch(batch_code)
            grade_level = await self._determine_grade_level(batch_code)
        else:
            total_students = regional_stats.total_students
            total_schools = regional_stats.total_schools
            grade_level = await self._determine_grade_level(batch_code)
        
        return {
            'batch_code': batch_code,
            'grade_level': grade_level,
            'total_schools': total_schools,
            'total_students': total_students,
            'calculation_time': datetime.utcnow().isoformat()
        }
    
    async def _collect_academic_subjects(self, batch_code: str) -> Dict[str, Any]:
        """收集学业科目统计数据"""
        logger.debug(f"收集批次 {batch_code} 学业科目数据")
        
        subjects_data = {}
        
        # 获取批次中所有考试类科目
        subjects = await self.config_repo.get_subjects_by_batch_and_type(
            batch_code, 'exam_type'
        )
        
        for subject in subjects:
            subject_stats = await self._calculate_subject_statistics(
                batch_code, subject.subject_id, subject.subject_name
            )
            subjects_data[subject.subject_name] = subject_stats
        
        return subjects_data
    
    async def _collect_non_academic_subjects(self, batch_code: str) -> Dict[str, Any]:
        """收集非学业科目统计数据（问卷类和人机交互类）"""
        logger.debug(f"收集批次 {batch_code} 非学业科目数据")
        
        non_academic_data = {}
        
        # 收集问卷类数据
        survey_subjects = await self.config_repo.get_subjects_by_batch_and_type(
            batch_code, 'questionnaire_type'
        )
        
        for subject in survey_subjects:
            survey_stats = await self.survey_calculator.calculate_survey_statistics(
                batch_code, subject.subject_id
            )
            non_academic_data[subject.subject_name] = {
                'subject_id': subject.subject_id,
                'subject_type': '问卷类',
                **survey_stats
            }
        
        # 收集人机交互类数据
        interactive_subjects = await self.config_repo.get_subjects_by_batch_and_type(
            batch_code, 'interactive_type'
        )
        
        for subject in interactive_subjects:
            interactive_stats = await self._calculate_interactive_statistics(
                batch_code, subject.subject_id, subject.subject_name
            )
            non_academic_data[subject.subject_name] = interactive_stats
        
        return non_academic_data
    
    async def _collect_dimensions_data(self, batch_code: str) -> Dict[str, Any]:
        """收集维度统计数据"""
        logger.debug(f"收集批次 {batch_code} 维度数据")
        
        dimensions_data = {}
        
        # 获取批次中所有维度定义
        dimensions = await self.dimension_repo.get_by_batch_code(batch_code)
        
        for dimension in dimensions:
            dimension_stats = await self._calculate_dimension_statistics(
                batch_code, dimension.dimension_id, dimension.dimension_name
            )
            dimensions_data[dimension.dimension_name] = dimension_stats
        
        return dimensions_data
    
    async def _collect_task_metadata(self, batch_code: str) -> Dict[str, Any]:
        """收集任务执行元数据"""
        logger.debug(f"收集批次 {batch_code} 任务元数据")
        
        # 获取最近的计算任务信息
        recent_task = await self.task_manager.get_latest_task_for_batch(batch_code)
        
        if recent_task:
            return {
                'task_id': recent_task.task_id,
                'batch_code': batch_code,
                'calculation_time': recent_task.completed_at.isoformat() if recent_task.completed_at else None,
                'duration': recent_task.duration,
                'status': recent_task.status
            }
        else:
            return {
                'batch_code': batch_code,
                'calculation_time': datetime.utcnow().isoformat(),
                'status': 'unknown'
            }
    
    async def _calculate_subject_statistics(
        self, batch_code: str, subject_id: str, subject_name: str
    ) -> Dict[str, Any]:
        """计算科目统计数据"""
        
        # 获取科目配置
        subject_config = await self.config_repo.get_by_batch_and_subject(
            batch_code, subject_id
        )
        total_score = subject_config.max_score if subject_config else 100
        
        # 获取学生分数数据
        scores_data = await self.score_repo.get_subject_scores(batch_code, subject_id)
        
        if not scores_data:
            return self._get_empty_subject_stats(subject_id, subject_name, total_score)
        
        # 使用计算引擎进行统计计算
        basic_stats = await self.calculation_service.calculate_basic_statistics(scores_data)
        percentiles = await self.calculation_service.calculate_percentiles(scores_data)
        educational_metrics = await self.calculation_service.calculate_educational_metrics(scores_data)
        grade_distribution = await self.calculation_service.calculate_grade_distribution(scores_data, total_score)
        
        # 计算学校排名
        school_rankings = await self._calculate_school_rankings(batch_code, subject_id)
        
        # 获取维度统计
        dimensions = await self._get_subject_dimensions(batch_code, subject_id)
        
        return {
            'subject_id': subject_id,
            'subject_type': '考试类',
            'total_score': total_score,
            'regional_stats': {
                'avg_score': round(basic_stats['mean'], 1),
                'score_rate': round(basic_stats['mean'] / total_score, 3),
                'difficulty': round(educational_metrics['difficulty'], 3),
                'discrimination': round(educational_metrics['discrimination'], 3),
                'std_dev': round(basic_stats['std'], 1),
                'max_score': basic_stats['max'],
                'min_score': basic_stats['min']
            },
            'grade_distribution': self._format_grade_distribution(grade_distribution),
            'school_rankings': school_rankings,
            'dimensions': dimensions
        }
    
    async def _calculate_interactive_statistics(
        self, batch_code: str, subject_id: str, subject_name: str
    ) -> Dict[str, Any]:
        """计算人机交互类科目统计数据"""
        
        # 获取科目配置
        subject_config = await self.config_repo.get_by_batch_and_subject(
            batch_code, subject_id
        )
        total_score = subject_config.max_score if subject_config else 60
        
        # 获取参与统计
        participation_stats = await self.score_repo.get_participation_stats(batch_code, subject_id)
        
        # 获取学生分数数据
        scores_data = await self.score_repo.get_subject_scores(batch_code, subject_id)
        
        if not scores_data:
            return self._get_empty_interactive_stats(subject_id, subject_name, total_score)
        
        # 基础统计计算
        basic_stats = await self.calculation_service.calculate_basic_statistics(scores_data)
        
        # 获取维度统计
        dimensions = await self._get_subject_dimensions(batch_code, subject_id)
        
        return {
            'subject_id': subject_id,
            'subject_type': '人机交互类',
            'total_schools_participated': participation_stats['schools'],
            'total_students_participated': participation_stats['students'],
            'regional_stats': {
                'avg_score': round(basic_stats['mean'], 1),
                'score_rate': round(basic_stats['mean'] / total_score, 3),
                'total_score': total_score,
                'std_dev': round(basic_stats['std'], 1)
            },
            'dimensions': dimensions
        }
    
    async def _calculate_dimension_statistics(
        self, batch_code: str, dimension_id: str, dimension_name: str
    ) -> Dict[str, Any]:
        """计算维度统计数据"""
        
        # 获取维度题目配置
        dimension_questions = await self.config_repo.get_dimension_questions(
            batch_code, dimension_id
        )
        
        total_score = sum(q.max_score for q in dimension_questions)
        
        # 获取维度分数数据
        dimension_scores = await self.score_repo.get_dimension_scores(
            batch_code, dimension_id
        )
        
        if not dimension_scores:
            return {
                'dimension_id': dimension_id,
                'dimension_name': dimension_name,
                'total_score': total_score,
                'avg_score': 0,
                'score_rate': 0,
                'regional_ranking_avg': 0
            }
        
        # 计算维度统计
        basic_stats = await self.calculation_service.calculate_basic_statistics(dimension_scores)
        
        return {
            'dimension_id': dimension_id,
            'dimension_name': dimension_name,
            'total_score': total_score,
            'avg_score': round(basic_stats['mean'], 1),
            'score_rate': round(basic_stats['mean'] / total_score, 3),
            'regional_ranking_avg': round(basic_stats['mean'] / total_score, 3)
        }
    
    async def _calculate_school_rankings(self, batch_code: str, subject_id: str) -> List[Dict[str, Any]]:
        """计算学校排名"""
        school_stats = await self.score_repo.get_school_subject_stats(batch_code, subject_id)
        
        # 按平均分排序
        sorted_schools = sorted(
            school_stats, 
            key=lambda x: x['avg_score'], 
            reverse=True
        )
        
        rankings = []
        for rank, school in enumerate(sorted_schools, 1):
            rankings.append({
                'school_id': school['school_id'],
                'school_name': school['school_name'],
                'avg_score': round(school['avg_score'], 1),
                'score_rate': round(school['score_rate'], 3),
                'ranking': rank
            })
        
        return rankings
    
    async def _get_subject_dimensions(self, batch_code: str, subject_id: str) -> Dict[str, Any]:
        """获取科目的维度统计"""
        dimensions_data = {}
        
        # 获取科目下的维度
        dimensions = await self.dimension_repo.get_by_batch_and_subject(
            batch_code, subject_id
        )
        
        for dimension in dimensions:
            dim_stats = await self._calculate_dimension_statistics(
                batch_code, dimension.dimension_id, dimension.dimension_name
            )
            dimensions_data[dimension.dimension_name] = dim_stats
        
        return dimensions_data
    
    async def _determine_grade_level(self, batch_code: str) -> str:
        """确定年级水平（小学/初中）"""
        # 从grade_aggregation_main表获取年级信息
        grade_info = await self.score_repo.get_batch_grade_info(batch_code)
        
        if not grade_info:
            return '初中'  # 默认返回初中
        
        # 判断年级级别
        primary_grades = ['1st_grade', '2nd_grade', '3rd_grade', '4th_grade', '5th_grade', '6th_grade']
        middle_grades = ['7th_grade', '8th_grade', '9th_grade']
        
        if grade_info['grade_level'] in primary_grades:
            return '小学'
        elif grade_info['grade_level'] in middle_grades:
            return '初中'
        else:
            return '初中'  # 默认
    
    def _format_grade_distribution(self, grade_stats: Dict[str, Any]) -> Dict[str, Any]:
        """格式化等级分布数据"""
        return {
            'excellent': {
                'count': grade_stats.get('excellent_count', 0),
                'percentage': round(grade_stats.get('excellent_rate', 0), 2)
            },
            'good': {
                'count': grade_stats.get('good_count', 0),
                'percentage': round(grade_stats.get('good_rate', 0), 2)
            },
            'pass': {
                'count': grade_stats.get('pass_count', 0),
                'percentage': round(grade_stats.get('pass_rate', 0), 2)
            },
            'fail': {
                'count': grade_stats.get('fail_count', 0),
                'percentage': round(grade_stats.get('fail_rate', 0), 2)
            }
        }
    
    def _get_empty_subject_stats(self, subject_id: str, subject_name: str, total_score: int) -> Dict[str, Any]:
        """获取空的科目统计数据"""
        return {
            'subject_id': subject_id,
            'subject_type': '考试类',
            'total_score': total_score,
            'regional_stats': {
                'avg_score': 0,
                'score_rate': 0,
                'difficulty': 0,
                'discrimination': 0,
                'std_dev': 0,
                'max_score': 0,
                'min_score': 0
            },
            'grade_distribution': {
                'excellent': {'count': 0, 'percentage': 0},
                'good': {'count': 0, 'percentage': 0},
                'pass': {'count': 0, 'percentage': 0},
                'fail': {'count': 0, 'percentage': 0}
            },
            'school_rankings': [],
            'dimensions': {}
        }
    
    def _get_empty_interactive_stats(self, subject_id: str, subject_name: str, total_score: int) -> Dict[str, Any]:
        """获取空的人机交互类统计数据"""
        return {
            'subject_id': subject_id,
            'subject_type': '人机交互类',
            'total_schools_participated': 0,
            'total_students_participated': 0,
            'regional_stats': {
                'avg_score': 0,
                'score_rate': 0,
                'total_score': total_score,
                'std_dev': 0
            },
            'dimensions': {}
        }
    
    async def collect_school_statistics(self, batch_code: str, school_id: str) -> Dict[str, Any]:
        """
        收集单个学校的统计数据
        
        Args:
            batch_code: 批次代码
            school_id: 学校ID
            
        Returns:
            学校统计数据字典
        """
        logger.info(f"开始收集学校 {school_id} 在批次 {batch_code} 的统计数据")
        
        try:
            school_data = {
                'batch_code': batch_code,
                'school_id': school_id,
                'collection_time': datetime.utcnow().isoformat()
            }
            
            # 收集学校基础信息
            school_info = await self._collect_school_info(batch_code, school_id)
            school_data['school_info'] = school_info
            
            # 收集学校学业科目统计
            school_academic = await self._collect_school_academic_subjects(batch_code, school_id)
            school_data['academic_subjects'] = school_academic
            
            # 收集学校非学业科目统计
            school_non_academic = await self._collect_school_non_academic_subjects(batch_code, school_id)
            school_data['non_academic_subjects'] = school_non_academic
            
            # 收集学校维度数据
            school_dimensions = await self._collect_school_dimensions_data(batch_code, school_id)
            school_data['dimensions'] = school_dimensions
            
            logger.info(f"学校 {school_id} 统计数据收集完成")
            return school_data
            
        except Exception as e:
            logger.error(f"收集学校 {school_id} 统计数据失败: {str(e)}")
            raise
    
    async def _collect_school_info(self, batch_code: str, school_id: str) -> Dict[str, Any]:
        """收集学校基础信息"""
        # TODO: 实现实际的数据库查询
        return {
            'school_id': school_id,
            'school_name': f'学校_{school_id}',
            'batch_code': batch_code,
            'total_students': 300,  # Mock data
            'calculation_time': datetime.utcnow().isoformat()
        }
    
    async def _collect_school_academic_subjects(self, batch_code: str, school_id: str) -> Dict[str, Any]:
        """收集学校学业科目数据"""
        # TODO: 实现实际的数据库查询和计算
        return {
            '数学': {
                'subject_id': 'MATH_001',
                'subject_type': '考试类',
                'total_score': 100,
                'school_stats': {
                    'avg_score': 85.2,
                    'score_rate': 0.852,
                    'std_dev': 10.5,
                    'max_score': 98,
                    'min_score': 58,
                    'regional_ranking': 1
                },
                'percentiles': {'P10': 95, 'P50': 86, 'P90': 68},
                'grade_distribution': {
                    'excellent': {'count': 120, 'percentage': 0.40},
                    'good': {'count': 120, 'percentage': 0.40},
                    'pass': {'count': 45, 'percentage': 0.15},
                    'fail': {'count': 15, 'percentage': 0.05}
                },
                'dimensions': {
                    '数学运算': {
                        'dimension_id': 'MATH_CALC',
                        'dimension_name': '数学运算',
                        'total_score': 40,
                        'school_avg_score': 34.8,
                        'school_score_rate': 0.87,
                        'regional_ranking': 2
                    }
                }
            }
        }
    
    async def _collect_school_non_academic_subjects(self, batch_code: str, school_id: str) -> Dict[str, Any]:
        """收集学校非学业科目数据"""
        # TODO: 实现实际的数据库查询
        return {
            '创新思维': {
                'subject_id': 'INNOVATION_001',
                'subject_type': '问卷类',
                'participated_students': 280,
                'dimensions': {
                    '好奇心': {
                        'dimension_id': 'CURIOSITY',
                        'dimension_name': '好奇心',
                        'total_score': 25,
                        'school_avg_score': 21.2,
                        'school_score_rate': 0.848,
                        'regional_ranking': 3
                    }
                }
            }
        }
    
    async def _collect_school_dimensions_data(self, batch_code: str, school_id: str) -> Dict[str, Any]:
        """收集学校维度数据"""
        # TODO: 实现实际的维度数据查询
        return {}