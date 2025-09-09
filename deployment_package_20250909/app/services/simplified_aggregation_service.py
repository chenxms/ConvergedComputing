"""
简化的汇聚服务
整合统计计算、排名、问卷处理等模块，提供区域级和学校级数据汇聚
"""

import logging
import pandas as pd
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..database.models import AggregationLevel, CalculationStatus
from ..schemas.simplified_aggregation_schema import (
    RegionalAggregationData, SchoolAggregationData, SubjectStatistics,
    SubjectCoreMetrics, SubjectRanking, DimensionMetrics,
    format_decimal, calculate_difficulty, calculate_score_rate
)
from .questionnaire_processor import QuestionnaireProcessor, QuestionnaireConfig, ScaleType
from .serialization.statistics_json_serializer import StatisticsJsonSerializer
from ..calculation.engine import CalculationEngine
from ..calculation.calculators import initialize_calculation_system

logger = logging.getLogger(__name__)


class SimplifiedAggregationService:
    """简化汇聚服务"""
    
    def __init__(self, db_session: Session):
        """
        初始化汇聚服务
        
        Args:
            db_session: 数据库会话
        """
        self.db_session = db_session
        self.questionnaire_processor = QuestionnaireProcessor()
        self.json_serializer = StatisticsJsonSerializer(db_session)
        self.calculation_engine = initialize_calculation_system()
        
        logger.info("初始化简化汇聚服务")
    
    def aggregate_batch_regional(
        self,
        batch_code: str,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        区域级汇聚
        
        Args:
            batch_code: 批次代码
            progress_callback: 进度回调函数
            
        Returns:
            区域级汇聚结果
        """
        logger.info(f"开始区域级汇聚，批次: {batch_code}")
        start_time = time.time()
        
        try:
            if progress_callback:
                progress_callback(5, "正在加载数据...")
            
            # 1. 获取批次基础数据
            batch_data = self._fetch_batch_data(batch_code)
            if batch_data.empty:
                raise ValueError(f"批次 {batch_code} 没有找到数据")
            
            if progress_callback:
                progress_callback(15, "正在分析科目结构...")
            
            # 2. 分析科目类型和结构
            subjects_info = self._analyze_batch_subjects(batch_data)
            
            if progress_callback:
                progress_callback(25, "正在计算科目统计...")
            
            # 3. 计算各科目统计
            subjects_stats = {}
            total_progress = len(subjects_info)
            
            for idx, (subject_id, subject_info) in enumerate(subjects_info.items()):
                try:
                    progress = 25 + int((idx / total_progress) * 50)
                    if progress_callback:
                        progress_callback(progress, f"正在处理科目: {subject_info['name']}")
                    
                    subject_data = batch_data[batch_data['subject_id'] == subject_id]
                    
                    if subject_info['type'] == 'questionnaire':
                        # 问卷类科目
                        subject_stats = self._calculate_questionnaire_subject_regional(
                            subject_data, subject_info, batch_code
                        )
                    else:
                        # 考试类科目
                        subject_stats = self._calculate_exam_subject_regional(
                            subject_data, subject_info, batch_code
                        )
                    
                    if subject_stats:
                        subjects_stats[subject_id] = subject_stats
                        
                except Exception as e:
                    logger.error(f"处理科目 {subject_id} 时发生错误: {str(e)}")
                    continue
            
            if progress_callback:
                progress_callback(85, "正在生成汇聚结果...")
            
            # 4. 生成区域级汇聚数据
            regional_data = self._build_regional_aggregation_data(
                batch_code, batch_data, subjects_stats
            )
            
            if progress_callback:
                progress_callback(100, "区域级汇聚完成")
            
            duration = time.time() - start_time
            logger.info(f"区域级汇聚完成，耗时: {duration:.2f}秒")
            
            return {
                'success': True,
                'data': regional_data,
                'duration': duration,
                'subjects_count': len(subjects_stats),
                'total_schools': len(batch_data['school_id'].unique()),
                'total_students': len(batch_data['student_id'].unique())
            }
            
        except Exception as e:
            logger.error(f"区域级汇聚失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'duration': time.time() - start_time
            }
    
    def aggregate_batch_school(
        self,
        batch_code: str,
        school_id: str,
        school_name: str,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        学校级汇聚
        
        Args:
            batch_code: 批次代码
            school_id: 学校ID
            school_name: 学校名称
            progress_callback: 进度回调函数
            
        Returns:
            学校级汇聚结果
        """
        logger.info(f"开始学校级汇聚，批次: {batch_code}, 学校: {school_name}")
        start_time = time.time()
        
        try:
            if progress_callback:
                progress_callback(5, "正在加载学校数据...")
            
            # 1. 获取学校数据
            school_data = self._fetch_school_data(batch_code, school_id)
            if school_data.empty:
                raise ValueError(f"学校 {school_name} 在批次 {batch_code} 中没有找到数据")
            
            # 2. 获取区域数据用于排名计算
            regional_data = self._fetch_batch_data(batch_code)
            
            if progress_callback:
                progress_callback(15, "正在分析科目结构...")
            
            # 3. 分析科目类型
            subjects_info = self._analyze_batch_subjects(school_data)
            
            if progress_callback:
                progress_callback(25, "正在计算科目统计...")
            
            # 4. 计算各科目统计
            subjects_stats = {}
            total_progress = len(subjects_info)
            
            for idx, (subject_id, subject_info) in enumerate(subjects_info.items()):
                try:
                    progress = 25 + int((idx / total_progress) * 50)
                    if progress_callback:
                        progress_callback(progress, f"正在处理科目: {subject_info['name']}")
                    
                    subject_data = school_data[school_data['subject_id'] == subject_id]
                    regional_subject_data = regional_data[regional_data['subject_id'] == subject_id]
                    
                    if subject_info['type'] == 'questionnaire':
                        # 问卷类科目
                        subject_stats = self._calculate_questionnaire_subject_school(
                            subject_data, subject_info, batch_code,
                            regional_subject_data
                        )
                    else:
                        # 考试类科目
                        subject_stats = self._calculate_exam_subject_school(
                            subject_data, subject_info, batch_code,
                            regional_subject_data, school_id
                        )
                    
                    if subject_stats:
                        subjects_stats[subject_id] = subject_stats
                        
                except Exception as e:
                    logger.error(f"处理科目 {subject_id} 时发生错误: {str(e)}")
                    continue
            
            if progress_callback:
                progress_callback(85, "正在生成汇聚结果...")
            
            # 5. 生成学校级汇聚数据
            school_aggregation_data = self._build_school_aggregation_data(
                batch_code, school_id, school_name, school_data, subjects_stats
            )
            
            if progress_callback:
                progress_callback(100, "学校级汇聚完成")
            
            duration = time.time() - start_time
            logger.info(f"学校级汇聚完成，耗时: {duration:.2f}秒")
            
            return {
                'success': True,
                'data': school_aggregation_data,
                'duration': duration,
                'subjects_count': len(subjects_stats),
                'total_students': len(school_data['student_id'].unique())
            }
            
        except Exception as e:
            logger.error(f"学校级汇聚失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'duration': time.time() - start_time
            }
    
    def aggregate_all_batches(
        self,
        batch_codes: List[str],
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        批量处理所有批次
        
        Args:
            batch_codes: 批次代码列表
            progress_callback: 进度回调函数
            
        Returns:
            批量处理结果
        """
        logger.info(f"开始批量汇聚，批次数量: {len(batch_codes)}")
        start_time = time.time()
        
        results = {
            'success_count': 0,
            'failed_count': 0,
            'results': {},
            'errors': []
        }
        
        total_batches = len(batch_codes)
        
        for idx, batch_code in enumerate(batch_codes):
            try:
                base_progress = int((idx / total_batches) * 100)
                
                if progress_callback:
                    progress_callback(base_progress, f"正在处理批次 {batch_code}...")
                
                # 1. 区域级汇聚
                regional_result = self.aggregate_batch_regional(
                    batch_code,
                    lambda p, msg: progress_callback(
                        base_progress + int(p * 0.5 / 100),
                        f"批次 {batch_code} 区域级: {msg}"
                    ) if progress_callback else None
                )
                
                if not regional_result['success']:
                    results['failed_count'] += 1
                    results['errors'].append({
                        'batch_code': batch_code,
                        'type': 'regional',
                        'error': regional_result['error']
                    })
                    continue
                
                # 2. 获取学校列表并处理学校级汇聚
                schools = self._get_batch_schools(batch_code)
                school_results = {}
                
                for school_idx, (school_id, school_name) in enumerate(schools):
                    try:
                        school_progress = base_progress + int(0.5 + (school_idx / len(schools)) * 0.5)
                        
                        if progress_callback:
                            progress_callback(
                                school_progress,
                                f"批次 {batch_code} 学校级: {school_name}"
                            )
                        
                        school_result = self.aggregate_batch_school(
                            batch_code, school_id, school_name
                        )
                        
                        if school_result['success']:
                            school_results[school_id] = school_result
                        else:
                            results['errors'].append({
                                'batch_code': batch_code,
                                'type': 'school',
                                'school_id': school_id,
                                'school_name': school_name,
                                'error': school_result['error']
                            })
                            
                    except Exception as e:
                        logger.error(f"处理学校 {school_name} 时发生错误: {str(e)}")
                        results['errors'].append({
                            'batch_code': batch_code,
                            'type': 'school',
                            'school_id': school_id,
                            'school_name': school_name,
                            'error': str(e)
                        })
                        continue
                
                results['success_count'] += 1
                results['results'][batch_code] = {
                    'regional': regional_result,
                    'schools': school_results
                }
                
            except Exception as e:
                logger.error(f"处理批次 {batch_code} 时发生错误: {str(e)}")
                results['failed_count'] += 1
                results['errors'].append({
                    'batch_code': batch_code,
                    'type': 'batch',
                    'error': str(e)
                })
                continue
        
        duration = time.time() - start_time
        logger.info(f"批量汇聚完成，成功: {results['success_count']}, 失败: {results['failed_count']}, 耗时: {duration:.2f}秒")
        
        results['duration'] = duration
        return results
    
    def _fetch_batch_data(self, batch_code: str) -> pd.DataFrame:
        """获取批次数据"""
        query = text("""
            SELECT 
                ssd.student_id,
                ssd.subject_id,
                ssd.question_id,
                ssd.raw_score,
                ssd.school_id,
                sqc.subject_name,
                sqc.subject_type,
                sqc.max_score as question_max_score,
                qdm.dimension_code,
                qdm.dimension_name
            FROM student_score_detail ssd
            LEFT JOIN subject_question_config sqc ON ssd.subject_id = sqc.subject_id 
                AND ssd.question_id = sqc.question_id
            LEFT JOIN question_dimension_mapping qdm ON ssd.question_id = qdm.question_id
            WHERE ssd.batch_code = :batch_code
                AND ssd.raw_score IS NOT NULL
            ORDER BY ssd.school_id, ssd.student_id, ssd.subject_id, ssd.question_id
        """)
        
        result = self.db_session.execute(query, {'batch_code': batch_code})
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        logger.info(f"加载批次 {batch_code} 数据: {len(data)} 条记录")
        return data
    
    def _fetch_school_data(self, batch_code: str, school_id: str) -> pd.DataFrame:
        """获取学校数据"""
        query = text("""
            SELECT 
                ssd.student_id,
                ssd.subject_id,
                ssd.question_id,
                ssd.raw_score,
                ssd.school_id,
                sqc.subject_name,
                sqc.subject_type,
                sqc.max_score as question_max_score,
                qdm.dimension_code,
                qdm.dimension_name
            FROM student_score_detail ssd
            LEFT JOIN subject_question_config sqc ON ssd.subject_id = sqc.subject_id 
                AND ssd.question_id = sqc.question_id
            LEFT JOIN question_dimension_mapping qdm ON ssd.question_id = qdm.question_id
            WHERE ssd.batch_code = :batch_code
                AND ssd.school_id = :school_id
                AND ssd.raw_score IS NOT NULL
            ORDER BY ssd.student_id, ssd.subject_id, ssd.question_id
        """)
        
        result = self.db_session.execute(
            query, 
            {'batch_code': batch_code, 'school_id': school_id}
        )
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        logger.info(f"加载学校 {school_id} 在批次 {batch_code} 的数据: {len(data)} 条记录")
        return data
    
    def _analyze_batch_subjects(self, data: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """分析批次科目结构"""
        subjects_info = {}
        
        subject_groups = data.groupby(['subject_id', 'subject_name', 'subject_type'])
        
        for (subject_id, subject_name, subject_type), group in subject_groups:
            subjects_info[subject_id] = {
                'name': subject_name or f"科目_{subject_id}",
                'type': subject_type or 'exam',
                'question_count': len(group['question_id'].unique()),
                'student_count': len(group['student_id'].unique()),
                'max_score': group['question_max_score'].sum() if 'question_max_score' in group.columns else 100.0
            }
        
        logger.info(f"分析得到 {len(subjects_info)} 个科目")
        return subjects_info
    
    def _calculate_exam_subject_regional(
        self,
        subject_data: pd.DataFrame,
        subject_info: Dict[str, Any],
        batch_code: str
    ) -> SubjectStatistics:
        """计算考试类科目区域级统计"""
        try:
            # 计算学生总分
            student_scores = subject_data.groupby('student_id')['raw_score'].sum()
            
            if student_scores.empty:
                return None
            
            # 核心指标计算
            avg_score = format_decimal(student_scores.mean())
            max_score = subject_info.get('max_score', 100.0)
            difficulty = calculate_difficulty(avg_score, max_score)
            std_dev = format_decimal(student_scores.std())
            
            # 百分位数计算
            p10 = format_decimal(student_scores.quantile(0.1))
            p50 = format_decimal(student_scores.median())
            p90 = format_decimal(student_scores.quantile(0.9))
            
            # 区分度计算（简化版本）
            discrimination = self._calculate_discrimination(student_scores)
            
            # 核心指标
            metrics = SubjectCoreMetrics(
                avg_score=avg_score,
                difficulty=difficulty,
                std_dev=std_dev,
                discrimination=discrimination,
                max_score=format_decimal(student_scores.max()),
                min_score=format_decimal(student_scores.min()),
                p10=p10,
                p50=p50,
                p90=p90,
                student_count=len(student_scores)
            )
            
            # 学校排名
            school_rankings = self._calculate_school_rankings(subject_data, 'raw_score')
            ranking = SubjectRanking(school_rankings=school_rankings)
            
            # 维度统计
            dimensions = self._calculate_exam_dimensions(subject_data)
            
            return SubjectStatistics(
                subject_id=subject_data['subject_id'].iloc[0],
                subject_name=subject_info['name'],
                subject_type='exam',
                metrics=metrics,
                ranking=ranking,
                dimensions=dimensions
            )
            
        except Exception as e:
            logger.error(f"计算考试科目区域级统计失败: {str(e)}")
            return None
    
    def _calculate_exam_subject_school(
        self,
        subject_data: pd.DataFrame,
        subject_info: Dict[str, Any],
        batch_code: str,
        regional_data: pd.DataFrame,
        school_id: str
    ) -> SubjectStatistics:
        """计算考试类科目学校级统计"""
        try:
            # 计算学生总分
            student_scores = subject_data.groupby('student_id')['raw_score'].sum()
            
            if student_scores.empty:
                return None
            
            # 核心指标计算
            avg_score = format_decimal(student_scores.mean())
            max_score = subject_info.get('max_score', 100.0)
            difficulty = calculate_difficulty(avg_score, max_score)
            std_dev = format_decimal(student_scores.std())
            
            # 百分位数计算
            p10 = format_decimal(student_scores.quantile(0.1))
            p50 = format_decimal(student_scores.median())
            p90 = format_decimal(student_scores.quantile(0.9))
            
            # 区分度计算
            discrimination = self._calculate_discrimination(student_scores)
            
            # 核心指标
            metrics = SubjectCoreMetrics(
                avg_score=avg_score,
                difficulty=difficulty,
                std_dev=std_dev,
                discrimination=discrimination,
                max_score=format_decimal(student_scores.max()),
                min_score=format_decimal(student_scores.min()),
                p10=p10,
                p50=p50,
                p90=p90,
                student_count=len(student_scores)
            )
            
            # 计算在区域内的排名
            regional_rank, total_schools = self._calculate_school_rank(
                regional_data, school_id, 'raw_score'
            )
            ranking = SubjectRanking(
                regional_rank=regional_rank,
                total_schools=total_schools
            )
            
            # 维度统计
            dimensions = self._calculate_exam_dimensions(subject_data)
            
            return SubjectStatistics(
                subject_id=subject_data['subject_id'].iloc[0],
                subject_name=subject_info['name'],
                subject_type='exam',
                metrics=metrics,
                ranking=ranking,
                dimensions=dimensions
            )
            
        except Exception as e:
            logger.error(f"计算考试科目学校级统计失败: {str(e)}")
            return None
    
    def _calculate_questionnaire_subject_regional(
        self,
        subject_data: pd.DataFrame,
        subject_info: Dict[str, Any],
        batch_code: str
    ) -> SubjectStatistics:
        """计算问卷类科目区域级统计"""
        try:
            # 构建问卷配置
            questionnaire_configs = self._build_questionnaire_configs(subject_data)
            
            # 处理问卷数据
            questionnaire_dimensions = self.questionnaire_processor.process_questionnaire_data(
                subject_data, questionnaire_configs, batch_code
            )
            
            # 计算整体问卷指标
            student_scores = subject_data.groupby('student_id')['raw_score'].mean()
            
            if student_scores.empty:
                return None
            
            avg_score = format_decimal(student_scores.mean())
            max_score = 5.0  # 默认5分制
            difficulty = calculate_difficulty(avg_score, max_score)
            std_dev = format_decimal(student_scores.std())
            
            metrics = SubjectCoreMetrics(
                avg_score=avg_score,
                difficulty=difficulty,
                std_dev=std_dev,
                discrimination=self._calculate_discrimination(student_scores),
                max_score=format_decimal(student_scores.max()),
                min_score=format_decimal(student_scores.min()),
                p10=format_decimal(student_scores.quantile(0.1)),
                p50=format_decimal(student_scores.median()),
                p90=format_decimal(student_scores.quantile(0.9)),
                student_count=len(student_scores)
            )
            
            # 学校排名（基于平均分）
            school_rankings = self._calculate_school_rankings(subject_data, 'raw_score')
            ranking = SubjectRanking(school_rankings=school_rankings)
            
            return SubjectStatistics(
                subject_id=subject_data['subject_id'].iloc[0],
                subject_name=subject_info['name'],
                subject_type='questionnaire',
                metrics=metrics,
                ranking=ranking,
                questionnaire_dimensions=questionnaire_dimensions
            )
            
        except Exception as e:
            logger.error(f"计算问卷科目区域级统计失败: {str(e)}")
            return None
    
    def _calculate_questionnaire_subject_school(
        self,
        subject_data: pd.DataFrame,
        subject_info: Dict[str, Any],
        batch_code: str,
        regional_data: pd.DataFrame
    ) -> SubjectStatistics:
        """计算问卷类科目学校级统计"""
        try:
            # 构建问卷配置
            questionnaire_configs = self._build_questionnaire_configs(subject_data)
            
            # 处理问卷数据
            questionnaire_dimensions = self.questionnaire_processor.process_questionnaire_data(
                subject_data, questionnaire_configs, batch_code
            )
            
            # 计算整体问卷指标
            student_scores = subject_data.groupby('student_id')['raw_score'].mean()
            
            if student_scores.empty:
                return None
            
            avg_score = format_decimal(student_scores.mean())
            max_score = 5.0  # 默认5分制
            difficulty = calculate_difficulty(avg_score, max_score)
            std_dev = format_decimal(student_scores.std())
            
            metrics = SubjectCoreMetrics(
                avg_score=avg_score,
                difficulty=difficulty,
                std_dev=std_dev,
                discrimination=self._calculate_discrimination(student_scores),
                max_score=format_decimal(student_scores.max()),
                min_score=format_decimal(student_scores.min()),
                p10=format_decimal(student_scores.quantile(0.1)),
                p50=format_decimal(student_scores.median()),
                p90=format_decimal(student_scores.quantile(0.9)),
                student_count=len(student_scores)
            )
            
            # 计算在区域内的排名
            school_id = subject_data['school_id'].iloc[0]
            regional_rank, total_schools = self._calculate_school_rank(
                regional_data, school_id, 'raw_score'
            )
            ranking = SubjectRanking(
                regional_rank=regional_rank,
                total_schools=total_schools
            )
            
            return SubjectStatistics(
                subject_id=subject_data['subject_id'].iloc[0],
                subject_name=subject_info['name'],
                subject_type='questionnaire',
                metrics=metrics,
                ranking=ranking,
                questionnaire_dimensions=questionnaire_dimensions
            )
            
        except Exception as e:
            logger.error(f"计算问卷科目学校级统计失败: {str(e)}")
            return None
    
    def _build_questionnaire_configs(self, data: pd.DataFrame) -> List[QuestionnaireConfig]:
        """构建问卷配置"""
        configs = []
        
        # 获取唯一的题目信息
        unique_questions = data[['question_id', 'dimension_code', 'dimension_name']].drop_duplicates()
        
        for _, row in unique_questions.iterrows():
            # 自动检测量表类型
            scale_type = self.questionnaire_processor.detect_scale_type(data, row['question_id'])
            
            config = QuestionnaireConfig(
                scale_type=scale_type,
                question_id=row['question_id'],
                question_name=f"问题_{row['question_id']}",  # 可以从配置表获取
                dimension_code=row['dimension_code'] or 'DEFAULT',
                dimension_name=row['dimension_name'] or '默认维度'
            )
            configs.append(config)
        
        return configs
    
    def _calculate_discrimination(self, scores: pd.Series) -> float:
        """计算区分度（简化版本）"""
        try:
            if len(scores) < 10:
                return 0.0
            
            # 使用27%分组计算区分度
            n = len(scores)
            top_n = max(1, int(n * 0.27))
            
            sorted_scores = scores.sort_values(ascending=False)
            top_group = sorted_scores.iloc[:top_n]
            bottom_group = sorted_scores.iloc[-top_n:]
            
            max_score = scores.max()
            if max_score == 0:
                return 0.0
            
            discrimination = (top_group.mean() - bottom_group.mean()) / max_score
            return format_decimal(discrimination, 3)
            
        except Exception:
            return 0.0
    
    def _calculate_school_rankings(
        self,
        data: pd.DataFrame,
        score_column: str
    ) -> List[Dict[str, Any]]:
        """计算学校排名"""
        try:
            # 按学校聚合计算平均分
            school_scores = data.groupby(['school_id'])[score_column].mean().reset_index()
            school_scores = school_scores.sort_values(score_column, ascending=False)
            
            rankings = []
            for rank, (_, row) in enumerate(school_scores.iterrows(), 1):
                rankings.append({
                    'school_id': row['school_id'],
                    'school_name': f"学校_{row['school_id']}",  # 可以从学校表获取真实名称
                    'avg_score': format_decimal(row[score_column]),
                    'rank': rank
                })
            
            return rankings
            
        except Exception as e:
            logger.error(f"计算学校排名失败: {str(e)}")
            return []
    
    def _calculate_school_rank(
        self,
        regional_data: pd.DataFrame,
        school_id: str,
        score_column: str
    ) -> Tuple[Optional[int], int]:
        """计算学校在区域内的排名"""
        try:
            # 计算各学校平均分
            school_scores = regional_data.groupby('school_id')[score_column].mean().reset_index()
            school_scores = school_scores.sort_values(score_column, ascending=False)
            
            total_schools = len(school_scores)
            
            # 找到目标学校的排名
            for rank, (_, row) in enumerate(school_scores.iterrows(), 1):
                if row['school_id'] == school_id:
                    return rank, total_schools
            
            return None, total_schools
            
        except Exception as e:
            logger.error(f"计算学校排名失败: {str(e)}")
            return None, 0
    
    def _calculate_exam_dimensions(self, data: pd.DataFrame) -> Optional[Dict[str, DimensionMetrics]]:
        """计算考试科目维度统计"""
        try:
            if 'dimension_code' not in data.columns or data['dimension_code'].isna().all():
                return None
            
            dimensions = {}
            dimension_groups = data.groupby(['dimension_code', 'dimension_name'])
            
            for (dim_code, dim_name), group in dimension_groups:
                if pd.isna(dim_code):
                    continue
                
                # 计算维度平均分
                dim_scores = group.groupby('student_id')['raw_score'].sum()
                dim_max_score = group['question_max_score'].sum()
                
                if not dim_scores.empty:
                    avg_score = format_decimal(dim_scores.mean())
                    score_rate = calculate_score_rate(avg_score, dim_max_score)
                    
                    dimensions[dim_code] = DimensionMetrics(
                        avg_score=avg_score,
                        score_rate=score_rate,
                        rank=None,  # 排名在上层服务中计算
                        student_count=len(dim_scores)
                    )
            
            return dimensions if dimensions else None
            
        except Exception as e:
            logger.error(f"计算维度统计失败: {str(e)}")
            return None
    
    def _build_regional_aggregation_data(
        self,
        batch_code: str,
        batch_data: pd.DataFrame,
        subjects_stats: Dict[str, SubjectStatistics]
    ) -> RegionalAggregationData:
        """构建区域级汇聚数据"""
        now = datetime.now().isoformat()
        
        return RegionalAggregationData(
            batch_code=batch_code,
            aggregation_level="REGIONAL",
            total_schools=len(batch_data['school_id'].unique()),
            total_students=len(batch_data['student_id'].unique()),
            subjects=subjects_stats,
            created_at=now,
            updated_at=now,
            data_version="2.0"
        )
    
    def _build_school_aggregation_data(
        self,
        batch_code: str,
        school_id: str,
        school_name: str,
        school_data: pd.DataFrame,
        subjects_stats: Dict[str, SubjectStatistics]
    ) -> SchoolAggregationData:
        """构建学校级汇聚数据"""
        now = datetime.now().isoformat()
        
        return SchoolAggregationData(
            batch_code=batch_code,
            aggregation_level="SCHOOL",
            school_id=school_id,
            school_name=school_name,
            total_students=len(school_data['student_id'].unique()),
            subjects=subjects_stats,
            created_at=now,
            updated_at=now,
            data_version="2.0"
        )
    
    def _get_batch_schools(self, batch_code: str) -> List[Tuple[str, str]]:
        """获取批次学校列表"""
        query = text("""
            SELECT DISTINCT school_id
            FROM student_score_detail
            WHERE batch_code = :batch_code
            ORDER BY school_id
        """)
        
        result = self.db_session.execute(query, {'batch_code': batch_code})
        schools = [(row[0], f"学校_{row[0]}") for row in result.fetchall()]
        
        return schools