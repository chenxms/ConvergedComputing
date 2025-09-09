# 统计计算服务
import json
import logging
import pandas as pd
import time
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session

from ..database.models import AggregationLevel, CalculationStatus
from .subjects_builder import SubjectsBuilder
from ..utils.precision import round2_json
from ..database.repositories import StatisticalAggregationRepository, DataAdapterRepository
from ..calculation.calculators import initialize_calculation_system
from ..calculation.engine import CalculationEngine

logger = logging.getLogger(__name__)


class CalculationService:
    """统计计算服务"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.repository = StatisticalAggregationRepository(db_session)
        self.data_adapter = DataAdapterRepository(db_session)
        self.engine = initialize_calculation_system()
        
    async def calculate_batch_statistics(self, batch_code: str, config: Dict[str, Any] = None, 
                                       progress_callback: callable = None) -> Dict[str, Any]:
        """计算批次统计数据 - 增强版本，自动生成区域级和学校级数据"""
        logger.info(f"开始增强计算批次 {batch_code} 的统计数据（区域级+学校级）")
        start_time = time.time()
        
        try:
            # 1. 获取学生分数数据
            if progress_callback:
                progress_callback(5, "正在加载学生数据...")
            data = await self._fetch_student_scores(batch_code)
            if data.empty:
                raise ValueError(f"批次 {batch_code} 没有找到学生分数数据")
            
            # 2. 获取配置信息
            calculation_config = config or await self._get_calculation_config(batch_code)
            
            # 3. 字段映射 (将total_score重命名为score以匹配计算引擎)
            if 'total_score' in data.columns:
                data = data.rename(columns={'total_score': 'score'})
            
            # 4. 数据验证
            if progress_callback:
                progress_callback(10, "正在验证数据完整性...")
            validation_result = self.engine.validator.validate_input_data(data, calculation_config)
            if not validation_result['is_valid']:
                raise ValueError(f"数据验证失败: {validation_result['errors']}")
            
            # 4. 执行多科目统计计算 (10-50%)
            if progress_callback:
                progress_callback(15, "正在处理多科目统计指标...")
            
            # 5. 整合多科目区域级结果
            consolidated_regional_results = await self._consolidate_multi_subject_results(
                batch_code, data, validation_result
            )
            
            if progress_callback:
                progress_callback(50, "多科目区域级计算完成")
            
            # 6. 保存区域级数据
            regional_duration = time.time() - start_time
            await self._save_regional_statistics(
                batch_code=batch_code,
                statistics_data=consolidated_regional_results,
                total_students=len(data),
                calculation_duration=regional_duration
            )
            
            logger.info(f"批次 {batch_code} 区域级统计计算完成，耗时 {regional_duration:.2f}s")
            
            # 7. 自动生成学校级数据 (50-90%)
            if progress_callback:
                progress_callback(55, "开始生成学校级统计数据...")
            
            school_results = await self.calculate_batch_all_schools(
                batch_code=batch_code,
                config=calculation_config,
                progress_callback=lambda p, msg: progress_callback(55 + int(p * 0.35), msg) if progress_callback else None
            )
            
            # 8. 整合最终结果 (90-100%)
            if progress_callback:
                progress_callback(90, "正在整合所有统计结果...")
            
            total_duration = time.time() - start_time
            final_results = {
                'batch_code': batch_code,
                'regional_statistics': consolidated_regional_results,
                'school_statistics_summary': {
                    'total_schools': school_results['total_schools'],
                    'successful_schools': school_results['successful_schools'],
                    'failed_schools': school_results['failed_schools'],
                    'school_details': school_results['school_results']
                },
                'calculation_duration': total_duration,
                'total_students': len(data),
                'validation_warnings': validation_result.get('warnings', [])
            }
            
            if progress_callback:
                progress_callback(100, "批次统计计算完成")
            
            logger.info(f"批次 {batch_code} 增强统计计算完成，总耗时 {total_duration:.2f}s，"
                       f"处理学生数: {len(data)}，成功生成 {school_results['successful_schools']} 个学校数据")
            
            return final_results
            
        except Exception as e:
            logger.error(f"批次 {batch_code} 增强统计计算失败: {str(e)}")
            # 更新失败状态
            await self._update_calculation_status(batch_code, CalculationStatus.FAILED, str(e))
            raise
    
    async def calculate_school_statistics(self, batch_code: str, school_id: str, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """计算学校级统计数据"""
        logger.info(f"开始计算批次 {batch_code} 学校 {school_id} 的统计数据")
        start_time = time.time()
        
        try:
            # 1. 获取学校学生分数数据
            data = await self._fetch_school_scores(batch_code, school_id)
            if data.empty:
                raise ValueError(f"学校 {school_id} 在批次 {batch_code} 中没有找到学生分数数据")
            
            # 2. 获取配置信息
            calculation_config = config or await self._get_calculation_config(batch_code)
            
            # 3. 字段映射 (将total_score重命名为score以匹配计算引擎)
            if 'total_score' in data.columns:
                data = data.rename(columns={'total_score': 'score'})
            
            # 4. 执行计算（复用区域级计算逻辑）
            results = {}
            
            basic_stats = self.engine.calculate('basic_statistics', data, calculation_config)
            results['basic_statistics'] = basic_stats
            
            educational_metrics = self.engine.calculate('educational_metrics', data, calculation_config)
            results['educational_metrics'] = educational_metrics
            
            percentiles = self.engine.calculate('percentiles', data, calculation_config)
            results['percentiles'] = percentiles
            
            # 区分度（如果学生数足够）
            if len(data) >= 10:
                discrimination = self.engine.calculate('discrimination', data, calculation_config)
                results['discrimination'] = discrimination
            else:
                logger.warning(f"学校 {school_id} 学生数不足({len(data)})，跳过区分度计算")
            
            # 4. 整合结果为标准JSON格式
            consolidated_results = {
                "basic_stats": results.get('basic_statistics', {}),
                "educational_metrics": results.get('educational_metrics', {}),
                "percentiles": results.get('percentiles', {}),
                "grade_distribution": results.get('grade_distribution', {}),
                "discrimination": results.get('discrimination', {})
            }
            
            # 5. 保存到数据库
            duration = time.time() - start_time
            school_name = await self._get_school_name(school_id)
            
            await self._save_school_statistics(
                batch_code=batch_code,
                school_id=school_id,
                school_name=school_name,
                statistics_data=consolidated_results,
                total_students=len(data),
                calculation_duration=duration
            )
            
            logger.info(f"学校 {school_id} 统计计算完成，耗时 {duration:.2f}s，处理学生数: {len(data)}")
            
            return {
                'batch_code': batch_code,
                'school_id': school_id,
                'school_name': school_name,
                'statistics': consolidated_results,
                'calculation_duration': duration,
                'total_students': len(data)
            }
            
        except Exception as e:
            logger.error(f"学校 {school_id} 统计计算失败: {str(e)}")
            raise
    
    async def calculate_batch_all_schools(self, batch_code: str, config: Dict[str, Any] = None, 
                                        progress_callback: callable = None) -> Dict[str, Any]:
        """计算批次所有学校的统计数据"""
        logger.info(f"开始批量计算批次 {batch_code} 所有学校的统计数据")
        start_time = time.time()
        
        try:
            # 1. 获取批次中所有学校列表
            if progress_callback:
                progress_callback(0, "正在获取学校列表...")
            school_ids = await self._get_batch_schools(batch_code)
            if not school_ids:
                raise ValueError(f"批次 {batch_code} 中没有找到学校数据")
            
            logger.info(f"批次 {batch_code} 共找到 {len(school_ids)} 所学校")
            
            # 2. 批量计算各学校统计数据
            results = []
            successful_count = 0
            failed_schools = []
            
            for i, school_id in enumerate(school_ids):
                try:
                    # 更新进度
                    progress = int((i / len(school_ids)) * 100)
                    if progress_callback:
                        progress_callback(progress, f"正在计算学校 {school_id} ({i+1}/{len(school_ids)})...")
                    
                    school_result = await self.calculate_school_statistics(batch_code, school_id, config)
                    results.append({
                        'school_id': school_id,
                        'school_name': school_result['school_name'],
                        'total_students': school_result['total_students'],
                        'calculation_duration': school_result['calculation_duration'],
                        'status': 'success'
                    })
                    successful_count += 1
                    logger.debug(f"学校 {school_id} 计算成功，学生数: {school_result['total_students']}")
                    
                except Exception as e:
                    logger.error(f"学校 {school_id} 计算失败: {str(e)}")
                    failed_schools.append({
                        'school_id': school_id, 
                        'error': str(e),
                        'status': 'failed'
                    })
            
            duration = time.time() - start_time
            
            if progress_callback:
                progress_callback(100, "所有学校数据计算完成")
            
            logger.info(f"批次 {batch_code} 所有学校计算完成，耗时 {duration:.2f}s，"
                       f"成功: {successful_count}, 失败: {len(failed_schools)}")
            
            return {
                'batch_code': batch_code,
                'total_schools': len(school_ids),
                'successful_schools': successful_count,
                'failed_schools': len(failed_schools),
                'school_results': results,
                'failed_details': failed_schools,
                'total_duration': duration
            }
            
        except Exception as e:
            logger.error(f"批次 {batch_code} 批量学校计算失败: {str(e)}")
            raise
    
    async def calculate_statistics(self, batch_code: str, aggregation_level: AggregationLevel,
                                 school_id: Optional[str] = None) -> Dict[str, Any]:
        """计算统计数据
        
        Args:
            batch_code: 批次代码
            aggregation_level: 汇聚级别
            school_id: 学校ID（学校级汇聚时必须）
            
        Returns:
            计算结果字典
        """
        logger.info(f"计算统计数据: batch_code={batch_code}, level={aggregation_level.value}, school_id={school_id}")
        
        try:
            if aggregation_level == AggregationLevel.REGIONAL:
                return await self.calculate_batch_statistics(batch_code)
            elif aggregation_level == AggregationLevel.SCHOOL:
                if not school_id:
                    raise ValueError("学校级计算需要提供school_id")
                return await self.calculate_school_statistics(batch_code, school_id)
            else:
                raise ValueError(f"不支持的汇聚级别: {aggregation_level}")
                
        except Exception as e:
            logger.error(f"统计计算失败: {str(e)}")
            raise
    
    async def recalculate_statistics(self, batch_code: str, aggregation_level: AggregationLevel, 
                                   school_id: Optional[str] = None) -> Dict[str, Any]:
        """重新计算统计数据"""
        logger.info(f"重新计算统计数据: batch_code={batch_code}, level={aggregation_level.value}, school_id={school_id}")
        
        try:
            if aggregation_level == AggregationLevel.REGIONAL:
                return await self.calculate_batch_statistics(batch_code)
            elif aggregation_level == AggregationLevel.SCHOOL:
                if not school_id:
                    raise ValueError("学校级重计算需要提供school_id")
                return await self.calculate_school_statistics(batch_code, school_id)
            else:
                raise ValueError(f"不支持的汇聚级别: {aggregation_level}")
                
        except Exception as e:
            logger.error(f"重新计算失败: {str(e)}")
            raise
    
    def get_engine_performance_stats(self) -> Dict[str, Any]:
        """获取计算引擎性能统计"""
        return self.engine.get_performance_stats()
    
    def reset_engine_performance_stats(self):
        """重置计算引擎性能统计"""
        self.engine.reset_performance_stats()
    
    # ================================
    # 私有辅助方法
    # ================================
    
    async def _fetch_student_scores(self, batch_code: str) -> pd.DataFrame:
        """获取批次学生分数数据 - 使用数据适配器"""
        logger.debug(f"使用数据适配器获取批次 {batch_code} 的学生分数数据")
        
        try:
            # 首先检查数据准备状态
            readiness = self.data_adapter.check_data_readiness(batch_code)
            if not readiness['is_ready']:
                logger.warning(f"批次 {batch_code} 数据准备状态: {readiness['completeness_ratio']:.2%}")
            
            # 使用适配器获取学生分数数据
            student_scores = self.data_adapter.get_student_scores(batch_code)
            
            if not student_scores:
                logger.warning(f"批次 {batch_code} 没有找到学生分数数据")
                return pd.DataFrame()
            
            # 转换为DataFrame，保持与原有格式兼容
            df_data = []
            for score_record in student_scores:
                df_data.append({
                    'student_id': score_record['student_id'],
                    'student_name': score_record.get('student_name', ''),
                    'school_id': score_record['school_id'],
                    'school_code': score_record.get('school_code', ''),
                    'school_name': score_record.get('school_name', ''),
                    'subject_name': score_record['subject_name'],
                    'total_score': score_record['total_score'],
                    'max_score': score_record['max_score'],
                    'subject_type': score_record['subject_type'],
                    'grade': score_record.get('grade', ''),
                    'dimensions': json.dumps(score_record.get('dimensions', {}), ensure_ascii=False) if isinstance(score_record.get('dimensions'), dict) else str(score_record.get('dimensions', '{}')),
                    'data_source': score_record.get('data_source', 'unknown')
                })
            
            df = pd.DataFrame(df_data)
            
            logger.info(f"获取到 {len(df)} 条学生分数记录，包含 {df['subject_name'].nunique()} 个科目")
            logger.info(f"数据源类型: {df['data_source'].value_counts().to_dict()}")
            return df
            
        except Exception as e:
            logger.error(f"获取学生分数数据失败: {e}")
            raise
    
    async def _fetch_school_scores(self, batch_code: str, school_id: str) -> pd.DataFrame:
        """获取学校学生分数数据 - 使用数据适配器"""
        logger.debug(f"使用数据适配器获取学校 {school_id} 在批次 {batch_code} 的学生分数数据")
        
        try:
            # 使用适配器获取特定学校的学生分数数据
            student_scores = self.data_adapter.get_student_scores(batch_code, school_id=school_id)
            
            if not student_scores:
                logger.warning(f"学校 {school_id} 在批次 {batch_code} 没有找到学生分数数据")
                return pd.DataFrame()
            
            # 转换为DataFrame，保持与原有格式兼容
            df_data = []
            for score_record in student_scores:
                df_data.append({
                    'student_id': score_record['student_id'],
                    'student_name': score_record.get('student_name', ''),
                    'school_id': score_record['school_id'],
                    'school_code': score_record.get('school_code', ''),
                    'school_name': score_record.get('school_name', ''),
                    'subject_name': score_record['subject_name'],
                    'total_score': score_record['total_score'],
                    'max_score': score_record['max_score'],
                    'subject_type': score_record['subject_type'],
                    'grade': score_record.get('grade', ''),
                    'dimensions': json.dumps(score_record.get('dimensions', {}), ensure_ascii=False) if isinstance(score_record.get('dimensions'), dict) else str(score_record.get('dimensions', '{}')),
                    'data_source': score_record.get('data_source', 'unknown')
                })
            
            df = pd.DataFrame(df_data)
            
            logger.info(f"学校 {school_id} 获取到 {len(df)} 条学生分数记录，包含 {df['subject_name'].nunique()} 个科目")
            try:
                data_source_counts = df['data_source'].astype(str).value_counts().to_dict()
                logger.info(f"数据源类型: {data_source_counts}")
            except Exception as e:
                logger.warning(f"无法统计数据源类型: {e}")
                logger.info(f"数据源样本: {df['data_source'].head().tolist()}")
            return df
            
        except Exception as e:
            logger.error(f"获取学校 {school_id} 分数数据失败: {e}")
            raise
    
    async def _get_calculation_config(self, batch_code: str) -> Dict[str, Any]:
        """获取计算配置 - 使用数据适配器"""
        try:
            # 从数据适配器获取批次摘要信息
            batch_summary = self.data_adapter.get_batch_summary(batch_code)
            
            # 从数据库获取年级信息
            grade_level = self._get_batch_grade_level(batch_code)
            
            # 构建计算配置
            config = {
                'max_score': 100,  # 默认值，会在具体计算时使用科目的实际满分
                'grade_level': grade_level,
                'percentiles': [10, 25, 50, 75, 90],
                'required_columns': ['score'],
                'batch_summary': batch_summary  # 包含批次的科目信息
            }
            
            logger.debug(f"批次 {batch_code} 计算配置: 年级={grade_level}, 数据源={batch_summary.get('data_source', 'unknown')}")
            return config
            
        except Exception as e:
            logger.warning(f"获取批次 {batch_code} 配置失败，使用默认配置: {e}")
            # 返回默认配置
            return {
                'max_score': 100,
                'grade_level': '7th_grade',  # 默认初中
                'percentiles': [10, 25, 50, 75, 90],
                'required_columns': ['score']
            }
    
    async def _get_batch_schools(self, batch_code: str) -> List[str]:
        """获取批次中的所有学校ID - 使用数据适配器"""
        logger.debug(f"从数据适配器获取批次 {batch_code} 的学校列表")
        
        try:
            # 获取批次的所有学生分数数据
            student_scores = self.data_adapter.get_student_scores(batch_code)
            
            if not student_scores:
                logger.warning(f"批次 {batch_code} 没有找到学生分数数据")
                return []
            
            # 提取所有学校ID
            school_ids = set()
            for score_record in student_scores:
                school_id = score_record.get('school_id')
                if school_id and not isinstance(school_id, dict):
                    school_ids.add(str(school_id))
            
            schools = sorted(list(school_ids))
            logger.info(f"批次 {batch_code} 包含 {len(schools)} 个学校")
            return schools
            
        except Exception as e:
            logger.error(f"获取学校列表失败: {e}")
            return []
    
    async def _get_school_name(self, school_id: str) -> str:
        """获取学校名称"""
        try:
            from sqlalchemy import text
            query = text("SELECT DISTINCT school_name FROM student_cleaned_scores WHERE school_id = :school_id LIMIT 1")
            result = self.db_session.execute(query, {'school_id': school_id})
            row = result.fetchone()
            if row:
                return row[0]
        except Exception as e:
            logger.warning(f"获取学校名称失败: {e}")
        
        # 返回默认名称
        return f"学校_{school_id}"
    
    async def _get_batch_subjects(self, batch_code: str) -> List[Dict[str, Any]]:
        """获取批次中的所有科目配置 - 使用数据适配器"""
        logger.debug(f"从数据适配器获取批次 {batch_code} 的科目配置")
        
        try:
            # 使用数据适配器获取科目配置
            subject_configs = self.data_adapter.get_subject_configurations(batch_code)
            
            if not subject_configs:
                logger.warning(f"批次 {batch_code} 没有找到科目配置")
                return []
            
            # 转换格式以保持兼容性
            subjects = []
            for config in subject_configs:
                subjects.append({
                    'subject_name': config['subject_name'],
                    'max_score': config['max_score'],
                    'question_count': config['question_count'],
                    'subject_type': config['subject_type'],  # 新增字段
                    'question_type_enum': config.get('question_type_enum')
                })
            
            logger.info(f"批次 {batch_code} 包含 {len(subjects)} 个科目")
            logger.debug(f"科目类型分布: {[s['subject_type'] for s in subjects]}")
            return subjects
            
        except Exception as e:
            logger.error(f"获取科目配置失败: {e}")
            return []
    
    def _normalize_subject_type(self, subject_config: Dict[str, Any]) -> str:
        """统一科目类型判断逻辑 - 与DataAdapterRepository保持一致"""
        subject_type = subject_config.get('subject_type', '')
        question_type_enum = subject_config.get('question_type_enum', '')
        
        if question_type_enum and question_type_enum.lower() == 'questionnaire':
            return 'questionnaire'
        elif subject_type:
            return subject_type.lower()
        else:
            return 'exam'  # 默认考试类型
    
    async def _consolidate_multi_subject_results(self, batch_code: str, scores_df: pd.DataFrame, 
                                                validation_result: Dict[str, Any] = None) -> Dict[str, Any]:
        """整合多科目计算结果"""
        logger.info(f"开始整合批次 {batch_code} 的多科目统计结果")
        
        # 获取科目配置信息
        subjects_config = await self._get_batch_subjects(batch_code)
        if not subjects_config:
            logger.warning(f"批次 {batch_code} 没有找到科目配置")
            return {}
        
        consolidated = {
            'academic_subjects': {},
            'non_academic_subjects': {},  # 用于问卷类科目
            'calculation_metadata': {
                'calculation_time': time.time(),
                'data_version': '1.0',
                'algorithm_versions': {},
                'total_subjects': len(subjects_config),
                'batch_code': batch_code
            }
        }
        
        # 为每个科目计算统计指标
        for subject_config in subjects_config:
            subject_name = subject_config['subject_name']
            max_score = subject_config['max_score']
            subject_type = self._normalize_subject_type(subject_config)
            
            logger.debug(f"处理科目: {subject_name} (满分: {max_score}, 类型: {subject_type})")
            
            # 筛选该科目的数据
            subject_data_df = scores_df[scores_df['subject_name'] == subject_name].copy()
            if subject_data_df.empty:
                logger.warning(f"科目 {subject_name} 没有找到学生分数数据")
                continue
            
            # 清洗表中的数据已经是每个学生每个科目一条记录
            logger.debug(f"清洗数据记录数: {len(subject_data_df)}")
            
            # 直接创建计算用的DataFrame（数据已经清洗和聚合）
            # 注意：在上面的字段映射中，total_score已经被重命名为score
            score_column = 'score' if 'score' in subject_data_df.columns else 'total_score'
            calculation_df = pd.DataFrame({
                'score': subject_data_df[score_column].fillna(0).astype(float),
                'student_id': subject_data_df['student_id'],
                'school_id': subject_data_df['school_id']
            })
            
            # 计算该科目的唯一学生数量
            unique_student_count = len(calculation_df)
            logger.debug(f"科目 {subject_name} 学生数: {unique_student_count}")
            
            # 从数据库中获取批次的真实年级信息
            grade_level = self._get_batch_grade_level(batch_code)
            
            # 科目专用配置
            subject_calculation_config = {
                'max_score': float(max_score),  # 确保是float类型
                'grade_level': grade_level,
                'percentiles': [10, 25, 50, 75, 90],  # 包含用户要求的P10, P50, P90
                'required_columns': ['score']
            }
            
            try:
                # 根据科目类型选择不同的计算策略
                if subject_type == 'questionnaire':
                    # 问卷类科目：使用专门的问卷处理逻辑
                    basic_stats, educational_metrics, percentiles, discrimination, dimension_statistics = \
                        await self._calculate_questionnaire_statistics(batch_code, subject_name, max_score, subject_calculation_config)
                else:
                    # 学业科目：使用标准计算流程
                    # 计算各项统计指标
                    logger.info(f"学业科目 {subject_name} 开始计算，数据量: {len(calculation_df)}, 配置: {subject_calculation_config}")
                    
                    basic_stats = self.engine.calculate('basic_statistics', calculation_df, subject_calculation_config)
                    logger.debug(f"科目 {subject_name} 基础统计完成: count={basic_stats.get('count', 0)}")
                    
                    # 验证数据
                    logger.info(f"科目 {subject_name} 数据样本: score范围[{calculation_df['score'].min():.1f}, {calculation_df['score'].max():.1f}], 平均{calculation_df['score'].mean():.1f}")
                    
                    educational_metrics = self.engine.calculate('educational_metrics', calculation_df, subject_calculation_config)
                    if educational_metrics is None:
                        logger.error(f"科目 {subject_name} 教育指标计算返回None!")
                        educational_metrics = {}
                    else:
                        logger.info(f"科目 {subject_name} 教育指标计算完成: grade_level={grade_level}, max_score={max_score}")
                        logger.info(f"科目 {subject_name} 教育指标结果: {educational_metrics}")
                    
                    percentiles = self.engine.calculate('percentiles', calculation_df, subject_calculation_config)
                    logger.debug(f"科目 {subject_name} 百分位数计算完成")
                    
                    # 计算区分度（如果数据量足够）
                    discrimination = None
                    if len(calculation_df) >= 10:
                        discrimination = self.engine.calculate('discrimination', calculation_df, subject_calculation_config)
                        logger.debug(f"科目 {subject_name} 区分度计算完成: {discrimination.get('discrimination_index', 0)}")
                    
                    # 计算维度统计
                    dimension_statistics = await self._calculate_subject_dimensions(batch_code, subject_name)
                    logger.debug(f"科目 {subject_name} 维度统计完成: {len(dimension_statistics)} 个维度")
                
                # 整合该科目的结果
                subject_result = self._build_subject_statistics(
                    subject_name, max_score, basic_stats, educational_metrics, 
                    percentiles, discrimination, unique_student_count, dimension_statistics
                )
                
                # 根据科目类型存储到对应的分类中
                if subject_type == 'questionnaire':
                    consolidated['non_academic_subjects'][subject_name] = subject_result
                    logger.info(f"问卷科目 {subject_name} 统计计算完成，学生数: {len(calculation_df)}")
                else:
                    consolidated['academic_subjects'][subject_name] = subject_result
                    logger.info(f"学业科目 {subject_name} 统计计算完成，学生数: {len(calculation_df)}")
                
            except Exception as e:
                logger.error(f"科目 {subject_name} 统计计算失败: {e}")
                # 创建空的统计结果
                empty_stats = self._create_empty_subject_stats(subject_name, max_score)
                if subject_type == 'questionnaire':
                    consolidated['non_academic_subjects'][subject_name] = empty_stats
                else:
                    consolidated['academic_subjects'][subject_name] = empty_stats
        
        # 验证警告
        if validation_result:
            consolidated['calculation_metadata']['validation_warnings'] = validation_result.get('warnings', [])
        
        logger.info(f"多科目统计整合完成，处理了 {len(consolidated['academic_subjects'])} 个科目")
        return consolidated
    
    def _build_subject_statistics(self, subject_name: str, max_score: float, 
                                basic_stats: Dict, educational_metrics: Dict,
                                percentiles: Dict, discrimination: Dict = None, 
                                unique_student_count: int = None, 
                                dimension_statistics: Dict[str, Dict[str, Any]] = None) -> Dict[str, Any]:
        """构建单个科目的完整统计数据"""
        
        subject_data = {
            'subject_name': subject_name,
            'max_score': max_score,
            'school_stats': {},
            'grade_distribution': {},
            'statistical_indicators': {},
            'percentiles': {},
            'dimensions': {}  # 为维度分析预留
        }
        
        # 基础统计
        if basic_stats:
            # 使用正确的唯一学生数量，而不是记录总数
            actual_student_count = unique_student_count if unique_student_count is not None else basic_stats.get('count', 0)
            
            subject_data['school_stats'] = {
                'avg_score': basic_stats.get('mean', 0),
                'std_score': basic_stats.get('std', 0),
                'min_score': basic_stats.get('min', 0),
                'max_score_achieved': basic_stats.get('max', 0),  # 最高得分
                'student_count': actual_student_count,  # 修复：使用唯一学生数量
                'score_rate': (basic_stats.get('mean', 0) / max_score) if max_score > 0 else 0  # 得分率
            }
        
        # 教育指标
        if educational_metrics:
            logger.debug(f"处理科目 {subject_name} 教育指标: {educational_metrics}")
            # 等级分布 - 根据年级标准映射
            grade_dist = educational_metrics.get('grade_distribution', {})
            logger.debug(f"科目 {subject_name} 等级分布数据: {grade_dist}")
            
            # 判断是否为初中标准（使用A/B/C/D等级）
            if 'a_count' in grade_dist:
                # 初中标准：A≥85, B70-84, C60-69, D<60
                subject_data['grade_distribution'] = {
                    'excellent': {  # A等级对应优秀
                        'count': grade_dist.get('a_count', 0),
                        'percentage': grade_dist.get('a_rate', 0)
                    },
                    'good': {  # B等级对应良好
                        'count': grade_dist.get('b_count', 0),
                        'percentage': grade_dist.get('b_rate', 0)
                    },
                    'pass': {  # C等级对应及格
                        'count': grade_dist.get('c_count', 0),
                        'percentage': grade_dist.get('c_rate', 0)
                    },
                    'fail': {  # D等级对应不及格
                        'count': grade_dist.get('d_count', 0),
                        'percentage': grade_dist.get('d_rate', 0)
                    }
                }
            else:
                # 小学标准：优秀≥90, 良好80-89, 及格60-79, 不及格<60
                subject_data['grade_distribution'] = {
                    'excellent': {
                        'count': grade_dist.get('excellent_count', 0),
                        'percentage': grade_dist.get('excellent_rate', 0)
                    },
                    'good': {
                        'count': grade_dist.get('good_count', 0),
                        'percentage': grade_dist.get('good_rate', 0)
                    },
                    'pass': {
                        'count': grade_dist.get('pass_count', 0),
                        'percentage': grade_dist.get('pass_rate', 0)
                    },
                    'fail': {
                        'count': grade_dist.get('fail_count', 0),
                        'percentage': grade_dist.get('fail_rate', 0)
                    }
                }
            
            # 统计指标
            subject_data['statistical_indicators'] = {
                'difficulty_coefficient': educational_metrics.get('difficulty_coefficient', 0),
                'pass_rate': educational_metrics.get('pass_rate', 0),
                'excellent_rate': educational_metrics.get('excellent_rate', 0),
                'average_score_rate': educational_metrics.get('average_score_rate', 0)
            }
        
        # 百分位数（包括用户要求的P10, P50, P90）
        if percentiles:
            subject_data['percentiles'] = {
                'P10': percentiles.get('P10', 0),
                'P25': percentiles.get('P25', 0),
                'P50': percentiles.get('P50', 0),  # 中位数
                'P75': percentiles.get('P75', 0),
                'P90': percentiles.get('P90', 0),
                'IQR': percentiles.get('IQR', 0)
            }
        
        # 区分度
        if discrimination:
            if 'statistical_indicators' not in subject_data:
                subject_data['statistical_indicators'] = {}
            subject_data['statistical_indicators']['discrimination_index'] = discrimination.get('discrimination_index', 0)
            subject_data['statistical_indicators']['discrimination_interpretation'] = discrimination.get('interpretation', 'unknown')
        
        # 维度统计
        if dimension_statistics:
            subject_data['dimensions'] = dimension_statistics
        else:
            subject_data['dimensions'] = {}
        
        return subject_data
    
    def _create_empty_subject_stats(self, subject_name: str, max_score: float) -> Dict[str, Any]:
        """创建空的科目统计结果"""
        return {
            'subject_name': subject_name,
            'max_score': max_score,
            'school_stats': {
                'avg_score': 0,
                'std_score': 0,
                'min_score': 0,
                'max_score_achieved': 0,
                'student_count': 0,
                'score_rate': 0
            },
            'grade_distribution': {
                'excellent': {'count': 0, 'percentage': 0},
                'good': {'count': 0, 'percentage': 0},
                'pass': {'count': 0, 'percentage': 0},
                'fail': {'count': 0, 'percentage': 0}
            },
            'statistical_indicators': {
                'difficulty_coefficient': 0,
                'pass_rate': 0,
                'excellent_rate': 0,
                'average_score_rate': 0,
                'discrimination_index': 0,
                'discrimination_interpretation': 'unknown'
            },
            'percentiles': {
                'P10': 0, 'P25': 0, 'P50': 0, 'P75': 0, 'P90': 0, 'IQR': 0
            },
            'dimensions': {}
        }
    
    async def _save_regional_statistics(self, batch_code: str, statistics_data: Dict[str, Any], 
                                      total_students: int, calculation_duration: float):
        """保存区域级统计数据"""
        # v1.2：计算完成即产出 subjects 结构（忽略旧结构，统一写入 v1.2）
        builder = SubjectsBuilder()
        subjects = builder.build_regional_subjects(batch_code)
        v12_json = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'REGIONAL',
            'subjects': subjects,
        }
        processed = round2_json(v12_json)
        aggregation_data = {
            'batch_code': batch_code,
            'aggregation_level': AggregationLevel.REGIONAL,
            'school_id': None,
            'school_name': None,
            'statistics_data': processed,
            'calculation_status': CalculationStatus.COMPLETED,
            'total_students': total_students,
            'total_schools': 0,
            'calculation_duration': calculation_duration
        }
        result = self.repository.upsert_statistics(aggregation_data)
        logger.debug(f"区域级统计数据已保存，记录ID: {result.id}")
    
    async def _save_school_statistics(self, batch_code: str, school_id: str, school_name: str,
                                    statistics_data: Dict[str, Any], total_students: int, 
                                    calculation_duration: float):
        """保存学校级统计数据"""
        # v1.2：计算完成即产出 subjects 结构
        builder = SubjectsBuilder()
        subjects = builder.build_school_subjects(batch_code, school_id)
        v12_json = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'SCHOOL',
            'school_code': school_id,
            'subjects': subjects,
        }
        processed = round2_json(v12_json)
        aggregation_data = {
            'batch_code': batch_code,
            'aggregation_level': AggregationLevel.SCHOOL,
            'school_id': school_id,
            'school_name': school_name,
            'statistics_data': processed,
            'calculation_status': CalculationStatus.COMPLETED,
            'total_students': total_students,
            'total_schools': 0,
            'calculation_duration': calculation_duration
        }
        result = self.repository.upsert_statistics(aggregation_data)
        logger.debug(f"学校级统计数据已保存，记录ID: {result.id}")
    
    # 注意: 以下方法已不再需要，因为现在直接从清洗表获取维度数据
    # _get_batch_dimensions, _get_dimension_question_mapping, _get_dimension_max_score
    # 这些方法基于原始表和题目映射，现在维度数据直接来自清洗表的JSON字段
    
    async def _get_students_score_detail_json(self, batch_code: str, subject_name: str) -> List[Dict[str, Any]]:
        """从清洗表获取学生维度分数JSON数据"""
        logger.debug(f"从清洗表获取批次 {batch_code} 科目 {subject_name} 的学生维度分数")
        
        try:
            from sqlalchemy import text
            query = text("""
                SELECT 
                    student_id,
                    student_name,
                    school_code,
                    school_name,
                    dimension_scores,
                    dimension_max_scores
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code 
                    AND subject_name = :subject_name
                    AND is_valid = 1
                ORDER BY student_id
            """)
            
            result = self.db_session.execute(query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            
            students_data = []
            for row in result.fetchall():
                students_data.append({
                    'student_id': row[0],
                    'student_name': row[1], 
                    'school_code': row[2],
                    'school_name': row[3],
                    'dimension_scores': row[4],  # 维度分数JSON
                    'dimension_max_scores': row[5]  # 维度满分JSON
                })
            
            logger.debug(f"从清洗表获取到 {len(students_data)} 个学生的维度分数数据")
            return students_data
            
        except Exception as e:
            logger.error(f"从清洗表获取学生维度分数失败: {e}")
            return []
    
    def _calculate_dimension_scores(self, students_data: List[Dict[str, Any]], 
                                   dimension_code: str) -> List[float]:
        """从清洗表的维度分数JSON中直接提取维度分数"""
        import json
        
        dimension_scores = []
        
        for student in students_data:
            try:
                dimension_scores_json = student.get('dimension_scores', '{}')
                if isinstance(dimension_scores_json, str):
                    dimension_data = json.loads(dimension_scores_json)
                else:
                    dimension_data = dimension_scores_json or {}
                
                # 直接从JSON中获取指定维度的分数
                dimension_score = 0.0
                if dimension_code in dimension_data:
                    score_info = dimension_data[dimension_code]
                    if isinstance(score_info, dict) and 'score' in score_info:
                        dimension_score = float(score_info['score'])
                    elif isinstance(score_info, (int, float)):
                        dimension_score = float(score_info)
                
                dimension_scores.append(dimension_score)
                
            except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
                logger.warning(f"学生 {student.get('student_id')} 维度 {dimension_code} 分数数据解析失败: {e}")
                dimension_scores.append(0.0)
        
        logger.debug(f"从清洗数据提取到 {len(dimension_scores)} 个维度 {dimension_code} 分数")
        return dimension_scores
    
    async def _calculate_subject_dimensions(self, batch_code: str, subject_name: str) -> Dict[str, Dict[str, Any]]:
        """计算科目的所有维度统计 - 基于数据适配器的JSON维度数据"""
        logger.debug(f"使用数据适配器计算科目 {subject_name} 的维度统计")
        
        try:
            # 1. 使用数据适配器获取该科目的学生分数数据（包含维度JSON）
            student_scores = self.data_adapter.get_student_scores(batch_code, subject_type=None, school_id=None)
            if not student_scores:
                logger.warning(f"批次 {batch_code} 没有找到学生分数数据")
                return {}
            
            # 2. 筛选该科目的数据
            subject_scores = [s for s in student_scores if s['subject_name'] == subject_name]
            if not subject_scores:
                logger.warning(f"科目 {subject_name} 没有找到学生维度数据")
                return {}
            
            logger.info(f"科目 {subject_name} 找到 {len(subject_scores)} 个学生的维度数据")
            
        except Exception as e:
            logger.error(f"获取科目 {subject_name} 维度数据失败: {e}")
            return {}
        
        # 3. 提取所有维度定义和数据
        available_dimensions = set()
        dimension_max_scores_info = {}
        
        for student_score in subject_scores:
            dimensions = student_score.get('dimensions', {})
            if dimensions:
                available_dimensions.update(dimensions.keys())
                
                # 保存维度满分信息（以第一个有效记录为准）
                for dim_code, dim_data in dimensions.items():
                    if dim_code not in dimension_max_scores_info and isinstance(dim_data, dict):
                        dimension_max_scores_info[dim_code] = {
                            'name': dim_code,
                            'max_score': dim_data.get('max_score', 0)
                        }
        
        if not available_dimensions:
            logger.warning(f"科目 {subject_name} 没有找到任何维度数据")
            return {}
        
        logger.info(f"科目 {subject_name} 发现 {len(available_dimensions)} 个维度: {list(available_dimensions)}")
        
        dimension_results = {}
        
        # 4. 为每个维度计算统计
        for dimension_code in available_dimensions:
            try:
                # 获取维度信息
                dimension_max_info = dimension_max_scores_info.get(dimension_code, {})
                dimension_name = dimension_max_info.get('name', dimension_code)
                dimension_max_score = float(dimension_max_info.get('max_score', 0))
                
                logger.debug(f"处理维度: {dimension_code} - {dimension_name} (满分: {dimension_max_score})")
                
                # 提取学生在该维度的分数
                dimension_scores = []
                for student_score in subject_scores:
                    dimensions = student_score.get('dimensions', {})
                    dim_data = dimensions.get(dimension_code, {})
                    if isinstance(dim_data, dict) and 'score' in dim_data:
                        dimension_scores.append(float(dim_data['score']))
                    else:
                        dimension_scores.append(0.0)
                
                if not dimension_scores or all(score == 0 for score in dimension_scores):
                    logger.warning(f"维度 {dimension_code} 没有有效分数数据")
                    continue
                
                # 创建DataFrame用于统计计算
                import pandas as pd
                dimension_df = pd.DataFrame({'score': dimension_scores})
                
                # 维度专用配置
                grade_level = self._get_batch_grade_level(batch_code)
                dimension_config = {
                    'max_score': dimension_max_score,
                    'grade_level': grade_level,
                    'percentiles': [10, 25, 50, 75, 90],
                    'required_columns': ['score']
                }
                
                # 计算基础统计
                basic_stats = self.engine.calculate('basic_statistics', dimension_df, dimension_config)
                
                # 计算教育指标
                educational_metrics = self.engine.calculate('educational_metrics', dimension_df, dimension_config)
                
                # 计算百分位数
                percentiles = self.engine.calculate('percentiles', dimension_df, dimension_config)
                
                # 计算区分度（如果数据量足够）
                discrimination = None
                if len(dimension_scores) >= 10:
                    discrimination = self.engine.calculate('discrimination', dimension_df, dimension_config)
                
                # 构建维度统计结果
                dimension_results[dimension_code] = {
                    'dimension_code': dimension_code,
                    'dimension_name': dimension_name,
                    'max_score': dimension_max_score,
                    'question_count': 0,  # 清洗后数据不再需要题目计数
                    'question_ids': [],   # 清洗后数据不再需要题目映射
                    'basic_stats': {
                        'avg_score': basic_stats.get('mean', 0),
                        'std_score': basic_stats.get('std', 0),
                        'min_score': basic_stats.get('min', 0),
                        'max_score_achieved': basic_stats.get('max', 0),
                        'student_count': basic_stats.get('count', 0),
                        'score_rate': (basic_stats.get('mean', 0) / dimension_max_score) if dimension_max_score > 0 else 0
                    },
                    'percentiles': {
                        'P10': percentiles.get('P10', 0),
                        'P25': percentiles.get('P25', 0),
                        'P50': percentiles.get('P50', 0),
                        'P75': percentiles.get('P75', 0),
                        'P90': percentiles.get('P90', 0),
                        'IQR': percentiles.get('IQR', 0)
                    },
                    'educational_metrics': {
                        'difficulty_coefficient': educational_metrics.get('difficulty_coefficient', 0),
                        'pass_rate': educational_metrics.get('pass_rate', 0),
                        'excellent_rate': educational_metrics.get('excellent_rate', 0),
                        'average_score_rate': educational_metrics.get('average_score_rate', 0)
                    }
                }
                
                # 添加区分度（如果有的话）
                if discrimination:
                    dimension_results[dimension_code]['discrimination'] = {
                        'discrimination_index': discrimination.get('discrimination_index', 0),
                        'interpretation': discrimination.get('interpretation', 'unknown')
                    }
                
                logger.info(f"维度 {dimension_code} 统计计算完成，学生数: {len(dimension_scores)}")
                
            except Exception as e:
                logger.error(f"维度 {dimension_code} 统计计算失败: {e}")
                continue
        
        logger.info(f"科目 {subject_name} 维度统计完成，处理了 {len(dimension_results)} 个维度")
        return dimension_results
    
    async def _calculate_questionnaire_statistics(self, batch_code: str, subject_name: str, 
                                                max_score: float, config: Dict[str, Any]) -> tuple:
        """计算问卷类科目的统计数据 - 使用专用问卷明细表"""
        logger.info(f"开始计算问卷科目 {subject_name} 的统计数据")
        
        try:
            # 1. 使用数据适配器获取问卷明细数据
            questionnaire_details = self.data_adapter.get_questionnaire_details(batch_code, subject_name)
            if not questionnaire_details:
                logger.warning(f"问卷科目 {subject_name} 没有找到明细数据")
                return {}, {}, {}, None, {}
            
            logger.info(f"问卷科目 {subject_name} 获取到 {len(questionnaire_details)} 条明细记录")
            
            # 2. 转换为计算引擎可用的DataFrame格式
            df_data = []
            for detail in questionnaire_details:
                df_data.append({
                    'score': detail['original_score'],
                    'student_id': detail['student_id'],
                    'school_id': detail['school_id'],
                    'question_id': detail['question_id'],
                    'max_score': detail['max_score'],
                    'scale_level': detail['scale_level'],
                    'instrument_type': detail['instrument_type']
                })
            
            # 3. 按学生汇总分数（问卷总分）
            import pandas as pd
            details_df = pd.DataFrame(df_data)
            
            # 按学生ID汇总得到每个学生的问卷总分
            student_totals = details_df.groupby('student_id')['score'].sum().reset_index()
            calculation_df = pd.DataFrame({
                'score': student_totals['score'],
                'student_id': student_totals['student_id']
            })
            
            logger.info(f"问卷科目 {subject_name} 学生总分范围: [{calculation_df['score'].min():.1f}, {calculation_df['score'].max():.1f}], 平均: {calculation_df['score'].mean():.1f}")
            
            # 4. 使用计算引擎进行统计计算
            basic_stats = self.engine.calculate('basic_statistics', calculation_df, config)
            educational_metrics = self.engine.calculate('educational_metrics', calculation_df, config)
            percentiles = self.engine.calculate('percentiles', calculation_df, config)
            
            # 区分度（如果数据量足够）
            discrimination = None
            if len(calculation_df) >= 10:
                discrimination = self.engine.calculate('discrimination', calculation_df, config)
            
            # 5. 计算问卷维度统计（基于JSON维度数据）
            dimension_statistics = await self._calculate_subject_dimensions(batch_code, subject_name)
            
            # 6. 获取选项分布统计（问卷特有）
            option_distributions = self.data_adapter.get_questionnaire_distribution(batch_code, subject_name)
            if option_distributions:
                logger.info(f"问卷科目 {subject_name} 获取到 {len(option_distributions)} 条选项分布记录")
                # 将选项分布信息添加到维度统计中
                dimension_statistics['_option_distributions'] = self._process_option_distributions(option_distributions)
            
            logger.info(f"问卷科目 {subject_name} 统计计算完成，学生数: {len(calculation_df)}")
            return basic_stats, educational_metrics, percentiles, discrimination, dimension_statistics
            
        except Exception as e:
            logger.error(f"问卷科目 {subject_name} 统计计算失败: {e}")
            return {}, {}, {}, None, {}
    
    def _process_option_distributions(self, distributions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """处理问卷选项分布数据"""
        processed = {}
        
        for dist in distributions:
            question_id = dist['question_id']
            if question_id not in processed:
                processed[question_id] = {
                    'question_id': question_id,
                    'scale_level': dist['scale_level'],
                    'options': {}
                }
            
            processed[question_id]['options'][dist['option_level']] = {
                'student_count': dist['student_count'],
                'percentage': dist['percentage']
            }
        
        return processed

    def _get_batch_grade_level(self, batch_code: str) -> str:
        """从grade_aggregation_main表中获取批次的真实年级信息"""
        try:
            from sqlalchemy import text
            query = text("""
                SELECT grade_level 
                FROM grade_aggregation_main 
                WHERE batch_code = :batch_code
                LIMIT 1
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            row = result.fetchone()
            
            if row:
                grade_level = row[0]
                logger.debug(f"从数据库获取批次 {batch_code} 年级: {grade_level}")
                return grade_level
            else:
                logger.warning(f"批次 {batch_code} 在grade_aggregation_main表中未找到，使用默认年级")
                
        except Exception as e:
            logger.error(f"获取批次年级失败: {e}")
        
        # 默认年级（如果数据库查询失败）
        return '7th_grade'

    async def _update_calculation_status(self, batch_code: str, status: CalculationStatus, error_message: str = None):
        """更新计算状态"""
        try:
            # 更新区域级状态
            regional_stat = self.repository.get_regional_statistics(batch_code)
            if regional_stat:
                self.repository.update_calculation_status(regional_stat.id, status)
                
            logger.debug(f"批次 {batch_code} 计算状态已更新为: {status.value}")
        except Exception as e:
            logger.error(f"更新计算状态失败: {str(e)}")
    
    async def validate_batch_data_quality(self, batch_code: str) -> Dict[str, Any]:
        """全面验证批次数据质量"""
        logger.info(f"开始验证批次 {batch_code} 的数据质量")
        
        validation_results = {
            'batch_code': batch_code,
            'validation_time': time.time(),
            'overall_status': 'UNKNOWN',
            'issues_found': 0,
            'warnings_found': 0,
            'subjects': {},
            'summary': {}
        }
        
        try:
            # 1. 基础数据完整性检查
            basic_checks = await self._validate_basic_data_integrity(batch_code)
            validation_results['basic_checks'] = basic_checks
            
            # 2. 分科目数据质量检查
            subjects_config = await self._get_batch_subjects(batch_code)
            for subject_config in subjects_config:
                subject_name = subject_config['subject_name']
                max_score = subject_config['max_score']
                
                logger.debug(f"验证科目 {subject_name} 数据质量")
                subject_validation = await self._validate_subject_data_quality(
                    batch_code, subject_name, max_score
                )
                validation_results['subjects'][subject_name] = subject_validation
            
            # 3. 维度数据完整性检查
            dimension_checks = await self._validate_dimension_data(batch_code)
            validation_results['dimension_checks'] = dimension_checks
            
            # 4. 汇总验证结果
            validation_results = self._summarize_validation_results(validation_results)
            
            logger.info(f"批次 {batch_code} 数据质量验证完成: {validation_results['overall_status']}")
            return validation_results
            
        except Exception as e:
            logger.error(f"数据质量验证失败: {e}")
            validation_results['overall_status'] = 'ERROR'
            validation_results['error'] = str(e)
            return validation_results
    
    async def _validate_basic_data_integrity(self, batch_code: str) -> Dict[str, Any]:
        """验证基础数据完整性"""
        logger.debug(f"验证批次 {batch_code} 基础数据完整性")
        
        checks = {
            'student_score_detail': {'exists': False, 'count': 0, 'issues': []},
            'subject_question_config': {'exists': False, 'count': 0, 'issues': []},
            'batch_dimension_definition': {'exists': False, 'count': 0, 'issues': []},
            'question_dimension_mapping': {'exists': False, 'count': 0, 'issues': []}
        }
        
        try:
            from sqlalchemy import text
            
            # 检查学生分数数据
            query1 = text("SELECT COUNT(*) FROM student_score_detail WHERE batch_code = :batch_code")
            result1 = self.db_session.execute(query1, {'batch_code': batch_code})
            count1 = result1.fetchone()[0]
            checks['student_score_detail'] = {
                'exists': count1 > 0,
                'count': count1,
                'issues': [] if count1 > 0 else ['无学生分数数据']
            }
            
            # 检查科目题目配置
            query2 = text("SELECT COUNT(*) FROM subject_question_config WHERE batch_code = :batch_code")
            result2 = self.db_session.execute(query2, {'batch_code': batch_code})
            count2 = result2.fetchone()[0]
            checks['subject_question_config'] = {
                'exists': count2 > 0,
                'count': count2,
                'issues': [] if count2 > 0 else ['无科目题目配置数据']
            }
            
            # 检查维度定义
            query3 = text("SELECT COUNT(*) FROM batch_dimension_definition WHERE batch_code = :batch_code")
            result3 = self.db_session.execute(query3, {'batch_code': batch_code})
            count3 = result3.fetchone()[0]
            checks['batch_dimension_definition'] = {
                'exists': count3 > 0,
                'count': count3,
                'issues': [] if count3 > 0 else ['无维度定义数据']
            }
            
            # 检查题目维度映射
            query4 = text("SELECT COUNT(*) FROM question_dimension_mapping WHERE batch_code = :batch_code")
            result4 = self.db_session.execute(query4, {'batch_code': batch_code})
            count4 = result4.fetchone()[0]
            checks['question_dimension_mapping'] = {
                'exists': count4 > 0,
                'count': count4,
                'issues': [] if count4 > 0 else ['无题目维度映射数据']
            }
            
        except Exception as e:
            logger.error(f"基础数据完整性检查失败: {e}")
            for table in checks:
                checks[table]['issues'].append(f"检查失败: {str(e)}")
        
        return checks
    
    async def _validate_subject_data_quality(self, batch_code: str, subject_name: str, max_score: float) -> Dict[str, Any]:
        """验证科目数据质量"""
        logger.debug(f"验证科目 {subject_name} 数据质量")
        
        validation = {
            'subject_name': subject_name,
            'max_score': max_score,
            'issues': [],
            'warnings': [],
            'statistics': {},
            'status': 'OK'
        }
        
        try:
            from sqlalchemy import text
            
            # 获取科目原始数据统计
            query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students,
                    MIN(total_score) as min_score,
                    MAX(total_score) as max_score,
                    AVG(total_score) as avg_score,
                    SUM(CASE WHEN total_score < 0 THEN 1 ELSE 0 END) as negative_scores,
                    SUM(CASE WHEN total_score > :max_score THEN 1 ELSE 0 END) as overmax_scores,
                    SUM(CASE WHEN total_score IS NULL THEN 1 ELSE 0 END) as null_scores
                FROM student_score_detail 
                WHERE batch_code = :batch_code AND subject_name = :subject_name
            """)
            
            result = self.db_session.execute(query, {
                'batch_code': batch_code,
                'subject_name': subject_name,
                'max_score': max_score
            })
            row = result.fetchone()
            
            if row and row[0] > 0:
                validation['statistics'] = {
                    'total_records': row[0],
                    'unique_students': row[1],
                    'min_score': float(row[2]) if row[2] is not None else 0,
                    'max_score': float(row[3]) if row[3] is not None else 0,
                    'avg_score': float(row[4]) if row[4] is not None else 0,
                    'negative_scores': row[5],
                    'overmax_scores': row[6],
                    'null_scores': row[7]
                }
                
                # 数据质量检查
                if row[5] > 0:  # 负分数
                    validation['issues'].append(f"发现 {row[5]} 个负分数")
                    validation['status'] = 'ISSUES'
                
                if row[6] > 0:  # 超出满分
                    validation['issues'].append(f"发现 {row[6]} 个超出满分({max_score})的分数")
                    validation['status'] = 'ISSUES'
                
                if row[7] > 0:  # 空值分数
                    validation['warnings'].append(f"发现 {row[7]} 个空值分数")
                    if validation['status'] == 'OK':
                        validation['status'] = 'WARNINGS'
                
                # 数据重复率检查
                if row[0] > row[1] * 2:  # 如果记录数是学生数的2倍以上，可能存在过多重复
                    duplication_ratio = row[0] / row[1]
                    validation['warnings'].append(f"数据重复率较高: {duplication_ratio:.1f}倍 ({row[0]}条记录/{row[1]}个学生)")
                    if validation['status'] == 'OK':
                        validation['status'] = 'WARNINGS'
            else:
                validation['issues'].append("无数据或数据为空")
                validation['status'] = 'ISSUES'
            
        except Exception as e:
            logger.error(f"科目 {subject_name} 数据质量验证失败: {e}")
            validation['issues'].append(f"验证失败: {str(e)}")
            validation['status'] = 'ERROR'
        
        return validation
    
    async def _validate_dimension_data(self, batch_code: str) -> Dict[str, Any]:
        """验证维度数据完整性"""
        logger.debug(f"验证批次 {batch_code} 维度数据")
        
        validation = {
            'status': 'OK',
            'issues': [],
            'warnings': [],
            'subjects_with_dimensions': 0,
            'total_dimensions': 0,
            'subjects_checked': 0
        }
        
        try:
            subjects_config = await self._get_batch_subjects(batch_code)
            validation['subjects_checked'] = len(subjects_config)
            
            for subject_config in subjects_config:
                subject_name = subject_config['subject_name']
                
                # 检查科目维度定义
                dimensions = await self._get_batch_dimensions(batch_code, subject_name)
                if dimensions:
                    validation['subjects_with_dimensions'] += 1
                    validation['total_dimensions'] += len(dimensions)
                    
                    # 检查每个维度的题目映射
                    for dimension in dimensions:
                        dimension_code = dimension['dimension_code']
                        questions = await self._get_dimension_question_mapping(
                            batch_code, subject_name, dimension_code
                        )
                        
                        if not questions:
                            validation['warnings'].append(
                                f"科目 {subject_name} 维度 {dimension_code} 无题目映射"
                            )
                            if validation['status'] == 'OK':
                                validation['status'] = 'WARNINGS'
                else:
                    validation['warnings'].append(f"科目 {subject_name} 无维度定义")
                    if validation['status'] == 'OK':
                        validation['status'] = 'WARNINGS'
            
            # 如果没有任何科目有维度，这是一个严重问题
            if validation['subjects_with_dimensions'] == 0:
                validation['issues'].append("所有科目都没有维度定义")
                validation['status'] = 'ISSUES'
            
        except Exception as e:
            logger.error(f"维度数据验证失败: {e}")
            validation['issues'].append(f"验证失败: {str(e)}")
            validation['status'] = 'ERROR'
        
        return validation
    
    def _summarize_validation_results(self, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """汇总验证结果"""
        issues_count = 0
        warnings_count = 0
        overall_status = 'OK'
        
        # 统计基础检查问题
        basic_checks = validation_results.get('basic_checks', {})
        for table, check in basic_checks.items():
            issues_count += len(check.get('issues', []))
        
        # 统计科目问题
        subjects = validation_results.get('subjects', {})
        for subject_name, subject_validation in subjects.items():
            issues_count += len(subject_validation.get('issues', []))
            warnings_count += len(subject_validation.get('warnings', []))
        
        # 统计维度检查问题
        dimension_checks = validation_results.get('dimension_checks', {})
        issues_count += len(dimension_checks.get('issues', []))
        warnings_count += len(dimension_checks.get('warnings', []))
        
        # 确定整体状态
        if issues_count > 0:
            overall_status = 'ISSUES'
        elif warnings_count > 0:
            overall_status = 'WARNINGS'
        
        # 更新结果
        validation_results['issues_found'] = issues_count
        validation_results['warnings_found'] = warnings_count
        validation_results['overall_status'] = overall_status
        
        # 创建汇总信息
        validation_results['summary'] = {
            'total_issues': issues_count,
            'total_warnings': warnings_count,
            'subjects_checked': len(subjects),
            'status': overall_status,
            'recommendation': self._get_validation_recommendation(overall_status, issues_count, warnings_count)
        }
        
        return validation_results
    
    def _get_validation_recommendation(self, status: str, issues: int, warnings: int) -> str:
        """获取验证建议"""
        if status == 'OK':
            return "数据质量良好，可以继续计算"
        elif status == 'WARNINGS':
            return f"发现 {warnings} 个警告，建议检查后继续计算"
        elif status == 'ISSUES':
            return f"发现 {issues} 个严重问题，建议修复后再进行计算"
        else:
            return "验证出现错误，需要检查系统状态"


def create_calculation_service(db_session: Session) -> CalculationService:
    """创建计算服务实例"""
    return CalculationService(db_session)
