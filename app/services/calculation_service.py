# 统计计算服务
import logging
import pandas as pd
import time
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session

from ..database.models import AggregationLevel, CalculationStatus
from ..database.repositories import StatisticalAggregationRepository
from ..calculation.calculators import initialize_calculation_system
from ..calculation.engine import CalculationEngine

logger = logging.getLogger(__name__)


class CalculationService:
    """统计计算服务"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.repository = StatisticalAggregationRepository(db_session)
        self.engine = initialize_calculation_system()
        
    async def calculate_batch_statistics(self, batch_code: str, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """计算批次统计数据"""
        logger.info(f"开始计算批次 {batch_code} 的统计数据")
        start_time = time.time()
        
        try:
            # 1. 获取学生分数数据
            data = await self._fetch_student_scores(batch_code)
            if data.empty:
                raise ValueError(f"批次 {batch_code} 没有找到学生分数数据")
            
            # 2. 获取配置信息
            calculation_config = config or await self._get_calculation_config(batch_code)
            
            # 3. 数据验证
            validation_result = self.engine.validator.validate_input_data(data, calculation_config)
            if not validation_result['is_valid']:
                raise ValueError(f"数据验证失败: {validation_result['errors']}")
            
            # 4. 执行统计计算
            results = {}
            
            # 基础统计
            basic_stats = self.engine.calculate('basic_statistics', data, calculation_config)
            results['basic_statistics'] = basic_stats
            
            # 教育指标
            educational_metrics = self.engine.calculate('educational_metrics', data, calculation_config)
            results['educational_metrics'] = educational_metrics
            
            # 百分位数
            percentiles = self.engine.calculate('percentiles', data, calculation_config)
            results['percentiles'] = percentiles
            
            # 区分度
            discrimination = self.engine.calculate('discrimination', data, calculation_config)
            results['discrimination'] = discrimination
            
            # 5. 整合结果
            consolidated_results = self._consolidate_results(results, validation_result)
            
            # 6. 保存到数据库
            duration = time.time() - start_time
            await self._save_regional_statistics(
                batch_code=batch_code,
                statistics_data=consolidated_results,
                total_students=len(data),
                calculation_duration=duration
            )
            
            logger.info(f"批次 {batch_code} 统计计算完成，耗时 {duration:.2f}s，处理学生数: {len(data)}")
            
            return {
                'batch_code': batch_code,
                'statistics': consolidated_results,
                'calculation_duration': duration,
                'total_students': len(data),
                'validation_warnings': validation_result.get('warnings', [])
            }
            
        except Exception as e:
            logger.error(f"批次 {batch_code} 统计计算失败: {str(e)}")
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
            
            # 3. 执行计算（复用区域级计算逻辑）
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
            
            # 4. 整合结果
            consolidated_results = self._consolidate_results(results)
            
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
    
    async def calculate_batch_all_schools(self, batch_code: str, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """计算批次所有学校的统计数据"""
        logger.info(f"开始批量计算批次 {batch_code} 所有学校的统计数据")
        start_time = time.time()
        
        try:
            # 1. 获取批次中所有学校列表
            school_ids = await self._get_batch_schools(batch_code)
            if not school_ids:
                raise ValueError(f"批次 {batch_code} 中没有找到学校数据")
            
            # 2. 并行计算各学校统计数据
            results = []
            successful_count = 0
            failed_schools = []
            
            for school_id in school_ids:
                try:
                    school_result = await self.calculate_school_statistics(batch_code, school_id, config)
                    results.append(school_result)
                    successful_count += 1
                except Exception as e:
                    logger.error(f"学校 {school_id} 计算失败: {str(e)}")
                    failed_schools.append({'school_id': school_id, 'error': str(e)})
            
            duration = time.time() - start_time
            
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
        """获取批次学生分数数据"""
        # 这里应该调用实际的数据获取逻辑
        # 暂时返回模拟数据用于测试
        logger.debug(f"获取批次 {batch_code} 的学生分数数据")
        
        # 模拟数据 - 在实际实现中应该从数据库查询
        import numpy as np
        np.random.seed(42)
        n_students = 1000
        scores = np.random.normal(75, 15, n_students)
        scores = np.clip(scores, 0, 100)
        
        return pd.DataFrame({
            'score': scores,
            'student_id': [f'STU_{i:06d}' for i in range(n_students)],
            'school_id': [f'SCH_{i%20:03d}' for i in range(n_students)]
        })
    
    async def _fetch_school_scores(self, batch_code: str, school_id: str) -> pd.DataFrame:
        """获取学校学生分数数据"""
        logger.debug(f"获取学校 {school_id} 在批次 {batch_code} 的学生分数数据")
        
        # 模拟数据
        import numpy as np
        np.random.seed(hash(school_id) % 1000)
        n_students = np.random.randint(30, 100)  # 每个学校30-100个学生
        scores = np.random.normal(75, 15, n_students)
        scores = np.clip(scores, 0, 100)
        
        return pd.DataFrame({
            'score': scores,
            'student_id': [f'STU_{school_id}_{i:03d}' for i in range(n_students)],
            'school_id': [school_id] * n_students
        })
    
    async def _get_calculation_config(self, batch_code: str) -> Dict[str, Any]:
        """获取计算配置"""
        # 默认配置，在实际实现中应该从数据库或配置服务获取
        return {
            'max_score': 100,
            'grade_level': '5th_grade',
            'percentiles': [10, 25, 50, 75, 90],
            'required_columns': ['score']
        }
    
    async def _get_batch_schools(self, batch_code: str) -> List[str]:
        """获取批次中的所有学校ID"""
        logger.debug(f"获取批次 {batch_code} 的学校列表")
        
        # 模拟返回学校列表
        return [f'SCH_{i:03d}' for i in range(20)]  # 20个学校
    
    async def _get_school_name(self, school_id: str) -> str:
        """获取学校名称"""
        # 模拟学校名称
        return f"学校_{school_id}"
    
    def _consolidate_results(self, results: Dict[str, Any], validation_result: Dict[str, Any] = None) -> Dict[str, Any]:
        """整合计算结果"""
        consolidated = {
            'academic_subjects': {
                '数学': {  # 暂时固定为数学科目
                    'school_stats': {},
                    'grade_distribution': {},
                    'statistical_indicators': {},
                    'percentiles': {}
                }
            },
            'calculation_metadata': {
                'calculation_time': time.time(),
                'data_version': '1.0',
                'algorithm_versions': {}
            }
        }
        
        subject_data = consolidated['academic_subjects']['数学']
        
        # 基础统计
        if 'basic_statistics' in results:
            basic_stats = results['basic_statistics']
            subject_data['school_stats'] = {
                'avg_score': basic_stats.get('mean', 0),
                'std_score': basic_stats.get('std', 0),
                'min_score': basic_stats.get('min', 0),
                'max_score': basic_stats.get('max', 0),
                'student_count': basic_stats.get('count', 0)
            }
            consolidated['calculation_metadata']['algorithm_versions']['basic_statistics'] = basic_stats.get('_meta', {}).get('algorithm_info', {}).get('version', '1.0')
        
        # 教育指标
        if 'educational_metrics' in results:
            edu_metrics = results['educational_metrics']
            
            # 等级分布
            grade_dist = edu_metrics.get('grade_distribution', {})
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
                'difficulty_coefficient': edu_metrics.get('difficulty_coefficient', 0),
                'pass_rate': edu_metrics.get('pass_rate', 0),
                'excellent_rate': edu_metrics.get('excellent_rate', 0),
                'average_score_rate': edu_metrics.get('average_score_rate', 0)
            }
        
        # 百分位数
        if 'percentiles' in results:
            percentile_data = results['percentiles']
            subject_data['percentiles'] = {
                f'P{k[1:]}': v for k, v in percentile_data.items() if k.startswith('P')
            }
            subject_data['percentiles']['IQR'] = percentile_data.get('IQR', 0)
        
        # 区分度
        if 'discrimination' in results:
            disc_data = results['discrimination']
            subject_data['statistical_indicators']['discrimination_index'] = disc_data.get('discrimination_index', 0)
            subject_data['statistical_indicators']['discrimination_interpretation'] = disc_data.get('interpretation', 'unknown')
        
        # 验证警告
        if validation_result:
            consolidated['calculation_metadata']['validation_warnings'] = validation_result.get('warnings', [])
        
        return consolidated
    
    async def _save_regional_statistics(self, batch_code: str, statistics_data: Dict[str, Any], 
                                      total_students: int, calculation_duration: float):
        """保存区域级统计数据"""
        aggregation_data = {
            'batch_code': batch_code,
            'aggregation_level': AggregationLevel.REGIONAL,
            'school_id': None,
            'school_name': None,
            'statistics_data': statistics_data,
            'calculation_status': CalculationStatus.COMPLETED,
            'total_students': total_students,
            'total_schools': 0,  # 这里应该从实际数据获取
            'calculation_duration': calculation_duration
        }
        
        result = self.repository.upsert_statistics(aggregation_data)
        logger.debug(f"区域级统计数据已保存，记录ID: {result.id}")
    
    async def _save_school_statistics(self, batch_code: str, school_id: str, school_name: str,
                                    statistics_data: Dict[str, Any], total_students: int, 
                                    calculation_duration: float):
        """保存学校级统计数据"""
        aggregation_data = {
            'batch_code': batch_code,
            'aggregation_level': AggregationLevel.SCHOOL,
            'school_id': school_id,
            'school_name': school_name,
            'statistics_data': statistics_data,
            'calculation_status': CalculationStatus.COMPLETED,
            'total_students': total_students,
            'total_schools': 0,
            'calculation_duration': calculation_duration
        }
        
        result = self.repository.upsert_statistics(aggregation_data)
        logger.debug(f"学校级统计数据已保存，记录ID: {result.id}")
    
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


def create_calculation_service(db_session: Session) -> CalculationService:
    """创建计算服务实例"""
    return CalculationService(db_session)