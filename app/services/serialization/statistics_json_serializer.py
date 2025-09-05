"""
统计JSON序列化器主类

作为JSON序列化系统的门面类，协调各个子序列化器的工作，
提供统一的序列化接口和数据缓存管理。
"""

import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from datetime import datetime
import json

from .data_integrator import StatisticsDataIntegrator
from .regional_data_serializer import RegionalDataSerializer
from .school_data_serializer import SchoolDataSerializer
from .radar_chart_formatter import RadarChartFormatter
from .schema_validator import SchemaValidator, ValidationResult
from .version_manager import VersionManager

from ...database.models import StatisticalAggregation
from ...database.enums import AggregationLevel, CalculationStatus
from ...database.repositories import StatisticalAggregationRepository

logger = logging.getLogger(__name__)


class SerializationException(Exception):
    """序列化异常"""
    pass


class StatisticsJsonSerializer:
    """统计JSON序列化器主类"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.data_integrator = StatisticsDataIntegrator(db_session)
        self.regional_serializer = RegionalDataSerializer()
        self.school_serializer = SchoolDataSerializer()
        self.radar_formatter = RadarChartFormatter()
        self.schema_validator = SchemaValidator()
        self.version_manager = VersionManager()
        self.aggregation_repo = StatisticalAggregationRepository(db_session)
    
    async def serialize_regional_data(
        self, 
        batch_code: str, 
        force_recalculate: bool = False,
        validate_output: bool = True
    ) -> Dict[str, Any]:
        """
        序列化区域级统计数据
        
        Args:
            batch_code: 批次代码
            force_recalculate: 是否强制重新计算
            validate_output: 是否验证输出格式
            
        Returns:
            区域级JSON数据
        """
        logger.info(f"开始序列化区域级数据: {batch_code}")
        
        try:
            # 检查是否已有缓存数据
            if not force_recalculate:
                cached_data = await self._get_cached_regional_data(batch_code)
                if cached_data:
                    logger.info(f"返回缓存的区域级数据: {batch_code}")
                    return cached_data
            
            # 收集统计数据
            integrated_data = await self.data_integrator.collect_all_statistics(batch_code)
            
            # 序列化为JSON格式
            regional_json = self.regional_serializer.serialize(integrated_data)
            
            # 添加版本信息
            regional_json = self.version_manager.ensure_backward_compatibility(regional_json)
            
            # 验证输出格式
            if validate_output:
                validation_result = self.schema_validator.validate_regional_data(regional_json)
                if not validation_result.is_valid:
                    logger.error(f"区域级数据验证失败: {validation_result.errors}")
                    raise SerializationException(f"数据验证失败: {validation_result.errors}")
                
                if validation_result.warnings:
                    logger.warning(f"区域级数据验证警告: {validation_result.warnings}")
            
            # 保存到数据库
            await self._save_regional_data(batch_code, regional_json, integrated_data)
            
            logger.info(f"区域级数据序列化完成: {batch_code}")
            return regional_json
            
        except Exception as e:
            logger.error(f"序列化区域级数据失败: {batch_code}, 错误: {str(e)}")
            raise SerializationException(f"序列化区域级数据失败: {str(e)}")
    
    async def serialize_school_data(
        self, 
        batch_code: str, 
        school_id: str,
        include_regional_comparison: bool = True,
        force_recalculate: bool = False,
        validate_output: bool = True
    ) -> Dict[str, Any]:
        """
        序列化学校级统计数据
        
        Args:
            batch_code: 批次代码
            school_id: 学校ID
            include_regional_comparison: 是否包含区域对比数据
            force_recalculate: 是否强制重新计算
            validate_output: 是否验证输出格式
            
        Returns:
            学校级JSON数据
        """
        logger.info(f"开始序列化学校级数据: {batch_code}, {school_id}")
        
        try:
            # 检查是否已有缓存数据
            if not force_recalculate:
                cached_data = await self._get_cached_school_data(batch_code, school_id)
                if cached_data:
                    logger.info(f"返回缓存的学校级数据: {batch_code}, {school_id}")
                    return cached_data
            
            # 收集学校统计数据
            school_data = await self._collect_school_statistics(batch_code, school_id)
            
            # 收集区域对比数据
            regional_data = None
            if include_regional_comparison:
                regional_data = await self._get_regional_data_for_comparison(batch_code)
            
            # 序列化为JSON格式
            school_json = self.school_serializer.serialize(school_data, regional_data)
            
            # 添加版本信息
            school_json = self.version_manager.ensure_backward_compatibility(school_json)
            
            # 验证输出格式
            if validate_output:
                validation_result = self.schema_validator.validate_school_data(school_json)
                if not validation_result.is_valid:
                    logger.error(f"学校级数据验证失败: {validation_result.errors}")
                    raise SerializationException(f"数据验证失败: {validation_result.errors}")
                
                if validation_result.warnings:
                    logger.warning(f"学校级数据验证警告: {validation_result.warnings}")
            
            # 保存到数据库
            await self._save_school_data(batch_code, school_id, school_json, school_data)
            
            logger.info(f"学校级数据序列化完成: {batch_code}, {school_id}")
            return school_json
            
        except Exception as e:
            logger.error(f"序列化学校级数据失败: {batch_code}, {school_id}, 错误: {str(e)}")
            raise SerializationException(f"序列化学校级数据失败: {str(e)}")
    
    async def serialize_all_schools_data(
        self, 
        batch_code: str,
        parallel_processing: bool = True,
        validate_consistency: bool = True
    ) -> Dict[str, Any]:
        """
        序列化批次中所有学校的数据
        
        Args:
            batch_code: 批次代码
            parallel_processing: 是否并行处理
            validate_consistency: 是否验证数据一致性
            
        Returns:
            包含所有学校数据的字典
        """
        logger.info(f"开始序列化批次所有学校数据: {batch_code}")
        
        try:
            # 获取批次中的所有学校
            school_list = await self._get_batch_school_list(batch_code)
            
            if not school_list:
                logger.warning(f"批次 {batch_code} 中没有找到学校数据")
                return {'batch_code': batch_code, 'schools': []}
            
            # 先序列化区域数据
            regional_data = await self.serialize_regional_data(batch_code)
            
            # 序列化学校数据
            schools_data = []
            
            if parallel_processing:
                # TODO: 实现并行处理
                # 目前先用串行处理
                for school_info in school_list:
                    school_json = await self.serialize_school_data(
                        batch_code, 
                        school_info['school_id'],
                        include_regional_comparison=True
                    )
                    schools_data.append(school_json)
            else:
                for school_info in school_list:
                    school_json = await self.serialize_school_data(
                        batch_code, 
                        school_info['school_id'],
                        include_regional_comparison=True
                    )
                    schools_data.append(school_json)
            
            # 验证数据一致性
            if validate_consistency:
                consistency_result = self.schema_validator.validate_data_consistency(
                    regional_data, schools_data
                )
                if not consistency_result.is_valid:
                    logger.error(f"数据一致性验证失败: {consistency_result.errors}")
                    raise SerializationException(f"数据一致性验证失败: {consistency_result.errors}")
                
                if consistency_result.warnings:
                    logger.warning(f"数据一致性验证警告: {consistency_result.warnings}")
            
            result = {
                'batch_code': batch_code,
                'generated_at': datetime.utcnow().isoformat(),
                'total_schools': len(schools_data),
                'regional_data': regional_data,
                'schools_data': schools_data
            }
            
            logger.info(f"批次所有学校数据序列化完成: {batch_code}, 学校数量: {len(schools_data)}")
            return result
            
        except Exception as e:
            logger.error(f"序列化批次所有学校数据失败: {batch_code}, 错误: {str(e)}")
            raise SerializationException(f"序列化批次所有学校数据失败: {str(e)}")
    
    async def get_radar_chart_data(
        self, 
        batch_code: str, 
        school_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取雷达图专用数据
        
        Args:
            batch_code: 批次代码
            school_id: 学校ID（如果指定则返回学校级数据）
            
        Returns:
            雷达图数据
        """
        logger.info(f"获取雷达图数据: {batch_code}, school_id={school_id}")
        
        try:
            if school_id:
                # 获取学校级雷达图数据
                school_data = await self.serialize_school_data(batch_code, school_id)
                return school_data.get('radar_chart_data', {})
            else:
                # 获取区域级雷达图数据
                regional_data = await self.serialize_regional_data(batch_code)
                return regional_data.get('radar_chart_data', {})
                
        except Exception as e:
            logger.error(f"获取雷达图数据失败: {batch_code}, school_id={school_id}, 错误: {str(e)}")
            raise SerializationException(f"获取雷达图数据失败: {str(e)}")
    
    async def validate_json_data(
        self, 
        json_data: Dict[str, Any], 
        data_type: str
    ) -> ValidationResult:
        """
        验证JSON数据格式
        
        Args:
            json_data: 待验证的JSON数据
            data_type: 数据类型（'regional' 或 'school'）
            
        Returns:
            验证结果
        """
        logger.debug(f"验证JSON数据格式: {data_type}")
        
        if data_type == 'regional':
            return self.schema_validator.validate_regional_data(json_data)
        elif data_type == 'school':
            return self.schema_validator.validate_school_data(json_data)
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")
    
    async def _get_cached_regional_data(self, batch_code: str) -> Optional[Dict[str, Any]]:
        """获取缓存的区域级数据"""
        try:
            aggregation = await self.aggregation_repo.get_by_batch_code_and_level(
                batch_code, AggregationLevel.REGIONAL
            )
            
            if aggregation and aggregation.calculation_status == CalculationStatus.COMPLETED:
                return aggregation.statistics_data
            
        except Exception as e:
            logger.debug(f"获取缓存区域数据失败: {str(e)}")
        
        return None
    
    async def _get_cached_school_data(self, batch_code: str, school_id: str) -> Optional[Dict[str, Any]]:
        """获取缓存的学校级数据"""
        try:
            aggregation = await self.aggregation_repo.get_by_batch_school(batch_code, school_id)
            
            if aggregation and aggregation.calculation_status == CalculationStatus.COMPLETED:
                return aggregation.statistics_data
            
        except Exception as e:
            logger.debug(f"获取缓存学校数据失败: {str(e)}")
        
        return None
    
    async def _collect_school_statistics(self, batch_code: str, school_id: str) -> Dict[str, Any]:
        """收集学校统计数据"""
        # 使用数据集成器收集学校级数据
        # 这里需要修改data_integrator以支持学校级数据收集
        school_data = await self.data_integrator.collect_school_statistics(batch_code, school_id)
        return school_data
    
    async def _get_regional_data_for_comparison(self, batch_code: str) -> Optional[Dict[str, Any]]:
        """获取用于对比的区域数据"""
        try:
            # 先尝试从缓存获取
            cached_data = await self._get_cached_regional_data(batch_code)
            if cached_data:
                return cached_data
            
            # 如果没有缓存，重新序列化
            return await self.serialize_regional_data(batch_code, validate_output=False)
            
        except Exception as e:
            logger.warning(f"获取区域对比数据失败: {str(e)}")
            return None
    
    async def _get_batch_school_list(self, batch_code: str) -> List[Dict[str, Any]]:
        """获取批次中的学校列表"""
        # 从数据库获取学校列表
        school_aggregations = await self.aggregation_repo.get_schools_by_batch_code(batch_code)
        
        school_list = []
        for agg in school_aggregations:
            if agg.school_id:
                school_list.append({
                    'school_id': agg.school_id,
                    'school_name': agg.school_name or agg.school_id
                })
        
        return school_list
    
    async def _save_regional_data(
        self, 
        batch_code: str, 
        regional_json: Dict[str, Any],
        integrated_data: Dict[str, Any]
    ):
        """保存区域级数据到数据库"""
        try:
            batch_info = integrated_data.get('batch_info', {})
            
            # 更新或创建区域级汇聚记录
            await self.aggregation_repo.create_or_update(
                batch_code=batch_code,
                aggregation_level=AggregationLevel.REGIONAL,
                statistics_data=regional_json,
                total_students=batch_info.get('total_students', 0),
                total_schools=batch_info.get('total_schools', 0),
                calculation_status=CalculationStatus.COMPLETED
            )
            
            logger.debug(f"区域级数据已保存到数据库: {batch_code}")
            
        except Exception as e:
            logger.error(f"保存区域级数据失败: {str(e)}")
            # 不抛出异常，因为数据已经成功序列化
    
    async def _save_school_data(
        self, 
        batch_code: str, 
        school_id: str,
        school_json: Dict[str, Any],
        school_data: Dict[str, Any]
    ):
        """保存学校级数据到数据库"""
        try:
            school_info = school_data.get('school_info', {})
            
            # 更新或创建学校级汇聚记录
            await self.aggregation_repo.create_or_update(
                batch_code=batch_code,
                aggregation_level=AggregationLevel.SCHOOL,
                school_id=school_id,
                school_name=school_info.get('school_name', school_id),
                statistics_data=school_json,
                total_students=school_info.get('total_students', 0),
                calculation_status=CalculationStatus.COMPLETED
            )
            
            logger.debug(f"学校级数据已保存到数据库: {batch_code}, {school_id}")
            
        except Exception as e:
            logger.error(f"保存学校级数据失败: {str(e)}")
            # 不抛出异常，因为数据已经成功序列化