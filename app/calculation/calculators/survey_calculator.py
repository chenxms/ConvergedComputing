# 问卷数据处理计算器
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional
from ..engine import CalculationEngine, get_calculation_engine
from ..survey.scale_config import ScaleConfigManager, SAMPLE_SURVEY_DIMENSIONS
from ..survey.survey_strategies import (
    ScaleTransformationStrategy,
    FrequencyAnalysisStrategy,
    DimensionAggregationStrategy,
    SurveyQualityStrategy
)

logger = logging.getLogger(__name__)


class SurveyCalculator:
    """问卷数据处理计算器
    
    集成量表转换、频率分析、维度汇总、质量检查等功能，
    提供完整的问卷数据处理管道。
    """
    
    def __init__(self):
        self.calculation_engine = get_calculation_engine()
        self.scale_manager = ScaleConfigManager()
        self._register_survey_strategies()
    
    def _register_survey_strategies(self):
        """注册问卷相关策略到计算引擎"""
        strategies = {
            'scale_transformation': ScaleTransformationStrategy(),
            'frequency_analysis': FrequencyAnalysisStrategy(),
            'dimension_aggregation': DimensionAggregationStrategy(),
            'survey_quality': SurveyQualityStrategy()
        }
        
        for name, strategy in strategies.items():
            if name not in self.calculation_engine.get_registered_strategies():
                self.calculation_engine.register_strategy(name, strategy)
                logger.info(f"已注册问卷策略: {name}")
    
    def process_survey_data(self, 
                          data: pd.DataFrame, 
                          survey_config: Dict[str, Any],
                          include_quality_check: bool = True,
                          include_frequencies: bool = True,
                          include_dimensions: bool = True) -> Dict[str, Any]:
        """处理问卷数据的完整管道
        
        Args:
            data: 原始问卷响应数据
            survey_config: 问卷配置，包含维度、量表配置等
            include_quality_check: 是否包含质量检查
            include_frequencies: 是否包含频率分析
            include_dimensions: 是否包含维度分析
            
        Returns:
            包含所有分析结果的字典
        """
        if data.empty:
            raise ValueError("数据集为空")
        
        logger.info(f"开始处理问卷数据，样本量: {len(data)}")
        
        results = {
            'processing_metadata': {
                'total_responses': len(data),
                'total_questions': len([col for col in data.columns if not col.startswith('_')]),
                'processing_steps': [],
                'config_version': survey_config.get('version', '1.0')
            }
        }
        
        try:
            # 1. 数据质量检查
            if include_quality_check:
                logger.info("执行数据质量检查...")
                quality_result = self.calculation_engine.calculate(
                    'survey_quality', data, survey_config
                )
                results['quality_analysis'] = quality_result
                results['processing_metadata']['processing_steps'].append('quality_check')
                
                # 记录质量指标
                validity_rate = quality_result.get('quality_summary', {}).get('validity_rate', 0)
                logger.info(f"数据质量检查完成，有效性: {validity_rate:.2%}")
            
            # 2. 量表转换
            logger.info("执行量表转换...")
            transformation_result = self.calculation_engine.calculate(
                'scale_transformation', data, survey_config
            )
            results['scale_transformation'] = transformation_result
            results['processing_metadata']['processing_steps'].append('scale_transformation')
            
            # 记录转换统计
            transformation_summary = transformation_result.get('transformation_summary', {})
            logger.info(f"量表转换完成，转换了 {len(transformation_summary)} 个题目")
            
            # 3. 频率分析
            if include_frequencies:
                logger.info("执行频率分析...")
                
                # 准备题目列表
                questions = []
                dimensions = survey_config.get('dimensions', {})
                for dim_config in dimensions.values():
                    questions.extend(dim_config.get('forward_questions', []))
                    questions.extend(dim_config.get('reverse_questions', []))
                
                if not questions:
                    questions = [col for col in data.columns 
                               if not col.endswith('_transformed') and not col.endswith('_score')]
                
                freq_config = survey_config.copy()
                freq_config['questions'] = questions
                
                frequency_result = self.calculation_engine.calculate(
                    'frequency_analysis', data, freq_config
                )
                results['frequency_analysis'] = frequency_result
                results['processing_metadata']['processing_steps'].append('frequency_analysis')
                
                # 记录频率统计
                analyzed_questions = len(frequency_result.get('question_frequencies', {}))
                logger.info(f"频率分析完成，分析了 {analyzed_questions} 个题目")
            
            # 4. 维度汇总分析
            if include_dimensions:
                logger.info("执行维度汇总分析...")
                dimension_result = self.calculation_engine.calculate(
                    'dimension_aggregation', data, survey_config
                )
                results['dimension_analysis'] = dimension_result
                results['processing_metadata']['processing_steps'].append('dimension_aggregation')
                
                # 记录维度统计
                analyzed_dimensions = len(dimension_result.get('dimension_statistics', {}))
                logger.info(f"维度分析完成，分析了 {analyzed_dimensions} 个维度")
            
            # 5. 生成综合报告
            results['summary_report'] = self._generate_summary_report(results)
            results['processing_metadata']['processing_steps'].append('summary_generation')
            
            logger.info("问卷数据处理完成")
            return results
            
        except Exception as e:
            logger.error(f"问卷数据处理失败: {str(e)}")
            raise
    
    def transform_likert_scale(self, 
                             data: pd.DataFrame,
                             question_configs: Dict[str, str],
                             scale_type: str = '5point') -> pd.DataFrame:
        """转换李克特量表数据
        
        Args:
            data: 原始数据
            question_configs: 题目配置 {question_id: 'forward'/'reverse'}
            scale_type: 量表类型，默认5点量表
            
        Returns:
            转换后的数据
        """
        if scale_type not in ['5point', '7point']:
            raise ValueError(f"不支持的量表类型: {scale_type}")
        
        # 构建简化的survey_config
        dimensions = {'default': {
            'forward_questions': [q for q, t in question_configs.items() if t == 'forward'],
            'reverse_questions': [q for q, t in question_configs.items() if t == 'reverse']
        }}
        
        survey_config = {
            'dimensions': dimensions,
            'scale_config': self.scale_manager.scale_configs
        }
        
        # 执行转换
        result = self.calculation_engine.calculate('scale_transformation', data, survey_config)
        
        # 返回转换后的数据DataFrame
        transformed_data = result.get('transformed_data', {})
        return pd.DataFrame(transformed_data)
    
    def calculate_dimension_scores(self,
                                 data: pd.DataFrame,
                                 dimension_config: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """计算维度得分
        
        Args:
            data: 已转换的问卷数据
            dimension_config: 维度配置
            
        Returns:
            包含维度得分的DataFrame
        """
        survey_config = {
            'dimensions': dimension_config,
            'scale_config': self.scale_manager.scale_configs
        }
        
        result = self.calculation_engine.calculate('dimension_aggregation', data, survey_config)
        
        # 提取维度得分
        dimension_stats = result.get('dimension_statistics', {})
        dimension_scores = {}
        
        for dim_name, stats in dimension_stats.items():
            dimension_scores[f'{dim_name}_mean'] = stats.get('mean')
            dimension_scores[f'{dim_name}_count'] = stats.get('count')
        
        return pd.DataFrame([dimension_scores])
    
    def analyze_response_quality(self,
                               data: pd.DataFrame,
                               quality_rules: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """分析响应质量
        
        Args:
            data: 问卷响应数据
            quality_rules: 质量检查规则
            
        Returns:
            质量分析结果
        """
        config = {
            'quality_rules': quality_rules or self.scale_manager.quality_rules,
            'questions': [col for col in data.columns 
                         if not col.endswith('_transformed') and not col.endswith('_score')]
        }
        
        return self.calculation_engine.calculate('survey_quality', data, config)
    
    def get_frequency_distribution(self,
                                 data: pd.DataFrame,
                                 questions: Optional[List[str]] = None) -> Dict[str, Any]:
        """获取选项频率分布
        
        Args:
            data: 问卷数据
            questions: 要分析的题目列表，None表示分析所有题目
            
        Returns:
            频率分布结果
        """
        if questions is None:
            questions = [col for col in data.columns 
                        if not col.endswith('_transformed') and not col.endswith('_score')]
        
        config = {'questions': questions}
        
        return self.calculation_engine.calculate('frequency_analysis', data, config)
    
    def create_survey_config_from_template(self,
                                         survey_id: str,
                                         survey_name: str,
                                         template: str = 'curiosity_observation') -> Dict[str, Any]:
        """从模板创建问卷配置
        
        Args:
            survey_id: 问卷ID
            survey_name: 问卷名称
            template: 模板类型
            
        Returns:
            问卷配置字典
        """
        if template == 'curiosity_observation':
            dimensions = SAMPLE_SURVEY_DIMENSIONS
        else:
            raise ValueError(f"未知的模板类型: {template}")
        
        survey_config = self.scale_manager.create_survey_config(
            survey_id, survey_name, dimensions
        )
        
        return self.scale_manager.export_config(survey_id)
    
    def validate_survey_data(self,
                           data: pd.DataFrame,
                           survey_config: Dict[str, Any]) -> Dict[str, Any]:
        """验证问卷数据
        
        Args:
            data: 问卷数据
            survey_config: 问卷配置
            
        Returns:
            验证结果
        """
        validation_results = {}
        
        # 使用各策略的验证方法
        strategies = [
            ('scale_transformation', ScaleTransformationStrategy()),
            ('frequency_analysis', FrequencyAnalysisStrategy()),
            ('dimension_aggregation', DimensionAggregationStrategy()),
            ('survey_quality', SurveyQualityStrategy())
        ]
        
        for strategy_name, strategy in strategies:
            try:
                validation_result = strategy.validate_input(data, survey_config)
                validation_results[strategy_name] = validation_result
            except Exception as e:
                validation_results[strategy_name] = {
                    'is_valid': False,
                    'errors': [str(e)],
                    'warnings': [],
                    'stats': {}
                }
        
        # 汇总验证结果
        overall_valid = all(result.get('is_valid', False) for result in validation_results.values())
        all_errors = []
        all_warnings = []
        
        for result in validation_results.values():
            all_errors.extend(result.get('errors', []))
            all_warnings.extend(result.get('warnings', []))
        
        return {
            'overall_valid': overall_valid,
            'all_errors': all_errors,
            'all_warnings': all_warnings,
            'strategy_validations': validation_results,
            'data_summary': {
                'total_rows': len(data),
                'total_columns': len(data.columns),
                'memory_usage': data.memory_usage(deep=True).sum()
            }
        }
    
    def _generate_summary_report(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """生成综合分析报告"""
        report = {
            'data_overview': {},
            'quality_summary': {},
            'transformation_summary': {},
            'dimension_summary': {},
            'key_findings': [],
            'recommendations': []
        }
        
        # 数据概览
        metadata = results.get('processing_metadata', {})
        report['data_overview'] = {
            'total_responses': metadata.get('total_responses', 0),
            'total_questions': metadata.get('total_questions', 0),
            'processing_steps_completed': len(metadata.get('processing_steps', []))
        }
        
        # 质量摘要
        quality_analysis = results.get('quality_analysis', {})
        if quality_analysis:
            quality_summary = quality_analysis.get('quality_summary', {})
            report['quality_summary'] = {
                'validity_rate': quality_summary.get('validity_rate', 0),
                'total_quality_issues': quality_summary.get('quality_issues', 0),
                'key_quality_flags': [
                    flag for flag, data in quality_analysis.get('quality_flags', {}).items()
                    if isinstance(data, dict) and data.get('count', 0) > 0
                ]
            }
        
        # 转换摘要
        transformation = results.get('scale_transformation', {})
        if transformation:
            transformation_summary = transformation.get('transformation_summary', {})
            report['transformation_summary'] = {
                'questions_transformed': len(transformation_summary),
                'forward_questions': len([q for q, info in transformation_summary.items() 
                                        if info.get('type') == 'forward']),
                'reverse_questions': len([q for q, info in transformation_summary.items() 
                                        if info.get('type') == 'reverse'])
            }
        
        # 维度摘要
        dimension_analysis = results.get('dimension_analysis', {})
        if dimension_analysis:
            dimension_stats = dimension_analysis.get('dimension_statistics', {})
            if dimension_stats:
                means = [stats.get('mean', 0) for stats in dimension_stats.values()]
                report['dimension_summary'] = {
                    'total_dimensions': len(dimension_stats),
                    'highest_scoring_dimension': max(dimension_stats.keys(), 
                                                   key=lambda k: dimension_stats[k].get('mean', 0)),
                    'lowest_scoring_dimension': min(dimension_stats.keys(), 
                                                  key=lambda k: dimension_stats[k].get('mean', 0)),
                    'overall_mean_score': np.mean(means) if means else 0,
                    'dimension_score_range': max(means) - min(means) if means else 0
                }
        
        # 关键发现
        findings = []
        
        # 质量相关发现
        if quality_analysis:
            validity_rate = quality_analysis.get('quality_summary', {}).get('validity_rate', 0)
            if validity_rate < 0.8:
                findings.append(f"数据质量需要关注，有效性仅为 {validity_rate:.1%}")
            elif validity_rate > 0.95:
                findings.append(f"数据质量优良，有效性达到 {validity_rate:.1%}")
        
        # 维度相关发现
        if dimension_analysis:
            correlations = dimension_analysis.get('dimension_correlations', {})
            strong_correlations = [k for k, v in correlations.items() 
                                 if v.get('strength') == 'strong']
            if strong_correlations:
                findings.append(f"发现 {len(strong_correlations)} 对维度间存在强相关性")
        
        report['key_findings'] = findings
        
        # 建议
        recommendations = []
        
        if quality_analysis:
            quality_recommendations = quality_analysis.get('recommendations', [])
            recommendations.extend(quality_recommendations)
        
        if not recommendations:
            recommendations.append("数据处理完成，结果可用于进一步分析")
        
        report['recommendations'] = recommendations
        
        return report
    
    def export_results_to_dict(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """导出结果为标准化字典格式"""
        export_data = {
            'survey_analysis_version': '1.0',
            'processing_timestamp': pd.Timestamp.now().isoformat(),
            'data_summary': results.get('processing_metadata', {}),
            'quality_analysis': results.get('quality_analysis', {}),
            'scale_transformation': results.get('scale_transformation', {}),
            'frequency_analysis': results.get('frequency_analysis', {}),
            'dimension_analysis': results.get('dimension_analysis', {}),
            'summary_report': results.get('summary_report', {})
        }
        
        return export_data
    
    def get_calculator_info(self) -> Dict[str, Any]:
        """获取计算器信息"""
        return {
            'name': 'SurveyCalculator',
            'version': '1.0',
            'description': '问卷数据处理计算器：集成量表转换、频率分析、维度汇总、质量检查等功能',
            'supported_strategies': self.calculation_engine.get_registered_strategies(),
            'supported_scale_types': list(self.scale_manager.scale_configs.keys()),
            'features': [
                'likert_scale_transformation',
                'frequency_analysis', 
                'dimension_aggregation',
                'quality_assessment',
                'comprehensive_reporting'
            ]
        }