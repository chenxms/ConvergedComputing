# 问卷数据处理策略
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Tuple
from ..engine import StatisticalStrategy
from .scale_config import ScaleConfigManager, SCALE_TYPES, QUALITY_RULES

logger = logging.getLogger(__name__)


class ScaleTransformationStrategy(StatisticalStrategy):
    """量表转换计算策略"""
    
    def __init__(self):
        self.scale_manager = ScaleConfigManager()
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行量表转换计算"""
        dimensions = config.get('dimensions', {})
        scale_config = config.get('scale_config', SCALE_TYPES)
        
        if not dimensions:
            raise ValueError("未提供维度配置")
        
        results = {
            'transformed_data': {},
            'transformation_summary': {},
            'dimension_scores': {}
        }
        
        transformed_data = data.copy()
        transformation_summary = {}
        
        # 按维度处理题目
        for dimension_name, dimension_config in dimensions.items():
            dim_transformed_cols = []
            
            # 处理正向题目
            forward_questions = dimension_config.get('forward_questions', [])
            for question in forward_questions:
                if question in data.columns:
                    transformed_col = f'{question}_transformed'
                    transformed_data[transformed_col] = data[question].map(
                        scale_config['forward']
                    ).astype('Int64')  # 使用nullable integer
                    dim_transformed_cols.append(transformed_col)
                    
                    # 记录转换统计
                    original_stats = data[question].value_counts().to_dict()
                    transformed_stats = transformed_data[transformed_col].value_counts().to_dict()
                    transformation_summary[question] = {
                        'type': 'forward',
                        'original_distribution': original_stats,
                        'transformed_distribution': transformed_stats,
                        'valid_count': transformed_data[transformed_col].notna().sum()
                    }
            
            # 处理反向题目
            reverse_questions = dimension_config.get('reverse_questions', [])
            for question in reverse_questions:
                if question in data.columns:
                    transformed_col = f'{question}_transformed'
                    transformed_data[transformed_col] = data[question].map(
                        scale_config['reverse']
                    ).astype('Int64')  # 使用nullable integer
                    dim_transformed_cols.append(transformed_col)
                    
                    # 记录转换统计
                    original_stats = data[question].value_counts().to_dict()
                    transformed_stats = transformed_data[transformed_col].value_counts().to_dict()
                    transformation_summary[question] = {
                        'type': 'reverse',
                        'original_distribution': original_stats,
                        'transformed_distribution': transformed_stats,
                        'valid_count': transformed_data[transformed_col].notna().sum()
                    }
            
            # 计算维度得分
            if dim_transformed_cols:
                dimension_scores = transformed_data[dim_transformed_cols].mean(axis=1, skipna=True)
                dimension_scores.name = f'{dimension_name}_score'
                transformed_data[dimension_scores.name] = dimension_scores
                
                # 维度统计
                dimension_weight = dimension_config.get('weight', 1.0)
                weighted_scores = dimension_scores * dimension_weight
                
                results['dimension_scores'][dimension_name] = {
                    'mean': float(dimension_scores.mean()),
                    'std': float(dimension_scores.std()),
                    'median': float(dimension_scores.median()),
                    'min': float(dimension_scores.min()),
                    'max': float(dimension_scores.max()),
                    'count': int(dimension_scores.notna().sum()),
                    'weight': dimension_weight,
                    'weighted_mean': float(weighted_scores.mean()),
                    'questions_count': len(dim_transformed_cols)
                }
        
        # 提取转换后的数据（仅包含转换列）
        transformed_cols = [col for col in transformed_data.columns if col.endswith('_transformed') or col.endswith('_score')]
        results['transformed_data'] = transformed_data[transformed_cols].to_dict()
        results['transformation_summary'] = transformation_summary
        
        return results
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        if data.empty:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空")
            return validation_result
        
        dimensions = config.get('dimensions', {})
        if not dimensions:
            validation_result['is_valid'] = False
            validation_result['errors'].append("未提供维度配置")
            return validation_result
        
        # 检查问卷题目是否存在于数据中
        all_questions = set()
        for dimension_config in dimensions.values():
            all_questions.update(dimension_config.get('forward_questions', []))
            all_questions.update(dimension_config.get('reverse_questions', []))
        
        missing_questions = all_questions - set(data.columns)
        if missing_questions:
            validation_result['warnings'].append(f"数据中缺少题目: {list(missing_questions)}")
        
        existing_questions = all_questions & set(data.columns)
        validation_result['stats']['total_questions'] = len(all_questions)
        validation_result['stats']['existing_questions'] = len(existing_questions)
        validation_result['stats']['total_responses'] = len(data)
        
        # 检查数据值范围（假设5级量表）
        for question in existing_questions:
            values = data[question].dropna()
            if len(values) > 0:
                invalid_values = values[(values < 1) | (values > 5)]
                if len(invalid_values) > 0:
                    validation_result['warnings'].append(
                        f"题目 {question} 包含 {len(invalid_values)} 个超出范围(1-5)的值"
                    )
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'ScaleTransformation',
            'version': '1.0',
            'description': '问卷量表转换：支持正向/反向量表的分值映射转换',
            'forward_mapping': 'identity_mapping',
            'reverse_mapping': 'reverse_5point_scale',
            'supports': 'likert_scale_5_point'
        }


class FrequencyAnalysisStrategy(StatisticalStrategy):
    """选项频率分析策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行频率分析计算"""
        questions = config.get('questions', [])
        if not questions:
            # 如果没有指定题目，分析所有非转换列
            questions = [col for col in data.columns 
                        if not col.endswith('_transformed') and not col.endswith('_score')]
        
        results = {
            'question_frequencies': {},
            'overall_summary': {},
            'missing_data_analysis': {}
        }
        
        total_responses = len(data)
        overall_response_counts = {}
        overall_missing_count = 0
        
        for question in questions:
            if question not in data.columns:
                logger.warning(f"题目 {question} 不存在于数据中")
                continue
            
            question_data = data[question]
            
            # 计算频率（包含缺失值）
            value_counts = question_data.value_counts(dropna=False)
            total_question_responses = len(question_data)
            missing_count = question_data.isna().sum()
            valid_count = total_question_responses - missing_count
            
            # 计算百分比（基于总响应数）
            percentages = (value_counts / total_question_responses).round(4)
            
            # 计算有效百分比（基于有效响应数）
            valid_percentages = {}
            if valid_count > 0:
                valid_value_counts = question_data.value_counts(dropna=True)
                valid_percentages = (valid_value_counts / valid_count).round(4).to_dict()
            
            # 统计信息
            frequency_data = {
                'frequencies': value_counts.to_dict(),
                'percentages': percentages.to_dict(),
                'valid_percentages': valid_percentages,
                'total_responses': int(total_question_responses),
                'valid_responses': int(valid_count),
                'missing_count': int(missing_count),
                'missing_rate': float(missing_count / total_question_responses),
                'response_rate': float(valid_count / total_question_responses)
            }
            
            # 描述统计（对数值型选项）
            if valid_count > 0:
                numeric_data = pd.to_numeric(question_data, errors='coerce').dropna()
                if len(numeric_data) > 0:
                    frequency_data['statistics'] = {
                        'mean': float(numeric_data.mean()),
                        'std': float(numeric_data.std()),
                        'median': float(numeric_data.median()),
                        'mode': float(numeric_data.mode().iloc[0]) if not numeric_data.mode().empty else None,
                        'min': float(numeric_data.min()),
                        'max': float(numeric_data.max())
                    }
            
            results['question_frequencies'][question] = frequency_data
            
            # 汇总统计
            for value, count in value_counts.items():
                if pd.notna(value):  # 排除缺失值
                    overall_response_counts[value] = overall_response_counts.get(value, 0) + count
            
            overall_missing_count += missing_count
        
        # 整体汇总
        total_possible_responses = total_responses * len([q for q in questions if q in data.columns])
        results['overall_summary'] = {
            'total_questions': len([q for q in questions if q in data.columns]),
            'total_possible_responses': int(total_possible_responses),
            'total_valid_responses': int(total_possible_responses - overall_missing_count),
            'total_missing_responses': int(overall_missing_count),
            'overall_response_rate': float((total_possible_responses - overall_missing_count) / total_possible_responses),
            'option_distribution': overall_response_counts
        }
        
        # 缺失数据分析
        if overall_missing_count > 0:
            missing_by_question = {}
            for question in questions:
                if question in data.columns:
                    missing_count = data[question].isna().sum()
                    if missing_count > 0:
                        missing_by_question[question] = {
                            'missing_count': int(missing_count),
                            'missing_rate': float(missing_count / total_responses)
                        }
            
            results['missing_data_analysis'] = {
                'questions_with_missing': missing_by_question,
                'total_missing_responses': int(overall_missing_count),
                'avg_missing_rate': float(overall_missing_count / total_possible_responses)
            }
        
        return results
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        if data.empty:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空")
            return validation_result
        
        questions = config.get('questions', [])
        if questions:
            missing_questions = [q for q in questions if q not in data.columns]
            if missing_questions:
                validation_result['warnings'].append(f"数据中缺少题目: {missing_questions}")
        
        validation_result['stats']['total_records'] = len(data)
        validation_result['stats']['total_columns'] = len(data.columns)
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'FrequencyAnalysis',
            'version': '1.0', 
            'description': '选项频率统计分析：计算各选项的频次、百分比分布和描述统计',
            'method': 'value_counts_with_percentages',
            'handles_missing': 'true'
        }


class DimensionAggregationStrategy(StatisticalStrategy):
    """维度汇总计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行维度汇总计算"""
        dimensions = config.get('dimensions', {})
        if not dimensions:
            raise ValueError("未提供维度配置")
        
        # 首先进行量表转换
        transformation_strategy = ScaleTransformationStrategy()
        transformation_result = transformation_strategy.calculate(data, config)
        
        results = {
            'dimension_statistics': {},
            'dimension_correlations': {},
            'overall_survey_metrics': {}
        }
        
        # 获取维度得分数据
        dimension_scores = {}
        for dimension_name, dimension_config in dimensions.items():
            score_col = f'{dimension_name}_score'
            
            # 从转换结果或数据中获取维度得分
            if score_col in transformation_result.get('transformed_data', {}):
                scores = pd.Series(transformation_result['transformed_data'][score_col])
            else:
                # 手动计算维度得分
                all_questions = (dimension_config.get('forward_questions', []) + 
                               dimension_config.get('reverse_questions', []))
                question_scores = []
                
                for question in all_questions:
                    transformed_col = f'{question}_transformed'
                    if transformed_col in transformation_result.get('transformed_data', {}):
                        question_scores.append(pd.Series(transformation_result['transformed_data'][transformed_col]))
                
                if question_scores:
                    scores_df = pd.DataFrame(question_scores).T
                    scores = scores_df.mean(axis=1, skipna=True)
                else:
                    continue
            
            scores = scores.dropna()
            if len(scores) == 0:
                continue
                
            dimension_scores[dimension_name] = scores
            
            # 计算维度统计
            weight = dimension_config.get('weight', 1.0)
            weighted_scores = scores * weight
            
            # 基础统计
            stats = {
                'count': int(len(scores)),
                'mean': float(scores.mean()),
                'weighted_mean': float(weighted_scores.mean()),
                'std': float(scores.std()),
                'median': float(scores.median()),
                'min': float(scores.min()),
                'max': float(scores.max()),
                'range': float(scores.max() - scores.min()),
                'weight': weight,
                'questions_count': len(dimension_config.get('forward_questions', []) + 
                                     dimension_config.get('reverse_questions', []))
            }
            
            # 百分位数
            for p in [25, 50, 75, 90]:
                percentile_value = np.percentile(scores, p)
                stats[f'P{p}'] = float(percentile_value)
            
            # 分布统计
            stats['skewness'] = float(scores.skew()) if len(scores) > 2 else 0.0
            stats['kurtosis'] = float(scores.kurtosis()) if len(scores) > 2 else 0.0
            
            # 得分分布（基于5级量表）
            distribution = {}
            for score_level in [1, 2, 3, 4, 5]:
                count = ((scores >= score_level - 0.5) & (scores < score_level + 0.5)).sum()
                distribution[f'level_{score_level}'] = {
                    'count': int(count),
                    'percentage': float(count / len(scores))
                }
            
            stats['score_distribution'] = distribution
            
            results['dimension_statistics'][dimension_name] = stats
        
        # 维度间相关性分析
        if len(dimension_scores) > 1:
            correlation_matrix = pd.DataFrame(dimension_scores).corr()
            correlations = {}
            
            dimensions_list = list(dimension_scores.keys())
            for i, dim1 in enumerate(dimensions_list):
                for dim2 in dimensions_list[i+1:]:
                    correlation = correlation_matrix.loc[dim1, dim2]
                    if pd.notna(correlation):
                        correlations[f'{dim1}_vs_{dim2}'] = {
                            'correlation': float(correlation),
                            'strength': self._interpret_correlation_strength(abs(correlation))
                        }
            
            results['dimension_correlations'] = correlations
        
        # 整体问卷指标
        all_scores = []
        all_weighted_scores = []
        total_weight = 0
        
        for dimension_name, scores in dimension_scores.items():
            weight = dimensions[dimension_name].get('weight', 1.0)
            all_scores.extend(scores.tolist())
            all_weighted_scores.extend((scores * weight).tolist())
            total_weight += weight
        
        if all_scores:
            results['overall_survey_metrics'] = {
                'total_dimensions': len(dimension_scores),
                'total_responses': len(data),
                'overall_mean': float(np.mean(all_scores)),
                'overall_std': float(np.std(all_scores, ddof=1)),
                'weighted_overall_mean': float(np.mean(all_weighted_scores)),
                'total_weight': total_weight,
                'dimension_balance': len(dimension_scores) / len(dimensions) if dimensions else 0.0
            }
        
        return results
    
    def _interpret_correlation_strength(self, correlation: float) -> str:
        """解释相关性强度"""
        if correlation < 0.3:
            return 'weak'
        elif correlation < 0.7:
            return 'moderate'
        else:
            return 'strong'
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        if data.empty:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空")
            return validation_result
        
        dimensions = config.get('dimensions', {})
        if not dimensions:
            validation_result['is_valid'] = False
            validation_result['errors'].append("未提供维度配置")
            return validation_result
        
        # 检查维度配置的完整性
        for dim_name, dim_config in dimensions.items():
            all_questions = (dim_config.get('forward_questions', []) + 
                           dim_config.get('reverse_questions', []))
            if not all_questions:
                validation_result['warnings'].append(f"维度 {dim_name} 没有配置任何题目")
        
        validation_result['stats']['total_dimensions'] = len(dimensions)
        validation_result['stats']['total_records'] = len(data)
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'DimensionAggregation',
            'version': '1.0',
            'description': '维度汇总统计：计算各维度得分统计、相关性分析和整体问卷指标',
            'aggregation_method': 'mean_with_weights',
            'correlation_method': 'pearson'
        }


class SurveyQualityStrategy(StatisticalStrategy):
    """问卷数据质量检查策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行数据质量检查"""
        quality_rules = config.get('quality_rules', QUALITY_RULES)
        questions = config.get('questions', [])
        
        if not questions:
            # 如果没有指定题目，检查所有非转换列
            questions = [col for col in data.columns 
                        if not col.endswith('_transformed') and not col.endswith('_score')]
        
        results = {
            'quality_summary': {
                'total_responses': len(data),
                'valid_responses': 0,
                'quality_issues': 0
            },
            'quality_flags': {},
            'detailed_analysis': {},
            'recommendations': []
        }
        
        valid_responses = 0
        total_issues = 0
        
        # 1. 完成率检查
        completion_rates = data[questions].notna().mean(axis=1)
        min_completion_rate = quality_rules.get('completion_rate_min', 0.8)
        low_completion = (completion_rates < min_completion_rate).sum()
        
        results['quality_flags']['low_completion'] = {
            'count': int(low_completion),
            'percentage': float(low_completion / len(data)),
            'threshold': min_completion_rate
        }
        total_issues += low_completion
        
        # 2. 连续相同选项检查（直线响应）
        straight_line_max = quality_rules.get('straight_line_max', 10)
        straight_line_responses = self._detect_straight_line_responses(
            data[questions], straight_line_max
        )
        
        results['quality_flags']['straight_line'] = {
            'count': int(straight_line_responses),
            'percentage': float(straight_line_responses / len(data)),
            'threshold': straight_line_max
        }
        total_issues += straight_line_responses
        
        # 3. 无变化响应检查
        variance_threshold = quality_rules.get('variance_threshold', 0.1)
        no_variance_count = self._detect_no_variance_responses(
            data[questions], variance_threshold
        )
        
        results['quality_flags']['no_variance'] = {
            'count': int(no_variance_count),
            'percentage': float(no_variance_count / len(data)),
            'threshold': variance_threshold
        }
        total_issues += no_variance_count
        
        # 4. 响应时间检查（如果有时间数据）
        if 'response_time' in data.columns:
            time_issues = self._check_response_time(data['response_time'], quality_rules)
            results['quality_flags']['response_time'] = time_issues
            total_issues += time_issues.get('too_fast_count', 0) + time_issues.get('too_slow_count', 0)
        
        # 5. 异常响应模式检测
        pattern_issues = self._detect_response_patterns(data[questions])
        results['quality_flags']['response_patterns'] = pattern_issues
        total_issues += sum(pattern_issues.values())
        
        # 计算有效响应数
        valid_responses = len(data) - len(set(
            data.index[completion_rates < min_completion_rate].tolist() +
            self._get_straight_line_indices(data[questions], straight_line_max) +
            self._get_no_variance_indices(data[questions], variance_threshold)
        ))
        
        # 更新汇总信息
        results['quality_summary']['valid_responses'] = valid_responses
        results['quality_summary']['quality_issues'] = total_issues
        results['quality_summary']['validity_rate'] = float(valid_responses / len(data))
        
        # 详细分析
        results['detailed_analysis'] = {
            'completion_analysis': self._analyze_completion_patterns(data[questions]),
            'response_variance_analysis': self._analyze_response_variance(data[questions]),
            'extreme_responses_analysis': self._analyze_extreme_responses(data[questions])
        }
        
        # 生成建议
        results['recommendations'] = self._generate_quality_recommendations(results['quality_flags'])
        
        return results
    
    def _detect_straight_line_responses(self, data: pd.DataFrame, max_consecutive: int) -> int:
        """检测连续相同选项响应"""
        straight_line_count = 0
        
        for index, row in data.iterrows():
            valid_responses = row.dropna()
            if len(valid_responses) < 3:  # 至少需要3个响应才能判断
                continue
            
            consecutive_count = 1
            max_consecutive_in_row = 1
            
            for i in range(1, len(valid_responses)):
                if valid_responses.iloc[i] == valid_responses.iloc[i-1]:
                    consecutive_count += 1
                    max_consecutive_in_row = max(max_consecutive_in_row, consecutive_count)
                else:
                    consecutive_count = 1
            
            if max_consecutive_in_row >= max_consecutive:
                straight_line_count += 1
        
        return straight_line_count
    
    def _get_straight_line_indices(self, data: pd.DataFrame, max_consecutive: int) -> List[int]:
        """获取直线响应的索引列表"""
        straight_line_indices = []
        
        for index, row in data.iterrows():
            valid_responses = row.dropna()
            if len(valid_responses) < 3:
                continue
            
            consecutive_count = 1
            max_consecutive_in_row = 1
            
            for i in range(1, len(valid_responses)):
                if valid_responses.iloc[i] == valid_responses.iloc[i-1]:
                    consecutive_count += 1
                    max_consecutive_in_row = max(max_consecutive_in_row, consecutive_count)
                else:
                    consecutive_count = 1
            
            if max_consecutive_in_row >= max_consecutive:
                straight_line_indices.append(index)
        
        return straight_line_indices
    
    def _detect_no_variance_responses(self, data: pd.DataFrame, variance_threshold: float) -> int:
        """检测无变化响应"""
        no_variance_count = 0
        
        for index, row in data.iterrows():
            valid_responses = row.dropna()
            if len(valid_responses) < 3:
                continue
            
            # 转换为数值类型
            numeric_responses = pd.to_numeric(valid_responses, errors='coerce').dropna()
            if len(numeric_responses) < 3:
                continue
            
            variance = numeric_responses.var()
            if pd.notna(variance) and variance <= variance_threshold:
                no_variance_count += 1
        
        return no_variance_count
    
    def _get_no_variance_indices(self, data: pd.DataFrame, variance_threshold: float) -> List[int]:
        """获取无变化响应的索引列表"""
        no_variance_indices = []
        
        for index, row in data.iterrows():
            valid_responses = row.dropna()
            if len(valid_responses) < 3:
                continue
            
            numeric_responses = pd.to_numeric(valid_responses, errors='coerce').dropna()
            if len(numeric_responses) < 3:
                continue
            
            variance = numeric_responses.var()
            if pd.notna(variance) and variance <= variance_threshold:
                no_variance_indices.append(index)
        
        return no_variance_indices
    
    def _check_response_time(self, response_times: pd.Series, quality_rules: Dict[str, Any]) -> Dict[str, Any]:
        """检查响应时间"""
        min_time = quality_rules.get('response_time_min', 30)
        max_time = quality_rules.get('response_time_max', 1800)
        
        valid_times = response_times.dropna()
        
        too_fast = (valid_times < min_time).sum()
        too_slow = (valid_times > max_time).sum()
        
        return {
            'too_fast_count': int(too_fast),
            'too_fast_percentage': float(too_fast / len(valid_times)) if len(valid_times) > 0 else 0,
            'too_slow_count': int(too_slow),
            'too_slow_percentage': float(too_slow / len(valid_times)) if len(valid_times) > 0 else 0,
            'min_threshold': min_time,
            'max_threshold': max_time,
            'mean_time': float(valid_times.mean()) if len(valid_times) > 0 else None
        }
    
    def _detect_response_patterns(self, data: pd.DataFrame) -> Dict[str, int]:
        """检测异常响应模式"""
        patterns = {
            'alternating_pattern': 0,  # 交替模式 (1,2,1,2,...)
            'ascending_pattern': 0,    # 递增模式 (1,2,3,4,5,...)
            'descending_pattern': 0,   # 递减模式 (5,4,3,2,1,...)
            'extreme_only': 0          # 只选择极值 (只选1和5)
        }
        
        for index, row in data.iterrows():
            valid_responses = row.dropna()
            numeric_responses = pd.to_numeric(valid_responses, errors='coerce').dropna()
            
            if len(numeric_responses) < 4:
                continue
            
            responses_list = numeric_responses.tolist()
            
            # 检测交替模式
            alternating = True
            for i in range(2, len(responses_list)):
                if responses_list[i] != responses_list[i-2]:
                    alternating = False
                    break
            if alternating and len(set(responses_list)) <= 2:
                patterns['alternating_pattern'] += 1
            
            # 检测递增模式
            if responses_list == sorted(responses_list):
                patterns['ascending_pattern'] += 1
            
            # 检测递减模式
            if responses_list == sorted(responses_list, reverse=True):
                patterns['descending_pattern'] += 1
            
            # 检测极值模式
            unique_values = set(responses_list)
            if unique_values.issubset({1, 5}) and len(unique_values) == 2:
                patterns['extreme_only'] += 1
        
        return patterns
    
    def _analyze_completion_patterns(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析完成模式"""
        completion_rates = data.notna().mean(axis=1)
        
        return {
            'mean_completion_rate': float(completion_rates.mean()),
            'std_completion_rate': float(completion_rates.std()),
            'min_completion_rate': float(completion_rates.min()),
            'max_completion_rate': float(completion_rates.max()),
            'full_completion_count': int((completion_rates == 1.0).sum()),
            'partial_completion_count': int(((completion_rates > 0.0) & (completion_rates < 1.0)).sum()),
            'no_completion_count': int((completion_rates == 0.0).sum())
        }
    
    def _analyze_response_variance(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析响应变异性"""
        variances = []
        
        for index, row in data.iterrows():
            numeric_row = pd.to_numeric(row, errors='coerce').dropna()
            if len(numeric_row) >= 3:
                variances.append(numeric_row.var())
        
        if not variances:
            return {'message': 'Insufficient data for variance analysis'}
        
        variances = pd.Series(variances)
        
        return {
            'mean_variance': float(variances.mean()),
            'std_variance': float(variances.std()),
            'median_variance': float(variances.median()),
            'low_variance_count': int((variances <= 0.5).sum()),
            'high_variance_count': int((variances >= 2.0).sum()),
            'zero_variance_count': int((variances == 0.0).sum())
        }
    
    def _analyze_extreme_responses(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析极值响应"""
        extreme_counts = []
        
        for index, row in data.iterrows():
            numeric_row = pd.to_numeric(row, errors='coerce').dropna()
            if len(numeric_row) > 0:
                extreme_count = ((numeric_row == 1) | (numeric_row == 5)).sum()
                extreme_counts.append(extreme_count / len(numeric_row))
        
        if not extreme_counts:
            return {'message': 'Insufficient data for extreme response analysis'}
        
        extreme_rates = pd.Series(extreme_counts)
        
        return {
            'mean_extreme_rate': float(extreme_rates.mean()),
            'std_extreme_rate': float(extreme_rates.std()),
            'high_extreme_users': int((extreme_rates > 0.8).sum()),
            'balanced_users': int(((extreme_rates >= 0.2) & (extreme_rates <= 0.6)).sum()),
            'low_extreme_users': int((extreme_rates < 0.2).sum())
        }
    
    def _generate_quality_recommendations(self, quality_flags: Dict[str, Any]) -> List[str]:
        """生成数据质量建议"""
        recommendations = []
        
        # 完成率建议
        low_completion = quality_flags.get('low_completion', {})
        if low_completion.get('percentage', 0) > 0.1:
            recommendations.append(
                f"发现 {low_completion['percentage']:.1%} 的响应完成率过低，建议检查问卷长度和题目设计"
            )
        
        # 直线响应建议
        straight_line = quality_flags.get('straight_line', {})
        if straight_line.get('percentage', 0) > 0.05:
            recommendations.append(
                f"发现 {straight_line['percentage']:.1%} 的直线响应，建议增加反向题目或注意力检查题"
            )
        
        # 无变化响应建议
        no_variance = quality_flags.get('no_variance', {})
        if no_variance.get('percentage', 0) > 0.05:
            recommendations.append(
                f"发现 {no_variance['percentage']:.1%} 的无变化响应，可能存在敷衍作答情况"
            )
        
        # 响应时间建议
        response_time = quality_flags.get('response_time', {})
        if response_time:
            too_fast_pct = response_time.get('too_fast_percentage', 0)
            too_slow_pct = response_time.get('too_slow_percentage', 0)
            
            if too_fast_pct > 0.05:
                recommendations.append(
                    f"发现 {too_fast_pct:.1%} 的响应时间过快，可能存在草率作答"
                )
            
            if too_slow_pct > 0.1:
                recommendations.append(
                    f"发现 {too_slow_pct:.1%} 的响应时间过慢，可能存在中途中断情况"
                )
        
        if not recommendations:
            recommendations.append("数据质量良好，无明显质量问题")
        
        return recommendations
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        if data.empty:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空")
            return validation_result
        
        questions = config.get('questions', [])
        if questions:
            missing_questions = [q for q in questions if q not in data.columns]
            if missing_questions:
                validation_result['warnings'].append(f"数据中缺少题目: {missing_questions}")
        
        validation_result['stats']['total_records'] = len(data)
        validation_result['stats']['total_columns'] = len(data.columns)
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'SurveyQuality',
            'version': '1.0',
            'description': '问卷数据质量检查：检测完成率、直线响应、响应时间等质量指标',
            'quality_dimensions': 'completion,straightlining,variance,timing,patterns',
            'recommendation_engine': 'rule_based'
        }