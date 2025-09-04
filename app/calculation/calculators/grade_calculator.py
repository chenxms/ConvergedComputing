# 年级差异化等级分布计算器
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Tuple
from ..engine import StatisticalStrategy

logger = logging.getLogger(__name__)


class GradeLevelConfig:
    """年级等级配置类"""
    
    # 年级分组配置
    ELEMENTARY_GRADES = [
        '1st_grade', '2nd_grade', '3rd_grade', 
        '4th_grade', '5th_grade', '6th_grade'
    ]
    
    MIDDLE_SCHOOL_GRADES = [
        '7th_grade', '8th_grade', '9th_grade'
    ]
    
    # 小学等级阈值配置（基于满分的百分比）
    ELEMENTARY_THRESHOLDS = {
        'excellent': 0.90,  # 优秀 ≥90%
        'good': 0.80,       # 良好 80-89%
        'pass': 0.60,       # 及格 60-79%
        'fail': 0.00        # 不及格 <60%
    }
    
    # 初中等级阈值配置（基于满分的百分比）
    MIDDLE_SCHOOL_THRESHOLDS = {
        'A': 0.85,          # A等 ≥85%
        'B': 0.70,          # B等 70-84%
        'C': 0.60,          # C等 60-69%
        'D': 0.00           # D等 <60%
    }
    
    # 等级名称映射
    ELEMENTARY_GRADE_NAMES = {
        'excellent': '优秀',
        'good': '良好',
        'pass': '及格',
        'fail': '不及格'
    }
    
    MIDDLE_SCHOOL_GRADE_NAMES = {
        'A': 'A等',
        'B': 'B等',
        'C': 'C等',
        'D': 'D等'
    }
    
    @classmethod
    def is_elementary_grade(cls, grade_level: str) -> bool:
        """判断是否为小学年级"""
        return grade_level in cls.ELEMENTARY_GRADES
    
    @classmethod
    def is_middle_school_grade(cls, grade_level: str) -> bool:
        """判断是否为初中年级"""
        return grade_level in cls.MIDDLE_SCHOOL_GRADES
    
    @classmethod
    def get_grade_type(cls, grade_level: str) -> str:
        """获取年级类型"""
        if cls.is_elementary_grade(grade_level):
            return 'elementary'
        elif cls.is_middle_school_grade(grade_level):
            return 'middle_school'
        else:
            return 'unknown'
    
    @classmethod
    def get_thresholds(cls, grade_level: str) -> Dict[str, float]:
        """获取等级阈值配置"""
        if cls.is_elementary_grade(grade_level):
            return cls.ELEMENTARY_THRESHOLDS.copy()
        elif cls.is_middle_school_grade(grade_level):
            return cls.MIDDLE_SCHOOL_THRESHOLDS.copy()
        else:
            # 默认使用小学标准
            logger.warning(f"未知年级 {grade_level}，使用小学等级标准")
            return cls.ELEMENTARY_THRESHOLDS.copy()
    
    @classmethod
    def get_grade_names(cls, grade_level: str) -> Dict[str, str]:
        """获取等级名称映射"""
        if cls.is_elementary_grade(grade_level):
            return cls.ELEMENTARY_GRADE_NAMES.copy()
        elif cls.is_middle_school_grade(grade_level):
            return cls.MIDDLE_SCHOOL_GRADE_NAMES.copy()
        else:
            return cls.ELEMENTARY_GRADE_NAMES.copy()


class GradeLevelDistributionCalculator(StatisticalStrategy):
    """年级差异化等级分布计算策略"""
    
    def __init__(self, custom_thresholds: Optional[Dict[str, Dict[str, float]]] = None):
        """
        初始化等级分布计算器
        
        Args:
            custom_thresholds: 自定义等级阈值配置
                格式: {
                    'elementary': {'excellent': 0.90, 'good': 0.80, 'pass': 0.60, 'fail': 0.00},
                    'middle_school': {'A': 0.85, 'B': 0.70, 'C': 0.60, 'D': 0.00}
                }
        """
        self.custom_thresholds = custom_thresholds or {}
        
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        计算等级分布
        
        Args:
            data: 包含score和grade_level列的数据框
            config: 配置参数，包含max_score等
            
        Returns:
            等级分布统计结果
        """
        # 验证必需字段
        if 'score' not in data.columns:
            raise ValueError("数据中缺少'score'列")
        
        # 获取配置参数
        max_score = config.get('max_score', 100)
        grade_level = config.get('grade_level')
        
        # 如果没有指定年级，尝试从数据中获取
        if not grade_level and 'grade_level' in data.columns:
            grade_levels = data['grade_level'].unique()
            if len(grade_levels) == 1:
                grade_level = grade_levels[0]
            else:
                # 多个年级混合数据，需要分组处理
                return self._calculate_mixed_grades(data, config)
        
        if not grade_level:
            raise ValueError("必须指定grade_level参数或数据中包含grade_level列")
        
        # 清理分数数据 - 安全转换
        scores = pd.to_numeric(data['score'], errors='coerce').dropna()
        
        if len(scores) == 0:
            raise ValueError("没有有效的分数数据")
        
        # 计算等级分布
        distribution = self._calculate_grade_distribution(scores, grade_level, max_score)
        
        # 计算统计指标
        statistics = self._calculate_statistics(scores, max_score)
        
        # 生成趋势分析
        trends = self._analyze_trends(data, grade_level, max_score)
        
        # 组合结果
        result = {
            'grade_level': grade_level,
            'grade_type': GradeLevelConfig.get_grade_type(grade_level),
            'total_count': len(scores),
            'max_score': max_score,
            'distribution': distribution,
            'statistics': statistics,
            'trends': trends,
            'thresholds_used': self._get_effective_thresholds(grade_level)
        }
        
        return result
    
    def _calculate_mixed_grades(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算多年级混合数据的等级分布"""
        results = {}
        max_score = config.get('max_score', 100)
        
        for grade_level in data['grade_level'].unique():
            if pd.isna(grade_level):
                continue
                
            grade_data = data[data['grade_level'] == grade_level]
            grade_config = config.copy()
            grade_config['grade_level'] = grade_level
            
            try:
                grade_result = self.calculate(grade_data, grade_config)
                results[grade_level] = grade_result
            except Exception as e:
                logger.error(f"计算年级 {grade_level} 等级分布失败: {e}")
                continue
        
        # 计算整体统计
        all_scores = data['score'].astype(float).dropna()
        overall_stats = self._calculate_statistics(all_scores, max_score)
        
        return {
            'type': 'mixed_grades',
            'grade_results': results,
            'overall_statistics': overall_stats,
            'total_students': len(all_scores),
            'grade_count': len(results)
        }
    
    def _calculate_grade_distribution(self, scores: pd.Series, grade_level: str, max_score: float) -> Dict[str, Any]:
        """计算等级分布"""
        thresholds = self._get_effective_thresholds(grade_level)
        grade_names = GradeLevelConfig.get_grade_names(grade_level)
        
        scores_array = scores.values
        total_count = len(scores_array)
        
        distribution = {
            'counts': {},
            'rates': {},
            'percentages': {},
            'labels': {}
        }
        
        if GradeLevelConfig.is_elementary_grade(grade_level):
            # 小学等级分布计算
            excellent_mask = scores_array >= (max_score * thresholds['excellent'])
            good_mask = (scores_array >= (max_score * thresholds['good'])) & \
                       (scores_array < (max_score * thresholds['excellent']))
            pass_mask = (scores_array >= (max_score * thresholds['pass'])) & \
                       (scores_array < (max_score * thresholds['good']))
            fail_mask = scores_array < (max_score * thresholds['pass'])
            
            distribution['counts'] = {
                'excellent': int(np.sum(excellent_mask)),
                'good': int(np.sum(good_mask)),
                'pass': int(np.sum(pass_mask)),
                'fail': int(np.sum(fail_mask))
            }
            
            distribution['rates'] = {
                'excellent': float(np.mean(excellent_mask)),
                'good': float(np.mean(good_mask)),
                'pass': float(np.mean(pass_mask)),
                'fail': float(np.mean(fail_mask))
            }
            
        else:
            # 初中等级分布计算
            a_mask = scores_array >= (max_score * thresholds['A'])
            b_mask = (scores_array >= (max_score * thresholds['B'])) & \
                    (scores_array < (max_score * thresholds['A']))
            c_mask = (scores_array >= (max_score * thresholds['C'])) & \
                    (scores_array < (max_score * thresholds['B']))
            d_mask = scores_array < (max_score * thresholds['C'])
            
            distribution['counts'] = {
                'A': int(np.sum(a_mask)),
                'B': int(np.sum(b_mask)),
                'C': int(np.sum(c_mask)),
                'D': int(np.sum(d_mask))
            }
            
            distribution['rates'] = {
                'A': float(np.mean(a_mask)),
                'B': float(np.mean(b_mask)),
                'C': float(np.mean(c_mask)),
                'D': float(np.mean(d_mask))
            }
        
        # 计算百分比
        for key, rate in distribution['rates'].items():
            distribution['percentages'][key] = round(rate * 100, 2)
        
        # 添加标签
        for key in distribution['counts'].keys():
            distribution['labels'][key] = grade_names.get(key, key)
        
        return distribution
    
    def _calculate_statistics(self, scores: pd.Series, max_score: float) -> Dict[str, Any]:
        """计算基础统计指标"""
        scores_array = scores.values
        
        return {
            'mean': float(np.mean(scores_array)),
            'median': float(np.median(scores_array)),
            'std': float(np.std(scores_array, ddof=1)),
            'min': float(np.min(scores_array)),
            'max': float(np.max(scores_array)),
            'range': float(np.max(scores_array) - np.min(scores_array)),
            'score_rate': float(np.mean(scores_array) / max_score),
            'pass_rate': float(np.mean(scores_array >= (max_score * 0.6))),
            'excellent_rate': float(np.mean(scores_array >= (max_score * 0.85)))
        }
    
    def _analyze_trends(self, data: pd.DataFrame, grade_level: str, max_score: float) -> Dict[str, Any]:
        """分析等级分布趋势"""
        trends = {
            'has_trend_data': False,
            'trend_analysis': None,
            'recommendations': []
        }
        
        # 如果有时间序列数据，可以进行趋势分析
        if 'exam_date' in data.columns or 'create_time' in data.columns:
            trends['has_trend_data'] = True
            # TODO: 实现时间序列趋势分析
            trends['trend_analysis'] = "时间序列趋势分析功能待实现"
        
        # 基于当前分布提供建议
        scores = data['score'].astype(float).dropna()
        distribution = self._calculate_grade_distribution(scores, grade_level, max_score)
        
        recommendations = self._generate_recommendations(distribution, grade_level)
        trends['recommendations'] = recommendations
        
        return trends
    
    def _generate_recommendations(self, distribution: Dict[str, Any], grade_level: str) -> List[str]:
        """基于等级分布生成建议"""
        recommendations = []
        rates = distribution['rates']
        
        if GradeLevelConfig.is_elementary_grade(grade_level):
            # 小学建议逻辑
            if rates.get('fail', 0) > 0.2:  # 不及格率超过20%
                recommendations.append("不及格率过高，建议加强基础知识教学")
            
            if rates.get('excellent', 0) < 0.1:  # 优秀率低于10%
                recommendations.append("优秀率偏低，建议提供拓展学习材料")
            
            if rates.get('excellent', 0) > 0.5:  # 优秀率超过50%
                recommendations.append("整体表现优秀，可适当提高教学难度")
                
        else:
            # 初中建议逻辑
            if rates.get('D', 0) > 0.15:  # D等率超过15%
                recommendations.append("D等学生比例较高，需要加强个别辅导")
            
            if rates.get('A', 0) < 0.15:  # A等率低于15%
                recommendations.append("A等学生比例偏低，建议增加挑战性内容")
            
            if rates.get('C', 0) + rates.get('D', 0) > 0.4:  # C+D等超过40%
                recommendations.append("中低分段学生较多，建议分层教学")
        
        # 通用建议
        total_low_performers = rates.get('fail', 0) + rates.get('D', 0)
        if total_low_performers > 0.25:
            recommendations.append("建议开展针对性的学困生帮扶工作")
        
        return recommendations
    
    def _get_effective_thresholds(self, grade_level: str) -> Dict[str, float]:
        """获取有效的等级阈值"""
        grade_type = GradeLevelConfig.get_grade_type(grade_level)
        
        # 优先使用自定义阈值
        if grade_type in self.custom_thresholds:
            return self.custom_thresholds[grade_type].copy()
        
        # 使用默认阈值
        return GradeLevelConfig.get_thresholds(grade_level)
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        # 基础检查
        if data.empty:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空")
            return validation_result
        
        # 必需字段检查
        if 'score' not in data.columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append("缺少必需字段: score")
            return validation_result
        
        # 分数数据检查
        scores = pd.to_numeric(data['score'], errors='coerce')
        null_count = scores.isna().sum()
        valid_count = len(data) - null_count
        
        if valid_count == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("没有有效的分数数据")
        elif null_count > 0:
            validation_result['warnings'].append(f"发现{null_count}个无效分数值")
        
        # 年级字段检查
        grade_level = config.get('grade_level')
        if not grade_level and 'grade_level' not in data.columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append("必须指定grade_level参数或数据中包含grade_level列")
        elif 'grade_level' in data.columns:
            grade_levels = data['grade_level'].dropna().unique()
            unknown_grades = [
                g for g in grade_levels 
                if not GradeLevelConfig.is_elementary_grade(g) and 
                   not GradeLevelConfig.is_middle_school_grade(g)
            ]
            if unknown_grades:
                validation_result['warnings'].append(f"发现未知年级: {unknown_grades}")
        
        # 满分配置检查
        max_score = config.get('max_score', 100)
        if max_score <= 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("满分配置无效")
        
        # 分数范围检查
        if valid_count > 0:
            valid_scores = scores.dropna()
            out_of_range = valid_scores[(valid_scores < 0) | (valid_scores > max_score)]
            if len(out_of_range) > 0:
                validation_result['warnings'].append(f"发现{len(out_of_range)}个超出范围的分数")
        
        # 统计信息
        validation_result['stats'] = {
            'total_records': len(data),
            'valid_scores': valid_count,
            'data_completeness': valid_count / len(data),
            'max_score': max_score
        }
        
        # 数据量检查
        if valid_count < 10:
            validation_result['warnings'].append(f"数据量较少({valid_count})，统计结果可能不够稳定")
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        """获取算法信息"""
        return {
            'name': 'GradeLevelDistribution',
            'version': '1.0',
            'description': '年级差异化等级分布计算',
            'elementary_standard': '优秀≥90%, 良好80-89%, 及格60-79%, 不及格<60%',
            'middle_school_standard': 'A≥85%, B70-84%, C60-69%, D<60%',
            'supports_custom_thresholds': True,
            'supports_trend_analysis': True
        }


def calculate_individual_grade(score: float, grade_level: str, max_score: float = 100) -> Dict[str, Any]:
    """
    计算单个学生的等级
    
    Args:
        score: 学生分数
        grade_level: 年级级别
        max_score: 满分
        
    Returns:
        等级信息
    """
    if pd.isna(score) or score < 0:
        return {'grade': None, 'grade_name': '无效分数', 'score_rate': 0}
    
    score_rate = score / max_score
    thresholds = GradeLevelConfig.get_thresholds(grade_level)
    grade_names = GradeLevelConfig.get_grade_names(grade_level)
    
    if GradeLevelConfig.is_elementary_grade(grade_level):
        if score_rate >= thresholds['excellent']:
            grade = 'excellent'
        elif score_rate >= thresholds['good']:
            grade = 'good'
        elif score_rate >= thresholds['pass']:
            grade = 'pass'
        else:
            grade = 'fail'
    else:
        if score_rate >= thresholds['A']:
            grade = 'A'
        elif score_rate >= thresholds['B']:
            grade = 'B'
        elif score_rate >= thresholds['C']:
            grade = 'C'
        else:
            grade = 'D'
    
    return {
        'grade': grade,
        'grade_name': grade_names.get(grade, grade),
        'score_rate': round(score_rate, 4),
        'threshold_met': score_rate >= min(thresholds.values())
    }


def batch_calculate_grades(data: pd.DataFrame, grade_level_col: str = 'grade_level', 
                         score_col: str = 'score', max_score: float = 100) -> pd.DataFrame:
    """
    批量计算学生等级
    
    Args:
        data: 学生数据
        grade_level_col: 年级列名
        score_col: 分数列名
        max_score: 满分
        
    Returns:
        包含等级信息的数据框
    """
    result_data = data.copy()
    
    def apply_grade_calculation(row):
        return calculate_individual_grade(
            row[score_col], 
            row[grade_level_col], 
            max_score
        )
    
    # 应用等级计算
    grade_info = result_data.apply(apply_grade_calculation, axis=1)
    
    # 提取等级信息到单独的列
    result_data['calculated_grade'] = grade_info.apply(lambda x: x['grade'])
    result_data['grade_name'] = grade_info.apply(lambda x: x['grade_name'])
    result_data['score_rate'] = grade_info.apply(lambda x: x['score_rate'])
    result_data['threshold_met'] = grade_info.apply(lambda x: x['threshold_met'])
    
    return result_data


def create_grade_summary_report(distribution_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    创建等级分布汇总报告
    
    Args:
        distribution_result: 等级分布计算结果
        
    Returns:
        汇总报告
    """
    if distribution_result.get('type') == 'mixed_grades':
        # 多年级汇总报告
        return _create_mixed_grade_report(distribution_result)
    else:
        # 单年级汇总报告
        return _create_single_grade_report(distribution_result)


def _create_single_grade_report(result: Dict[str, Any]) -> Dict[str, Any]:
    """创建单年级汇总报告"""
    distribution = result.get('distribution', {})
    statistics = result.get('statistics', {})
    
    return {
        'grade_level': result.get('grade_level'),
        'grade_type': result.get('grade_type'),
        'summary': {
            'total_students': result.get('total_count', 0),
            'average_score': round(statistics.get('mean', 0), 2),
            'score_rate': round(statistics.get('score_rate', 0) * 100, 2),
            'pass_rate': round(statistics.get('pass_rate', 0) * 100, 2),
            'excellent_rate': round(statistics.get('excellent_rate', 0) * 100, 2)
        },
        'distribution_summary': distribution.get('percentages', {}),
        'recommendations': result.get('trends', {}).get('recommendations', []),
        'performance_level': _assess_performance_level(statistics),
        'report_generated_at': pd.Timestamp.now().isoformat()
    }


def _create_mixed_grade_report(result: Dict[str, Any]) -> Dict[str, Any]:
    """创建多年级汇总报告"""
    grade_results = result.get('grade_results', {})
    overall_stats = result.get('overall_statistics', {})
    
    grade_summaries = {}
    for grade_level, grade_result in grade_results.items():
        grade_summaries[grade_level] = _create_single_grade_report(grade_result)
    
    return {
        'type': 'mixed_grades_report',
        'overall_summary': {
            'total_students': result.get('total_students', 0),
            'grade_count': result.get('grade_count', 0),
            'average_score': round(overall_stats.get('mean', 0), 2),
            'overall_pass_rate': round(overall_stats.get('pass_rate', 0) * 100, 2)
        },
        'grade_summaries': grade_summaries,
        'cross_grade_analysis': _analyze_cross_grade_performance(grade_results),
        'report_generated_at': pd.Timestamp.now().isoformat()
    }


def _assess_performance_level(statistics: Dict[str, Any]) -> str:
    """评估整体表现水平"""
    score_rate = statistics.get('score_rate', 0)
    pass_rate = statistics.get('pass_rate', 0)
    
    if score_rate >= 0.85 and pass_rate >= 0.9:
        return 'excellent'
    elif score_rate >= 0.75 and pass_rate >= 0.8:
        return 'good'
    elif score_rate >= 0.65 and pass_rate >= 0.6:
        return 'acceptable'
    else:
        return 'needs_improvement'


def _analyze_cross_grade_performance(grade_results: Dict[str, Any]) -> Dict[str, Any]:
    """分析跨年级表现"""
    analysis = {
        'performance_ranking': [],
        'achievement_gaps': {},
        'recommendations': []
    }
    
    # 计算各年级表现排名
    grade_scores = {}
    for grade_level, result in grade_results.items():
        stats = result.get('statistics', {})
        grade_scores[grade_level] = stats.get('score_rate', 0)
    
    # 按得分率排序
    sorted_grades = sorted(grade_scores.items(), key=lambda x: x[1], reverse=True)
    analysis['performance_ranking'] = [
        {'grade': grade, 'score_rate': round(rate * 100, 2)} 
        for grade, rate in sorted_grades
    ]
    
    # 分析成绩差距
    if len(sorted_grades) >= 2:
        best_performance = sorted_grades[0][1]
        worst_performance = sorted_grades[-1][1]
        analysis['achievement_gaps']['max_gap'] = round((best_performance - worst_performance) * 100, 2)
    
    # 跨年级建议
    if analysis['achievement_gaps'].get('max_gap', 0) > 20:
        analysis['recommendations'].append("年级间成绩差距较大，建议加强教学衔接")
    
    return analysis