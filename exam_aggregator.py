#!/usr/bin/env python3
"""
考试学科汇聚器
处理考试和人机交互学科的数据汇聚
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from typing import Dict, List, Any, Optional
import json
from sqlalchemy import text
from aggregation_engine import BaseAggregationEngine, AggregationLevel, SubjectType

class ExamAggregator(BaseAggregationEngine):
    """考试学科汇聚器"""
    
    def aggregate_subject_level(self, batch_code: str, school_code: str = None, 
                              subject_name: str = None) -> Dict[str, Any]:
        """
        学科层级汇聚
        
        计算学科的平均分、得分率、标准差、区分度、难度系数、分位数等指标
        """
        # 参数验证
        valid, msg = self.validate_parameters(batch_code, school_code)
        if not valid:
            return {'error': msg}
        
        # 检查缓存
        cache_key = self.get_cache_key('exam_subject', batch_code, school_code, subject_name)
        cached_result = self.get_cache(cache_key)
        if cached_result:
            return cached_result
        
        session = self.get_session()
        try:
            # 构建查询条件
            where_conditions = ["batch_code = :batch_code", "subject_type = 'exam'"]
            params = {'batch_code': batch_code}
            
            if school_code:
                where_conditions.append("school_code = :school_code")
                params['school_code'] = school_code
                
            if subject_name:
                where_conditions.append("subject_name = :subject_name")
                params['subject_name'] = subject_name
            
            where_clause = " AND ".join(where_conditions)
            
            # 获取学科汇聚数据
            query = text(f"""
                SELECT 
                    subject_name,
                    COUNT(*) as student_count,
                    AVG(total_score) as mean_score,
                    STDDEV(total_score) as std_dev,
                    AVG(total_score / max_score) as avg_score_rate,
                    MAX(max_score) as subject_max_score,
                    GROUP_CONCAT(total_score) as all_scores,
                    GROUP_CONCAT(max_score) as all_max_scores
                FROM student_cleaned_scores
                WHERE {where_clause}
                GROUP BY subject_name
                ORDER BY student_count DESC
            """)
            
            result = session.execute(query, params)
            subjects_data = result.fetchall()
            
            if not subjects_data:
                return self.build_response(
                    AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                    batch_code, school_code,
                    subject_analysis={'message': '未找到考试学科数据'},
                    metadata=self.get_aggregation_metadata(batch_code, school_code)
                )
            
            # 处理每个学科的数据
            subject_analysis = {}
            
            for row in subjects_data:
                subj_name = row[0]
                student_count = row[1]
                mean_score = float(row[2]) if row[2] else 0
                std_dev = float(row[3]) if row[3] else 0
                avg_score_rate = float(row[4]) if row[4] else 0
                max_score = float(row[5]) if row[5] else 100
                
                # 解析所有分数
                scores_str = row[6]
                max_scores_str = row[7]
                
                if scores_str and max_scores_str:
                    scores = [float(x) for x in scores_str.split(',') if x.strip()]
                    max_scores = [float(x) for x in max_scores_str.split(',') if x.strip()]
                    
                    # 使用统计计算器计算完整指标
                    basic_stats = self.calculator.calculate_basic_stats(scores, max_scores)
                    discrimination = self.calculator.calculate_discrimination(scores, max_scores)
                    difficulty = self.calculator.calculate_difficulty(scores, max_scores)
                    
                    subject_analysis[subj_name] = {
                        'student_count': student_count,
                        'mean_score': basic_stats['mean'],
                        'std_deviation': basic_stats['std_dev'],
                        'score_rate': basic_stats['score_rate'],
                        'discrimination': discrimination,
                        'difficulty': difficulty,
                        'max_score': max_score,
                        'percentiles': basic_stats['percentiles']
                    }
                else:
                    # 降级处理：使用数据库聚合结果
                    subject_analysis[subj_name] = {
                        'student_count': student_count,
                        'mean_score': round(mean_score, 2),
                        'std_deviation': round(std_dev, 2),
                        'score_rate': round(avg_score_rate, 4),
                        'discrimination': 0,  # 无法计算
                        'difficulty': round(avg_score_rate, 4),
                        'max_score': max_score,
                        'percentiles': {'p10': 0, 'p50': 0, 'p90': 0}
                    }
            
            # 构建响应
            response = self.build_response(
                AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                batch_code, school_code,
                subject_analysis=subject_analysis,
                metadata=self.get_aggregation_metadata(batch_code, school_code)
            )
            
            # 设置缓存
            self.set_cache(cache_key, response, ttl=300)
            
            return response
            
        except Exception as e:
            return {'error': f'学科汇聚失败: {str(e)}'}
        finally:
            self.close_session(session)
    
    def aggregate_dimension_level(self, batch_code: str, school_code: str = None,
                                subject_name: str = None, dimension_code: str = None) -> Dict[str, Any]:
        """
        维度层级汇聚
        
        从dimension_scores JSON字段解析维度数据并计算统计指标
        """
        # 参数验证
        valid, msg = self.validate_parameters(batch_code, school_code)
        if not valid:
            return {'error': msg}
        
        # 检查缓存
        cache_key = self.get_cache_key('exam_dimension', batch_code, school_code, subject_name, dimension_code)
        cached_result = self.get_cache(cache_key)
        if cached_result:
            return cached_result
        
        session = self.get_session()
        try:
            # 构建查询条件
            where_conditions = ["batch_code = :batch_code", "subject_type = 'exam'"]
            params = {'batch_code': batch_code}
            
            if school_code:
                where_conditions.append("school_code = :school_code")
                params['school_code'] = school_code
                
            if subject_name:
                where_conditions.append("subject_name = :subject_name")
                params['subject_name'] = subject_name
            
            where_clause = " AND ".join(where_conditions)
            
            # 获取维度数据
            query = text(f"""
                SELECT 
                    subject_name,
                    dimension_scores,
                    dimension_max_scores
                FROM student_cleaned_scores
                WHERE {where_clause}
                AND dimension_scores IS NOT NULL 
                AND dimension_scores != '{{}}'
            """)
            
            result = session.execute(query, params)
            rows = result.fetchall()
            
            if not rows:
                return self.build_response(
                    AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                    batch_code, school_code,
                    dimension_analysis={'message': '未找到维度数据'},
                    metadata=self.get_aggregation_metadata(batch_code, school_code)
                )
            
            # 按学科组织维度数据
            subject_dimensions = {}
            
            for row in rows:
                subj_name = row[0]
                dimension_scores = row[1]
                dimension_max_scores = row[2]
                
                if subj_name not in subject_dimensions:
                    subject_dimensions[subj_name] = {
                        'dimension_scores': [],
                        'dimension_max_scores': []
                    }
                
                subject_dimensions[subj_name]['dimension_scores'].append(dimension_scores)
                subject_dimensions[subj_name]['dimension_max_scores'].append(dimension_max_scores)
            
            # 计算每个学科的维度统计
            dimension_analysis = {}
            
            for subj_name, data in subject_dimensions.items():
                dimension_stats = self.calculator.calculate_dimension_stats_from_json(
                    data['dimension_scores'], 
                    data['dimension_max_scores']
                )
                
                # 如果指定了维度代码，只返回该维度
                if dimension_code:
                    if dimension_code in dimension_stats:
                        dimension_analysis[subj_name] = {
                            dimension_code: dimension_stats[dimension_code]
                        }
                else:
                    dimension_analysis[subj_name] = dimension_stats
            
            # 获取维度名称映射
            dimension_names = self._get_dimension_names(batch_code, list(subject_dimensions.keys()))
            
            # 添加维度名称
            for subj_name in dimension_analysis:
                for dim_code in list(dimension_analysis[subj_name].keys()):
                    if dim_code in dimension_names:
                        dimension_analysis[subj_name][dim_code]['dimension_name'] = dimension_names[dim_code]
            
            # 构建响应
            response = self.build_response(
                AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                batch_code, school_code,
                dimension_analysis=dimension_analysis,
                metadata=self.get_aggregation_metadata(batch_code, school_code)
            )
            
            # 设置缓存
            self.set_cache(cache_key, response, ttl=300)
            
            return response
            
        except Exception as e:
            return {'error': f'维度汇聚失败: {str(e)}'}
        finally:
            self.close_session(session)
    
    def _get_dimension_names(self, batch_code: str, subject_names: List[str]) -> Dict[str, str]:
        """获取维度名称映射"""
        if not subject_names:
            return {}
        
        session = self.get_session()
        try:
            subject_list = "','".join(subject_names)
            query = text(f"""
                SELECT DISTINCT dimension_code, dimension_name
                FROM batch_dimension_definition
                WHERE batch_code = :batch_code 
                AND subject_name IN ('{subject_list}')
            """)
            
            result = session.execute(query, {'batch_code': batch_code})
            return {row[0]: row[1] for row in result.fetchall()}
            
        except Exception:
            return {}
        finally:
            self.close_session(session)
    
    def get_subject_comparison(self, batch_code: str, school_codes: List[str] = None) -> Dict[str, Any]:
        """
        获取学科对比数据
        
        Args:
            batch_code: 批次代码
            school_codes: 学校代码列表（可选）
            
        Returns:
            学科对比结果
        """
        session = self.get_session()
        try:
            if school_codes:
                school_list = "','".join(school_codes)
                where_clause = f"batch_code = :batch_code AND subject_type = 'exam' AND school_code IN ('{school_list}')"
            else:
                where_clause = "batch_code = :batch_code AND subject_type = 'exam'"
            
            query = text(f"""
                SELECT 
                    subject_name,
                    school_code,
                    school_name,
                    COUNT(*) as student_count,
                    AVG(total_score) as mean_score,
                    AVG(total_score / max_score) as score_rate
                FROM student_cleaned_scores
                WHERE {where_clause}
                GROUP BY subject_name, school_code, school_name
                ORDER BY subject_name, mean_score DESC
            """)
            
            result = session.execute(query, {'batch_code': batch_code})
            rows = result.fetchall()
            
            # 按学科组织数据
            comparison_data = {}
            for row in rows:
                subject_name = row[0]
                school_code = row[1]
                school_name = row[2]
                student_count = row[3]
                mean_score = float(row[4]) if row[4] else 0
                score_rate = float(row[5]) if row[5] else 0
                
                if subject_name not in comparison_data:
                    comparison_data[subject_name] = {
                        'schools': [],
                        'stats': {
                            'total_students': 0,
                            'avg_mean_score': 0,
                            'avg_score_rate': 0
                        }
                    }
                
                comparison_data[subject_name]['schools'].append({
                    'school_code': school_code,
                    'school_name': school_name,
                    'student_count': student_count,
                    'mean_score': round(mean_score, 2),
                    'score_rate': round(score_rate, 4)
                })
                
                comparison_data[subject_name]['stats']['total_students'] += student_count
            
            # 计算学科统计
            for subject_name in comparison_data:
                schools = comparison_data[subject_name]['schools']
                if schools:
                    total_students = sum(school['student_count'] for school in schools)
                    weighted_mean = sum(school['mean_score'] * school['student_count'] for school in schools) / total_students if total_students > 0 else 0
                    weighted_rate = sum(school['score_rate'] * school['student_count'] for school in schools) / total_students if total_students > 0 else 0
                    
                    comparison_data[subject_name]['stats'] = {
                        'total_students': total_students,
                        'avg_mean_score': round(weighted_mean, 2),
                        'avg_score_rate': round(weighted_rate, 4),
                        'school_count': len(schools)
                    }
            
            return {
                'batch_code': batch_code,
                'aggregation_level': 'comparison',
                'subject_comparison': comparison_data,
                'generated_at': self.get_aggregation_metadata(batch_code)['aggregation_time']
            }
            
        finally:
            self.close_session(session)

def test_exam_aggregator():
    """测试考试学科汇聚器"""
    print("=== 测试考试学科汇聚器 ===\n")
    
    aggregator = ExamAggregator()
    
    # 1. 测试区域层级学科汇聚
    print("1. 区域层级学科汇聚 (G4-2025):")
    result = aggregator.aggregate_subject_level('G4-2025')
    
    if 'error' not in result and 'subject_analysis' in result:
        print(f"   成功汇聚 {len(result['subject_analysis'])} 个学科")
        for subject, stats in list(result['subject_analysis'].items())[:2]:  # 显示前2个
            print(f"   {subject}: 平均分{stats['mean_score']}, 得分率{stats['score_rate']:.2%}")
    else:
        print(f"   失败: {result.get('error', '未知错误')}")
    
    # 2. 测试学校层级学科汇聚
    print("\n2. 学校层级学科汇聚:")
    schools = aggregator.get_school_info('G4-2025')
    if schools:
        school_code = schools[0][0]  # 取第一个学校
        result = aggregator.aggregate_subject_level('G4-2025', school_code)
        
        if 'error' not in result and 'subject_analysis' in result:
            print(f"   学校 {school_code} 汇聚 {len(result['subject_analysis'])} 个学科")
        else:
            print(f"   失败: {result.get('error', '未知错误')}")
    
    # 3. 测试维度汇聚
    print("\n3. 维度层级汇聚:")
    result = aggregator.aggregate_dimension_level('G4-2025')
    
    if 'error' not in result and 'dimension_analysis' in result:
        total_dimensions = sum(len(dims) for dims in result['dimension_analysis'].values())
        print(f"   成功汇聚 {total_dimensions} 个维度")
        
        if result['dimension_analysis']:
            # 显示第一个学科的维度
            first_subject = list(result['dimension_analysis'].keys())[0]
            dimensions = result['dimension_analysis'][first_subject]
            if dimensions:
                first_dim = list(dimensions.keys())[0]
                dim_stats = dimensions[first_dim]
                print(f"   示例 {first_subject}-{first_dim}: 平均分{dim_stats['mean']}, 区分度{dim_stats.get('discrimination', 0)}")
            else:
                print(f"   {first_subject} 学科无维度数据")
        else:
            print("   无维度数据可显示")
    else:
        print(f"   失败: {result.get('error', '未知错误')}")
    
    print("\n=== 考试汇聚器测试完成 ===")

if __name__ == "__main__":
    test_exam_aggregator()