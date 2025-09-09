#!/usr/bin/env python3
"""
问卷学科汇聚器
处理问卷类型学科的数据汇聚，包括选项分布分析
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from typing import Dict, List, Any, Optional
import json
from sqlalchemy import text
from aggregation_engine import BaseAggregationEngine, AggregationLevel, SubjectType

class QuestionnaireAggregator(BaseAggregationEngine):
    """问卷学科汇聚器"""
    
    def aggregate_subject_level(self, batch_code: str, school_code: str = None, 
                              subject_name: str = None) -> Dict[str, Any]:
        """
        学科层级汇聚
        
        计算问卷学科的维度统计指标
        """
        # 参数验证
        valid, msg = self.validate_parameters(batch_code, school_code)
        if not valid:
            return {'error': msg}
        
        # 检查缓存
        cache_key = self.get_cache_key('questionnaire_subject', batch_code, school_code, subject_name)
        cached_result = self.get_cache(cache_key)
        if cached_result:
            return cached_result
        
        session = self.get_session()
        try:
            # 构建查询条件
            where_conditions = ["batch_code = :batch_code", "subject_type = 'questionnaire'"]
            params = {'batch_code': batch_code}
            
            if school_code:
                where_conditions.append("school_code = :school_code")
                params['school_code'] = school_code
                
            if subject_name:
                where_conditions.append("subject_name = :subject_name")
                params['subject_name'] = subject_name
            
            where_clause = " AND ".join(where_conditions)
            
            # 获取问卷学科汇聚数据
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
                    subject_analysis={'message': '未找到问卷学科数据'},
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
            return {'error': f'问卷学科汇聚失败: {str(e)}'}
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
        cache_key = self.get_cache_key('questionnaire_dimension', batch_code, school_code, subject_name, dimension_code)
        cached_result = self.get_cache(cache_key)
        if cached_result:
            return cached_result
        
        session = self.get_session()
        try:
            # 构建查询条件
            where_conditions = ["batch_code = :batch_code", "subject_type = 'questionnaire'"]
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
            return {'error': f'问卷维度汇聚失败: {str(e)}'}
        finally:
            self.close_session(session)
    
    def get_option_distribution(self, batch_code: str, school_code: str = None,
                              dimension_code: str = None) -> Dict[str, Any]:
        """
        获取选项分布分析
        
        Args:
            batch_code: 批次代码
            school_code: 学校代码（可选）
            dimension_code: 维度代码（可选）
            
        Returns:
            选项分布分析结果
        """
        # 检查缓存
        cache_key = self.get_cache_key('option_distribution', batch_code, school_code, dimension_code)
        cached_result = self.get_cache(cache_key)
        if cached_result:
            return cached_result
        
        session = self.get_session()
        try:
            # 构建查询条件
            where_conditions = ["batch_code = :batch_code"]
            params = {'batch_code': batch_code}
            
            if school_code:
                where_conditions.append("school_code = :school_code")
                params['school_code'] = school_code
                
            if dimension_code:
                where_conditions.append("dimension_code = :dimension_code")
                params['dimension_code'] = dimension_code
            
            where_clause = " AND ".join(where_conditions)
            
            # 查询选项分布数据
            query = text(f"""
                SELECT 
                    dimension_code,
                    dimension_name,
                    question_id,
                    option_label,
                    option_level,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY dimension_code, question_id), 2) as percentage
                FROM questionnaire_question_scores
                WHERE {where_clause}
                GROUP BY dimension_code, dimension_name, question_id, option_label, option_level
                ORDER BY dimension_code, question_id, option_level
            """)
            
            result = session.execute(query, params)
            rows = result.fetchall()
            
            if not rows:
                return {
                    'batch_code': batch_code,
                    'aggregation_level': 'option_distribution',
                    'message': '未找到选项分布数据',
                    'generated_at': self.get_aggregation_metadata(batch_code, school_code)['aggregation_time']
                }
            
            # 组织数据结构
            option_analysis = {}
            
            for row in rows:
                dim_code = row[0]
                dim_name = row[1]
                question_id = row[2]
                option_label = row[3]
                option_level = row[4]
                count = row[5]
                percentage = float(row[6])
                
                # 初始化维度结构
                if dim_code not in option_analysis:
                    option_analysis[dim_code] = {
                        'dimension_name': dim_name,
                        'questions': {}
                    }
                
                # 初始化题目结构
                if question_id not in option_analysis[dim_code]['questions']:
                    option_analysis[dim_code]['questions'][question_id] = {
                        'options': [],
                        'total_responses': 0
                    }
                
                # 添加选项数据
                option_analysis[dim_code]['questions'][question_id]['options'].append({
                    'label': option_label,
                    'level': option_level,
                    'count': count,
                    'percentage': percentage
                })
                
                option_analysis[dim_code]['questions'][question_id]['total_responses'] += count
            
            # 计算维度级别的统计
            for dim_code in option_analysis:
                questions = option_analysis[dim_code]['questions']
                total_questions = len(questions)
                total_responses = sum(q['total_responses'] for q in questions.values())
                
                option_analysis[dim_code]['stats'] = {
                    'total_questions': total_questions,
                    'total_responses': total_responses,
                    'avg_responses_per_question': round(total_responses / total_questions, 2) if total_questions > 0 else 0
                }
            
            # 构建响应
            response = {
                'batch_code': batch_code,
                'aggregation_level': AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                'option_distribution': option_analysis,
                'metadata': self.get_aggregation_metadata(batch_code, school_code),
                'generated_at': self.get_aggregation_metadata(batch_code, school_code)['aggregation_time']
            }
            
            if school_code:
                response['school_code'] = school_code
            
            # 设置缓存
            self.set_cache(cache_key, response, ttl=300)
            
            return response
            
        except Exception as e:
            return {'error': f'选项分布分析失败: {str(e)}'}
        finally:
            self.close_session(session)
    
    def _get_dimension_names(self, batch_code: str, subject_names: List[str]) -> Dict[str, str]:
        """获取维度名称映射"""
        if not subject_names:
            return {}
        
        session = self.get_session()
        try:
            # 问卷学科通常是'问卷'，维度定义在batch_dimension_definition表中
            query = text("""
                SELECT DISTINCT dimension_code, dimension_name
                FROM batch_dimension_definition
                WHERE batch_code = :batch_code 
                AND (subject_name = '问卷' OR subject_name IN :subject_names)
            """)
            
            result = session.execute(query, {'batch_code': batch_code, 'subject_names': tuple(subject_names)})
            return {row[0]: row[1] for row in result.fetchall()}
            
        except Exception:
            return {}
        finally:
            self.close_session(session)
    
    def get_questionnaire_summary(self, batch_code: str, school_code: str = None) -> Dict[str, Any]:
        """
        获取问卷汇总分析
        
        结合维度统计和选项分布的完整分析
        """
        try:
            # 获取维度统计
            dimension_result = self.aggregate_dimension_level(batch_code, school_code)
            
            # 获取选项分布
            option_result = self.get_option_distribution(batch_code, school_code)
            
            # 合并结果
            summary = {
                'batch_code': batch_code,
                'aggregation_level': AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                'questionnaire_analysis': {
                    'dimension_statistics': dimension_result.get('dimension_analysis', {}),
                    'option_distribution': option_result.get('option_distribution', {})
                },
                'metadata': self.get_aggregation_metadata(batch_code, school_code),
                'generated_at': self.get_aggregation_metadata(batch_code, school_code)['aggregation_time']
            }
            
            if school_code:
                summary['school_code'] = school_code
            
            return summary
            
        except Exception as e:
            return {'error': f'问卷汇总分析失败: {str(e)}'}

def test_questionnaire_aggregator():
    """测试问卷学科汇聚器"""
    print("=== 测试问卷学科汇聚器 ===\n")
    
    aggregator = QuestionnaireAggregator()
    
    # 1. 测试区域层级学科汇聚
    print("1. 区域层级问卷学科汇聚 (G4-2025):")
    result = aggregator.aggregate_subject_level('G4-2025')
    
    if 'error' not in result and 'subject_analysis' in result:
        print(f"   成功汇聚 {len(result['subject_analysis'])} 个问卷学科")
        for subject, stats in list(result['subject_analysis'].items())[:2]:  # 显示前2个
            print(f"   {subject}: 平均分{stats['mean_score']}, 得分率{stats['score_rate']:.2%}")
    else:
        print(f"   失败: {result.get('error', '未知错误')}")
    
    # 2. 测试维度汇聚
    print("\n2. 维度层级汇聚:")
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
        print(f"   失败: {result.get('error', '未知错误')}")
    
    # 3. 测试选项分布分析
    print("\n3. 选项分布分析:")
    result = aggregator.get_option_distribution('G4-2025')
    
    if 'error' not in result and 'option_distribution' in result:
        dimensions = result['option_distribution']
        print(f"   成功分析 {len(dimensions)} 个维度的选项分布")
        
        if dimensions:
            # 显示第一个维度的选项分布
            first_dim = list(dimensions.keys())[0]
            dim_data = dimensions[first_dim]
            questions = dim_data['questions']
            
            print(f"   示例维度 {first_dim} ({dim_data['dimension_name']}):")
            print(f"      总题目数: {dim_data['stats']['total_questions']}")
            
            if questions:
                first_question = list(questions.keys())[0]
                options = questions[first_question]['options']
                print(f"      示例题目 {first_question} 选项分布:")
                for option in options[:3]:  # 显示前3个选项
                    print(f"         {option['label']}: {option['count']}人 ({option['percentage']}%)")
    else:
        print(f"   失败: {result.get('error', '未知错误')}")
    
    # 4. 测试完整汇总
    print("\n4. 问卷完整汇总:")
    result = aggregator.get_questionnaire_summary('G4-2025')
    
    if 'error' not in result and 'questionnaire_analysis' in result:
        analysis = result['questionnaire_analysis']
        dim_count = len(analysis.get('dimension_statistics', {}))
        option_count = len(analysis.get('option_distribution', {}))
        print(f"   汇总完成: {dim_count}个维度统计, {option_count}个维度选项分布")
    else:
        print(f"   失败: {result.get('error', '未知错误')}")
    
    print("\n=== 问卷汇聚器测试完成 ===")

if __name__ == "__main__":
    test_questionnaire_aggregator()