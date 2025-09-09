#!/usr/bin/env python3
"""
多层级汇聚器
统一协调考试和问卷汇聚器，支持学校和区域层级汇聚
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from typing import Dict, List, Any, Optional
from exam_aggregator import ExamAggregator
from questionnaire_aggregator import QuestionnaireAggregator
from aggregation_engine import AggregationLevel, SubjectType

class MultiLayerAggregator:
    """多层级汇聚器"""
    
    def __init__(self, database_url: str = None):
        """
        初始化多层级汇聚器
        
        Args:
            database_url: 数据库连接URL
        """
        self.exam_aggregator = ExamAggregator(database_url)
        self.questionnaire_aggregator = QuestionnaireAggregator(database_url)
    
    def aggregate_all_subjects(self, batch_code: str, school_code: str = None) -> Dict[str, Any]:
        """
        汇聚所有学科类型
        
        Args:
            batch_code: 批次代码
            school_code: 学校代码（None时进行区域层级汇聚）
            
        Returns:
            统一的汇聚结果，包含考试学科和问卷学科
        """
        try:
            # 获取考试学科汇聚结果（包含exam和interaction类型）
            exam_result = self.exam_aggregator.aggregate_subject_level(batch_code, school_code)
            
            # 获取问卷学科汇聚结果
            questionnaire_result = self.questionnaire_aggregator.aggregate_subject_level(batch_code, school_code)
            
            # 合并结果
            combined_result = {
                'batch_code': batch_code,
                'aggregation_level': AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                'analysis': {
                    'exam_subjects': exam_result.get('subject_analysis', {}),
                    'questionnaire_subjects': questionnaire_result.get('subject_analysis', {})
                },
                'metadata': exam_result.get('metadata', {}),
                'generated_at': exam_result.get('generated_at', '')
            }
            
            if school_code:
                combined_result['school_code'] = school_code
            
            # 添加汇总统计
            combined_result['summary'] = self._calculate_summary_stats(
                exam_result.get('subject_analysis', {}),
                questionnaire_result.get('subject_analysis', {})
            )
            
            return combined_result
            
        except Exception as e:
            return {'error': f'多层级学科汇聚失败: {str(e)}'}
    
    def aggregate_all_dimensions(self, batch_code: str, school_code: str = None, 
                               subject_name: str = None) -> Dict[str, Any]:
        """
        汇聚所有维度数据
        
        Args:
            batch_code: 批次代码
            school_code: 学校代码（None时进行区域层级汇聚）
            subject_name: 学科名称（可选）
            
        Returns:
            统一的维度汇聚结果
        """
        try:
            # 获取考试学科维度汇聚结果
            exam_dimension_result = self.exam_aggregator.aggregate_dimension_level(
                batch_code, school_code, subject_name
            )
            
            # 获取问卷学科维度汇聚结果
            questionnaire_dimension_result = self.questionnaire_aggregator.aggregate_dimension_level(
                batch_code, school_code, subject_name
            )
            
            # 获取问卷选项分布（问卷特有功能）
            questionnaire_options = self.questionnaire_aggregator.get_option_distribution(
                batch_code, school_code
            )
            
            # 合并结果
            combined_result = {
                'batch_code': batch_code,
                'aggregation_level': AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                'dimension_analysis': {
                    'exam_dimensions': exam_dimension_result.get('dimension_analysis', {}),
                    'questionnaire_dimensions': questionnaire_dimension_result.get('dimension_analysis', {}),
                    'questionnaire_options': questionnaire_options.get('option_distribution', {})
                },
                'metadata': exam_dimension_result.get('metadata', {}),
                'generated_at': exam_dimension_result.get('generated_at', '')
            }
            
            if school_code:
                combined_result['school_code'] = school_code
            
            return combined_result
            
        except Exception as e:
            return {'error': f'多层级维度汇聚失败: {str(e)}'}
    
    def get_complete_analysis(self, batch_code: str, school_code: str = None) -> Dict[str, Any]:
        """
        获取完整分析报告
        
        包含学科汇聚、维度汇聚和选项分布的完整报告
        """
        try:
            # 获取学科汇聚
            subject_analysis = self.aggregate_all_subjects(batch_code, school_code)
            
            # 获取维度汇聚  
            dimension_analysis = self.aggregate_all_dimensions(batch_code, school_code)
            
            # 获取学科对比（如果是区域层级）
            subject_comparison = None
            if not school_code:
                subject_comparison = self.exam_aggregator.get_subject_comparison(batch_code)
            
            # 构建完整报告
            complete_report = {
                'batch_code': batch_code,
                'aggregation_level': AggregationLevel.SCHOOL if school_code else AggregationLevel.REGION,
                'analysis_type': 'complete',
                'subject_analysis': subject_analysis.get('analysis', {}),
                'dimension_analysis': dimension_analysis.get('dimension_analysis', {}),
                'metadata': subject_analysis.get('metadata', {}),
                'generated_at': subject_analysis.get('generated_at', '')
            }
            
            if school_code:
                complete_report['school_code'] = school_code
            
            if subject_comparison and 'subject_comparison' in subject_comparison:
                complete_report['subject_comparison'] = subject_comparison['subject_comparison']
            
            return complete_report
            
        except Exception as e:
            return {'error': f'完整分析报告生成失败: {str(e)}'}
    
    def get_school_ranking(self, batch_code: str, subject_name: str = None) -> Dict[str, Any]:
        """
        获取学校排名分析（仅区域层级）
        
        Args:
            batch_code: 批次代码
            subject_name: 学科名称（可选，不指定则分析所有学科）
            
        Returns:
            学校排名分析结果
        """
        try:
            # 获取所有学校信息
            schools = self.exam_aggregator.get_school_info(batch_code)
            
            if not schools:
                return {'error': '未找到学校数据'}
            
            # 收集各学校的学科分析数据
            school_rankings = []
            
            for school_code, school_name, student_count in schools:
                # 获取该学校的学科汇聚结果
                school_analysis = self.aggregate_all_subjects(batch_code, school_code)
                
                if 'error' not in school_analysis:
                    exam_subjects = school_analysis['analysis']['exam_subjects']
                    questionnaire_subjects = school_analysis['analysis']['questionnaire_subjects']
                    
                    # 计算学校总体表现指标
                    school_performance = self._calculate_school_performance(
                        exam_subjects, questionnaire_subjects, subject_name
                    )
                    
                    school_rankings.append({
                        'school_code': school_code,
                        'school_name': school_name,
                        'student_count': student_count,
                        'performance': school_performance
                    })
            
            # 按平均得分率排序
            school_rankings.sort(
                key=lambda x: x['performance']['overall_score_rate'], 
                reverse=True
            )
            
            # 添加排名
            for i, school in enumerate(school_rankings):
                school['rank'] = i + 1
            
            return {
                'batch_code': batch_code,
                'aggregation_level': AggregationLevel.REGION,
                'analysis_type': 'school_ranking',
                'rankings': school_rankings,
                'total_schools': len(school_rankings),
                'subject_filter': subject_name,
                'generated_at': self.exam_aggregator.get_aggregation_metadata(batch_code)['aggregation_time']
            }
            
        except Exception as e:
            return {'error': f'学校排名分析失败: {str(e)}'}
    
    def _calculate_summary_stats(self, exam_subjects: Dict, questionnaire_subjects: Dict) -> Dict[str, Any]:
        """计算汇总统计信息"""
        try:
            total_exam_subjects = len(exam_subjects)
            total_questionnaire_subjects = len(questionnaire_subjects)
            total_students_exam = sum(s.get('student_count', 0) for s in exam_subjects.values())
            total_students_questionnaire = sum(s.get('student_count', 0) for s in questionnaire_subjects.values())
            
            # 计算平均得分率
            exam_score_rates = [s.get('score_rate', 0) for s in exam_subjects.values()]
            questionnaire_score_rates = [s.get('score_rate', 0) for s in questionnaire_subjects.values()]
            
            avg_exam_score_rate = sum(exam_score_rates) / len(exam_score_rates) if exam_score_rates else 0
            avg_questionnaire_score_rate = sum(questionnaire_score_rates) / len(questionnaire_score_rates) if questionnaire_score_rates else 0
            
            return {
                'exam_subjects_count': total_exam_subjects,
                'questionnaire_subjects_count': total_questionnaire_subjects,
                'total_subjects': total_exam_subjects + total_questionnaire_subjects,
                'exam_students_total': total_students_exam,
                'questionnaire_students_total': total_students_questionnaire,
                'avg_exam_score_rate': round(avg_exam_score_rate, 4),
                'avg_questionnaire_score_rate': round(avg_questionnaire_score_rate, 4)
            }
        except Exception:
            return {}
    
    def _calculate_school_performance(self, exam_subjects: Dict, questionnaire_subjects: Dict, 
                                    subject_name: str = None) -> Dict[str, Any]:
        """计算学校表现指标"""
        try:
            # 筛选指定学科
            if subject_name:
                exam_subjects = {k: v for k, v in exam_subjects.items() if k == subject_name}
                questionnaire_subjects = {k: v for k, v in questionnaire_subjects.items() if k == subject_name}
            
            # 收集得分率
            all_score_rates = []
            all_mean_scores = []
            
            for subject_stats in exam_subjects.values():
                all_score_rates.append(subject_stats.get('score_rate', 0))
                all_mean_scores.append(subject_stats.get('mean_score', 0))
            
            for subject_stats in questionnaire_subjects.values():
                all_score_rates.append(subject_stats.get('score_rate', 0))
                all_mean_scores.append(subject_stats.get('mean_score', 0))
            
            # 计算总体表现
            overall_score_rate = sum(all_score_rates) / len(all_score_rates) if all_score_rates else 0
            overall_mean_score = sum(all_mean_scores) / len(all_mean_scores) if all_mean_scores else 0
            
            return {
                'overall_score_rate': round(overall_score_rate, 4),
                'overall_mean_score': round(overall_mean_score, 2),
                'subjects_analyzed': len(all_score_rates)
            }
        except Exception:
            return {
                'overall_score_rate': 0,
                'overall_mean_score': 0,
                'subjects_analyzed': 0
            }
    
    def get_batch_overview(self, batch_code: str) -> Dict[str, Any]:
        """
        获取批次概览
        
        提供批次的整体数据概况
        """
        try:
            # 获取基础信息
            metadata = self.exam_aggregator.get_aggregation_metadata(batch_code)
            
            # 获取学科信息
            exam_subjects = self.exam_aggregator.get_subject_info(batch_code, SubjectType.EXAM)
            questionnaire_subjects = self.exam_aggregator.get_subject_info(batch_code, SubjectType.QUESTIONNAIRE)
            
            # 获取学校信息
            schools = self.exam_aggregator.get_school_info(batch_code)
            
            # 构建概览
            overview = {
                'batch_code': batch_code,
                'overview': {
                    'total_students': metadata['total_students'],
                    'total_schools': metadata['total_schools'],
                    'exam_subjects': len(exam_subjects),
                    'questionnaire_subjects': len(questionnaire_subjects),
                    'total_subjects': len(exam_subjects) + len(questionnaire_subjects)
                },
                'subject_details': {
                    'exam': [{'name': s[0], 'student_count': s[2]} for s in exam_subjects],
                    'questionnaire': [{'name': s[0], 'student_count': s[2]} for s in questionnaire_subjects]
                },
                'school_details': [
                    {'code': s[0], 'name': s[1], 'student_count': s[2]} for s in schools
                ],
                'generated_at': metadata['aggregation_time']
            }
            
            return overview
            
        except Exception as e:
            return {'error': f'批次概览生成失败: {str(e)}'}

def test_multi_layer_aggregator():
    """测试多层级汇聚器"""
    print("=== 测试多层级汇聚器 ===\n")
    
    aggregator = MultiLayerAggregator()
    
    # 1. 测试批次概览
    print("1. 批次概览 (G4-2025):")
    overview = aggregator.get_batch_overview('G4-2025')
    
    if 'error' not in overview:
        overview_data = overview['overview']
        print(f"   总学生数: {overview_data['total_students']}")
        print(f"   总学校数: {overview_data['total_schools']}")  
        print(f"   考试学科: {overview_data['exam_subjects']}个")
        print(f"   问卷学科: {overview_data['questionnaire_subjects']}个")
    else:
        print(f"   失败: {overview.get('error', '未知错误')}")
    
    # 2. 测试区域层级汇聚
    print("\n2. 区域层级多学科汇聚:")
    result = aggregator.aggregate_all_subjects('G4-2025')
    
    if 'error' not in result:
        exam_count = len(result['analysis']['exam_subjects'])
        questionnaire_count = len(result['analysis']['questionnaire_subjects'])
        summary = result.get('summary', {})
        
        print(f"   考试学科: {exam_count}个")
        print(f"   问卷学科: {questionnaire_count}个")
        print(f"   考试平均得分率: {summary.get('avg_exam_score_rate', 0):.2%}")
        print(f"   问卷平均得分率: {summary.get('avg_questionnaire_score_rate', 0):.2%}")
    else:
        print(f"   失败: {result.get('error', '未知错误')}")
    
    # 3. 测试学校层级汇聚
    print("\n3. 学校层级多学科汇聚:")
    schools = aggregator.exam_aggregator.get_school_info('G4-2025')
    
    if schools:
        school_code = schools[0][0]
        result = aggregator.aggregate_all_subjects('G4-2025', school_code)
        
        if 'error' not in result:
            exam_count = len(result['analysis']['exam_subjects'])
            questionnaire_count = len(result['analysis']['questionnaire_subjects'])
            print(f"   学校 {school_code}: 考试学科{exam_count}个, 问卷学科{questionnaire_count}个")
        else:
            print(f"   失败: {result.get('error', '未知错误')}")
    
    # 4. 测试维度汇聚
    print("\n4. 多层级维度汇聚:")
    result = aggregator.aggregate_all_dimensions('G4-2025')
    
    if 'error' not in result:
        exam_dims = len(result['dimension_analysis']['exam_dimensions'])
        questionnaire_dims = len(result['dimension_analysis']['questionnaire_dimensions'])  
        option_dims = len(result['dimension_analysis']['questionnaire_options'])
        
        print(f"   考试维度: {exam_dims}个学科")
        print(f"   问卷维度: {questionnaire_dims}个学科")
        print(f"   选项分布: {option_dims}个维度")
    else:
        print(f"   失败: {result.get('error', '未知错误')}")
    
    # 5. 测试学校排名
    print("\n5. 学校排名分析:")
    result = aggregator.get_school_ranking('G4-2025')
    
    if 'error' not in result and 'rankings' in result:
        rankings = result['rankings']
        print(f"   成功分析 {len(rankings)} 所学校")
        
        # 显示前3名
        for i, school in enumerate(rankings[:3]):
            performance = school['performance']
            print(f"   第{school['rank']}名: {school['school_name']} "
                 f"(得分率{performance['overall_score_rate']:.2%}, "
                 f"平均分{performance['overall_mean_score']})")
    else:
        print(f"   失败: {result.get('error', '未知错误')}")
    
    print("\n=== 多层级汇聚器测试完成 ===")

if __name__ == "__main__":
    test_multi_layer_aggregator()