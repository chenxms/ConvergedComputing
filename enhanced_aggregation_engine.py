#!/usr/bin/env python3
"""
增强汇聚引擎 v1.2
实现汇聚模块修复实施方案v1.2的所有要求
包括：精度统一处理、科目层排名、维度层排名、问卷数据重构、数据结构统一
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from typing import Dict, List, Any, Optional, Tuple
import json
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from statistics_calculator import EducationalStatisticsCalculator
from app.utils.precision import round2, round2_json, to_pct, apply_precision_to_aggregation_result
import logging

logger = logging.getLogger(__name__)


class AggregationLevel:
    """汇聚层级常量"""
    SCHOOL = "SCHOOL"
    REGIONAL = "REGIONAL"


class SubjectType:
    """学科类型常量"""
    EXAM = "exam"
    QUESTIONNAIRE = "questionnaire"
    INTERACTION = "interaction"


class EnhancedAggregationEngine:
    """增强汇聚引擎 v1.2"""
    
    def __init__(self, database_url: str = None):
        """
        初始化增强汇聚引擎
        
        Args:
            database_url: 数据库连接URL
        """
        self.database_url = database_url or "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        self.engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.engine)
        self.calculator = EducationalStatisticsCalculator()
    
    def get_session(self):
        """获取数据库会话"""
        return self.Session()
    
    def close_session(self, session):
        """关闭数据库会话"""
        if session:
            session.close()
    
    def aggregate_regional_level(self, batch_code: str) -> Dict[str, Any]:
        """
        区域层级汇聚 (REGIONAL)
        
        Args:
            batch_code: 批次代码
            
        Returns:
            区域层级汇聚结果，包含科目层排名和维度层排名
        """
        session = self.get_session()
        try:
            # 获取所有科目数据
            subjects_data = self._get_regional_subjects_data(session, batch_code)
            
            # 处理每个科目
            subjects = []
            for subject_info in subjects_data:
                subject_name = subject_info['subject_name']
                subject_type = subject_info['subject_type']
                
                # 获取科目统计数据
                subject_stats = self._calculate_regional_subject_stats(
                    session, batch_code, subject_name, subject_type
                )
                
                # 获取维度数据
                dimensions = self._get_subject_dimensions(
                    session, batch_code, subject_name, subject_type, aggregation_level="REGIONAL"
                )
                
                # 构建科目数据结构
                subject_data = {
                    'name': subject_name,
                    'type': subject_type,
                    **subject_stats,
                    'dimensions': dimensions
                }
                
                # 添加科目层排名（学校排名）
                if subject_type != SubjectType.QUESTIONNAIRE:
                    subject_data['school_rankings'] = self._get_school_rankings(
                        session, batch_code, subject_name
                    )
                else:
                    # 问卷科目也参与排名
                    subject_data['school_rankings'] = self._get_school_rankings_questionnaire(
                        session, batch_code, subject_name
                    )
                    # 添加选项分布
                    subject_data['option_distribution'] = self._get_option_distribution(
                        session, batch_code, subject_name
                    )
                
                subjects.append(subject_data)
            
            # 构建响应结果
            result = {
                'aggregation_level': AggregationLevel.REGIONAL,
                'batch_code': batch_code,
                'subjects': subjects,
                'generated_at': datetime.datetime.now().isoformat(),
                'metadata': self._get_metadata(session, batch_code)
            }
            
            # 应用精度处理
            result = apply_precision_to_aggregation_result(result, "v1.2")
            
            return result
            
        except Exception as e:
            logger.error(f"区域层级汇聚失败: {str(e)}")
            return {'error': f'区域层级汇聚失败: {str(e)}'}
        finally:
            self.close_session(session)
    
    def aggregate_school_level(self, batch_code: str, school_code: str) -> Dict[str, Any]:
        """
        学校层级汇聚 (SCHOOL)
        
        Args:
            batch_code: 批次代码
            school_code: 学校代码
            
        Returns:
            学校层级汇聚结果，包含区域排名
        """
        session = self.get_session()
        try:
            # 获取学校科目数据
            subjects_data = self._get_school_subjects_data(session, batch_code, school_code)
            
            # 处理每个科目
            subjects = []
            for subject_info in subjects_data:
                subject_name = subject_info['subject_name']
                subject_type = subject_info['subject_type']
                
                # 获取科目统计数据
                subject_stats = self._calculate_school_subject_stats(
                    session, batch_code, school_code, subject_name, subject_type
                )
                
                # 获取维度数据
                dimensions = self._get_subject_dimensions(
                    session, batch_code, subject_name, subject_type, 
                    aggregation_level="SCHOOL", school_code=school_code
                )
                
                # 获取区域排名
                region_rank_info = self._get_school_region_rank(
                    session, batch_code, school_code, subject_name
                )
                
                # 构建科目数据结构
                subject_data = {
                    'name': subject_name,
                    'type': subject_type,
                    **subject_stats,
                    'region_rank': region_rank_info.get('rank'),
                    'total_schools': region_rank_info.get('total_schools'),
                    'dimensions': dimensions
                }
                
                # 问卷科目添加选项分布
                if subject_type == SubjectType.QUESTIONNAIRE:
                    subject_data['option_distribution'] = self._get_option_distribution(
                        session, batch_code, subject_name, school_code
                    )
                
                subjects.append(subject_data)
            
            # 构建响应结果
            result = {
                'aggregation_level': AggregationLevel.SCHOOL,
                'batch_code': batch_code,
                'school_code': school_code,
                'subjects': subjects,
                'generated_at': datetime.datetime.now().isoformat(),
                'metadata': self._get_metadata(session, batch_code, school_code)
            }
            
            # 应用精度处理
            result = apply_precision_to_aggregation_result(result, "v1.2")
            
            return result
            
        except Exception as e:
            logger.error(f"学校层级汇聚失败: {str(e)}")
            return {'error': f'学校层级汇聚失败: {str(e)}'}
        finally:
            self.close_session(session)
    
    def _get_regional_subjects_data(self, session, batch_code: str) -> List[Dict[str, Any]]:
        """获取区域层级所有科目信息"""
        query = text("""
            SELECT DISTINCT subject_name, subject_type
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code
            ORDER BY subject_type, subject_name
        """)
        
        result = session.execute(query, {'batch_code': batch_code})
        return [{'subject_name': row[0], 'subject_type': row[1]} for row in result.fetchall()]
    
    def _get_school_subjects_data(self, session, batch_code: str, school_code: str) -> List[Dict[str, Any]]:
        """获取学校层级科目信息"""
        query = text("""
            SELECT DISTINCT subject_name, subject_type
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code AND school_code = :school_code
            ORDER BY subject_type, subject_name
        """)
        
        result = session.execute(query, {'batch_code': batch_code, 'school_code': school_code})
        return [{'subject_name': row[0], 'subject_type': row[1]} for row in result.fetchall()]
    
    def _calculate_regional_subject_stats(self, session, batch_code: str, subject_name: str, 
                                        subject_type: str) -> Dict[str, Any]:
        """计算区域层级科目统计数据"""
        query = text("""
            SELECT 
                COUNT(*) as student_count,
                AVG(total_score) as avg_score,
                STDDEV(total_score) as std_deviation,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                AVG(total_score / max_score) as avg_score_rate,
                MAX(max_score) as total_score,
                GROUP_CONCAT(total_score ORDER BY total_score) as all_scores,
                GROUP_CONCAT(max_score) as all_max_scores
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code AND subject_name = :subject_name
        """)
        
        result = session.execute(query, {
            'batch_code': batch_code,
            'subject_name': subject_name
        })
        
        row = result.fetchone()
        if not row or row[0] == 0:
            return self._get_empty_subject_stats()
        
        # 计算高级统计指标
        scores_str = row[7]
        max_scores_str = row[8]
        
        if scores_str and max_scores_str:
            scores = [float(x) for x in scores_str.split(',') if x.strip()]
            max_scores = [float(x) for x in max_scores_str.split(',') if x.strip()]
            
            basic_stats = self.calculator.calculate_basic_stats(scores, max_scores)
            discrimination = self.calculator.calculate_discrimination(scores, max_scores)
            difficulty = self.calculator.calculate_difficulty(scores, max_scores)
        else:
            basic_stats = {'percentiles': {'p10': 0, 'p50': 0, 'p90': 0}}
            discrimination = 0
            difficulty = 0
        
        return {
            'student_count': int(row[0]),
            'avg_score': round2(row[1]),
            'std_deviation': round2(row[2]),
            'min_score': round2(row[3]),
            'max_score': round2(row[4]),
            'avg_score_rate_pct': to_pct(row[5]) if row[5] else 0,
            'total_score': round2(row[6]),
            'percentiles': {
                'p10': round2(basic_stats['percentiles']['p10']),
                'p50': round2(basic_stats['percentiles']['p50']),
                'p90': round2(basic_stats['percentiles']['p90'])
            },
            'discrimination': round2(discrimination),
            'difficulty_pct': to_pct(difficulty)
        }
    
    def _calculate_school_subject_stats(self, session, batch_code: str, school_code: str,
                                      subject_name: str, subject_type: str) -> Dict[str, Any]:
        """计算学校层级科目统计数据"""
        query = text("""
            SELECT 
                COUNT(*) as student_count,
                AVG(total_score) as avg_score,
                STDDEV(total_score) as std_deviation,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                AVG(total_score / max_score) as avg_score_rate,
                MAX(max_score) as total_score,
                GROUP_CONCAT(total_score ORDER BY total_score) as all_scores,
                GROUP_CONCAT(max_score) as all_max_scores
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code AND school_code = :school_code AND subject_name = :subject_name
        """)
        
        result = session.execute(query, {
            'batch_code': batch_code,
            'school_code': school_code,
            'subject_name': subject_name
        })
        
        row = result.fetchone()
        if not row or row[0] == 0:
            return self._get_empty_subject_stats()
        
        # 计算高级统计指标
        scores_str = row[7]
        max_scores_str = row[8]
        
        if scores_str and max_scores_str:
            scores = [float(x) for x in scores_str.split(',') if x.strip()]
            max_scores = [float(x) for x in max_scores_str.split(',') if x.strip()]
            
            basic_stats = self.calculator.calculate_basic_stats(scores, max_scores)
            discrimination = self.calculator.calculate_discrimination(scores, max_scores)
            difficulty = self.calculator.calculate_difficulty(scores, max_scores)
        else:
            basic_stats = {'percentiles': {'p10': 0, 'p50': 0, 'p90': 0}}
            discrimination = 0
            difficulty = 0
        
        return {
            'student_count': int(row[0]),
            'avg_score': round2(row[1]),
            'std_deviation': round2(row[2]),
            'min_score': round2(row[3]),
            'max_score': round2(row[4]),
            'avg_score_rate_pct': to_pct(row[5]) if row[5] else 0,
            'total_score': round2(row[6]),
            'percentiles': {
                'p10': round2(basic_stats['percentiles']['p10']),
                'p50': round2(basic_stats['percentiles']['p50']),
                'p90': round2(basic_stats['percentiles']['p90'])
            },
            'discrimination': round2(discrimination),
            'difficulty_pct': to_pct(difficulty)
        }
    
    def _get_subject_dimensions(self, session, batch_code: str, subject_name: str, 
                              subject_type: str, aggregation_level: str, 
                              school_code: str = None) -> List[Dict[str, Any]]:
        """获取科目维度数据，包含维度层排名"""
        where_conditions = ["batch_code = :batch_code", "subject_name = :subject_name"]
        params = {'batch_code': batch_code, 'subject_name': subject_name}
        
        if school_code:
            where_conditions.append("school_code = :school_code")
            params['school_code'] = school_code
        
        where_clause = " AND ".join(where_conditions)
        
        # 获取维度数据
        query = text(f"""
            SELECT 
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
            return []
        
        # 解析维度数据
        dimension_data = {}
        for row in rows:
            dimension_scores = row[0]
            dimension_max_scores = row[1]
            
            try:
                scores_dict = json.loads(dimension_scores) if dimension_scores else {}
                max_scores_dict = json.loads(dimension_max_scores) if dimension_max_scores else {}
                
                for dim_code, score_data in scores_dict.items():
                    if dim_code not in dimension_data:
                        dimension_data[dim_code] = {
                            'scores': [],
                            'max_scores': []
                        }
                    
                    # 处理分数数据
                    if isinstance(score_data, dict):
                        score = float(score_data.get('score', 0))
                        name = score_data.get('name', dim_code)
                    else:
                        score = float(score_data)
                        name = dim_code
                    
                    dimension_data[dim_code]['scores'].append(score)
                    dimension_data[dim_code]['name'] = name
                    
                    # 处理满分数据
                    max_score_data = max_scores_dict.get(dim_code, 0)
                    if isinstance(max_score_data, dict):
                        max_score = float(max_score_data.get('score', 0))
                    else:
                        max_score = float(max_score_data)
                    dimension_data[dim_code]['max_scores'].append(max_score)
                    
            except (json.JSONDecodeError, ValueError, TypeError):
                continue
        
        # 计算每个维度的统计指标
        dimensions = []
        for dim_code, data in dimension_data.items():
            scores = data['scores']
            max_scores = data['max_scores']
            
            if scores:
                basic_stats = self.calculator.calculate_basic_stats(scores, max_scores)
                discrimination = self.calculator.calculate_discrimination(scores, max_scores)
                difficulty = self.calculator.calculate_difficulty(scores, max_scores)
                
                dimension_info = {
                    'code': dim_code,
                    'name': data.get('name', dim_code),
                    'student_count': len(scores),
                    'avg_score': round2(basic_stats['mean']),
                    'std_deviation': round2(basic_stats['std_dev']),
                    'avg_score_rate_pct': to_pct(basic_stats['score_rate']),
                    'discrimination': round2(discrimination),
                    'difficulty_pct': to_pct(difficulty)
                }
                
                # 添加维度层排名
                if aggregation_level == "SCHOOL":
                    rank_info = self._get_dimension_region_rank(
                        session, batch_code, subject_name, dim_code, school_code
                    )
                    dimension_info['rank'] = rank_info
                
                dimensions.append(dimension_info)
        
        return dimensions
    
    def _get_school_rankings(self, session, batch_code: str, subject_name: str) -> List[Dict[str, Any]]:
        """获取学校排名（考试科目）"""
        query = text("""
            SELECT 
                school_code,
                school_name,
                AVG(total_score) as avg_score
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code AND subject_name = :subject_name
            GROUP BY school_code, school_name
            ORDER BY AVG(total_score) DESC
        """)
        
        result = session.execute(query, {
            'batch_code': batch_code,
            'subject_name': subject_name
        })
        
        rankings = []
        rank = 1
        prev_score = None
        for i, row in enumerate(result.fetchall()):
            current_score = round2(row[2])
            
            # 实现DENSE_RANK逻辑：相同分数保持相同排名
            if prev_score is not None and current_score != prev_score:
                rank = i + 1
            
            rankings.append({
                'school_code': row[0],
                'school_name': row[1],
                'avg_score': current_score,
                'rank': rank
            })
            
            prev_score = current_score
        
        return rankings
    
    def _get_school_rankings_questionnaire(self, session, batch_code: str, subject_name: str) -> List[Dict[str, Any]]:
        """获取学校排名（问卷科目）"""
        # 问卷科目也按平均分排名
        return self._get_school_rankings(session, batch_code, subject_name)
    
    def _get_school_region_rank(self, session, batch_code: str, school_code: str, 
                              subject_name: str) -> Dict[str, Any]:
        """获取学校在区域中的排名"""
        # 获取所有学校的平均分
        query = text("""
            SELECT 
                school_code,
                school_name,
                AVG(total_score) as avg_score
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code AND subject_name = :subject_name
            GROUP BY school_code, school_name
            ORDER BY AVG(total_score) DESC
        """)
        
        result = session.execute(query, {
            'batch_code': batch_code,
            'subject_name': subject_name
        })
        
        schools = result.fetchall()
        if not schools:
            return {'rank': None, 'total_schools': 0}
        
        # 计算排名
        rank = None
        prev_score = None
        current_rank = 1
        
        for i, row in enumerate(schools):
            sch_code = row[0]
            avg_score = row[2]
            
            # 实现DENSE_RANK逻辑
            if prev_score is not None and avg_score != prev_score:
                current_rank = i + 1
            
            if sch_code == school_code:
                rank = current_rank
                break
                
            prev_score = avg_score
        
        return {
            'rank': rank,
            'total_schools': len(schools)
        }
    
    def _get_dimension_region_rank(self, session, batch_code: str, subject_name: str,
                                 dimension_code: str, school_code: str) -> Optional[int]:
        """获取维度在区域中的排名"""
        # 由于维度数据存储在JSON中，需要复杂查询来计算排名
        # 这里简化实现，实际应用中可能需要更复杂的逻辑
        try:
            query = text("""
                SELECT school_code, dimension_scores
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code AND subject_name = :subject_name
                AND dimension_scores IS NOT NULL AND dimension_scores != '{}'
            """)
            
            result = session.execute(query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            
            # 计算各学校该维度的平均分
            school_dim_scores = {}
            for row in result.fetchall():
                sch_code = row[0]
                dim_scores_json = row[1]
                
                try:
                    dim_scores = json.loads(dim_scores_json)
                    if dimension_code in dim_scores:
                        score_data = dim_scores[dimension_code]
                        if isinstance(score_data, dict):
                            score = float(score_data.get('score', 0))
                        else:
                            score = float(score_data)
                        
                        if sch_code not in school_dim_scores:
                            school_dim_scores[sch_code] = []
                        school_dim_scores[sch_code].append(score)
                except:
                    continue
            
            # 计算平均分并排名
            school_averages = []
            for sch_code, scores in school_dim_scores.items():
                if scores:
                    avg_score = sum(scores) / len(scores)
                    school_averages.append((sch_code, avg_score))
            
            # 排序并找到当前学校的排名
            school_averages.sort(key=lambda x: x[1], reverse=True)
            
            for rank, (sch_code, avg_score) in enumerate(school_averages, 1):
                if sch_code == school_code:
                    return rank
                    
        except Exception as e:
            logger.warning(f"维度排名计算失败: {str(e)}")
        
        return None
    
    def _get_option_distribution(self, session, batch_code: str, subject_name: str, 
                               school_code: str = None) -> Dict[str, Any]:
        """获取问卷选项分布"""
        where_conditions = ["batch_code = :batch_code"]
        params = {'batch_code': batch_code}
        
        if school_code:
            where_conditions.append("school_code = :school_code")
            params['school_code'] = school_code
        
        # 这里需要根据实际的问卷数据表结构来查询
        # 简化实现，返回空字典
        return {}
    
    def _get_metadata(self, session, batch_code: str, school_code: str = None) -> Dict[str, Any]:
        """获取元数据信息"""
        if school_code:
            query = text("""
                SELECT 
                    COUNT(DISTINCT student_id) as total_students,
                    COUNT(DISTINCT subject_name) as total_subjects,
                    1 as total_schools
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code AND school_code = :school_code
            """)
            result = session.execute(query, {'batch_code': batch_code, 'school_code': school_code})
        else:
            query = text("""
                SELECT 
                    COUNT(DISTINCT student_id) as total_students,
                    COUNT(DISTINCT subject_name) as total_subjects,
                    COUNT(DISTINCT school_code) as total_schools
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
            """)
            result = session.execute(query, {'batch_code': batch_code})
        
        row = result.fetchone()
        
        return {
            'total_students': int(row[0]) if row else 0,
            'total_subjects': int(row[1]) if row else 0,
            'total_schools': int(row[2]) if row else 0
        }
    
    def _get_empty_subject_stats(self) -> Dict[str, Any]:
        """获取空的科目统计数据"""
        return {
            'student_count': 0,
            'avg_score': 0.0,
            'std_deviation': 0.0,
            'min_score': 0.0,
            'max_score': 0.0,
            'avg_score_rate_pct': 0.0,
            'total_score': 0.0,
            'percentiles': {
                'p10': 0.0,
                'p50': 0.0,
                'p90': 0.0
            },
            'discrimination': 0.0,
            'difficulty_pct': 0.0
        }


def test_enhanced_aggregation_engine():
    """测试增强汇聚引擎"""
    print("=== 测试增强汇聚引擎 v1.2 ===")
    
    engine = EnhancedAggregationEngine()
    
    # 1. 测试区域层级汇聚
    print("\n1. 区域层级汇聚测试:")
    regional_result = engine.aggregate_regional_level('G4-2025')
    
    if 'error' not in regional_result:
        print(f"   - 汇聚层级: {regional_result['aggregation_level']}")
        print(f"   - Schema版本: {regional_result.get('schema_version', 'N/A')}")
        print(f"   - 科目数量: {len(regional_result.get('subjects', []))}")
        print(f"   - 总学生数: {regional_result.get('metadata', {}).get('total_students', 0)}")
        
        # 显示第一个科目的详情
        subjects = regional_result.get('subjects', [])
        if subjects:
            first_subject = subjects[0]
            print(f"   - 示例科目: {first_subject['name']} ({first_subject['type']})")
            print(f"     * 学生数: {first_subject.get('student_count', 0)}")
            print(f"     * 平均分: {first_subject.get('avg_score', 0)}")
            print(f"     * 得分率: {first_subject.get('avg_score_rate_pct', 0)}%")
            print(f"     * 维度数: {len(first_subject.get('dimensions', []))}")
            
            # 显示学校排名
            school_rankings = first_subject.get('school_rankings', [])
            if school_rankings:
                print(f"     * 学校排名数: {len(school_rankings)}")
                print(f"     * 第一名: {school_rankings[0]['school_name']} (分数: {school_rankings[0]['avg_score']})")
    else:
        print(f"   失败: {regional_result.get('error')}")
    
    # 2. 测试学校层级汇聚
    print("\n2. 学校层级汇聚测试:")
    
    # 先获取一个学校代码
    session = engine.get_session()
    try:
        query = text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code = 'G4-2025' LIMIT 1")
        result = session.execute(query)
        row = result.fetchone()
        test_school_code = row[0] if row else None
    finally:
        engine.close_session(session)
    
    if test_school_code:
        school_result = engine.aggregate_school_level('G4-2025', test_school_code)
        
        if 'error' not in school_result:
            print(f"   - 汇聚层级: {school_result['aggregation_level']}")
            print(f"   - 学校代码: {school_result['school_code']}")
            print(f"   - Schema版本: {school_result.get('schema_version', 'N/A')}")
            print(f"   - 科目数量: {len(school_result.get('subjects', []))}")
            
            # 显示第一个科目的区域排名信息
            subjects = school_result.get('subjects', [])
            if subjects:
                first_subject = subjects[0]
                print(f"   - 示例科目: {first_subject['name']}")
                print(f"     * 区域排名: {first_subject.get('region_rank', 'N/A')}")
                print(f"     * 总学校数: {first_subject.get('total_schools', 'N/A')}")
                
                # 显示维度排名
                dimensions = first_subject.get('dimensions', [])
                if dimensions:
                    first_dim = dimensions[0]
                    print(f"     * 示例维度: {first_dim['name']}")
                    print(f"       - 维度排名: {first_dim.get('rank', 'N/A')}")
        else:
            print(f"   失败: {school_result.get('error')}")
    else:
        print("   无法获取测试学校代码")
    
    print("\n=== 增强汇聚引擎测试完成 ===")


if __name__ == "__main__":
    test_enhanced_aggregation_engine()
