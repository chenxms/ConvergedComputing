#!/usr/bin/env python3
"""
简化版汇聚计算引擎
"""
import asyncio
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
import numpy as np
from decimal import Decimal, ROUND_HALF_UP

from simplified_aggregation_models import (
    SubjectStats, SubjectDimensionStats,
    SchoolRanking, SchoolSubjectStats,
    QuestionnaireStats, QuestionnaireDimensionStats,
    QuestionnaireOption, QuestionnaireQuestionStats,
    RegionAggregationData, SchoolAggregationData,
    get_option_label, round_to_2
)


class SimplifiedAggregationEngine:
    """简化版汇聚计算引擎"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        
    async def aggregate_region(self, batch_code: str, grade_code: str, 
                              region_code: str) -> RegionAggregationData:
        """区域级汇聚"""
        print(f"开始区域级汇聚: {batch_code} - {grade_code} - {region_code}")
        
        # 获取区域信息
        region_name = await self._get_region_name(region_code)
        
        # 获取考试科目列表
        exam_subjects = await self._get_exam_subjects(batch_code, grade_code)
        
        # 计算每个科目的统计
        subjects = []
        school_rankings = {}
        
        for subject_code, subject_name in exam_subjects:
            # 计算科目统计
            subject_stats = await self._calculate_subject_stats_region(
                batch_code, grade_code, region_code, subject_code, subject_name
            )
            subjects.append(subject_stats)
            
            # 计算学校排名
            rankings = await self._calculate_school_rankings(
                batch_code, grade_code, region_code, subject_code
            )
            school_rankings[subject_code] = rankings
        
        # 获取问卷统计
        questionnaires = await self._calculate_questionnaire_stats_region(
            batch_code, grade_code, region_code
        )
        
        return RegionAggregationData(
            batch_code=batch_code,
            grade_code=grade_code,
            region_code=region_code,
            region_name=region_name,
            subjects=subjects,
            school_rankings=school_rankings,
            questionnaires=questionnaires
        )
    
    async def aggregate_school(self, batch_code: str, grade_code: str,
                              school_code: str) -> SchoolAggregationData:
        """学校级汇聚"""
        print(f"开始学校级汇聚: {batch_code} - {grade_code} - {school_code}")
        
        # 获取学校和区域信息
        school_info = await self._get_school_info(school_code)
        school_name = school_info['school_name']
        region_code = school_info['region_code']
        region_name = school_info['region_name']
        
        # 获取考试科目列表
        exam_subjects = await self._get_exam_subjects(batch_code, grade_code)
        
        # 计算每个科目的统计
        subjects = []
        
        for subject_code, subject_name in exam_subjects:
            # 计算科目统计
            subject_stats = await self._calculate_subject_stats_school(
                batch_code, grade_code, school_code, subject_code, subject_name
            )
            
            # 获取在区域中的排名
            region_rank = await self._get_school_rank_in_region(
                batch_code, grade_code, region_code, school_code, subject_code
            )
            
            # 创建带排名的学校科目统计
            school_subject_stats = SchoolSubjectStats(
                subject_name=subject_stats.subject_name,
                subject_code=subject_stats.subject_code,
                average_score=subject_stats.average_score,
                max_score=subject_stats.max_score,
                min_score=subject_stats.min_score,
                std_deviation=subject_stats.std_deviation,
                difficulty=subject_stats.difficulty,
                discrimination=subject_stats.discrimination,
                p10=subject_stats.p10,
                p50=subject_stats.p50,
                p90=subject_stats.p90,
                dimensions=subject_stats.dimensions,
                region_rank=region_rank
            )
            subjects.append(school_subject_stats)
        
        # 获取问卷统计
        questionnaires = await self._calculate_questionnaire_stats_school(
            batch_code, grade_code, school_code
        )
        
        return SchoolAggregationData(
            batch_code=batch_code,
            grade_code=grade_code,
            school_code=school_code,
            school_name=school_name,
            region_code=region_code,
            region_name=region_name,
            subjects=subjects,
            questionnaires=questionnaires
        )
    
    async def _calculate_subject_stats_region(self, batch_code: str, grade_code: str,
                                             region_code: str, subject_code: str,
                                             subject_name: str) -> SubjectStats:
        """计算区域级科目统计"""
        # 获取学生分数数据
        query = text("""
            SELECT 
                ssd.student_id,
                ssd.total_score,
                sqc.max_score as full_score
            FROM student_score_detail ssd
            JOIN subject_question_config sqc ON ssd.subject_code = sqc.subject_code
            WHERE ssd.batch_code = :batch_code
            AND ssd.grade_code = :grade_code
            AND ssd.subject_code = :subject_code
            AND ssd.region_code = :region_code
            AND ssd.is_valid = 1
        """)
        
        result = await self.session.execute(query, {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'subject_code': subject_code,
            'region_code': region_code
        })
        
        rows = result.fetchall()
        if not rows:
            return SubjectStats(
                subject_name=subject_name,
                subject_code=subject_code,
                average_score=0, max_score=0, min_score=0,
                std_deviation=0, difficulty=0, discrimination=0,
                p10=0, p50=0, p90=0
            )
        
        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=['student_id', 'total_score', 'full_score'])
        full_score = float(df['full_score'].iloc[0])
        
        # 基础统计
        scores = df['total_score'].values
        average_score = float(np.mean(scores))
        max_score = float(np.max(scores))
        min_score = float(np.min(scores))
        std_deviation = float(np.std(scores))
        
        # 难度系数
        difficulty = average_score / full_score if full_score > 0 else 0
        
        # 区分度（前27%和后27%的差异）
        discrimination = self._calculate_discrimination(scores, full_score)
        
        # 百分位数
        p10 = float(np.percentile(scores, 10))
        p50 = float(np.percentile(scores, 50))
        p90 = float(np.percentile(scores, 90))
        
        # 计算维度统计
        dimensions = await self._calculate_dimension_stats(
            batch_code, grade_code, region_code, subject_code, None
        )
        
        return SubjectStats(
            subject_name=subject_name,
            subject_code=subject_code,
            average_score=average_score,
            max_score=max_score,
            min_score=min_score,
            std_deviation=std_deviation,
            difficulty=difficulty,
            discrimination=discrimination,
            p10=p10,
            p50=p50,
            p90=p90,
            dimensions=dimensions
        )
    
    async def _calculate_subject_stats_school(self, batch_code: str, grade_code: str,
                                             school_code: str, subject_code: str,
                                             subject_name: str) -> SubjectStats:
        """计算学校级科目统计"""
        # 获取学生分数数据
        query = text("""
            SELECT 
                ssd.student_id,
                ssd.total_score,
                sqc.max_score as full_score
            FROM student_score_detail ssd
            JOIN subject_question_config sqc ON ssd.subject_code = sqc.subject_code
            WHERE ssd.batch_code = :batch_code
            AND ssd.grade_code = :grade_code
            AND ssd.subject_code = :subject_code
            AND ssd.school_code = :school_code
            AND ssd.is_valid = 1
        """)
        
        result = await self.session.execute(query, {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'subject_code': subject_code,
            'school_code': school_code
        })
        
        rows = result.fetchall()
        if not rows:
            return SubjectStats(
                subject_name=subject_name,
                subject_code=subject_code,
                average_score=0, max_score=0, min_score=0,
                std_deviation=0, difficulty=0, discrimination=0,
                p10=0, p50=0, p90=0
            )
        
        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=['student_id', 'total_score', 'full_score'])
        full_score = float(df['full_score'].iloc[0])
        
        # 基础统计
        scores = df['total_score'].values
        average_score = float(np.mean(scores))
        max_score = float(np.max(scores))
        min_score = float(np.min(scores))
        std_deviation = float(np.std(scores))
        
        # 难度系数
        difficulty = average_score / full_score if full_score > 0 else 0
        
        # 区分度
        discrimination = self._calculate_discrimination(scores, full_score)
        
        # 百分位数
        p10 = float(np.percentile(scores, 10))
        p50 = float(np.percentile(scores, 50))
        p90 = float(np.percentile(scores, 90))
        
        # 计算维度统计
        dimensions = await self._calculate_dimension_stats(
            batch_code, grade_code, None, subject_code, school_code
        )
        
        return SubjectStats(
            subject_name=subject_name,
            subject_code=subject_code,
            average_score=average_score,
            max_score=max_score,
            min_score=min_score,
            std_deviation=std_deviation,
            difficulty=difficulty,
            discrimination=discrimination,
            p10=p10,
            p50=p50,
            p90=p90,
            dimensions=dimensions
        )
    
    def _calculate_discrimination(self, scores: np.ndarray, full_score: float) -> float:
        """计算区分度"""
        if len(scores) < 4:
            return 0
        
        # 排序
        sorted_scores = np.sort(scores)
        
        # 计算前27%和后27%的分界
        n = len(sorted_scores)
        top_n = int(n * 0.27)
        bottom_n = int(n * 0.27)
        
        # 前27%和后27%的平均分
        top_mean = np.mean(sorted_scores[-top_n:])
        bottom_mean = np.mean(sorted_scores[:bottom_n])
        
        # 区分度 = (高分组平均分 - 低分组平均分) / 满分
        discrimination = (top_mean - bottom_mean) / full_score if full_score > 0 else 0
        
        return float(discrimination)
    
    async def _calculate_dimension_stats(self, batch_code: str, grade_code: str,
                                        region_code: Optional[str], subject_code: str,
                                        school_code: Optional[str]) -> List[SubjectDimensionStats]:
        """计算维度统计"""
        # 构建查询条件
        where_conditions = [
            "ssd.batch_code = :batch_code",
            "ssd.grade_code = :grade_code",
            "ssd.subject_code = :subject_code",
            "ssd.is_valid = 1"
        ]
        
        params = {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'subject_code': subject_code
        }
        
        if region_code:
            where_conditions.append("ssd.region_code = :region_code")
            params['region_code'] = region_code
        
        if school_code:
            where_conditions.append("ssd.school_code = :school_code")
            params['school_code'] = school_code
        
        # 查询维度统计
        query = text(f"""
            SELECT 
                qdm.dimension_name,
                AVG(ssd.question_score) as avg_score,
                AVG(sqc.question_score) as dimension_full_score
            FROM student_score_detail ssd
            JOIN question_dimension_mapping qdm ON ssd.question_code = qdm.question_code
            JOIN subject_question_config sqc ON ssd.question_code = sqc.question_code
            WHERE {' AND '.join(where_conditions)}
            GROUP BY qdm.dimension_name
        """)
        
        result = await self.session.execute(query, params)
        rows = result.fetchall()
        
        dimensions = []
        for row in rows:
            dimension_name = row[0]
            avg_score = float(row[1])
            full_score = float(row[2])
            score_rate = avg_score / full_score if full_score > 0 else 0
            
            dimensions.append(SubjectDimensionStats(
                dimension_name=dimension_name,
                average_score=avg_score,
                score_rate=score_rate
            ))
        
        return dimensions
    
    async def _calculate_school_rankings(self, batch_code: str, grade_code: str,
                                        region_code: str, subject_code: str) -> List[SchoolRanking]:
        """计算学校排名"""
        query = text("""
            SELECT 
                ssd.school_code,
                sch.school_name,
                AVG(ssd.total_score) as avg_score
            FROM student_score_detail ssd
            JOIN school sch ON ssd.school_code = sch.school_code
            WHERE ssd.batch_code = :batch_code
            AND ssd.grade_code = :grade_code
            AND ssd.subject_code = :subject_code
            AND ssd.region_code = :region_code
            AND ssd.is_valid = 1
            GROUP BY ssd.school_code, sch.school_name
            ORDER BY avg_score DESC
        """)
        
        result = await self.session.execute(query, {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'subject_code': subject_code,
            'region_code': region_code
        })
        
        rows = result.fetchall()
        
        rankings = []
        for rank, row in enumerate(rows, 1):
            rankings.append(SchoolRanking(
                school_code=row[0],
                school_name=row[1],
                average_score=float(row[2]),
                rank=rank
            ))
        
        return rankings
    
    async def _get_school_rank_in_region(self, batch_code: str, grade_code: str,
                                        region_code: str, school_code: str,
                                        subject_code: str) -> int:
        """获取学校在区域中的排名"""
        rankings = await self._calculate_school_rankings(
            batch_code, grade_code, region_code, subject_code
        )
        
        for ranking in rankings:
            if ranking.school_code == school_code:
                return ranking.rank
        
        return 0  # 未找到排名
    
    async def _calculate_questionnaire_stats_region(self, batch_code: str, grade_code: str,
                                                   region_code: str) -> List[QuestionnaireStats]:
        """计算区域级问卷统计"""
        # 获取问卷列表
        query = text("""
            SELECT DISTINCT subject_code, subject_name
            FROM subject_question_config
            WHERE batch_code = :batch_code
            AND grade_code = :grade_code
            AND subject_type = 'questionnaire'
        """)
        
        result = await self.session.execute(query, {
            'batch_code': batch_code,
            'grade_code': grade_code
        })
        
        questionnaires = []
        for row in result:
            q_code = row[0]
            q_name = row[1]
            
            # 计算问卷维度统计
            dimensions = await self._calculate_questionnaire_dimensions(
                batch_code, grade_code, region_code, None, q_code
            )
            
            questionnaires.append(QuestionnaireStats(
                questionnaire_name=q_name,
                questionnaire_code=q_code,
                dimensions=dimensions
            ))
        
        return questionnaires
    
    async def _calculate_questionnaire_stats_school(self, batch_code: str, grade_code: str,
                                                   school_code: str) -> List[QuestionnaireStats]:
        """计算学校级问卷统计"""
        # 获取问卷列表
        query = text("""
            SELECT DISTINCT subject_code, subject_name
            FROM subject_question_config
            WHERE batch_code = :batch_code
            AND grade_code = :grade_code
            AND subject_type = 'questionnaire'
        """)
        
        result = await self.session.execute(query, {
            'batch_code': batch_code,
            'grade_code': grade_code
        })
        
        questionnaires = []
        for row in result:
            q_code = row[0]
            q_name = row[1]
            
            # 计算问卷维度统计
            dimensions = await self._calculate_questionnaire_dimensions(
                batch_code, grade_code, None, school_code, q_code
            )
            
            questionnaires.append(QuestionnaireStats(
                questionnaire_name=q_name,
                questionnaire_code=q_code,
                dimensions=dimensions
            ))
        
        return questionnaires
    
    async def _calculate_questionnaire_dimensions(self, batch_code: str, grade_code: str,
                                                 region_code: Optional[str], 
                                                 school_code: Optional[str],
                                                 questionnaire_code: str) -> List[QuestionnaireDimensionStats]:
        """计算问卷维度统计"""
        # 构建查询条件
        where_conditions = [
            "ssd.batch_code = :batch_code",
            "ssd.grade_code = :grade_code",
            "ssd.subject_code = :questionnaire_code",
            "ssd.is_valid = 1"
        ]
        
        params = {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'questionnaire_code': questionnaire_code
        }
        
        if region_code:
            where_conditions.append("ssd.region_code = :region_code")
            params['region_code'] = region_code
        
        if school_code:
            where_conditions.append("ssd.school_code = :school_code")
            params['school_code'] = school_code
        
        # 获取维度列表
        dim_query = text(f"""
            SELECT DISTINCT qdm.dimension_name
            FROM question_dimension_mapping qdm
            JOIN student_score_detail ssd ON qdm.question_code = ssd.question_code
            WHERE {' AND '.join(where_conditions)}
        """)
        
        result = await self.session.execute(dim_query, params)
        dimension_names = [row[0] for row in result]
        
        dimensions = []
        for dim_name in dimension_names:
            # 计算维度得分率
            score_rate = await self._calculate_dimension_score_rate(
                batch_code, grade_code, region_code, school_code,
                questionnaire_code, dim_name
            )
            
            # 计算维度选项分布
            option_distribution = await self._calculate_dimension_option_distribution(
                batch_code, grade_code, region_code, school_code,
                questionnaire_code, dim_name
            )
            
            # 获取维度下的题目统计
            questions = await self._calculate_dimension_questions(
                batch_code, grade_code, region_code, school_code,
                questionnaire_code, dim_name
            )
            
            dimensions.append(QuestionnaireDimensionStats(
                dimension_name=dim_name,
                score_rate=score_rate,
                option_distribution=option_distribution,
                questions=questions
            ))
        
        return dimensions
    
    async def _calculate_dimension_score_rate(self, batch_code: str, grade_code: str,
                                             region_code: Optional[str], school_code: Optional[str],
                                             questionnaire_code: str, dimension_name: str) -> float:
        """计算维度得分率"""
        # 构建查询条件
        where_conditions = [
            "ssd.batch_code = :batch_code",
            "ssd.grade_code = :grade_code",
            "ssd.subject_code = :questionnaire_code",
            "qdm.dimension_name = :dimension_name",
            "ssd.is_valid = 1"
        ]
        
        params = {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'questionnaire_code': questionnaire_code,
            'dimension_name': dimension_name
        }
        
        if region_code:
            where_conditions.append("ssd.region_code = :region_code")
            params['region_code'] = region_code
        
        if school_code:
            where_conditions.append("ssd.school_code = :school_code")
            params['school_code'] = school_code
        
        query = text(f"""
            SELECT 
                AVG(ssd.question_score) as avg_score,
                AVG(sqc.question_score) as max_score
            FROM student_score_detail ssd
            JOIN question_dimension_mapping qdm ON ssd.question_code = qdm.question_code
            JOIN subject_question_config sqc ON ssd.question_code = sqc.question_code
            WHERE {' AND '.join(where_conditions)}
        """)
        
        result = await self.session.execute(query, params)
        row = result.fetchone()
        
        if row and row[1] > 0:
            return float(row[0]) / float(row[1])
        return 0
    
    async def _calculate_dimension_option_distribution(self, batch_code: str, grade_code: str,
                                                      region_code: Optional[str], school_code: Optional[str],
                                                      questionnaire_code: str, dimension_name: str) -> List[QuestionnaireOption]:
        """计算维度选项分布"""
        # 构建查询条件
        where_conditions = [
            "ssd.batch_code = :batch_code",
            "ssd.grade_code = :grade_code",
            "ssd.subject_code = :questionnaire_code",
            "qdm.dimension_name = :dimension_name",
            "ssd.is_valid = 1"
        ]
        
        params = {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'questionnaire_code': questionnaire_code,
            'dimension_name': dimension_name
        }
        
        if region_code:
            where_conditions.append("ssd.region_code = :region_code")
            params['region_code'] = region_code
        
        if school_code:
            where_conditions.append("ssd.school_code = :school_code")
            params['school_code'] = school_code
        
        # 查询选项分布
        query = text(f"""
            SELECT 
                ssd.question_score as option_value,
                COUNT(*) as count,
                sqc.question_score as max_score
            FROM student_score_detail ssd
            JOIN question_dimension_mapping qdm ON ssd.question_code = qdm.question_code
            JOIN subject_question_config sqc ON ssd.question_code = sqc.question_code
            WHERE {' AND '.join(where_conditions)}
            GROUP BY ssd.question_score, sqc.question_score
        """)
        
        result = await self.session.execute(query, params)
        rows = result.fetchall()
        
        # 统计总数和各选项数量
        option_counts = {}
        total_count = 0
        max_score = 5  # 默认5级量表
        
        for row in rows:
            option_value = int(row[0])
            count = int(row[1])
            max_score = int(row[2])
            
            if option_value not in option_counts:
                option_counts[option_value] = 0
            option_counts[option_value] += count
            total_count += count
        
        # 计算百分比
        options = []
        for value in range(1, max_score + 1):
            count = option_counts.get(value, 0)
            percentage = (count / total_count * 100) if total_count > 0 else 0
            
            options.append(QuestionnaireOption(
                option_value=value,
                option_label=get_option_label(value, max_score),
                percentage=percentage
            ))
        
        return options
    
    async def _calculate_dimension_questions(self, batch_code: str, grade_code: str,
                                            region_code: Optional[str], school_code: Optional[str],
                                            questionnaire_code: str, dimension_name: str) -> List[QuestionnaireQuestionStats]:
        """计算维度下的题目统计"""
        # 构建查询条件
        where_conditions = [
            "qdm.dimension_name = :dimension_name",
            "sqc.batch_code = :batch_code",
            "sqc.grade_code = :grade_code",
            "sqc.subject_code = :questionnaire_code"
        ]
        
        params = {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'questionnaire_code': questionnaire_code,
            'dimension_name': dimension_name
        }
        
        # 获取维度下的题目列表
        q_query = text(f"""
            SELECT DISTINCT 
                sqc.question_code,
                sqc.question_name,
                sqc.question_score as max_score
            FROM subject_question_config sqc
            JOIN question_dimension_mapping qdm ON sqc.question_code = qdm.question_code
            WHERE {' AND '.join(where_conditions)}
            ORDER BY sqc.question_code
        """)
        
        result = await self.session.execute(q_query, params)
        question_list = result.fetchall()
        
        questions = []
        for q_code, q_text, max_score in question_list:
            # 计算每题的选项分布
            option_distribution = await self._calculate_question_option_distribution(
                batch_code, grade_code, region_code, school_code,
                q_code, int(max_score)
            )
            
            questions.append(QuestionnaireQuestionStats(
                question_code=q_code,
                question_text=q_text,
                option_distribution=option_distribution
            ))
        
        return questions
    
    async def _calculate_question_option_distribution(self, batch_code: str, grade_code: str,
                                                     region_code: Optional[str], school_code: Optional[str],
                                                     question_code: str, max_score: int) -> List[QuestionnaireOption]:
        """计算题目选项分布"""
        # 构建查询条件
        where_conditions = [
            "ssd.batch_code = :batch_code",
            "ssd.grade_code = :grade_code",
            "ssd.question_code = :question_code",
            "ssd.is_valid = 1"
        ]
        
        params = {
            'batch_code': batch_code,
            'grade_code': grade_code,
            'question_code': question_code
        }
        
        if region_code:
            where_conditions.append("ssd.region_code = :region_code")
            params['region_code'] = region_code
        
        if school_code:
            where_conditions.append("ssd.school_code = :school_code")
            params['school_code'] = school_code
        
        # 查询选项分布
        query = text(f"""
            SELECT 
                ssd.question_score as option_value,
                COUNT(*) as count
            FROM student_score_detail ssd
            WHERE {' AND '.join(where_conditions)}
            GROUP BY ssd.question_score
        """)
        
        result = await self.session.execute(query, params)
        rows = result.fetchall()
        
        # 统计各选项数量
        option_counts = {}
        total_count = 0
        
        for row in rows:
            option_value = int(row[0])
            count = int(row[1])
            option_counts[option_value] = count
            total_count += count
        
        # 计算百分比
        options = []
        for value in range(1, max_score + 1):
            count = option_counts.get(value, 0)
            percentage = (count / total_count * 100) if total_count > 0 else 0
            
            options.append(QuestionnaireOption(
                option_value=value,
                option_label=get_option_label(value, max_score),
                percentage=percentage
            ))
        
        return options
    
    async def _get_region_name(self, region_code: str) -> str:
        """获取区域名称"""
        query = text("SELECT region_name FROM region WHERE region_code = :region_code")
        result = await self.session.execute(query, {'region_code': region_code})
        row = result.fetchone()
        return row[0] if row else region_code
    
    async def _get_school_info(self, school_code: str) -> Dict:
        """获取学校信息"""
        query = text("""
            SELECT 
                s.school_name,
                s.region_code,
                r.region_name
            FROM school s
            JOIN region r ON s.region_code = r.region_code
            WHERE s.school_code = :school_code
        """)
        result = await self.session.execute(query, {'school_code': school_code})
        row = result.fetchone()
        
        if row:
            return {
                'school_name': row[0],
                'region_code': row[1],
                'region_name': row[2]
            }
        return {
            'school_name': school_code,
            'region_code': '',
            'region_name': ''
        }
    
    async def _get_exam_subjects(self, batch_code: str, grade_code: str) -> List[Tuple[str, str]]:
        """获取考试科目列表"""
        query = text("""
            SELECT DISTINCT subject_code, subject_name
            FROM subject_question_config
            WHERE batch_code = :batch_code
            AND grade_code = :grade_code
            AND subject_type = 'exam'
            ORDER BY subject_code
        """)
        
        result = await self.session.execute(query, {
            'batch_code': batch_code,
            'grade_code': grade_code
        })
        
        return [(row[0], row[1]) for row in result]