#!/usr/bin/env python3
"""
数据清洗服务 - 第一阶段：清洗原始数据
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import pandas as pd
import json
from typing import Dict, List, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

class DataCleaningService:
    """数据清洗服务"""
    
    def __init__(self, db_session):
        self.db_session = db_session
    
    async def clean_batch_scores(self, batch_code: str) -> Dict[str, Any]:
        """清洗批次分数数据"""
        print(f"开始清洗批次 {batch_code} 的分数数据...")
        
        cleaning_result = {
            'batch_code': batch_code,
            'subjects_processed': 0,
            'total_raw_records': 0,
            'total_cleaned_records': 0,
            'anomalous_records': 0,
            'subjects': {}
        }
        
        try:
            # 1. 获取科目配置
            subjects_config = await self._get_batch_subjects(batch_code)
            if not subjects_config:
                print(f"批次 {batch_code} 没有找到科目配置")
                return cleaning_result
            
            print(f"找到 {len(subjects_config)} 个科目")
            
            # 2. 清理旧数据
            await self._clear_existing_cleaned_data(batch_code)
            
            # 3. 逐科目处理
            for subject_config in subjects_config:
                subject_name = subject_config['subject_name'] 
                max_score = subject_config['max_score']
                question_count = subject_config['question_count']
                is_questionnaire = subject_config.get('is_questionnaire', False)
                instrument_id = subject_config.get('instrument_id')
                
                if is_questionnaire:
                    print(f"处理问卷科目: {subject_name} (量表ID: {instrument_id})")
                    subject_result = await self._clean_questionnaire_scores(
                        batch_code, subject_name, instrument_id, question_count
                    )
                else:
                    print(f"处理考试科目: {subject_name} (满分: {max_score})")
                    subject_result = await self._clean_subject_scores(
                        batch_code, subject_name, max_score, question_count
                    )
                
                cleaning_result['subjects'][subject_name] = subject_result
                cleaning_result['total_raw_records'] += subject_result['raw_records']
                cleaning_result['total_cleaned_records'] += subject_result['cleaned_records']
                cleaning_result['anomalous_records'] += subject_result['anomalous_records']
                cleaning_result['subjects_processed'] += 1
            
            print(f"批次 {batch_code} 清洗完成:")
            print(f"  处理科目: {cleaning_result['subjects_processed']} 个")
            print(f"  原始记录: {cleaning_result['total_raw_records']} 条")
            print(f"  清洗后记录: {cleaning_result['total_cleaned_records']} 条") 
            print(f"  异常记录: {cleaning_result['anomalous_records']} 条")
            
            return cleaning_result
            
        except Exception as e:
            print(f"数据清洗失败: {e}")
            import traceback
            traceback.print_exc()
            return cleaning_result
    
    async def _get_batch_subjects(self, batch_code: str) -> List[Dict[str, Any]]:
        """获取批次科目配置，包含问卷类型识别"""
        try:
            query = text("""
                SELECT 
                    subject_name,
                    SUM(CASE WHEN question_type_enum IN ('exam','interaction') THEN max_score ELSE 0 END) as total_max_score,
                    SUM(CASE WHEN question_type_enum IN ('exam','interaction') THEN 1 ELSE 0 END) as question_count,
                    MAX(question_type_enum) as subject_type,
                    MAX(instrument_id) as instrument_id
                FROM subject_question_config 
                WHERE batch_code = :batch_code
                GROUP BY subject_name
                ORDER BY subject_name
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            subjects = []
            for row in result.fetchall():
                subject_type = row[3] if row[3] else 'exam'  # 默认为考试类型
                is_questionnaire = subject_type == 'questionnaire'
                
                subjects.append({
                    'subject_name': row[0],
                    'max_score': float(row[1]) if row[1] else 100.0,
                    'question_count': int(row[2]) if row[2] else 0,
                    'subject_type': subject_type,
                    'is_questionnaire': is_questionnaire,
                    'instrument_id': row[4] if row[4] else None
                })
                
                if is_questionnaire:
                    print(f"  问卷科目: {row[0]} (量表ID: {row[4]})")
                else:
                    print(f"  考试科目: {row[0]} (满分: {row[1]})")
            
            return subjects
            
        except Exception as e:
            print(f"获取科目配置失败: {e}")
            return []
    
    async def _clear_existing_cleaned_data(self, batch_code: str):
        """清理已存在的清洗数据（包括问卷数据）"""
        try:
            # 清理常规清洗数据
            query1 = text("DELETE FROM student_cleaned_scores WHERE batch_code = :batch_code")
            result1 = self.db_session.execute(query1, {'batch_code': batch_code})
            
            # 清理问卷详细数据
            query2 = text("DELETE FROM questionnaire_question_scores WHERE batch_code = :batch_code")
            result2 = self.db_session.execute(query2, {'batch_code': batch_code})
            
            self.db_session.commit()
            print(f"清理了 {result1.rowcount} 条常规清洗数据，{result2.rowcount} 条问卷详细数据")
        except Exception as e:
            print(f"清理旧数据失败: {e}")
            self.db_session.rollback()
    
    async def _clean_subject_scores(self, batch_code: str, subject_name: str, 
                                  max_score: float, question_count: int) -> Dict[str, Any]:
        """清洗单个科目的分数数据"""
        result = {
            'subject_name': subject_name,
            'max_score': max_score,
            'raw_records': 0,
            'cleaned_records': 0,
            'anomalous_records': 0,
            'unique_students': 0
        }
        
        try:
            # 1. 计算维度满分
            dimension_max_scores = await self._calculate_dimension_max_scores(batch_code, subject_name)
            print(f"  计算得到 {len(dimension_max_scores)} 个维度满分")
            
            # 2. 获取原始数据（包含subject_scores）
            query = text("""
                SELECT 
                    student_id,
                    student_name,
                    school_id, 
                    school_code,
                    school_name,
                    class_name,
                    subject_id,
                    total_score,
                    subject_scores
                FROM student_score_detail
                WHERE batch_code = :batch_code AND subject_name = :subject_name
                ORDER BY student_id
            """)
            
            raw_result = self.db_session.execute(query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            
            raw_data = raw_result.fetchall()
            result['raw_records'] = len(raw_data)
            
            if not raw_data:
                print(f"  科目 {subject_name} 没有原始数据")
                return result
            
            # 3. 转换为DataFrame进行聚合
            df = pd.DataFrame(raw_data, columns=[
                'student_id', 'student_name', 'school_id', 'school_code', 
                'school_name', 'class_name', 'subject_id', 'total_score', 'subject_scores'
            ])
            
            # 4. 按学生分组聚合分数
            print(f"  原始记录: {len(df)} 条")
            
            # 聚合：每个学生的总分 = SUM(所有题目分数)
            # 逐题求和生成标准总分（仅 exam/interaction 题目）
            qid_query = text("""
                SELECT question_id FROM subject_question_config
                WHERE batch_code=:batch_code AND subject_name=:subject_name
                  AND question_type_enum IN ('exam','interaction')
            """)
            qids = self.db_session.execute(qid_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            }).fetchall()
            qid_set = set(str(r[0]) for r in qids if r[0] is not None)

            def _sum_scores(json_text: str) -> float:
                try:
                    if not json_text:
                        return 0.0
                    data = json.loads(json_text)
                    s = 0.0
                    for k, v in data.items():
                        if str(k) in qid_set:
                            try:
                                s += float(v) if v is not None else 0.0
                            except (TypeError, ValueError):
                                continue
                    return s
                except Exception:
                    return 0.0

            df['total_score'] = df['subject_scores'].apply(_sum_scores)

            aggregated = df.groupby(['student_id']).agg({
                'student_name': 'first',
                'school_id': 'first', 
                'school_code': 'first',
                'school_name': 'first',
                'class_name': 'first',
                'subject_id': 'first',
                'total_score': 'sum',  # 关键：按学生汇总总分
                'subject_scores': 'first'  # 保留subject_scores用于维度计算
            }).reset_index()
            
            print(f"  按学生聚合后: {len(aggregated)} 条")
            result['unique_students'] = len(aggregated)
            
            # 5. 计算学生维度分数
            if len(dimension_max_scores) > 0:
                print(f"  开始计算学生维度分数...")
                aggregated['dimension_scores'] = await self._calculate_student_dimension_scores_batch(
                    batch_code, subject_name, aggregated['subject_scores'].tolist()
                )
                print(f"  维度分数计算完成")
            else:
                aggregated['dimension_scores'] = [{}] * len(aggregated)
                print(f"  未找到维度定义，跳过维度分数计算")
            
            # 6. 过滤异常分数
            valid_mask = (aggregated['total_score'] >= 0) & (aggregated['total_score'] <= max_score)
            anomalous_data = aggregated[~valid_mask]
            clean_data = aggregated[valid_mask]
            
            result['anomalous_records'] = len(anomalous_data)
            result['cleaned_records'] = len(clean_data)
            
            if len(anomalous_data) > 0:
                print(f"  发现 {len(anomalous_data)} 个异常分数 (范围: 0-{max_score})")
                print(f"    异常分数范围: {anomalous_data['total_score'].min():.2f} - {anomalous_data['total_score'].max():.2f}")
            
            # 7. 写入清洗表
            if len(clean_data) > 0:
                await self._insert_cleaned_scores(batch_code, subject_name, clean_data, max_score, question_count, dimension_max_scores)
                print(f"  成功写入 {len(clean_data)} 条清洗数据")
            else:
                print(f"  科目 {subject_name} 没有有效数据")
            
            return result
            
        except Exception as e:
            print(f"清洗科目 {subject_name} 失败: {e}")
            import traceback
            traceback.print_exc()
            return result
    
    async def _insert_cleaned_scores(self, batch_code: str, subject_name: str, 
                                   clean_data: pd.DataFrame, max_score: float, question_count: int, dimension_max_scores: Dict[str, Any]):
        """批量插入清洗后的分数数据"""
        try:
            # 准备批量插入数据
            insert_data = []
            for _, row in clean_data.iterrows():
                dimension_scores_json = json.dumps(row.get('dimension_scores', {}), ensure_ascii=False)
                dimension_max_scores_json = json.dumps(dimension_max_scores, ensure_ascii=False)
                
                insert_data.append({
                    'batch_code': batch_code,
                    'student_id': row['student_id'],
                    'student_name': row['student_name'],
                    'school_id': row['school_id'],
                    'school_code': row['school_code'],
                    'school_name': row['school_name'],
                    'class_name': row['class_name'],
                    'subject_id': row['subject_id'],
                    'subject_name': subject_name,
                    'total_score': float(row['total_score']),
                    'max_score': max_score,
                    'question_count': question_count,
                    'is_valid': 1,
                    'dimension_scores': dimension_scores_json,
                    'dimension_max_scores': dimension_max_scores_json,
                    'subject_type': 'exam'
                })
            
            # 批量插入
            query = text("""
                INSERT INTO student_cleaned_scores 
                (batch_code, student_id, student_name, school_id, school_code, school_name,
                 class_name, subject_id, subject_name, total_score, max_score, question_count, is_valid,
                 dimension_scores, dimension_max_scores, subject_type)
                VALUES 
                (:batch_code, :student_id, :student_name, :school_id, :school_code, :school_name,
                 :class_name, :subject_id, :subject_name, :total_score, :max_score, :question_count, :is_valid,
                 :dimension_scores, :dimension_max_scores, :subject_type)
            """)
            
            self.db_session.execute(query, insert_data)
            self.db_session.commit()
            
        except Exception as e:
            print(f"插入清洗数据失败: {e}")
            self.db_session.rollback()
            raise
    
    async def _calculate_dimension_max_scores(self, batch_code: str, subject_name: str) -> Dict[str, Any]:
        """计算维度满分"""
        try:
            # 1. 获取科目的维度列表
            dimension_query = text("""
                SELECT dimension_code, dimension_name
                FROM batch_dimension_definition 
                WHERE batch_code = :batch_code AND subject_name = :subject_name
            """)
            
            dimension_result = self.db_session.execute(dimension_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            
            dimensions = dimension_result.fetchall()
            if not dimensions:
                print(f"    科目 {subject_name} 未找到维度定义")
                return {}
            
            dimension_max_scores = {}
            
            # 2. 为每个维度计算满分
            for dim_code, dim_name in dimensions:
                # 获取该维度下的所有题目及其满分
                max_score_query = text("""
                    SELECT SUM(sqc.max_score) as total_max_score
                    FROM question_dimension_mapping qdm
                    JOIN subject_question_config sqc ON qdm.question_id = sqc.question_id 
                        AND qdm.batch_code = sqc.batch_code 
                        AND qdm.subject_name = sqc.subject_name
                    WHERE qdm.batch_code = :batch_code 
                        AND qdm.subject_name = :subject_name
                        AND qdm.dimension_code = :dimension_code
                        AND sqc.question_type_enum IN ('exam','interaction')
                """)
                
                max_score_result = self.db_session.execute(max_score_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name,
                    'dimension_code': dim_code
                })
                
                max_score_row = max_score_result.fetchone()
                max_score = float(max_score_row[0]) if max_score_row[0] else 0.0
                
                dimension_max_scores[dim_code] = {
                    'max_score': max_score,
                    'name': dim_name
                }
            
            return dimension_max_scores
            
        except Exception as e:
            print(f"计算维度满分失败: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    async def _calculate_student_dimension_scores_batch(self, batch_code: str, subject_name: str, subject_scores_list: List[str]) -> List[Dict[str, Any]]:
        """批量计算学生维度分数"""
        try:
            # 1. 获取维度-题目映射
            mapping_query = text("""
                SELECT qdm.dimension_code, bdd.dimension_name, qdm.question_id
                FROM question_dimension_mapping qdm
                JOIN batch_dimension_definition bdd ON qdm.dimension_code = bdd.dimension_code
                    AND qdm.batch_code = bdd.batch_code
                    AND qdm.subject_name = bdd.subject_name  
                WHERE qdm.batch_code = :batch_code AND qdm.subject_name = :subject_name
            """)
            
            mapping_result = self.db_session.execute(mapping_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            
            mappings = mapping_result.fetchall()
            if not mappings:
                return [{}] * len(subject_scores_list)
            
            # 2. 构建维度映射字典
            dimension_questions = {}
            for dim_code, dim_name, question_id in mappings:
                if dim_code not in dimension_questions:
                    dimension_questions[dim_code] = {
                        'name': dim_name,
                        'questions': []
                    }
                dimension_questions[dim_code]['questions'].append(question_id)
            
            # 3. 批量处理每个学生的维度分数
            results = []
            for subject_scores_json in subject_scores_list:
                student_dimension_scores = await self._calculate_student_dimension_scores(
                    subject_scores_json, dimension_questions
                )
                results.append(student_dimension_scores)
            
            return results
            
        except Exception as e:
            print(f"批量计算学生维度分数失败: {e}")
            import traceback
            traceback.print_exc()
            return [{}] * len(subject_scores_list)
    
    async def _calculate_student_dimension_scores(self, subject_scores_json: str, dimension_questions: Dict[str, Any]) -> Dict[str, Any]:
        """计算单个学生的维度分数"""
        try:
            # 1. 解析学生分数JSON
            if not subject_scores_json:
                return {}
            
            try:
                subject_scores = json.loads(subject_scores_json)
            except (json.JSONDecodeError, TypeError) as e:
                print(f"    解析subject_scores JSON失败: {e}")
                return {}
            
            if not isinstance(subject_scores, dict):
                return {}
            
            # 2. 按维度汇总分数
            student_dimension_scores = {}
            
            for dim_code, dim_info in dimension_questions.items():
                dimension_score = 0.0
                question_count = 0
                
                for question_id in dim_info['questions']:
                    if question_id in subject_scores:
                        try:
                            score = float(subject_scores[question_id])
                            dimension_score += score
                            question_count += 1
                        except (ValueError, TypeError):
                            continue
                
                student_dimension_scores[dim_code] = {
                    'score': dimension_score,
                    'name': dim_info['name']
                }
            
            return student_dimension_scores
            
        except Exception as e:
            print(f"计算学生维度分数失败: {e}")
            return {}
    
    async def _clean_questionnaire_scores(self, batch_code: str, subject_name: str, 
                                        instrument_id: str, question_count: int) -> Dict[str, Any]:
        """清洗问卷科目（SQL 一次性落地 + 物化 + 汇总）。

        - 明细：INSERT…SELECT via JOIN + JSON_EXTRACT
        - 分布：REPLACE INTO questionnaire_option_distribution
        - 汇总：INSERT INTO student_cleaned_scores（subject_type='questionnaire'）
        - instrument_type = instrument_id；is_reverse = 0
        """
        result = {
            'subject_name': subject_name,
            'instrument_id': instrument_id,
            'raw_records': 0,
            'cleaned_records': 0,
            'anomalous_records': 0,
            'unique_students': 0
        }

        try:
            # 0) 原始学生数
            raw_cnt = self.db_session.execute(text(
                "SELECT COUNT(*) FROM student_score_detail WHERE batch_code=:batch_code AND subject_name=:subject_name"
            ), {'batch_code': batch_code, 'subject_name': subject_name}).scalar() or 0
            result['raw_records'] = int(raw_cnt)
            result['unique_students'] = int(raw_cnt)
            if raw_cnt == 0:
                print(f"  问卷科目 {subject_name} 没有原始数据")
                return result

            # 1) 清理旧明细
            self.db_session.execute(text(
                "DELETE FROM questionnaire_question_scores WHERE batch_code=:batch_code AND subject_name=:subject_name"
            ), {'batch_code': batch_code, 'subject_name': subject_name})

            # 2) 插入明细（每生×每题）
            self.db_session.execute(text(
                """
                INSERT INTO questionnaire_question_scores
                    (batch_code, subject_name, student_id, question_id,
                     original_score, max_score, scale_level, instrument_type, is_reverse)
                SELECT
                    ssd.batch_code,
                    ssd.subject_name,
                    CAST(ssd.student_id AS UNSIGNED),
                    sqc.question_id,
                    CAST(JSON_UNQUOTE(JSON_EXTRACT(ssd.subject_scores, CONCAT('$."', sqc.question_id, '"'))) AS DECIMAL(10,2)) AS original_score,
                    sqc.max_score,
                    CASE
                        WHEN sqc.instrument_id LIKE '%10%' THEN 10
                        WHEN sqc.instrument_id LIKE '%7%' THEN 7
                        WHEN sqc.instrument_id LIKE '%5%' THEN 5
                        ELSE 4
                    END AS scale_level,
                    sqc.instrument_id AS instrument_type,
                    0 AS is_reverse
                FROM student_score_detail ssd
                JOIN subject_question_config sqc
                  ON BINARY sqc.batch_code = BINARY ssd.batch_code
                 AND BINARY sqc.subject_name = BINARY ssd.subject_name
                 AND sqc.question_type_enum = 'questionnaire'
                WHERE BINARY ssd.batch_code = BINARY :batch_code
                  AND BINARY ssd.subject_name = BINARY :subject_name
                  AND JSON_EXTRACT(ssd.subject_scores, CONCAT('$."', sqc.question_id, '"')) IS NOT NULL
                  AND ssd.student_id REGEXP '^[0-9]+$'
                """
            ), {'batch_code': batch_code, 'subject_name': subject_name})

            cnt_detail = self.db_session.execute(text(
                "SELECT COUNT(*) FROM questionnaire_question_scores WHERE batch_code=:batch_code AND subject_name=:subject_name"
            ), {'batch_code': batch_code, 'subject_name': subject_name}).scalar() or 0
            result['cleaned_records'] = int(cnt_detail)

            # 3) 物化选项分布
            self.db_session.execute(text(
                """
                REPLACE INTO questionnaire_option_distribution
                    (batch_code, subject_name, question_id, option_level, count, updated_at)
                SELECT batch_code, subject_name, question_id, option_level, COUNT(*), NOW()
                FROM (
                    SELECT 
                        batch_code,
                        subject_name,
                        question_id,
                        GREATEST(1, LEAST(scale_level,
                            ROUND(COALESCE(original_score,0) / NULLIF(max_score,0) * scale_level, 0)
                        )) AS option_level
                    FROM questionnaire_question_scores
                    WHERE BINARY batch_code = BINARY :batch_code
                      AND BINARY subject_name = BINARY :subject_name
                ) x
                GROUP BY batch_code, subject_name, question_id, option_level
                """
            ), {'batch_code': batch_code, 'subject_name': subject_name})

            # 4) 写入汇总 student_cleaned_scores（逐题求和 / 满分求和）
            self.db_session.execute(text(
                "DELETE FROM student_cleaned_scores WHERE batch_code=:batch_code AND subject_name=:subject_name AND subject_type='questionnaire'"
            ), {'batch_code': batch_code, 'subject_name': subject_name})

            self.db_session.execute(text(
                """
                INSERT INTO student_cleaned_scores 
                    (batch_code, student_id, student_name, school_id, school_code, school_name,
                     class_name, subject_id, subject_name, total_score, max_score,
                     question_count, is_valid, dimension_scores, dimension_max_scores, subject_type)
                SELECT
                    :batch_code AS batch_code,
                    CAST(ssd.student_id AS UNSIGNED) AS student_id,
                    ssd.student_name,
                    ssd.school_id,
                    ssd.school_code,
                    ssd.school_name,
                    ssd.class_name,
                    ssd.subject_id,
                    :subject_name AS subject_name,
                    ROUND(SUM(qqs.original_score), 2) AS total_score,
                    (
                        SELECT SUM(sqc2.max_score)
                        FROM subject_question_config sqc2
                        WHERE BINARY sqc2.batch_code = BINARY :batch_code
                          AND BINARY sqc2.subject_name = BINARY :subject_name
                          AND sqc2.question_type_enum = 'questionnaire'
                    ) AS max_score,
                    (
                        SELECT COUNT(*)
                        FROM subject_question_config sqc3
                        WHERE BINARY sqc3.batch_code = BINARY :batch_code
                          AND BINARY sqc3.subject_name = BINARY :subject_name
                          AND sqc3.question_type_enum = 'questionnaire'
                    ) AS question_count,
                    1 AS is_valid,
                    '{}' AS dimension_scores,
                    '{}' AS dimension_max_scores,
                    'questionnaire' AS subject_type
                FROM questionnaire_question_scores qqs
                JOIN student_score_detail ssd
                  ON BINARY ssd.batch_code = BINARY qqs.batch_code
                 AND BINARY ssd.subject_name = BINARY qqs.subject_name
                 AND ssd.student_id = qqs.student_id
                WHERE BINARY qqs.batch_code = BINARY :batch_code
                  AND BINARY qqs.subject_name = BINARY :subject_name
                  AND ssd.student_id REGEXP '^[0-9]+$'
                GROUP BY ssd.student_id, ssd.student_name, ssd.school_id, ssd.school_code,
                         ssd.school_name, ssd.class_name, ssd.subject_id
                """
            ), {'batch_code': batch_code, 'subject_name': subject_name})

            self.db_session.commit()
            print(f"  问卷科目 {subject_name} 处理完成（明细 {result['cleaned_records']} 条）")
            return result
            
        except Exception as e:
            print(f"问卷科目清洗失败: {e}")
            import traceback
            traceback.print_exc()
            self.db_session.rollback()
            return result
    
    async def _get_scale_info(self, instrument_id: str) -> Dict[str, Any]:
        """获取量表配置信息"""
        try:
            # 这里需要根据instrument_id获取量表信息
            # 由于缺少具体的量表配置表，暂时使用映射逻辑
            scale_mapping = {
                'LIKERT_4_POSITIV': {'instrument_type': 'LIKERT_4_POSITIV', 'scale_level': 4, 'is_reverse': False},
                'LIKERT_4_NEGATIVE': {'instrument_type': 'LIKERT_4_NEGATIVE', 'scale_level': 4, 'is_reverse': True},
                'LIKERT_5_POSITIV': {'instrument_type': 'LIKERT_5_POSITIV', 'scale_level': 5, 'is_reverse': False},
                'LIKERT_5_NEGATIVE': {'instrument_type': 'LIKERT_5_NEGATIVE', 'scale_level': 5, 'is_reverse': True},
                'SATISFACTION_7': {'instrument_type': 'SATISFACTION_7', 'scale_level': 7, 'is_reverse': False},
                'SATISFACTION_10': {'instrument_type': 'SATISFACTION_10', 'scale_level': 10, 'is_reverse': False}
            }
            
            return scale_mapping.get(instrument_id, {
                'instrument_type': 'LIKERT_4_POSITIV', 
                'scale_level': 4, 
                'is_reverse': False
            })
            
        except Exception as e:
            print(f"获取量表信息失败: {e}")
            return {}
    
    async def _map_score_to_option(self, score: float, scale_info: Dict[str, Any], max_score: float) -> Dict[str, Any]:
        """将分数映射到选项标签"""
        try:
            instrument_type = scale_info['instrument_type']
            scale_level = scale_info['scale_level']
            is_reverse = scale_info.get('is_reverse', False)
            
            # 将分数标准化到量表级别
            if max_score > 0:
                normalized_score = (score / max_score) * scale_level
                option_level = max(1, min(scale_level, round(normalized_score)))
            else:
                option_level = 1
            
            # 获取选项标签
            query = text("""
                SELECT option_label, option_description
                FROM questionnaire_scale_options
                WHERE instrument_type = :instrument_type
                AND scale_level = :scale_level
                AND option_level = :option_level
                AND is_reverse = :is_reverse
                LIMIT 1
            """)
            
            result = self.db_session.execute(query, {
                'instrument_type': instrument_type,
                'scale_level': scale_level,
                'option_level': option_level,
                'is_reverse': is_reverse
            })
            
            row = result.fetchone()
            if row:
                return {
                    'option_level': option_level,
                    'option_label': row[0],
                    'option_description': row[1]
                }
            else:
                # 默认选项
                return {
                    'option_level': option_level,
                    'option_label': f"选项{option_level}",
                    'option_description': f"量表第{option_level}级"
                }
                
        except Exception as e:
            print(f"分数映射失败: {e}")
            return None
    
    async def _insert_questionnaire_scores(self, records: List[Dict[str, Any]]):
        """批量插入问卷详细分数"""
        try:
            if not records:
                return
            
            # 准备批量插入语句
            insert_query = text("""
                INSERT INTO questionnaire_question_scores 
                (student_id, subject_name, batch_code, dimension_code, dimension_name,
                 question_id, question_name, original_score, scale_level, instrument_type,
                 is_reverse, option_label, option_level, max_score)
                VALUES 
                (:student_id, :subject_name, :batch_code, :dimension_code, :dimension_name,
                 :question_id, :question_name, :original_score, :scale_level, :instrument_type,
                 :is_reverse, :option_label, :option_level, :max_score)
            """)
            
            # 批量执行插入
            self.db_session.execute(insert_query, records)
            self.db_session.commit()
            
        except Exception as e:
            print(f"批量插入问卷分数失败: {e}")
            import traceback
            traceback.print_exc()
            self.db_session.rollback()
    
    async def _create_questionnaire_summary(self, batch_code: str, subject_name: str, 
                                          student_data: List, scale_info: Dict[str, Any]):
        """为问卷创建汇总清洗数据（用于兼容现有统计逻辑）"""
        try:
            # 插入到student_cleaned_scores表（用于兼容现有统计）
            summary_records = []
            for student_row in student_data:
                student_id = student_row[0]
                student_name = student_row[1]
                school_id = student_row[2]
                school_code = student_row[3]
                school_name = student_row[4]
                class_name = student_row[5]
                subject_id = student_row[6]
                subject_scores_json = student_row[7]
                
                # 从原始数据中获取总分，或计算平均分
                try:
                    if subject_scores_json:
                        subject_scores = json.loads(subject_scores_json)
                        # 计算问卷平均分
                        scores = [float(score) for score in subject_scores.values() if score is not None]
                        avg_score = sum(scores) / len(scores) if scores else 0.0
                    else:
                        avg_score = 0.0
                except (json.JSONDecodeError, TypeError, ValueError):
                    avg_score = 0.0
                
                summary_records.append({
                    'student_id': int(student_id),
                    'student_name': student_name,
                    'school_id': int(school_id) if school_id else 0,
                    'school_code': school_code,
                    'school_name': school_name,
                    'class_name': class_name,
                    'subject_id': int(subject_id) if subject_id else 0,
                    'subject_name': subject_name,
                    'batch_code': batch_code,
                    'total_score': avg_score,
                    'max_score': float(scale_info['scale_level']),  # 使用量表级别作为满分
                    'dimension_scores': '{}',  # 问卷的维度分析在详情表中
                    'dimension_max_scores': '{}',
                    'is_anomalous': False,
                    'subject_type': 'questionnaire'
                })
            
            if summary_records:
                insert_query = text("""
                    INSERT INTO student_cleaned_scores 
                    (student_id, student_name, school_id, school_code, school_name, class_name,
                     subject_id, subject_name, batch_code, total_score, max_score,
                     dimension_scores, dimension_max_scores, is_anomalous, subject_type)
                    VALUES 
                    (:student_id, :student_name, :school_id, :school_code, :school_name, :class_name,
                     :subject_id, :subject_name, :batch_code, :total_score, :max_score,
                     :dimension_scores, :dimension_max_scores, :is_anomalous, :subject_type)
                """)
                
                self.db_session.execute(insert_query, summary_records)
                self.db_session.commit()
                print(f"  创建问卷汇总记录: {len(summary_records)} 条")
                
        except Exception as e:
            print(f"创建问卷汇总数据失败: {e}")
            import traceback
            traceback.print_exc()
            self.db_session.rollback()

async def main():
    """主函数 - 批量清洗所有待处理批次"""
    import time
    print("=== 数据清洗服务 - 批量清洗 ===\n")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # 创建清洗服务
    cleaning_service = DataCleaningService(session)
    
    # 待清洗的批次列表
    batches_to_clean = ['G7-2025', 'G8-2025']
    
    total_start_time = time.time()
    all_results = {}
    
    try:
        for batch_code in batches_to_clean:
            print(f"\n{'='*60}")
            print(f"开始清洗批次: {batch_code}")
            print(f"{'='*60}")
            
            batch_start_time = time.time()
            
            # 执行清洗
            result = await cleaning_service.clean_batch_scores(batch_code)
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            # 记录处理时间
            result['processing_time_seconds'] = round(batch_duration, 2)
            result['processing_time_minutes'] = round(batch_duration / 60, 2)
            
            all_results[batch_code] = result
            
            # 输出单批次结果
            print(f"\n=== 批次 {batch_code} 清洗完成 ===")
            print(f"处理科目: {result['subjects_processed']} 个")
            print(f"原始记录: {result['total_raw_records']} 条")
            print(f"清洗记录: {result['total_cleaned_records']} 条")
            print(f"异常记录: {result['anomalous_records']} 条")
            print(f"处理时间: {result['processing_time_minutes']} 分钟")
            
            # 验证清洗结果
            print(f"\n验证批次 {batch_code} 清洗结果...")
            verification_result = await verify_cleaning_result(session, batch_code)
            result['verification'] = verification_result
            
            if verification_result['success']:
                print(f"✅ 批次 {batch_code} 验证通过")
            else:
                print(f"❌ 批次 {batch_code} 验证失败: {verification_result.get('error', '未知错误')}")
        
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        
        # 输出总体结果汇总
        print(f"\n{'='*80}")
        print(f"=== 批量清洗总结果汇总 ===")
        print(f"{'='*80}")
        
        total_subjects = 0
        total_raw_records = 0
        total_cleaned_records = 0
        total_anomalous_records = 0
        
        for batch_code, result in all_results.items():
            print(f"\n批次 {batch_code}:")
            print(f"  - 处理科目: {result['subjects_processed']} 个")
            print(f"  - 原始记录: {result['total_raw_records']} 条")
            print(f"  - 清洗记录: {result['total_cleaned_records']} 条")
            print(f"  - 异常记录: {result['anomalous_records']} 条")
            print(f"  - 处理时间: {result['processing_time_minutes']} 分钟")
            print(f"  - 验证状态: {'✅ 通过' if result['verification']['success'] else '❌ 失败'}")
            
            total_subjects += result['subjects_processed']
            total_raw_records += result['total_raw_records']
            total_cleaned_records += result['total_cleaned_records']
            total_anomalous_records += result['anomalous_records']
        
        print(f"\n总计:")
        print(f"  - 处理批次: {len(all_results)} 个")
        print(f"  - 处理科目: {total_subjects} 个")
        print(f"  - 原始记录: {total_raw_records} 条")
        print(f"  - 清洗记录: {total_cleaned_records} 条")
        print(f"  - 异常记录: {total_anomalous_records} 条")
        print(f"  - 总处理时间: {round(total_duration / 60, 2)} 分钟")
        
        # 生成处理报告
        await generate_processing_report(all_results, total_duration)
        
    except Exception as e:
        print(f"批量清洗过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        session.close()

async def verify_cleaning_result(session, batch_code: str) -> Dict[str, Any]:
    """验证清洗结果的完整性和正确性"""
    try:
        # 检查清洗表中是否有数据
        query = text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT subject_name) as unique_subjects,
                COUNT(DISTINCT student_id) as unique_students,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                AVG(total_score) as avg_score
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
        """)
        
        result = session.execute(query, {'batch_code': batch_code})
        row = result.fetchone()
        
        if not row or row[0] == 0:
            return {
                'success': False,
                'error': f'清洗表中未找到批次 {batch_code} 的数据'
            }
        
        return {
            'success': True,
            'total_records': row[0],
            'unique_subjects': row[1],
            'unique_students': row[2],
            'score_range': {
                'min': float(row[3]) if row[3] else 0,
                'max': float(row[4]) if row[4] else 0,
                'avg': round(float(row[5]), 2) if row[5] else 0
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f'验证过程发生错误: {str(e)}'
        }

async def generate_processing_report(all_results: Dict[str, Any], total_duration: float):
    """生成详细的处理报告"""
    import datetime
    
    report_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n{'='*80}")
    print(f"=== 数据清洗处理报告 ===")
    print(f"报告生成时间: {report_time}")
    print(f"{'='*80}")
    
    print(f"\n【处理概览】")
    print(f"总处理时间: {round(total_duration / 60, 2)} 分钟")
    print(f"处理批次数: {len(all_results)}")
    
    success_batches = 0
    failed_batches = 0
    
    for batch_code, result in all_results.items():
        if result.get('verification', {}).get('success', False):
            success_batches += 1
        else:
            failed_batches += 1
    
    print(f"成功批次: {success_batches}")
    print(f"失败批次: {failed_batches}")
    
    print(f"\n【批次详情】")
    for batch_code, result in all_results.items():
        verification = result.get('verification', {})
        print(f"\n批次: {batch_code}")
        print(f"  状态: {'✅ 成功' if verification.get('success') else '❌ 失败'}")
        print(f"  处理科目: {result['subjects_processed']}")
        print(f"  原始记录: {result['total_raw_records']}")
        print(f"  清洗记录: {result['total_cleaned_records']}")
        print(f"  异常记录: {result['anomalous_records']}")
        print(f"  处理时间: {result['processing_time_minutes']} 分钟")
        
        if verification.get('success'):
            print(f"  验证结果:")
            print(f"    - 总记录数: {verification['total_records']}")
            print(f"    - 科目数: {verification['unique_subjects']}")
            print(f"    - 学生数: {verification['unique_students']}")
            print(f"    - 分数范围: {verification['score_range']['min']:.2f} ~ {verification['score_range']['max']:.2f}")
            print(f"    - 平均分: {verification['score_range']['avg']:.2f}")
        else:
            print(f"  验证错误: {verification.get('error', '未知错误')}")
    
    print(f"\n{'='*80}")
    print("报告生成完成")
    print(f"{'='*80}")

    
def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


async def _create_questionnaire_summary_total(self, batch_code: str, subject_name: str,
                                              student_data: List) -> None:
    """Create questionnaire summary using total score / total max score.

    - total_score: sum of item scores per student
    - max_score: sum of per-item max_score from subject_question_config
    """
    from sqlalchemy import text
    try:
        qc_query = text(
            """
            SELECT question_id, COALESCE(max_score, 0) AS max_score
            FROM subject_question_config
            WHERE batch_code = :batch_code
              AND subject_name = :subject_name
              AND question_type_enum = 'questionnaire'
            """
        )
        rows = self.db_session.execute(qc_query, {
            'batch_code': batch_code,
            'subject_name': subject_name
        }).fetchall()
        per_q_max = {str(r[0]): _safe_float(r[1]) for r in rows if r[0] is not None}
        total_max_score = sum(per_q_max.values()) if per_q_max else 0.0

        # Build insert rows
        summary_records = []
        for student_row in student_data:
            student_id = student_row[0]
            student_name = student_row[1]
            school_id = student_row[2]
            school_code = student_row[3]
            school_name = student_row[4]
            class_name = student_row[5]
            subject_id = student_row[6]
            subject_scores_json = student_row[7]

            total_score = 0.0
            try:
                if subject_scores_json:
                    subject_scores = json.loads(subject_scores_json)
                    for qid, sc in subject_scores.items():
                        total_score += _safe_float(sc)
            except Exception:
                total_score = 0.0

            summary_records.append({
                'student_id': int(student_id),
                'student_name': student_name,
                'school_id': int(school_id) if school_id else 0,
                'school_code': school_code,
                'school_name': school_name,
                'class_name': class_name,
                'subject_id': int(subject_id) if subject_id else 0,
                'subject_name': subject_name,
                'batch_code': batch_code,
                'total_score': total_score,
                'max_score': float(total_max_score),
                'dimension_scores': '{}',
                'dimension_max_scores': '{}',
                'is_anomalous': False,
                'subject_type': 'questionnaire'
            })

        if summary_records:
            insert_query = text(
                """
                INSERT INTO student_cleaned_scores 
                (student_id, student_name, school_id, school_code, school_name, class_name,
                 subject_id, subject_name, batch_code, total_score, max_score,
                 dimension_scores, dimension_max_scores, is_anomalous, subject_type)
                VALUES 
                (:student_id, :student_name, :school_id, :school_code, :school_name, :class_name,
                 :subject_id, :subject_name, :batch_code, :total_score, :max_score,
                 :dimension_scores, :dimension_max_scores, :is_anomalous, :subject_type)
                """
            )
            self.db_session.execute(insert_query, summary_records)
            self.db_session.commit()
            print(f"  写入问卷汇总记录(总分/满分): {len(summary_records)} 条")

    except Exception as e:
        print(f"生成问卷汇总(总分/满分)失败: {e}")


if __name__ == "__main__":
    asyncio.run(main())


    

