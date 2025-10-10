# 统计计算服务
import json
import logging
import pandas as pd
import time
from collections import defaultdict
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session

from ..database.models import AggregationLevel, CalculationStatus, BatchDimensionDefinition
from .subjects_builder import SubjectsBuilder
from ..utils.precision import round2_json
from ..database.repositories import StatisticalAggregationRepository, DataAdapterRepository
from ..calculation.calculators import initialize_calculation_system
from ..calculation.engine import CalculationEngine
from sqlalchemy import text

logger = logging.getLogger(__name__)


class CalculationService:
    """统计计算服务"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.repository = StatisticalAggregationRepository(db_session)
        self.data_adapter = DataAdapterRepository(db_session)
        self.engine = initialize_calculation_system()
        # 维度名称缓存 {batch_code: {subject_name: {dimension_code: dimension_name}}}
        self._dimension_name_cache = {}
        # 满分缓存 {batch_code: {subject_name: max_score, dimensions: {dimension_code: max_score}}}
        self._max_score_cache = {}
        # 维度统计缓存 {batch_code: {subject_name: dimension_stats}}
        self._dimension_statistics_cache = {}
        self._questionnaire_keywords = ('问卷', '调查', '满意度', '测评', 'survey', 'questionnaire')
        self._questionnaire_normalized = {kw.lower() for kw in self._questionnaire_keywords}
    
    def _get_dimension_name(self, batch_code: str, subject_name: str, dimension_code: str) -> str:
        """获取维度中文名称，带缓存机制"""
        # 检查缓存
        if (batch_code in self._dimension_name_cache and 
            subject_name in self._dimension_name_cache[batch_code] and
            dimension_code in self._dimension_name_cache[batch_code][subject_name]):
            return self._dimension_name_cache[batch_code][subject_name][dimension_code]
        
        # 查询数据库
        try:
            dimension_def = self.db_session.query(BatchDimensionDefinition).filter(
                BatchDimensionDefinition.batch_code == batch_code,
                BatchDimensionDefinition.subject_name == subject_name,
                BatchDimensionDefinition.dimension_code == dimension_code
            ).first()
            
            dimension_name = dimension_def.dimension_name if dimension_def else dimension_code
            
            # 更新缓存
            if batch_code not in self._dimension_name_cache:
                self._dimension_name_cache[batch_code] = {}
            if subject_name not in self._dimension_name_cache[batch_code]:
                self._dimension_name_cache[batch_code][subject_name] = {}
            self._dimension_name_cache[batch_code][subject_name][dimension_code] = dimension_name
            
            return dimension_name
            
        except Exception as e:
            logger.warning(f"获取维度名称失败: batch_code={batch_code}, subject_name={subject_name}, dimension_code={dimension_code}, error={e}")
            return dimension_code
    
    def _batch_load_dimension_names(self, batch_code: str, subject_name: str) -> Dict[str, str]:
        """批量加载维度名称（优化性能）"""
        try:
            dimension_defs = self.db_session.query(BatchDimensionDefinition).filter(
                BatchDimensionDefinition.batch_code == batch_code,
                BatchDimensionDefinition.subject_name == subject_name
            ).all()
            
            dimension_mapping = {}
            for def_record in dimension_defs:
                dimension_mapping[def_record.dimension_code] = def_record.dimension_name
            
            # 更新缓存
            if batch_code not in self._dimension_name_cache:
                self._dimension_name_cache[batch_code] = {}
            if subject_name not in self._dimension_name_cache[batch_code]:
                self._dimension_name_cache[batch_code][subject_name] = {}
            
            self._dimension_name_cache[batch_code][subject_name].update(dimension_mapping)
            
            return dimension_mapping
            
        except Exception as e:
            logger.warning(f"批量加载维度名称失败: batch_code={batch_code}, subject_name={subject_name}, error={e}")
            return {}
    
    def _get_subject_max_score(self, batch_code: str, subject_name: str) -> float:
        """????????? subject_question_config??? grade_aggregation_main."""
        cache = self._max_score_cache.setdefault(batch_code, {})
        if subject_name in cache:
            return cache[subject_name]

        try:
            query = text(
                """
                SELECT SUM(max_score) AS total_max_score
                FROM subject_question_config
                WHERE batch_code = :batch_code AND subject_name = :subject_name
                """
            )
            row = self.db_session.execute(
                query,
                {'batch_code': batch_code, 'subject_name': subject_name},
            ).fetchone()
            max_score = float(row[0]) if row and row[0] is not None else None

            if max_score is None:
                fallback_row = self.db_session.execute(
                    text(
                        """
                        SELECT subjects
                        FROM grade_aggregation_main
                        WHERE batch_code = :batch_code
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {'batch_code': batch_code},
                ).fetchone()
                if fallback_row and fallback_row[0]:
                    import json as _json

                    payload = fallback_row[0]
                    subjects_array = _json.loads(payload) if isinstance(payload, str) else (payload or [])
                    if isinstance(subjects_array, list):
                        for item in subjects_array:
                            try:
                                if isinstance(item, dict):
                                    name_candidate = (
                                        item.get('subject_name')
                                        or item.get('subjectName')
                                        or item.get('name')
                                        or item.get('code')
                                        or item.get('subjectCode')
                                    )
                                    if str(name_candidate) == str(subject_name):
                                        score_candidate = (
                                            item.get('max_score')
                                            or item.get('maxScore')
                                            or item.get('full_score')
                                            or item.get('fullScore')
                                            or item.get('total_score')
                                            or item.get('totalScore')
                                        )
                                        if score_candidate is not None:
                                            max_score = float(score_candidate)
                                        break
                                elif isinstance(item, str) and item == subject_name:
                                    max_score = 100.0
                                    break
                            except Exception:
                                continue

            if max_score is None:
                max_score = 100.0

            cache[subject_name] = max_score
            logger.debug("?? %s ??: %s", subject_name, max_score)
            return max_score

        except Exception as e:
            logger.warning(
                "????????: batch_code=%s, subject_name=%s, error=%s", batch_code, subject_name, e
            )
            return 100.0


    def _get_dimension_max_score(self, batch_code: str, subject_name: str, dimension_code: str) -> float:
        """获取维度满分，统一从subject_question_config表计算"""
        # 检查缓存
        if (batch_code in self._max_score_cache and 
            'dimensions' in self._max_score_cache[batch_code] and
            subject_name in self._max_score_cache[batch_code]['dimensions'] and
            dimension_code in self._max_score_cache[batch_code]['dimensions'][subject_name]):
            return self._max_score_cache[batch_code]['dimensions'][subject_name][dimension_code]
        
        try:
            # 查询维度满分
            query = text("""
                SELECT SUM(sqc.max_score) as dimension_max_score
                FROM subject_question_config sqc
                LEFT JOIN question_dimension_mapping qdm ON sqc.question_id = qdm.question_id
                WHERE sqc.batch_code = :batch_code 
                    AND sqc.subject_name = :subject_name
                    AND (qdm.dimension_code = :dimension_code OR sqc.instrument_id = :dimension_code)
            """)
            
            result = self.db_session.execute(query, {
                'batch_code': batch_code,
                'subject_name': subject_name,
                'dimension_code': dimension_code
            }).fetchone()
            
            max_score = float(result[0]) if result and result[0] else 0.0
            
            # 如果没有找到维度映射，尝试直接从题目配置获取
            if max_score == 0.0:
                query_fallback = text("""
                    SELECT SUM(max_score) as dimension_max_score
                    FROM subject_question_config
                    WHERE batch_code = :batch_code 
                        AND subject_name = :subject_name
                        AND (question_id LIKE CONCAT(:dimension_code, '%') OR instrument_id = :dimension_code)
                """)
                
                result_fallback = self.db_session.execute(query_fallback, {
                    'batch_code': batch_code,
                    'subject_name': subject_name,
                    'dimension_code': dimension_code
                }).fetchone()
                
                max_score = float(result_fallback[0]) if result_fallback and result_fallback[0] else 0.0
            
            # 更新缓存
            if batch_code not in self._max_score_cache:
                self._max_score_cache[batch_code] = {'dimensions': {}}
            if 'dimensions' not in self._max_score_cache[batch_code]:
                self._max_score_cache[batch_code]['dimensions'] = {}
            if subject_name not in self._max_score_cache[batch_code]['dimensions']:
                self._max_score_cache[batch_code]['dimensions'][subject_name] = {}
            
            self._max_score_cache[batch_code]['dimensions'][subject_name][dimension_code] = max_score
            
            logger.debug(f"获取维度满分: {subject_name}/{dimension_code} = {max_score}")
            return max_score
            
        except Exception as e:
            logger.warning(f"获取维度满分失败: batch_code={batch_code}, subject_name={subject_name}, dimension_code={dimension_code}, error={e}")
            return 0.0  # 维度满分失败时返回0
    

    def _batch_get_max_scores(self, batch_code: str) -> Dict[str, Any]:
        """批量获取批次所有满分信息（性能优化）"""
        try:
            # 批量获取科目满分
            subject_query = text("""
                SELECT subject_name, SUM(max_score) as total_max_score
                FROM subject_question_config
                WHERE batch_code = :batch_code
                GROUP BY subject_name
                ORDER BY subject_name
            """)
            
            subject_results = self.db_session.execute(subject_query, {'batch_code': batch_code}).fetchall()
            
            # 批量获取维度满分
            dimension_query = text("""
                SELECT 
                    sqc.subject_name,
                    COALESCE(qdm.dimension_code, sqc.instrument_id) as dimension_code,
                    SUM(sqc.max_score) as dimension_max_score
                FROM subject_question_config sqc
                LEFT JOIN question_dimension_mapping qdm ON sqc.question_id = qdm.question_id
                WHERE sqc.batch_code = :batch_code
                    AND (qdm.dimension_code IS NOT NULL OR sqc.instrument_id IS NOT NULL)
                GROUP BY sqc.subject_name, COALESCE(qdm.dimension_code, sqc.instrument_id)
                ORDER BY sqc.subject_name, dimension_code
            """)
            
            dimension_results = self.db_session.execute(dimension_query, {'batch_code': batch_code}).fetchall()
            
            # 构建缓存数据
            max_scores = {
                'subjects': {},
                'dimensions': {}
            }
            
            # 处理科目满分
            for row in subject_results:
                subject_name = row.subject_name
                max_score = float(row.total_max_score) if row.total_max_score else 100.0
                max_scores['subjects'][subject_name] = max_score
            
            # 处理维度满分
            for row in dimension_results:
                subject_name = row.subject_name
                dimension_code = row.dimension_code
                max_score = float(row.dimension_max_score) if row.dimension_max_score else 0.0
                
                if subject_name not in max_scores['dimensions']:
                    max_scores['dimensions'][subject_name] = {}
                max_scores['dimensions'][subject_name][dimension_code] = max_score
            
            # 更新缓存
            if batch_code not in self._max_score_cache:
                self._max_score_cache[batch_code] = {}
            
            self._max_score_cache[batch_code].update(max_scores['subjects'])
            
            if 'dimensions' not in self._max_score_cache[batch_code]:
                self._max_score_cache[batch_code]['dimensions'] = {}
            self._max_score_cache[batch_code]['dimensions'].update(max_scores['dimensions'])
            
            logger.info(f"批量获取满分完成: 批次={batch_code}, 科目数={len(max_scores['subjects'])}, 维度数={sum(len(dims) for dims in max_scores['dimensions'].values())}")
            return max_scores
            
        except Exception as e:
            logger.error(f"批量获取满分失败: batch_code={batch_code}, error={e}")
            return {'subjects': {}, 'dimensions': {}}
        
    async def calculate_batch_statistics(self, batch_code: str, config: Dict[str, Any] = None, 
                                       progress_callback: callable = None) -> Dict[str, Any]:
        """计算批次统计数据 - 增强版本，自动生成区域级和学校级数据"""
        logger.info(f"开始增强计算批次 {batch_code} 的统计数据（区域级+学校级）")
        start_time = time.time()
        
        try:
            # 1. 获取学生分数数据
            if progress_callback:
                progress_callback(5, "正在加载学生数据...")
            data = await self._fetch_student_scores(batch_code)
            if data.empty:
                raise ValueError(f"批次 {batch_code} 没有找到学生分数数据")
            
            # 2. 获取配置信息
            calculation_config = config or await self._get_calculation_config(batch_code)
            
            # 3. 字段映射 (将total_score重命名为score以匹配计算引擎)
            if 'total_score' in data.columns:
                data = data.rename(columns={'total_score': 'score'})
            
            # 4. 数据验证
            if progress_callback:
                progress_callback(10, "正在验证数据完整性...")
            validation_result = self.engine.validator.validate_input_data(data, calculation_config)
            if not validation_result['is_valid']:
                raise ValueError(f"数据验证失败: {validation_result['errors']}")
            
            # 4. 执行多科目统计计算 (10-50%)
            if progress_callback:
                progress_callback(15, "正在处理多科目统计指标...")
            
            # 5. 整合多科目区域级结果
            consolidated_regional_results = await self._consolidate_multi_subject_results(
                batch_code, data, validation_result
            )
            
            if progress_callback:
                progress_callback(50, "多科目区域级计算完成")
            
            # 6. 保存区域级数据
            regional_duration = time.time() - start_time
            await self._save_regional_statistics(
                batch_code=batch_code,
                statistics_data=consolidated_regional_results,
                total_students=len(data),
                calculation_duration=regional_duration
            )
            
            logger.info(f"批次 {batch_code} 区域级统计计算完成，耗时 {regional_duration:.2f}s")
            
            # 7. 自动生成学校级数据 (50-90%)
            if progress_callback:
                progress_callback(55, "开始生成学校级统计数据...")
            
            school_results = await self.calculate_batch_all_schools(
                batch_code=batch_code,
                config=calculation_config,
                progress_callback=lambda p, msg: progress_callback(55 + int(p * 0.35), msg) if progress_callback else None
            )
            
            # 8. 整合最终结果 (90-100%)
            if progress_callback:
                progress_callback(90, "正在整合所有统计结果...")
            
            total_duration = time.time() - start_time
            final_results = {
                'batch_code': batch_code,
                'regional_statistics': consolidated_regional_results,
                'school_statistics_summary': {
                    'total_schools': school_results['total_schools'],
                    'successful_schools': school_results['successful_schools'],
                    'failed_schools': school_results['failed_schools'],
                    'school_details': school_results['school_results']
                },
                'calculation_duration': total_duration,
                'total_students': len(data),
                'validation_warnings': validation_result.get('warnings', [])
            }
            
            if progress_callback:
                progress_callback(100, "批次统计计算完成")
            
            logger.info(f"批次 {batch_code} 增强统计计算完成，总耗时 {total_duration:.2f}s，"
                       f"处理学生数: {len(data)}，成功生成 {school_results['successful_schools']} 个学校数据")
            
            return final_results
            
        except Exception as e:
            logger.error(f"批次 {batch_code} 增强统计计算失败: {str(e)}")
            # 更新失败状态
            await self._update_calculation_status(batch_code, CalculationStatus.FAILED, str(e))
            raise
    
    async def calculate_school_statistics(self, batch_code: str, school_id: str, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """计算学校级统计数据"""
        logger.info(f"开始计算批次 {batch_code} 学校 {school_id} 的统计数据")
        start_time = time.time()

        try:
            # 1. 获取学校学生分数数据
            school_scores = await self._fetch_school_scores(batch_code, school_id)
            if school_scores.empty:
                raise ValueError(f"学校 {school_id} 在批次 {batch_code} 中未找到学生分数数据")

            # 2. 将 total_score 映射为 score 以复用批次级统一流程
            if 'total_score' in school_scores.columns:
                school_scores = school_scores.rename(columns={'total_score': 'score'})

            # 3. 复用批次级多科目整合逻辑，对单校数据生成增强指标
            consolidated_results = await self._consolidate_multi_subject_results(
                batch_code,
                school_scores,
                validation_result=None,
            )

            # 4. 写入数据库
            duration = time.time() - start_time
            school_name = await self._get_school_name(school_id)
            total_students = int(school_scores['student_id'].nunique()) if 'student_id' in school_scores else len(school_scores)

            await self._save_school_statistics(
                batch_code=batch_code,
                school_id=school_id,
                school_name=school_name,
                statistics_data=consolidated_results,
                total_students=total_students,
                calculation_duration=duration
            )

            logger.info(f"学校 {school_id} 统计计算完成，用时 {duration:.2f}s，处理学生数: {total_students}")

            return {
                'batch_code': batch_code,
                'school_id': school_id,
                'school_name': school_name,
                'statistics': consolidated_results,
                'calculation_duration': duration,
                'total_students': total_students
            }

        except Exception as e:
            logger.error(f"学校 {school_id} 统计计算失败: {str(e)}")
            raise

    async def calculate_batch_all_schools(self, batch_code: str, config: Dict[str, Any] = None,
                                        progress_callback: callable = None) -> Dict[str, Any]:
        """?????????????"""
        logger.info("???????? %s ?????", batch_code)
        start_time = time.time()

        try:
            if progress_callback:
                progress_callback(0, "????????...")
            school_ids = await self._get_batch_schools(batch_code)
            if not school_ids:
                raise ValueError(f"?? {batch_code} ?????????")

            logger.info("?? %s ?? %s ???", batch_code, len(school_ids))

            results: List[Dict[str, Any]] = []
            successful = 0
            failed: List[Dict[str, Any]] = []

            for index, school_id in enumerate(school_ids):
                try:
                    progress = int((index / len(school_ids)) * 100)
                    if progress_callback:
                        progress_callback(progress, f"?????? {school_id} ({index + 1}/{len(school_ids)})...")

                    school_result = await self.calculate_school_statistics(batch_code, school_id, config)
                    results.append({
                        'school_id': school_id,
                        'school_name': school_result['school_name'],
                        'total_students': school_result['total_students'],
                        'calculation_duration': school_result['calculation_duration'],
                        'status': 'success',
                    })
                    successful += 1
                    logger.debug("?? %s ????????: %s", school_id, school_result['total_students'])

                except Exception as exc:
                    logger.error("?? %s ????: %s", school_id, exc)
                    failed.append({'school_id': school_id, 'error': str(exc), 'status': 'failed'})

            duration = time.time() - start_time
            if progress_callback:
                progress_callback(100, "??????????")

            logger.info(
                "?? %s ????????? %.2fs??? %s??? %s",
                batch_code,
                duration,
                successful,
                len(failed),
            )

            return {
                'batch_code': batch_code,
                'total_schools': len(school_ids),
                'successful_schools': successful,
                'failed_schools': failed,
                'school_results': results,
                'processing_time': duration,
            }

        except Exception as exc:
            logger.exception("??????????: %s", exc)
            raise

    async def calculate_statistics(self, batch_code: str, aggregation_level: AggregationLevel,
                                 school_id: Optional[str] = None) -> Dict[str, Any]:
        """计算统计数据
        
        Args:
            batch_code: 批次代码
            aggregation_level: 汇聚级别
            school_id: 学校ID（学校级汇聚时必须）
            
        Returns:
            计算结果字典
        """
        logger.info(f"计算统计数据: batch_code={batch_code}, level={aggregation_level.value}, school_id={school_id}")
        
        try:
            if aggregation_level == AggregationLevel.REGIONAL:
                return await self.calculate_batch_statistics(batch_code)
            elif aggregation_level == AggregationLevel.SCHOOL:
                if not school_id:
                    raise ValueError("学校级计算需要提供school_id")
                return await self.calculate_school_statistics(batch_code, school_id)
            else:
                raise ValueError(f"不支持的汇聚级别: {aggregation_level}")
                
        except Exception as e:
            logger.error(f"统计计算失败: {str(e)}")
            raise
    
    async def recalculate_statistics(self, batch_code: str, aggregation_level: AggregationLevel, 
                                   school_id: Optional[str] = None) -> Dict[str, Any]:
        """重新计算统计数据"""
        logger.info(f"重新计算统计数据: batch_code={batch_code}, level={aggregation_level.value}, school_id={school_id}")
        
        try:
            if aggregation_level == AggregationLevel.REGIONAL:
                return await self.calculate_batch_statistics(batch_code)
            elif aggregation_level == AggregationLevel.SCHOOL:
                if not school_id:
                    raise ValueError("学校级重计算需要提供school_id")
                return await self.calculate_school_statistics(batch_code, school_id)
            else:
                raise ValueError(f"不支持的汇聚级别: {aggregation_level}")
                
        except Exception as e:
            logger.error(f"重新计算失败: {str(e)}")
            raise
    
    def get_engine_performance_stats(self) -> Dict[str, Any]:
        """获取计算引擎性能统计"""
        return self.engine.get_performance_stats()
    
    def reset_engine_performance_stats(self):
        """重置计算引擎性能统计"""
        self.engine.reset_performance_stats()

    # ================================
    # 输出结构稳健性修复（问卷选项去重兜底）
    # ================================
    def _dedupe_option_list(self, options: Any) -> Any:
        try:
            if not isinstance(options, list) or not options:
                return options
            seen = {}
            for it in options:
                try:
                    lvl = int(it.get('option_level')) if isinstance(it, dict) and it.get('option_level') is not None else None
                except Exception:
                    lvl = None
                if lvl is None:
                    continue
                if lvl not in seen:
                    seen[lvl] = dict(it)
            return sorted(seen.values(), key=lambda x: x.get('option_level', 0))
        except Exception:
            return options

    def _sanitize_questionnaire_subjects(self, subjects: Any) -> Any:
        """对问卷subjects做最终兜底清洗：按等级去重选项分布。"""
        if not isinstance(subjects, list):
            return subjects
        for subj in subjects:
            try:
                if not isinstance(subj, dict) or str(subj.get('type', '')).lower() != 'questionnaire':
                    continue
                dims = subj.get('dimensions')
                if isinstance(dims, list):
                    for dim in dims:
                        if not isinstance(dim, dict):
                            continue
                        # 维度级选项分布
                        if isinstance(dim.get('option_distribution'), list):
                            dim['option_distribution'] = self._dedupe_option_list(dim['option_distribution'])
                        # 题目级选项分布
                        qs = dim.get('questions')
                        if isinstance(qs, list):
                            for q in qs:
                                if isinstance(q, dict) and isinstance(q.get('option_distribution'), list):
                                    q['option_distribution'] = self._dedupe_option_list(q['option_distribution'])
            except Exception:
                continue
        return subjects

    # ================================
    # 私有辅助方法
    # ================================
    
    async def _fetch_student_scores(self, batch_code: str) -> pd.DataFrame:
        """获取批次学生分数数据 - 使用数据适配器"""
        logger.debug(f"使用数据适配器获取批次 {batch_code} 的学生分数数据")
        
        try:
            # 首先检查数据准备状态
            readiness = self.data_adapter.check_data_readiness(batch_code)
            if not readiness['is_ready']:
                logger.warning(f"批次 {batch_code} 数据准备状态: {readiness['completeness_ratio']:.2%}")
            
            # 使用适配器获取学生分数数据
            student_scores = self.data_adapter.get_student_scores(batch_code)
            
            if not student_scores:
                logger.warning(f"批次 {batch_code} 没有找到学生分数数据")
                return pd.DataFrame()
            
            # 转换为DataFrame，保持与原有格式兼容
            df_data = []
            for score_record in student_scores:
                df_data.append({
                    'student_id': score_record['student_id'],
                    'student_name': score_record.get('student_name', ''),
                    'school_id': score_record['school_id'],
                    'school_code': score_record.get('school_code', ''),
                    'school_name': score_record.get('school_name', ''),
                    'subject_name': score_record['subject_name'],
                    'total_score': score_record['total_score'],
                    'max_score': score_record['max_score'],
                    'subject_type': score_record['subject_type'],
                    'grade': score_record.get('grade', ''),
                    'dimensions': json.dumps(score_record.get('dimensions', {}), ensure_ascii=False) if isinstance(score_record.get('dimensions'), dict) else str(score_record.get('dimensions', '{}')),
                    'data_source': score_record.get('data_source', 'unknown')
                })
            
            df = pd.DataFrame(df_data)
            
            logger.info(f"获取到 {len(df)} 条学生分数记录，包含 {df['subject_name'].nunique()} 个科目")
            logger.info(f"数据源类型: {df['data_source'].value_counts().to_dict()}")
            return df
            
        except Exception as e:
            logger.error(f"获取学生分数数据失败: {e}")
            raise
    
    async def _fetch_school_scores(self, batch_code: str, school_id: str) -> pd.DataFrame:
        """获取学校学生分数数据 - 使用数据适配器"""
        logger.debug(f"使用数据适配器获取学校 {school_id} 在批次 {batch_code} 的学生分数数据")
        
        try:
            # 使用适配器获取特定学校的学生分数数据
            student_scores = self.data_adapter.get_student_scores(batch_code, school_id=school_id)
            
            if not student_scores:
                logger.warning(f"学校 {school_id} 在批次 {batch_code} 没有找到学生分数数据")
                return pd.DataFrame()
            
            # 转换为DataFrame，保持与原有格式兼容
            df_data = []
            for score_record in student_scores:
                df_data.append({
                    'student_id': score_record['student_id'],
                    'student_name': score_record.get('student_name', ''),
                    'school_id': score_record['school_id'],
                    'school_code': score_record.get('school_code', ''),
                    'school_name': score_record.get('school_name', ''),
                    'subject_name': score_record['subject_name'],
                    'total_score': score_record['total_score'],
                    'max_score': score_record['max_score'],
                    'subject_type': score_record['subject_type'],
                    'grade': score_record.get('grade', ''),
                    'dimensions': json.dumps(score_record.get('dimensions', {}), ensure_ascii=False) if isinstance(score_record.get('dimensions'), dict) else str(score_record.get('dimensions', '{}')),
                    'data_source': score_record.get('data_source', 'unknown')
                })
            
            df = pd.DataFrame(df_data)
            
            logger.info(f"学校 {school_id} 获取到 {len(df)} 条学生分数记录，包含 {df['subject_name'].nunique()} 个科目")
            try:
                data_source_counts = df['data_source'].astype(str).value_counts().to_dict()
                logger.info(f"数据源类型: {data_source_counts}")
            except Exception as e:
                logger.warning(f"无法统计数据源类型: {e}")
                logger.info(f"数据源样本: {df['data_source'].head().tolist()}")
            return df
            
        except Exception as e:
            logger.error(f"获取学校 {school_id} 分数数据失败: {e}")
            raise
    
    async def _get_calculation_config(self, batch_code: str) -> Dict[str, Any]:
        """获取计算配置 - 使用数据适配器"""
        try:
            # 从数据适配器获取批次摘要信息
            batch_summary = self.data_adapter.get_batch_summary(batch_code)
            
            # 从数据库获取年级信息
            grade_level = self._get_batch_grade_level(batch_code)
            
            # 构建计算配置 - 使用默认值，具体计算时会使用实际满分
            config = {
                'max_score': 100,  # 默认值，实际计算时会被替换
                'grade_level': grade_level,
                'percentiles': [10, 25, 50, 75, 90],
                'required_columns': ['score'],
                'batch_summary': batch_summary  # 包含批次的科目信息
            }
            
            logger.debug(f"批次 {batch_code} 计算配置: 年级={grade_level}, 数据源={batch_summary.get('data_source', 'unknown')}")
            return config
            
        except Exception as e:
            logger.warning(f"获取批次 {batch_code} 配置失败，使用默认配置: {e}")
            # 返回默认配置
            return {
                'max_score': 100,
                'grade_level': '7th_grade',  # 默认初中
                'percentiles': [10, 25, 50, 75, 90],
                'required_columns': ['score']
            }
    
    async def _get_batch_schools(self, batch_code: str) -> List[str]:
        """获取批次中的所有学校ID - 使用数据适配器"""
        logger.debug(f"从数据适配器获取批次 {batch_code} 的学校列表")
        
        try:
            # 获取批次的所有学生分数数据
            student_scores = self.data_adapter.get_student_scores(batch_code)
            
            if not student_scores:
                logger.warning(f"批次 {batch_code} 没有找到学生分数数据")
                return []
            
            # 提取所有学校ID
            school_ids = set()
            for score_record in student_scores:
                school_id = score_record.get('school_id')
                if school_id and not isinstance(school_id, dict):
                    school_ids.add(str(school_id))
            
            schools = sorted(list(school_ids))
            logger.info(f"批次 {batch_code} 包含 {len(schools)} 个学校")
            return schools
            
        except Exception as e:
            logger.error(f"获取学校列表失败: {e}")
            return []
    
    async def _get_school_name(self, school_id: str) -> str:
        """获取学校名称"""
        try:
            from sqlalchemy import text
            query = text("SELECT DISTINCT school_name FROM student_cleaned_scores WHERE school_code = :school_code LIMIT 1")
            result = self.db_session.execute(query, {'school_code': school_id})
            row = result.fetchone()
            if row:
                return row[0]
        except Exception as e:
            logger.warning(f"获取学校名称失败: {e}")
        
        # 返回默认名称
        return f"学校_{school_id}"
    
    async def _get_batch_subjects(self, batch_code: str) -> List[Dict[str, Any]]:
        """获取批次中的所有科目配置 - 使用数据适配器"""
        logger.debug(f"从数据适配器获取批次 {batch_code} 的科目配置")
        
        try:
            # 使用数据适配器获取科目配置
            subject_configs = self.data_adapter.get_subject_configurations(batch_code)
            
            if not subject_configs:
                logger.warning(f"批次 {batch_code} 没有找到科目配置")
                return []
            
            # 转换格式以保持兼容性
            subjects = []
            for config in subject_configs:
                normalized_type = self._normalize_subject_type(config)
                subjects.append({
                    'subject_name': config.get('subject_name'),
                    'max_score': config.get('max_score'),
                    'question_count': config.get('question_count'),
                    'subject_type': normalized_type,
                    'question_type_enum': config.get('question_type_enum'),
                    'instrument_id': config.get('instrument_id'),
                })
            logger.info(f"批次 {batch_code} 包含 {len(subjects)} 个科目")
            logger.debug(f"科目类型分布: {[s['subject_type'] for s in subjects]}")
            return subjects
            
        except Exception as e:
            logger.error(f"获取科目配置失败: {e}")
            return []
    
    def _normalize_subject_type(self, subject_config: Dict[str, Any]) -> str:
        """统一科目类型判断逻辑 - 与DataAdapterRepository保持一致"""
        subject_name = subject_config.get('subject_name', '')
        subject_type = subject_config.get('subject_type', '')
        question_type_enum = subject_config.get('question_type_enum', '')
        instrument_id = subject_config.get('instrument_id')

        subject_type_value = (subject_type or '').strip().lower()
        question_type_value = (question_type_enum or '').strip().lower()

        if question_type_value == 'questionnaire' or subject_type_value == 'questionnaire':
            return 'questionnaire'

        if instrument_id:
            instrument_value = str(instrument_id).strip().lower()
            if any(token in instrument_value for token in ('likert', 'survey', 'questionnaire', '问卷')):
                return 'questionnaire'

        name_value = (subject_name or '').strip()
        name_lower = name_value.lower()
        for kw in self._questionnaire_keywords:
            if kw and kw in name_value:
                return 'questionnaire'
        for kw in self._questionnaire_normalized:
            if kw and kw in name_lower:
                return 'questionnaire'

        if subject_type_value:
            return subject_type_value
        if question_type_value:
            return question_type_value
        return 'exam'  # 默认考试类型
    
    async def _consolidate_multi_subject_results(self, batch_code: str, scores_df: pd.DataFrame, 
                                                validation_result: Dict[str, Any] = None,
                                                precomputed_dimension_stats: Dict[str, Dict[str, Any]] = None) -> Dict[str, Any]:
        """整合多科目计算结果"""
        logger.info(f"开始整合批次 {batch_code} 的多科目统计结果")
        
        # 获取科目配置信息
        subjects_config = await self._get_batch_subjects(batch_code)
        if not subjects_config:
            raise ValueError(f"批次 {batch_code} 缺少科目配置")
        self._dimension_statistics_cache[batch_code] = {}

        consolidated = {
            'academic_subjects': {},
            'non_academic_subjects': {},  # 用于问卷类科目
            'calculation_metadata': {
                'calculation_time': time.time(),
                'data_version': '1.0',
                'algorithm_versions': {},
                'total_subjects': len(subjects_config),
                'batch_code': batch_code
            }
        }
        
        # 批量获取所有满分信息（性能优化）
        batch_max_scores = self._batch_get_max_scores(batch_code)
        
        # 为每个科目计算统计指标
        for subject_config in subjects_config:
            subject_name = subject_config['subject_name']
            # 使用统一的满分计算方法
            max_score = self._get_subject_max_score(batch_code, subject_name)
            subject_type = self._normalize_subject_type(subject_config)
            
            logger.debug(f"处理科目: {subject_name} (满分: {max_score}, 类型: {subject_type}, 来源: subject_question_config)")
            
            # 筛选该科目的数据
            subject_data_df = scores_df[scores_df['subject_name'] == subject_name].copy()
            if subject_data_df.empty:
                # 容错：当清洗阶段因数据缺失/过滤导致该科目无数据时，跳过该科目并记录
                logger.warning(f"科目 {subject_name} 在批次 {batch_code} 的清洗数据缺失，已跳过该科目")
                try:
                    consolidated['calculation_metadata'].setdefault('skipped_subjects', []).append(subject_name)
                except Exception:
                    pass
                continue

            # 清洗表中的数据已经是每个学生每个科目一条记录
            logger.debug(f"清洗数据记录数: {len(subject_data_df)}")
            
            # 直接创建计算用的DataFrame（数据已经清洗和聚合）
            # 注意：在上面的字段映射中，total_score已经被重命名为score
            score_column = 'score' if 'score' in subject_data_df.columns else 'total_score'
            calculation_df = pd.DataFrame({
                'score': subject_data_df[score_column].fillna(0).astype(float),
                'student_id': subject_data_df['student_id'],
                'school_id': subject_data_df['school_id']
            })
            
            # 计算该科目的唯一学生数量
            unique_student_count = len(calculation_df)
            logger.debug(f"科目 {subject_name} 学生数: {unique_student_count}")
            
            # 从数据库中获取批次的真实年级信息
            grade_level = self._get_batch_grade_level(batch_code)
            
            # 科目专用配置，使用统一计算的满分
            subject_calculation_config = {
                'max_score': float(max_score),  # 使用从 subject_question_config 表计算的满分
                'grade_level': grade_level,
                'percentiles': [10, 25, 50, 75, 90],  # 包含用户要求的P10, P50, P90
                'required_columns': ['score']
            }
            
            try:
                # 根据科目类型选择不同的计算策略
                if subject_type == 'questionnaire':
                    # 问卷类科目：使用学校/区域上下文数据直接计算，避免不必要的全批次明细扫描
                    basic_stats, educational_metrics, percentiles, discrimination, dimension_statistics = \
                        await self._calculate_questionnaire_statistics(
                            batch_code, subject_name, max_score, subject_calculation_config,
                            calculation_df=None,
                            subject_data_df=subject_data_df
                        )
                    # 使用预计算维度统计（若提供）避免重复全表扫描
                    if precomputed_dimension_stats is not None:
                        if subject_name not in precomputed_dimension_stats:
                            raise ValueError(f"预计算维度统计缺少科目: {subject_name}")
                        dimension_statistics = precomputed_dimension_stats.get(subject_name) or {}
                else:
                    # 学业科目：使用标准计算流程
                    # 计算各项统计指标
                    logger.info(f"学业科目 {subject_name} 开始计算，数据量: {len(calculation_df)}, 配置: {subject_calculation_config}")
                    
                    basic_stats = self.engine.calculate('basic_statistics', calculation_df, subject_calculation_config)
                    logger.debug(f"科目 {subject_name} 基础统计完成: count={basic_stats.get('count', 0)}")
                    
                    # 验证数据
                    logger.info(f"科目 {subject_name} 数据样本: score范围[{calculation_df['score'].min():.1f}, {calculation_df['score'].max():.1f}], 平均{calculation_df['score'].mean():.1f}")
                    
                    educational_metrics = self.engine.calculate('educational_metrics', calculation_df, subject_calculation_config)
                    if educational_metrics is None:
                        logger.error(f"科目 {subject_name} 教育指标计算返回None!")
                        educational_metrics = {}
                    else:
                        logger.info(f"科目 {subject_name} 教育指标计算完成: grade_level={grade_level}, max_score={max_score}")
                        logger.info(f"科目 {subject_name} 教育指标结果: {educational_metrics}")
                    
                    percentiles = self.engine.calculate('percentiles', calculation_df, subject_calculation_config)
                    logger.debug(f"科目 {subject_name} 百分位数计算完成")
                    
                    # 计算区分度（如果数据量足够）
                    discrimination = None
                    if len(calculation_df) >= 10:
                        discrimination = self.engine.calculate('discrimination', calculation_df, subject_calculation_config)
                        logger.debug(f"科目 {subject_name} 区分度计算完成: {discrimination.get('discrimination_index', 0)}")
                    
                    # 维度统计：优先使用预计算缓存，减少重复全表扫描
                    if precomputed_dimension_stats is not None:
                        if subject_name not in precomputed_dimension_stats:
                            raise ValueError(f"预计算维度统计缺少科目: {subject_name}")
                        dimension_statistics = precomputed_dimension_stats.get(subject_name) or {}
                    else:
                        dimension_statistics = await self._calculate_subject_dimensions(
                            batch_code, subject_name, subject_data_df, subject_type
                        )

                        if not percentiles:
                            raise ValueError(f"科目 {subject_name} 百分位统计缺失")
                        if discrimination is None:
                            raise ValueError(f"科目 {subject_name} 区分度统计缺失")
                        if not educational_metrics or not educational_metrics.get('grade_distribution'):
                            raise ValueError(f"科目 {subject_name} 等级分布统计缺失")
                    logger.debug(f"科目 {subject_name} 维度统计完成: {len(dimension_statistics)} 个维度")
                
                # 整合该科目的结果
                subject_result = self._build_subject_statistics(
                    subject_name, max_score, basic_stats, educational_metrics, 
                    percentiles, discrimination, unique_student_count, dimension_statistics
                )

                if subject_type == 'questionnaire' and not dimension_statistics:
                    raise ValueError(f"问卷科目 {subject_name} 缺少维度统计数据")
                cache_for_batch = self._dimension_statistics_cache.setdefault(batch_code, {})
                cache_for_batch[subject_name] = dimension_statistics or {}

                # 根据科目类型存储到对应的分类中
                if subject_type == 'questionnaire':
                    consolidated['non_academic_subjects'][subject_name] = subject_result
                    logger.info(f"问卷科目 {subject_name} 统计计算完成，学生数: {len(calculation_df)}")
                else:
                    consolidated['academic_subjects'][subject_name] = subject_result
                    logger.info(f"学业科目 {subject_name} 统计计算完成，学生数: {len(calculation_df)}")
                
            except Exception as e:
                logger.exception(f"科目 {subject_name} 统计计算失败: {e}")
                raise ValueError(f"科目 {subject_name} 增强统计失败: {e}") from e
        
        # 验证警告
        if validation_result:
            consolidated['calculation_metadata']['validation_warnings'] = validation_result.get('warnings', [])
        
        logger.info(f"多科目统计整合完成，处理了 {len(consolidated['academic_subjects'])} 个科目")
        return consolidated
    
    def _build_subject_statistics(self, subject_name: str, max_score: float, 
                                basic_stats: Dict, educational_metrics: Dict,
                                percentiles: Dict, discrimination: Dict = None, 
                                unique_student_count: int = None, 
                                dimension_statistics: Dict[str, Dict[str, Any]] = None) -> Dict[str, Any]:
        """构建单个科目的完整统计数据"""
        
        subject_data = {
            'subject_name': subject_name,
            'max_score': max_score,
            'school_stats': {},
            'grade_distribution': {},
            'statistical_indicators': {},
            'percentiles': {},
            'dimensions': {}  # 为维度分析预留
        }
        
        # 基础统计
        if basic_stats:
            # 使用正确的唯一学生数量，而不是记录总数
            actual_student_count = unique_student_count if unique_student_count is not None else basic_stats.get('count', 0)
            
            subject_data['school_stats'] = {
                'avg_score': basic_stats.get('mean', 0),
                'std_score': basic_stats.get('std', 0),
                'min_score': basic_stats.get('min', 0),
                'max_score_achieved': basic_stats.get('max', 0),  # 最高得分
                'student_count': actual_student_count,  # 修复：使用唯一学生数量
                'score_rate': (basic_stats.get('mean', 0) / max_score) if max_score > 0 else 0  # 得分率
            }
        
        # 教育指标
        if educational_metrics:
            logger.debug(f"处理科目 {subject_name} 教育指标: {educational_metrics}")
            # 等级分布 - 根据年级标准映射
            grade_dist = educational_metrics.get('grade_distribution', {})
            logger.debug(f"科目 {subject_name} 等级分布数据: {grade_dist}")
            
            # 判断是否为初中标准（使用A/B/C/D等级）
            if 'a_count' in grade_dist:
                # 初中标准：A≥80, B70-79, C60-69, D<60
                subject_data['grade_distribution'] = {
                    'excellent': {  # A等级对应优秀
                        'count': grade_dist.get('a_count', 0),
                        'percentage': grade_dist.get('a_rate', 0)
                    },
                    'good': {  # B等级对应良好
                        'count': grade_dist.get('b_count', 0),
                        'percentage': grade_dist.get('b_rate', 0)
                    },
                    'pass': {  # C等级对应及格
                        'count': grade_dist.get('c_count', 0),
                        'percentage': grade_dist.get('c_rate', 0)
                    },
                    'fail': {  # D等级对应不及格
                        'count': grade_dist.get('d_count', 0),
                        'percentage': grade_dist.get('d_rate', 0)
                    }
                }
            else:
                # 小学标准：优秀≥85, 良好70-84, 及格60-69, 不及格<60
                subject_data['grade_distribution'] = {
                    'excellent': {
                        'count': grade_dist.get('excellent_count', 0),
                        'percentage': grade_dist.get('excellent_rate', 0)
                    },
                    'good': {
                        'count': grade_dist.get('good_count', 0),
                        'percentage': grade_dist.get('good_rate', 0)
                    },
                    'pass': {
                        'count': grade_dist.get('pass_count', 0),
                        'percentage': grade_dist.get('pass_rate', 0)
                    },
                    'fail': {
                        'count': grade_dist.get('fail_count', 0),
                        'percentage': grade_dist.get('fail_rate', 0)
                    }
                }
            
            # 统计指标
            subject_data['statistical_indicators'] = {
                'difficulty_coefficient': educational_metrics.get('difficulty_coefficient', 0),
                'pass_rate': educational_metrics.get('pass_rate', 0),
                'excellent_rate': educational_metrics.get('excellent_rate', 0),
                'average_score_rate': educational_metrics.get('average_score_rate', 0)
            }
        
        # 百分位数（包括用户要求的P10, P50, P90）
        if percentiles:
            subject_data['percentiles'] = {
                'P10': percentiles.get('P10', 0),
                'P25': percentiles.get('P25', 0),
                'P50': percentiles.get('P50', 0),  # 中位数
                'P75': percentiles.get('P75', 0),
                'P90': percentiles.get('P90', 0),
                'IQR': percentiles.get('IQR', 0)
            }
        
        # 区分度
        if discrimination:
            if 'statistical_indicators' not in subject_data:
                subject_data['statistical_indicators'] = {}
            subject_data['statistical_indicators']['discrimination_index'] = discrimination.get('discrimination_index', 0)
            subject_data['statistical_indicators']['discrimination_interpretation'] = discrimination.get('interpretation', 'unknown')
        
        # 维度统计
        if dimension_statistics:
            subject_data['dimensions'] = dimension_statistics
        else:
            subject_data['dimensions'] = {}
        
        return subject_data
    
    def _validate_enhanced_subjects(self, scope: str, subjects: List[Dict[str, Any]]) -> None:
        """确保增强subjects结构完整，缺失关键字段时直接抛错。"""
        if not subjects:
            raise ValueError(f"{scope} subjects 构建结果为空")
        for subject in subjects:
            subject_name = subject.get('subject_name')
            metrics = subject.get('metrics')
            if not isinstance(metrics, dict) or not metrics:
                raise ValueError(f"{scope} subjects 缺少metrics: {subject_name}")

            required_metric_keys = ['avg', 'stddev', 'max', 'min', 'subject_full_score', 'student_count', 'score_rate']
            for key in required_metric_keys:
                if metrics.get(key) is None:
                    raise ValueError(f"{scope} subjects 缺少关键指标 {key}: {subject_name}")

            subject_type = (subject.get('type') or 'exam').lower()

            if subject_type != 'questionnaire':
                for key in ('p10', 'p50', 'p90'):
                    if metrics.get(key) is None:
                        raise ValueError(f"{scope} subjects 缺少百分位 {key}: {subject_name}")
                if metrics.get('discrimination') is None:
                    raise ValueError(f"{scope} subjects 缺少区分度: {subject_name}")
                for key in ('difficulty', 'pass_rate', 'excellent_rate', 'good_rate', 'fail_rate'):
                    if metrics.get(key) is None:
                        raise ValueError(f"{scope} subjects 缺少 {key}: {subject_name}")
                if not subject.get('grade_distribution'):
                    raise ValueError(f"{scope} subjects 缺少等级分布: {subject_name}")
            else:
                dimensions_payload = subject.get('dimensions')
                if not isinstance(dimensions_payload, list) or not dimensions_payload:
                    raise ValueError(f"{scope} 问卷subjects缺少维度信息: {subject_name}")
                has_question_distribution = False
                for dim_entry in dimensions_payload:
                    dim_questions = dim_entry.get('questions') if isinstance(dim_entry, dict) else None
                    if isinstance(dim_questions, list) and dim_questions:
                        for q_entry in dim_questions:
                            if isinstance(q_entry, dict) and isinstance(q_entry.get('option_distribution'), list) and q_entry['option_distribution']:
                                has_question_distribution = True
                                break
                    if has_question_distribution:
                        break
                if not has_question_distribution:
                    raise ValueError(f"{scope} 问卷subjects缺少题目选项分布: {subject_name}")

            if scope == 'regional':
                rankings = subject.get('school_rankings')
                if not isinstance(rankings, list) or not rankings:
                    raise ValueError(f"区域subjects缺少学校排名: {subject_name}")
                if subject_type == 'questionnaire':
                    dims = subject.get('dimensions')
                    if not isinstance(dims, list) or not dims:
                        raise ValueError(f"区域问卷subjects缺少维度分布: {subject_name}")
            if scope == 'school':
                # 学校级：问卷科目必须包含题目分布；考试科目不强制维度
                dims = subject.get('dimensions')
                if subject_type == 'questionnaire':
                    if not isinstance(dims, list) or not dims:
                        raise ValueError(f"学校问卷subjects缺少维度信息: {subject_name}")


    async def _save_regional_statistics(self, batch_code: str, statistics_data: Dict[str, Any], 
                                      total_students: int, calculation_duration: float):
        """保存区域级统计数据"""
        # v1.2：计算完成即产出 subjects 结构（传递计算的统计数据）
        builder = SubjectsBuilder()
        try:
            subjects = builder.build_regional_subjects_v12(batch_code, enhanced_stats=statistics_data)
            # 兜底去重，避免任何上游重复平铺泄漏进最终JSON
            subjects = self._sanitize_questionnaire_subjects(subjects)
            logger.debug(f"区域级subjects(v1.2)构建完成，包含 {len(subjects)} 个科目")
        except Exception as e:
            logger.exception(f"构建区域级subjects v1.2 失败: {e}")
            raise RuntimeError(f"批次 {batch_code} 区域subjects增强构建失败") from e
        self._validate_enhanced_subjects('regional', subjects)
        v12_json = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'REGIONAL',
            'subjects': subjects,
        }
        processed = round2_json(v12_json)
        # 统计 ACTIVE 学校数
        try:
            total_schools = self.db_session.execute(text(
                "SELECT COUNT(*) FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"
            ), {"b": batch_code}).scalar() or 0
        except Exception:
            total_schools = 0
        aggregation_data = {
            'batch_code': batch_code,
            'aggregation_level': AggregationLevel.REGIONAL,
            'school_id': 'REGIONAL',
            'school_name': '区域汇总',
            'statistics_data': processed,
            'data_version': 'v1.2',
            'calculation_status': CalculationStatus.COMPLETED,
            'total_students': total_students,
            'total_schools': total_schools,
            'calculation_duration': calculation_duration
        }
        result = self.repository.upsert_statistics(aggregation_data)
        logger.debug(f"区域级统计数据已保存，记录ID: {result.id}")
    
    async def _save_school_statistics(self, batch_code: str, school_id: str, school_name: str,
                                    statistics_data: Dict[str, Any], total_students: int, 
                                    calculation_duration: float):
        """保存学校级统计数据"""
        # v1.2：计算完成即产出 subjects 结构（传递计算的统计数据）
        builder = SubjectsBuilder()
        try:
            # 规范化 school_id，避免诸如 '5001.0' 导致预计算查找不到
            try:
                sid = str(school_id).strip()
                if "." in sid:
                    head, tail = sid.split(".", 1)
                    if tail and set(tail) <= {"0"} and head.lstrip("-+").isdigit():
                        sid = head
            except Exception:
                sid = str(school_id)

            subjects = builder.build_school_subjects_v12(batch_code, sid, enhanced_stats=statistics_data)
            # 兜底去重
            subjects = self._sanitize_questionnaire_subjects(subjects)
            logger.debug(f"学校 {school_id} subjects(v1.2) 构建完成，包含 {len(subjects)} 个科目")
        except Exception as e:
            logger.exception(f"构建学校 {school_id} subjects v1.2 失败: {e}")
            raise RuntimeError(f"批次 {batch_code} 学校 {school_id} subjects增强构建失败") from e
        self._validate_enhanced_subjects('school', subjects)
        v12_json = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'SCHOOL',
            'school_id': sid,
            'school_name': school_name,
            'subjects': subjects,
        }
        processed = round2_json(v12_json)
        # 统计 ACTIVE 学校总数（用于排名口径的一致性）
        try:
            total_schools = self.db_session.execute(text(
                "SELECT COUNT(*) FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"
            ), {"b": batch_code}).scalar() or 0
        except Exception:
            total_schools = 0
        aggregation_data = {
            'batch_code': batch_code,
            'aggregation_level': AggregationLevel.SCHOOL,
            'school_id': school_id,
            'school_name': school_name,
            'statistics_data': processed,
            'data_version': 'v1.2',
            'calculation_status': CalculationStatus.COMPLETED,
            'total_students': total_students,
            'total_schools': total_schools,
            'calculation_duration': calculation_duration
        }
        result = self.repository.upsert_statistics(aggregation_data)
        logger.debug(f"学校级统计数据已保存，记录ID: {result.id}")
    
    # 注意: 以下方法已不再需要，因为现在直接从清洗表获取维度数据
    # _get_batch_dimensions, _get_dimension_question_mapping, _get_dimension_max_score
    # 这些方法基于原始表和题目映射，现在维度数据直接来自清洗表的JSON字段
    
    async def _get_students_score_detail_json(self, batch_code: str, subject_name: str) -> List[Dict[str, Any]]:
        """从清洗表获取学生维度分数JSON数据"""
        logger.debug(f"从清洗表获取批次 {batch_code} 科目 {subject_name} 的学生维度分数")
        
        try:
            from sqlalchemy import text
            query = text("""
                SELECT 
                    student_id,
                    student_name,
                    school_code,
                    school_name,
                    dimension_scores,
                    dimension_max_scores
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code 
                    AND subject_name = :subject_name
                    AND is_valid = 1
                ORDER BY student_id
            """)
            
            result = self.db_session.execute(query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            
            students_data = []
            for row in result.fetchall():
                students_data.append({
                    'student_id': row[0],
                    'student_name': row[1], 
                    'school_code': row[2],
                    'school_name': row[3],
                    'dimension_scores': row[4],  # 维度分数JSON
                    'dimension_max_scores': row[5]  # 维度满分JSON
                })
            
            logger.debug(f"从清洗表获取到 {len(students_data)} 个学生的维度分数数据")
            return students_data
            
        except Exception as e:
            logger.error(f"从清洗表获取学生维度分数失败: {e}")
            return []
    
    def _calculate_dimension_scores(self, students_data: List[Dict[str, Any]], 
                                   dimension_code: str) -> List[float]:
        """从清洗表的维度分数JSON中直接提取维度分数"""
        import json
        
        dimension_scores = []
        
        for student in students_data:
            try:
                dimension_scores_json = student.get('dimension_scores', '{}')
                if isinstance(dimension_scores_json, str):
                    dimension_data = json.loads(dimension_scores_json)
                else:
                    dimension_data = dimension_scores_json or {}
                
                # 直接从JSON中获取指定维度的分数
                dimension_score = 0.0
                if dimension_code in dimension_data:
                    score_info = dimension_data[dimension_code]
                    if isinstance(score_info, dict) and 'score' in score_info:
                        dimension_score = float(score_info['score'])
                    elif isinstance(score_info, (int, float)):
                        dimension_score = float(score_info)
                
                dimension_scores.append(dimension_score)
                
            except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
                logger.warning(f"学生 {student.get('student_id')} 维度 {dimension_code} 分数数据解析失败: {e}")
                dimension_scores.append(0.0)
        
        logger.debug(f"从清洗数据提取到 {len(dimension_scores)} 个维度 {dimension_code} 分数")
        return dimension_scores
    
    async def _calculate_subject_dimensions(
        self,
        batch_code: str,
        subject_name: str,
        subject_data_df: 'pd.DataFrame',
        subject_type: str,
    ) -> Dict[str, Dict[str, Any]]:
        """基于清洗阶段已加载的数据计算科目维度统计。"""
        if subject_data_df.empty:
            raise ValueError(f"科目 {subject_name} 维度计算缺少学生数据")

        dimension_scores_map: Dict[str, List[float]] = defaultdict(list)
        dimension_max_scores_info: Dict[str, Dict[str, Any]] = {}
        dimension_name_mapping = self._batch_load_dimension_names(batch_code, subject_name)

        for _, row in subject_data_df.iterrows():
            raw_dims = row.get('dimensions')
            dims: Dict[str, Any] = {}
            if isinstance(raw_dims, str):
                raw = raw_dims.strip()
                if raw:
                    try:
                        dims = json.loads(raw)
                    except Exception:
                        dims = {}
            elif isinstance(raw_dims, dict):
                dims = raw_dims
            if not isinstance(dims, dict):
                continue

            for dim_code, dim_data in dims.items():
                if not isinstance(dim_data, dict):
                    continue
                score_value = dim_data.get('score')
                if score_value is None:
                    continue
                try:
                    score = float(score_value)
                except (TypeError, ValueError):
                    continue
                dim_code_str = str(dim_code)
                dimension_scores_map[dim_code_str].append(score)
                if dim_code_str not in dimension_max_scores_info:
                    dimension_name = dimension_name_mapping.get(dim_code_str, dim_code_str)
                    dimension_max_scores_info[dim_code_str] = {
                        'name': dimension_name,
                        'max_score': dim_data.get('max_score', 0),
                    }

        if not dimension_scores_map:
            logger.warning(f"科目 {subject_name} 缺少维度JSON数据，将跳过维度统计并继续生成其它指标")
            return {}

        grade_level = self._get_batch_grade_level(batch_code)
        dimension_results: Dict[str, Dict[str, Any]] = {}

        for dimension_code, scores in sorted(dimension_scores_map.items()):
            if not scores or all(s == 0 for s in scores):
                logger.warning(f"科目 {subject_name} 的维度 {dimension_code} 缺少有效得分，已跳过该维度")
                continue

            dimension_max_score = self._get_dimension_max_score(batch_code, subject_name, dimension_code)
            if not dimension_max_score:
                dimension_max_score = float(dimension_max_scores_info.get(dimension_code, {}).get('max_score', 0))
            if dimension_max_score <= 0:
                logger.warning(f"维度 {dimension_code} 缺少有效满分配置，已跳过该维度")
                continue

            dimension_name = dimension_name_mapping.get(dimension_code, dimension_code)
            dimension_df = pd.DataFrame({'score': scores})
            dimension_config = {
                'max_score': float(dimension_max_score),
                'grade_level': grade_level,
                'percentiles': [10, 25, 50, 75, 90],
                'required_columns': ['score'],
            }

            basic_stats = self.engine.calculate('basic_statistics', dimension_df, dimension_config)
            educational_metrics = self.engine.calculate('educational_metrics', dimension_df, dimension_config)
            percentiles = self.engine.calculate('percentiles', dimension_df, dimension_config)
            discrimination = None
            if len(scores) >= 10:
                discrimination = self.engine.calculate('discrimination', dimension_df, dimension_config)

            if not basic_stats or not educational_metrics or not percentiles:
                raise ValueError(f"维度 {dimension_code} 统计结果不完整")

            dimension_entry = {
                'dimension_code': dimension_code,
                'dimension_name': dimension_name,
                'max_score': float(dimension_max_score),
                'question_count': 0,
                'question_ids': [],
                'basic_stats': {
                    'name': dimension_name,
                    'avg_score': basic_stats.get('mean', 0),
                    'std_score': basic_stats.get('std', 0),
                    'min_score': basic_stats.get('min', 0),
                    'max_score_achieved': basic_stats.get('max', 0),
                    'student_count': basic_stats.get('count', 0),
                    'score_rate': (basic_stats.get('mean', 0) / float(dimension_max_score)) if dimension_max_score else 0,
                },
                'percentiles': {
                    'P10': percentiles.get('P10', 0),
                    'P25': percentiles.get('P25', 0),
                    'P50': percentiles.get('P50', 0),
                    'P75': percentiles.get('P75', 0),
                    'P90': percentiles.get('P90', 0),
                    'IQR': percentiles.get('IQR', 0),
                },
                'educational_metrics': {
                    'difficulty_coefficient': educational_metrics.get('difficulty_coefficient', 0),
                    'pass_rate': educational_metrics.get('pass_rate', 0),
                    'excellent_rate': educational_metrics.get('excellent_rate', 0),
                    'average_score_rate': educational_metrics.get('average_score_rate', 0),
                },
            }

            if discrimination:
                dimension_entry['discrimination'] = {
                    'discrimination_index': discrimination.get('discrimination_index', 0),
                    'interpretation': discrimination.get('interpretation', 'unknown'),
                }

            dimension_results[dimension_code] = dimension_entry

        return dimension_results
    async def _calculate_questionnaire_statistics(self, batch_code: str, subject_name: str, 
                                                max_score: float, config: Dict[str, Any],
                                                calculation_df: 'pd.DataFrame' = None,
                                                subject_data_df: 'pd.DataFrame' = None,
                                                school_id: Optional[str] = None) -> tuple:
        """计算问卷类科目的统计数据
        - 区域/默认：从问卷明细聚合到学生总分
        - 学校/已提供calculation_df：直接使用学校数据（每生每科一行的总分），避免全批扫描
        """
        import pandas as pd
        logger.info(f"开始计算问卷科目 {subject_name} 的统计数据")
        
        try:
            if calculation_df is not None:
                # 学校上下文：直接使用传入数据，避免全批次扫描问卷明细
                if not isinstance(calculation_df, pd.DataFrame) or calculation_df.empty:
                    raise ValueError(f"问卷科目 {subject_name} 学校数据为空或格式不符")
                calc_df = calculation_df[['score', 'student_id']].copy()
                logger.info(
                    f"问卷科目 {subject_name} 学校上下文统计: 学生数={len(calc_df)}, "
                    f"范围=[{calc_df['score'].min():.1f}, {calc_df['score'].max():.1f}], 平均={calc_df['score'].mean():.1f}"
                )
                basic_stats = self.engine.calculate('basic_statistics', calc_df, config)
                educational_metrics = self.engine.calculate('educational_metrics', calc_df, config)
                percentiles = self.engine.calculate('percentiles', calc_df, config)
                discrimination = None
                if len(calc_df) >= 10:
                    discrimination = self.engine.calculate('discrimination', calc_df, config)
                # 维度与题目分布：学校级由 SubjectsBuilder 生成，这里不再全批计算
                dimension_statistics = {}
                return basic_stats, educational_metrics, percentiles, discrimination, dimension_statistics

            if subject_data_df is None or subject_data_df.empty:
                raise ValueError(f"问卷科目 {subject_name} 缺少清洗后的维度数据")

            # 区域/默认路径：从清洗后的学生总分聚合
            calc_df = subject_data_df[['score', 'student_id']].copy()
            logger.info(
                f"问卷科目 {subject_name} 区域上下文统计: 学生数={len(calc_df)}, "
                f"范围=[{calc_df['score'].min():.1f}, {calc_df['score'].max():.1f}], 平均={calc_df['score'].mean():.1f}"
            )

            basic_stats = self.engine.calculate('basic_statistics', calc_df, config)
            educational_metrics = self.engine.calculate('educational_metrics', calc_df, config)
            percentiles = self.engine.calculate('percentiles', calc_df, config)
            discrimination = None
            if len(calc_df) >= 10:
                discrimination = self.engine.calculate('discrimination', calc_df, config)

            dimension_statistics = await self._calculate_subject_dimensions(
                batch_code, subject_name, subject_data_df, 'questionnaire'
            )

            if not percentiles:
                raise ValueError(f"问卷科目 {subject_name} 百分位统计缺失")
            if discrimination is None:
                raise ValueError(f"问卷科目 {subject_name} 区分度统计缺失")
            if not educational_metrics or not educational_metrics.get('grade_distribution'):
                raise ValueError(f"问卷科目 {subject_name} 等级分布统计缺失")

            option_distributions = self.data_adapter.get_questionnaire_distribution(batch_code, subject_name)
            if option_distributions:
                logger.info(f"问卷科目 {subject_name} 获取到 {len(option_distributions)} 条选项分布记录")
                dimension_statistics['_option_distributions'] = self._process_option_distributions(option_distributions)

            logger.info(f"问卷科目 {subject_name} 统计计算完成，学生数: {len(calc_df)}")
            return basic_stats, educational_metrics, percentiles, discrimination, dimension_statistics
            
        except Exception as e:
            logger.exception(f"问卷科目 {subject_name} 统计计算失败: {e}")
            raise
    
    def _process_option_distributions(self, distributions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """处理问卷选项分布数据"""
        processed = {}
        
        for dist in distributions:
            question_id = dist['question_id']
            if question_id not in processed:
                processed[question_id] = {
                    'question_id': question_id,
                    'scale_level': dist['scale_level'],
                    'options': {}
                }
            
            processed[question_id]['options'][dist['option_level']] = {
                'student_count': dist['student_count'],
                'percentage': dist['percentage']
            }
        
        return processed

    def _get_batch_grade_level(self, batch_code: str) -> str:
        """从grade_aggregation_main表中获取批次的真实年级信息"""
        try:
            from sqlalchemy import text
            query = text("""
                SELECT grade_level 
                FROM grade_aggregation_main 
                WHERE batch_code = :batch_code
                LIMIT 1
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            row = result.fetchone()
            
            if row:
                grade_level = row[0]
                logger.debug(f"从数据库获取批次 {batch_code} 年级: {grade_level}")
                return grade_level
            else:
                logger.warning(f"批次 {batch_code} 在grade_aggregation_main表中未找到，使用默认年级")
                
        except Exception as e:
            logger.error(f"获取批次年级失败: {e}")
        
        # 默认年级（如果数据库查询失败）
        return '7th_grade'

    async def _update_calculation_status(self, batch_code: str, status: CalculationStatus, error_message: str = None):
        """更新计算状态"""
        try:
            # 更新区域级状态
            regional_stat = self.repository.get_regional_statistics(batch_code)
            if regional_stat:
                self.repository.update_calculation_status(regional_stat.id, status)
                
            logger.debug(f"批次 {batch_code} 计算状态已更新为: {status.value}")
        except Exception as e:
            logger.error(f"更新计算状态失败: {str(e)}")
    
    async def validate_batch_data_quality(self, batch_code: str) -> Dict[str, Any]:
        """全面验证批次数据质量"""
        logger.info(f"开始验证批次 {batch_code} 的数据质量")
        
        validation_results = {
            'batch_code': batch_code,
            'validation_time': time.time(),
            'overall_status': 'UNKNOWN',
            'issues_found': 0,
            'warnings_found': 0,
            'subjects': {},
            'summary': {}
        }
        
        try:
            # 1. 基础数据完整性检查
            basic_checks = await self._validate_basic_data_integrity(batch_code)
            validation_results['basic_checks'] = basic_checks
            
            # 2. 分科目数据质量检查
            subjects_config = await self._get_batch_subjects(batch_code)
            for subject_config in subjects_config:
                subject_name = subject_config['subject_name']
                max_score = subject_config['max_score']
                
                logger.debug(f"验证科目 {subject_name} 数据质量")
                subject_validation = await self._validate_subject_data_quality(
                    batch_code, subject_name, max_score
                )
                validation_results['subjects'][subject_name] = subject_validation
            
            # 3. 维度数据完整性检查
            dimension_checks = await self._validate_dimension_data(batch_code)
            validation_results['dimension_checks'] = dimension_checks
            
            # 4. 汇总验证结果
            validation_results = self._summarize_validation_results(validation_results)
            
            logger.info(f"批次 {batch_code} 数据质量验证完成: {validation_results['overall_status']}")
            return validation_results
            
        except Exception as e:
            logger.error(f"数据质量验证失败: {e}")
            validation_results['overall_status'] = 'ERROR'
            validation_results['error'] = str(e)
            return validation_results
    
    async def _validate_basic_data_integrity(self, batch_code: str) -> Dict[str, Any]:
        """验证基础数据完整性"""
        logger.debug(f"验证批次 {batch_code} 基础数据完整性")
        
        checks = {
            'student_score_detail': {'exists': False, 'count': 0, 'issues': []},
            'subject_question_config': {'exists': False, 'count': 0, 'issues': []},
            'batch_dimension_definition': {'exists': False, 'count': 0, 'issues': []},
            'question_dimension_mapping': {'exists': False, 'count': 0, 'issues': []}
        }
        
        try:
            from sqlalchemy import text
            
            # 检查学生分数数据
            query1 = text("SELECT COUNT(*) FROM student_score_detail WHERE batch_code = :batch_code")
            result1 = self.db_session.execute(query1, {'batch_code': batch_code})
            count1 = result1.fetchone()[0]
            checks['student_score_detail'] = {
                'exists': count1 > 0,
                'count': count1,
                'issues': [] if count1 > 0 else ['无学生分数数据']
            }
            
            # 检查科目题目配置
            query2 = text("SELECT COUNT(*) FROM subject_question_config WHERE batch_code = :batch_code")
            result2 = self.db_session.execute(query2, {'batch_code': batch_code})
            count2 = result2.fetchone()[0]
            checks['subject_question_config'] = {
                'exists': count2 > 0,
                'count': count2,
                'issues': [] if count2 > 0 else ['无科目题目配置数据']
            }
            
            # 检查维度定义
            query3 = text("SELECT COUNT(*) FROM batch_dimension_definition WHERE batch_code = :batch_code")
            result3 = self.db_session.execute(query3, {'batch_code': batch_code})
            count3 = result3.fetchone()[0]
            checks['batch_dimension_definition'] = {
                'exists': count3 > 0,
                'count': count3,
                'issues': [] if count3 > 0 else ['无维度定义数据']
            }
            
            # 检查题目维度映射
            query4 = text("SELECT COUNT(*) FROM question_dimension_mapping WHERE batch_code = :batch_code")
            result4 = self.db_session.execute(query4, {'batch_code': batch_code})
            count4 = result4.fetchone()[0]
            checks['question_dimension_mapping'] = {
                'exists': count4 > 0,
                'count': count4,
                'issues': [] if count4 > 0 else ['无题目维度映射数据']
            }
            
        except Exception as e:
            logger.error(f"基础数据完整性检查失败: {e}")
            for table in checks:
                checks[table]['issues'].append(f"检查失败: {str(e)}")
        
        return checks
    
    async def _validate_subject_data_quality(self, batch_code: str, subject_name: str, max_score: float) -> Dict[str, Any]:
        """验证科目数据质量"""
        logger.debug(f"验证科目 {subject_name} 数据质量")
        
        validation = {
            'subject_name': subject_name,
            'max_score': max_score,
            'issues': [],
            'warnings': [],
            'statistics': {},
            'status': 'OK'
        }
        
        try:
            from sqlalchemy import text
            
            # 获取科目原始数据统计
            query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students,
                    MIN(total_score) as min_score,
                    MAX(total_score) as max_score,
                    AVG(total_score) as avg_score,
                    SUM(CASE WHEN total_score < 0 THEN 1 ELSE 0 END) as negative_scores,
                    SUM(CASE WHEN total_score > :max_score THEN 1 ELSE 0 END) as overmax_scores,
                    SUM(CASE WHEN total_score IS NULL THEN 1 ELSE 0 END) as null_scores
                FROM student_score_detail 
                WHERE batch_code = :batch_code AND subject_name = :subject_name
            """)
            
            result = self.db_session.execute(query, {
                'batch_code': batch_code,
                'subject_name': subject_name,
                'max_score': max_score
            })
            row = result.fetchone()
            
            if row and row[0] > 0:
                validation['statistics'] = {
                    'total_records': row[0],
                    'unique_students': row[1],
                    'min_score': float(row[2]) if row[2] is not None else 0,
                    'max_score': float(row[3]) if row[3] is not None else 0,
                    'avg_score': float(row[4]) if row[4] is not None else 0,
                    'negative_scores': row[5],
                    'overmax_scores': row[6],
                    'null_scores': row[7]
                }
                
                # 数据质量检查
                if row[5] > 0:  # 负分数
                    validation['issues'].append(f"发现 {row[5]} 个负分数")
                    validation['status'] = 'ISSUES'
                
                if row[6] > 0:  # 超出满分
                    validation['issues'].append(f"发现 {row[6]} 个超出满分({max_score})的分数")
                    validation['status'] = 'ISSUES'
                
                if row[7] > 0:  # 空值分数
                    validation['warnings'].append(f"发现 {row[7]} 个空值分数")
                    if validation['status'] == 'OK':
                        validation['status'] = 'WARNINGS'
                
                # 数据重复率检查
                if row[0] > row[1] * 2:  # 如果记录数是学生数的2倍以上，可能存在过多重复
                    duplication_ratio = row[0] / row[1]
                    validation['warnings'].append(f"数据重复率较高: {duplication_ratio:.1f}倍 ({row[0]}条记录/{row[1]}个学生)")
                    if validation['status'] == 'OK':
                        validation['status'] = 'WARNINGS'
            else:
                validation['issues'].append("无数据或数据为空")
                validation['status'] = 'ISSUES'
            
        except Exception as e:
            logger.error(f"科目 {subject_name} 数据质量验证失败: {e}")
            validation['issues'].append(f"验证失败: {str(e)}")
            validation['status'] = 'ERROR'
        
        return validation
    
    async def _validate_dimension_data(self, batch_code: str) -> Dict[str, Any]:
        """验证维度数据完整性"""
        logger.debug(f"验证批次 {batch_code} 维度数据")
        
        validation = {
            'status': 'OK',
            'issues': [],
            'warnings': [],
            'subjects_with_dimensions': 0,
            'total_dimensions': 0,
            'subjects_checked': 0
        }
        
        try:
            subjects_config = await self._get_batch_subjects(batch_code)
            validation['subjects_checked'] = len(subjects_config)
            
            for subject_config in subjects_config:
                subject_name = subject_config['subject_name']
                
                # 检查科目维度定义
                dimensions = await self._get_batch_dimensions(batch_code, subject_name)
                if dimensions:
                    validation['subjects_with_dimensions'] += 1
                    validation['total_dimensions'] += len(dimensions)
                    
                    # 检查每个维度的题目映射
                    for dimension in dimensions:
                        dimension_code = dimension['dimension_code']
                        questions = await self._get_dimension_question_mapping(
                            batch_code, subject_name, dimension_code
                        )
                        
                        if not questions:
                            validation['warnings'].append(
                                f"科目 {subject_name} 维度 {dimension_code} 无题目映射"
                            )
                            if validation['status'] == 'OK':
                                validation['status'] = 'WARNINGS'
                else:
                    validation['warnings'].append(f"科目 {subject_name} 无维度定义")
                    if validation['status'] == 'OK':
                        validation['status'] = 'WARNINGS'
            
            # 如果没有任何科目有维度，这是一个严重问题
            if validation['subjects_with_dimensions'] == 0:
                validation['issues'].append("所有科目都没有维度定义")
                validation['status'] = 'ISSUES'
            
        except Exception as e:
            logger.error(f"维度数据验证失败: {e}")
            validation['issues'].append(f"验证失败: {str(e)}")
            validation['status'] = 'ERROR'
        
        return validation
    
    def _summarize_validation_results(self, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """汇总验证结果"""
        issues_count = 0
        warnings_count = 0
        overall_status = 'OK'
        
        # 统计基础检查问题
        basic_checks = validation_results.get('basic_checks', {})
        for table, check in basic_checks.items():
            issues_count += len(check.get('issues', []))
        
        # 统计科目问题
        subjects = validation_results.get('subjects', {})
        for subject_name, subject_validation in subjects.items():
            issues_count += len(subject_validation.get('issues', []))
            warnings_count += len(subject_validation.get('warnings', []))
        
        # 统计维度检查问题
        dimension_checks = validation_results.get('dimension_checks', {})
        issues_count += len(dimension_checks.get('issues', []))
        warnings_count += len(dimension_checks.get('warnings', []))
        
        # 确定整体状态
        if issues_count > 0:
            overall_status = 'ISSUES'
        elif warnings_count > 0:
            overall_status = 'WARNINGS'
        
        # 更新结果
        validation_results['issues_found'] = issues_count
        validation_results['warnings_found'] = warnings_count
        validation_results['overall_status'] = overall_status
        
        # 创建汇总信息
        validation_results['summary'] = {
            'total_issues': issues_count,
            'total_warnings': warnings_count,
            'subjects_checked': len(subjects),
            'status': overall_status,
            'recommendation': self._get_validation_recommendation(overall_status, issues_count, warnings_count)
        }
        
        return validation_results
    
    def _get_validation_recommendation(self, status: str, issues: int, warnings: int) -> str:
        """获取验证建议"""
        if status == 'OK':
            return "数据质量良好，可以继续计算"
        elif status == 'WARNINGS':
            return f"发现 {warnings} 个警告，建议检查后继续计算"
        elif status == 'ISSUES':
            return f"发现 {issues} 个严重问题，建议修复后再进行计算"
        else:
            return "验证出现错误，需要检查系统状态"


def create_calculation_service(db_session: Session) -> CalculationService:
    """创建计算服务实例"""
    return CalculationService(db_session)
