# 题目选项分布服务
import logging
import pandas as pd
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database.connection import get_db_context
from ..database.models import QuestionOptionDistribution
from ..database import repositories

logger = logging.getLogger(__name__)


class QuestionOptionDistributionService:
    """题目选项分布服务
    
    负责填充和查询问卷题目选项分布数据，实现v1.2规范中
    "题目选项分布独立表与接口，不再嵌入学科JSON"的要求
    """

    @staticmethod
    def _normalize_label(label: Any) -> Optional[str]:
        """将数据库返回的选项标签标准化，过滤掉空值或空白字符串。"""
        if label is None:
            return None
        if isinstance(label, bytes):
            try:
                label = label.decode("utf-8")
            except Exception:
                label = label.decode(errors="ignore")
        text = str(label).strip()
        return text or None
    
    def __init__(self):
        pass  # 使用直接的数据库操作而非repository
        
    def populate_school_option_distributions(self, batch_code: str, subject_name: str, 
                                          school_id: str = None) -> Dict[str, Any]:
        """填充学校级题目选项分布数据
        
        Args:
            batch_code: 批次代码
            subject_name: 科目名称 
            school_id: 学校ID，如果为None则填充所有学校
            
        Returns:
            填充结果统计
        """
        logger.info(f"开始填充题目选项分布数据 - 批次:{batch_code}, 科目:{subject_name}, 学校:{school_id}")
        
        # 获取需要处理的学校列表
        schools_to_process = self._get_schools_for_processing(batch_code, subject_name, school_id)
        
        total_records = 0
        processed_schools = 0
        errors = []
        
        for school in schools_to_process:
            school_code = school['school_id']
            try:
                # 计算该学校的题目选项分布
                school_distributions = self._calculate_school_option_distributions(
                    batch_code, subject_name, school_code
                )
                
                # 保存到数据库
                records_inserted = self._save_option_distributions(
                    batch_code, subject_name, school_code, school_distributions
                )
                
                total_records += records_inserted
                processed_schools += 1
                
                logger.debug(f"学校 {school_code} 填充完成，插入 {records_inserted} 条记录")
                
            except Exception as e:
                error_msg = f"学校 {school_code} 处理失败: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        result = {
            'batch_code': batch_code,
            'subject_name': subject_name,
            'processed_schools': processed_schools,
            'total_schools': len(schools_to_process),
            'total_records_inserted': total_records,
            'errors': errors,
            'success': len(errors) == 0
        }
        
        logger.info(f"题目选项分布填充完成: {result}")
        return result
        
    def get_school_option_distributions(self, batch_code: str, subject_name: str, 
                                      school_id: str) -> Dict[str, Any]:
        """查询学校级题目选项分布
        
        Args:
            batch_code: 批次代码
            subject_name: 科目名称
            school_id: 学校ID
            
        Returns:
            题目选项分布数据
        """
        with get_db_context() as db:
            # 查询选项标签映射（科目级兜底 + 题目级主量表优先）
            label_map = self._get_scale_label_map(batch_code, subject_name, db)
            q_label_maps = self._get_question_scale_label_maps(db, batch_code, subject_name)
            
            # 查询选项分布数据
            sql = text("""
                SELECT question_id, option_level, option_label, count, n_total, pct
                FROM questionnaire_option_distribution
                WHERE batch_code   COLLATE utf8mb4_unicode_ci = CAST(:batch   AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                  AND subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                  AND school_id    COLLATE utf8mb4_unicode_ci = CAST(:school  AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                ORDER BY question_id, option_level
            """)
            
            rows = db.execute(sql, {
                'batch': batch_code,
                'subject': subject_name, 
                'school': school_id
            }).fetchall()
            
            # 组织数据结构
            questions_distribution = {}
            for row in rows:
                question_id = row[0]
                option_level = int(row[1])
                # 读取时优先使用字典映射，避免历史数据中的数值型标签（如“3分”）泄漏到接口
                option_label = (
                    (q_label_maps.get(str(question_id), {}) or {}).get(option_level)
                    or label_map.get(option_level)
                    or row[2]
                    or f"选项{option_level}"
                )
                count = int(row[3])
                n_total = int(row[4])
                pct = float(row[5])
                
                if question_id not in questions_distribution:
                    questions_distribution[question_id] = {
                        'question_id': question_id,
                        'total_responses': n_total,
                        'options': []
                    }
                
                questions_distribution[question_id]['options'].append({
                    'option_level': option_level,
                    'option_label': option_label,
                    'count': count,
                    'pct': pct
                })
            
            return {
                'batch_code': batch_code,
                'subject_name': subject_name,
                'school_id': school_id,
                'questions': list(questions_distribution.values())
            }
    
    def get_regional_option_distributions(self, batch_code: str, subject_name: str) -> Dict[str, Any]:
        """查询区域级题目选项分布（汇总所有学校数据）
        
        Args:
            batch_code: 批次代码
            subject_name: 科目名称
            
        Returns:
            区域级题目选项分布数据
        """
        with get_db_context() as db:
            # 查询选项标签映射（科目级兜底 + 题目级主量表优先）
            label_map = self._get_scale_label_map(batch_code, subject_name, db)
            q_label_maps = self._get_question_scale_label_maps(db, batch_code, subject_name)
            
            # 汇总所有学校的数据
            sql = text("""
                SELECT 
                    question_id,
                    option_level,
                    MIN(option_label) AS option_label,
                    SUM(count) as total_count,
                    SUM(n_total) as total_responses,
                    ROUND(SUM(count) * 100.0 / NULLIF(SUM(n_total), 0), 2) as pct
                FROM questionnaire_option_distribution
                WHERE batch_code   COLLATE utf8mb4_unicode_ci = CAST(:batch   AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                  AND subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                GROUP BY question_id, option_level
                ORDER BY question_id, option_level
            """)
            
            rows = db.execute(sql, {
                'batch': batch_code,
                'subject': subject_name
            }).fetchall()
            
            # 组织数据结构
            questions_distribution = {}
            for row in rows:
                question_id = row[0]
                option_level = int(row[1])
                # 读取时优先使用字典映射，统一区域各校标签
                option_label = (
                    (q_label_maps.get(str(question_id), {}) or {}).get(option_level)
                    or label_map.get(option_level)
                    or row[2]
                    or f"选项{option_level}"
                )
                count = int(row[3])
                n_total = int(row[4])
                pct = float(row[5])
                
                if question_id not in questions_distribution:
                    questions_distribution[question_id] = {
                        'question_id': question_id,
                        'total_responses': n_total,
                        'options': []
                    }
                
                questions_distribution[question_id]['options'].append({
                    'option_level': option_level,
                    'option_label': option_label,
                    'count': count,
                    'pct': pct
                })
            
            return {
                'batch_code': batch_code,
                'subject_name': subject_name,
                'level': 'regional',
                'questions': list(questions_distribution.values())
            }
    
    def _get_schools_for_processing(self, batch_code: str, subject_name: str, 
                                  school_id: str = None) -> List[Dict[str, str]]:
        """获取需要处理的学校列表"""
        with get_db_context() as db:
            if school_id:
                # 优先从问卷明细取该校是否存在数据，名称从主数据左联，不强制要求 ACTIVE
                sql = text("""
                    SELECT q.school_id,
                           COALESCE(smd.standard_school_name, q.school_id) AS standard_school_name
                    FROM (
                        SELECT DISTINCT qqs.school_id
                        FROM questionnaire_question_scores qqs
                        WHERE qqs.batch_code   COLLATE utf8mb4_unicode_ci = CAST(:batch   AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                          AND qqs.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                          AND qqs.school_id    COLLATE utf8mb4_unicode_ci = CAST(:school  AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                    ) q
                    LEFT JOIN school_master_data smd
                      ON smd.batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                     AND smd.school_id  COLLATE utf8mb4_unicode_ci = q.school_id COLLATE utf8mb4_unicode_ci
                """)
                rows = db.execute(sql, {'batch': batch_code, 'school': school_id, 'subject': subject_name}).fetchall()
            else:
                # 从问卷明细直接发现有数据的学校，名称左联主数据
                sql = text("""
                    SELECT q.school_id,
                           COALESCE(smd.standard_school_name, q.school_id) AS standard_school_name
                    FROM (
                        SELECT DISTINCT qqs.school_id
                        FROM questionnaire_question_scores qqs
                        WHERE qqs.batch_code   COLLATE utf8mb4_unicode_ci = CAST(:batch   AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                          AND qqs.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                    ) q
                    LEFT JOIN school_master_data smd
                      ON smd.batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                     AND smd.school_id  COLLATE utf8mb4_unicode_ci = q.school_id COLLATE utf8mb4_unicode_ci
                """)
                rows = db.execute(sql, {'batch': batch_code, 'subject': subject_name}).fetchall()
            
            return [{'school_id': row[0], 'school_name': row[1]} for row in rows]
    
    def _calculate_school_option_distributions(self, batch_code: str, subject_name: str, 
                                             school_code: str) -> Dict[str, List[Dict[str, Any]]]:
        """计算学校级题目选项分布"""
        with get_db_context() as db:
            # 从问卷明细计算分布，优先使用记录的 option_level；缺失时按原始分数映射，并考虑反向题 is_reverse。
            # 为兼容 ONLY_FULL_GROUP_BY，将等级计算放入子查询，再在外层分组聚合。
            sql = text("""
                SELECT 
                    t.question_id,
                    t.option_level,
                    COUNT(*) as count,
                    SUM(COUNT(*)) OVER (PARTITION BY t.question_id) as n_total
                FROM (
                    SELECT 
                        qqs.question_id,
                        COALESCE(
                            qqs.option_level,
                            (
                                CASE 
                                    WHEN qqs.max_score IS NULL OR qqs.max_score = 0 OR qqs.scale_level IS NULL THEN NULL
                                    ELSE 
                                        (
                                            CASE WHEN COALESCE(qqs.is_reverse, 0) = 1
                                                THEN (qqs.scale_level + 1 - 
                                                    GREATEST(
                                                        1, 
                                                        LEAST(
                                                            qqs.scale_level,
                                                            ROUND(COALESCE(qqs.original_score,0) / NULLIF(qqs.max_score,0) * qqs.scale_level, 0)
                                                        )
                                                    )
                                                )
                                                ELSE GREATEST(
                                                        1, 
                                                        LEAST(
                                                            qqs.scale_level,
                                                            ROUND(COALESCE(qqs.original_score,0) / NULLIF(qqs.max_score,0) * qqs.scale_level, 0)
                                                        )
                                                    )
                                            END
                                        )
                                END
                            )
                        ) AS option_level
                    FROM questionnaire_question_scores qqs
                    WHERE qqs.batch_code   COLLATE utf8mb4_unicode_ci = CAST(:batch   AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                      AND qqs.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                      AND qqs.school_id    COLLATE utf8mb4_unicode_ci = CAST(:school  AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                ) t
                GROUP BY t.question_id, t.option_level
                ORDER BY t.question_id, t.option_level
            """)
            
            rows = db.execute(sql, {
                'batch': batch_code,
                'subject': subject_name,
                'school': school_code
            }).fetchall()
            
            # 组织数据结构并计算百分比
            distributions = {}
            for row in rows:
                question_id = row[0]
                option_level = int(row[1])
                count = int(row[2])
                n_total = int(row[3])
                pct = round(count * 100.0 / n_total, 2) if n_total > 0 else 0.0
                
                if question_id not in distributions:
                    distributions[question_id] = []
                
                distributions[question_id].append({
                    'option_level': option_level,
                    'count': count,
                    'n_total': n_total,
                    'pct': pct
                })
            
            return distributions
    
    def _save_option_distributions(self, batch_code: str, subject_name: str, school_code: str,
                                 distributions: Dict[str, List[Dict[str, Any]]]) -> int:
        """保存题目选项分布数据到数据库"""
        total_inserted = 0
        
        with get_db_context() as db:
            # 获取量表标签映射（科目级兜底 + 题目级主量表）
            subject_label_map = self._get_scale_label_map(batch_code, subject_name, db)
            q_label_maps = self._get_question_scale_label_maps(db, batch_code, subject_name)
            
            # 先删除已有记录
            db.execute(text("""
                DELETE FROM questionnaire_option_distribution 
                WHERE batch_code = :batch 
                  AND subject_name = :subject 
                  AND school_id = :school
            """), {
                'batch': batch_code,
                'subject': subject_name,
                'school': school_code
            })
            
            # 插入新记录
            for question_id, options in distributions.items():
                for option_data in options:
                    option_level = option_data['option_level']
                    q_map = q_label_maps.get(str(question_id), {})
                    option_label = q_map.get(option_level) or subject_label_map.get(option_level) or f"选项{option_level}"
                    
                    db.execute(text("""
                        INSERT INTO questionnaire_option_distribution 
                        (batch_code, school_id, subject_name, question_id, 
                         option_level, option_label, count, n_total, pct)
                        VALUES (:batch, :school, :subject, :question, 
                                :level, :label, :count, :total, :pct)
                    """), {
                        'batch': batch_code,
                        'school': school_code,
                        'subject': subject_name,
                        'question': question_id,
                        'level': option_level,
                        'label': option_label,
                        'count': option_data['count'],
                        'total': option_data['n_total'],
                        'pct': option_data['pct']
                    })
                    
                    total_inserted += 1
            
            db.commit()
        
        return total_inserted

    def _get_question_scale_label_maps(self, db: Session, batch_code: str, subject_name: str) -> Dict[str, Dict[int, str]]:
        """构建题目级标签映射：{question_id: {option_level: option_label}}。
        优先使用 questionnaire_scale_options（按该题目的主 instrument_type+scale_level），
        再回退该题在明细中的最常见标签。
        """
        pick_sql = text(
            """
            SELECT question_id, instrument_type, scale_level, MAX(COALESCE(is_reverse, 0)) AS is_reverse, COUNT(*) AS cnt
            FROM questionnaire_question_scores
            WHERE batch_code=:batch AND subject_name=:subject
              AND instrument_type IS NOT NULL AND scale_level IS NOT NULL
            GROUP BY question_id, instrument_type, scale_level
            ORDER BY question_id, cnt DESC
            """
        )
        rows = db.execute(pick_sql, {"batch": batch_code, "subject": subject_name}).fetchall()
        main_scale: Dict[str, Dict[str, str]] = {}
        for qid, inst, scale, rev, cnt in rows:
            qid_s = str(qid)
            if qid_s not in main_scale:
                main_scale[qid_s] = {"instrument_type": str(inst), "scale_level": str(scale), "is_reverse": int(rev or 0)}

        result: Dict[str, Dict[int, str]] = {}
        for qid, meta in main_scale.items():
            inst = meta["instrument_type"]
            scale = meta["scale_level"]
            is_rev = meta.get("is_reverse", 0)
            opts: List[Any] = []
            # 先尝试含 is_reverse 的量表字典（若表结构支持），失败则回退不带 is_reverse 的匹配
            try:
                opt_sql_rev = text(
                    """
                    SELECT option_level, option_label
                    FROM questionnaire_scale_options
                    WHERE instrument_type COLLATE utf8mb4_unicode_ci = CAST(:inst AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                      AND scale_level   COLLATE utf8mb4_unicode_ci = CAST(:scale AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                      AND is_reverse = :rev
                    ORDER BY option_level
                    """
                )
                opts = db.execute(opt_sql_rev, {"inst": inst, "scale": scale, "rev": int(is_rev)}).fetchall()
            except Exception:
                opt_sql = text(
                    """
                    SELECT option_level, option_label
                    FROM questionnaire_scale_options
                    WHERE instrument_type COLLATE utf8mb4_unicode_ci = CAST(:inst AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                      AND scale_level   COLLATE utf8mb4_unicode_ci = CAST(:scale AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                    ORDER BY option_level
                    """
                )
                opts = db.execute(opt_sql, {"inst": inst, "scale": scale}).fetchall()

            normalized_map: Dict[int, str] = {}
            expected_levels = {int(lvl) for (lvl, _lbl) in opts}
            for lvl, lbl in opts:
                label_text = self._normalize_label(lbl)
                if not label_text:
                    continue
                normalized_map[int(lvl)] = label_text

            need_guess = not expected_levels or set(normalized_map.keys()) != expected_levels

            if need_guess:
                # 回退：每题在明细里的最常见 label
                guess_sql = text(
                    """
                    SELECT option_level, option_label, COUNT(*) AS cnt
                    FROM questionnaire_question_scores
                    WHERE batch_code=:batch AND subject_name=:subject AND question_id=:qid AND option_label IS NOT NULL
                    GROUP BY option_level, option_label
                    ORDER BY option_level, cnt DESC
                    """
                )
                rows_guess = db.execute(guess_sql, {"batch": batch_code, "subject": subject_name, "qid": qid}).fetchall()
                best: Dict[int, tuple] = {}
                for lvl, label, cnt in rows_guess:
                    normalized = self._normalize_label(label)
                    if not normalized:
                        continue
                    lvl_int = int(lvl)
                    cnt_int = int(cnt or 0)
                    if lvl_int not in best or cnt_int > best[lvl_int][1]:
                        best[lvl_int] = (normalized, cnt_int)
                for lvl_int, (label_text, _cnt) in best.items():
                    if lvl_int not in normalized_map:
                        normalized_map[lvl_int] = label_text

            if normalized_map:
                result[qid] = normalized_map
        return result
    
    def _get_scale_label_map(self, batch_code: str, subject_name: str, db: Session) -> Dict[int, str]:
        """获取量表标签映射（优先 questionnaire_scale_options，回退问卷明细推断/通用标签）。"""
        try:
            # 先从问卷明细中选择出现次数最多的 instrument_type + scale_level 组合
            pick_sql = text(
                """
                SELECT instrument_type, scale_level, COUNT(*) AS cnt
                FROM questionnaire_question_scores
                WHERE batch_code=:batch AND subject_name=:subject
                  AND instrument_type IS NOT NULL AND scale_level IS NOT NULL
                GROUP BY instrument_type, scale_level
                ORDER BY cnt DESC
                LIMIT 1
                """
            )
            row = db.execute(pick_sql, {"batch": batch_code, "subject": subject_name}).fetchone()

            label_map: Dict[int, str] = {}
            expected_levels = set()
            if row:
                inst, scale = row[0], row[1]
                opt_sql = text(
                    """
                    SELECT option_level, option_label
                    FROM questionnaire_scale_options
                    WHERE instrument_type COLLATE utf8mb4_unicode_ci = CAST(:inst AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                      AND scale_level   COLLATE utf8mb4_unicode_ci = CAST(:scale AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                    ORDER BY option_level
                    """
                )
                opts = db.execute(opt_sql, {"inst": inst, "scale": scale}).fetchall()
                expected_levels = {int(lvl) for (lvl, _lbl) in opts}
                for lvl, lbl in opts:
                    normalized = self._normalize_label(lbl)
                    if not normalized:
                        continue
                    label_map[int(lvl)] = normalized

            if not expected_levels or set(label_map.keys()) != expected_levels:
                # 回退：从问卷明细猜测每个等级最常见的标签
                guess_sql = text(
                    """
                    SELECT option_level, option_label, COUNT(*) AS cnt
                    FROM questionnaire_question_scores
                    WHERE batch_code=:batch AND subject_name=:subject AND option_label IS NOT NULL
                    GROUP BY option_level, option_label
                    ORDER BY option_level, cnt DESC
                    """
                )
                rows = db.execute(guess_sql, {"batch": batch_code, "subject": subject_name}).fetchall()
                best: Dict[int, tuple] = {}
                for lvl, label, cnt in rows:
                    normalized = self._normalize_label(label)
                    if not normalized:
                        continue
                    lvl_int = int(lvl)
                    cnt_int = int(cnt or 0)
                    if lvl_int not in best or cnt_int > best[lvl_int][1]:
                        best[lvl_int] = (normalized, cnt_int)
                for lvl_int, (label_text, _cnt) in best.items():
                    if lvl_int not in label_map:
                        label_map[lvl_int] = label_text

            if label_map:
                return label_map

            # 最后回退：按量表等级数给出通用标签（优先明细，无则用物化表）
            levels_sql = text(
                """
                SELECT COUNT(DISTINCT option_level) AS levels
                FROM questionnaire_question_scores
                WHERE batch_code=:batch AND subject_name=:subject
                """
            )
            lv_row = db.execute(levels_sql, {"batch": batch_code, "subject": subject_name}).fetchone()
            levels = int(lv_row[0] or 0) if lv_row else 0
            if levels == 0:
                levels_sql_qod = text(
                    """
                    SELECT COUNT(DISTINCT option_level) AS levels
                    FROM questionnaire_option_distribution
                    WHERE batch_code=:batch AND subject_name=:subject
                    """
                )
                lv_row2 = db.execute(levels_sql_qod, {"batch": batch_code, "subject": subject_name}).fetchone()
                levels = int(lv_row2[0] or 0) if lv_row2 else 0

            # 尝试：若能识别等级数，优先按 scale_level 从量表表中回退一次
            if levels > 0:
                try:
                    opt_sql_scale = text(
                        """
                        SELECT option_level, option_label, COUNT(*) AS cnt
                        FROM questionnaire_scale_options
                        WHERE scale_level   COLLATE utf8mb4_unicode_ci = CAST(:scale AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                        GROUP BY option_level, option_label
                        ORDER BY option_level, cnt DESC
                        """
                    )
                    rows_so = db.execute(opt_sql_scale, {"scale": str(levels)}).fetchall()
                    if rows_so:
                        best: Dict[int, tuple] = {}
                        for lvl, label, cnt in rows_so:
                            lvl = int(lvl)
                            cnt = int(cnt or 0)
                            if lvl not in best or cnt > best[lvl][1]:
                                best[lvl] = (str(label), cnt)
                        if best:
                            picked_levels = sorted(best.keys())
                            if picked_levels and picked_levels[0] == 0:
                                return {k+1: v for k, (v, _) in best.items()}
                            return {k: v for k, (v, _) in best.items()}
                except Exception:
                    pass

            # 通用标签（4级无中立项）
            if levels == 5:
                return {1: "完全不符合", 2: "基本不符合", 3: "不确定", 4: "基本符合", 5: "完全符合"}
            elif levels == 4:
                return {1: '非常不满意', 2: '不满意', 3: '满意', 4: '非常满意'}
            elif levels == 3:
                return {1: '不满意', 2: '一般', 3: '满意'}

            # 兜底：给出序号标签
            return {i: f"选项{i}" for i in range(1, max(levels, 5) + 1)}

        except Exception as e:
            logger.warning(f"获取量表标签映射失败: {e}，使用默认标签")
            return {1: "选项1", 2: "选项2", 3: "选项3", 4: "选项4", 5: "选项5"}
    
    def cleanup_old_distributions(self, batch_code: str, subject_name: str = None,
                                school_id: str = None) -> Dict[str, Any]:
        """清理旧的题目选项分布数据
        
        Args:
            batch_code: 批次代码
            subject_name: 科目名称，可选
            school_id: 学校ID，可选
            
        Returns:
            清理结果
        """
        logger.info(f"清理题目选项分布数据 - 批次:{batch_code}, 科目:{subject_name}, 学校:{school_id}")
        
        with get_db_context() as db:
            # 构建删除条件
            conditions = ["batch_code = :batch"]
            params = {'batch': batch_code}
            
            if subject_name:
                conditions.append("subject_name = :subject")
                params['subject'] = subject_name
                
            if school_id:
                conditions.append("school_id = :school")
                params['school'] = school_id
            
            # 先查询要删除的记录数
            count_sql = f"SELECT COUNT(*) FROM questionnaire_option_distribution WHERE {' AND '.join(conditions)}"
            count_result = db.execute(text(count_sql), params).fetchone()
            records_to_delete = int(count_result[0]) if count_result else 0
            
            # 执行删除
            delete_sql = f"DELETE FROM questionnaire_option_distribution WHERE {' AND '.join(conditions)}"
            db.execute(text(delete_sql), params)
            db.commit()
            
            result = {
                'batch_code': batch_code,
                'subject_name': subject_name,
                'school_id': school_id,
                'records_deleted': records_to_delete
            }
            
            logger.info(f"题目选项分布数据清理完成: {result}")
            return result

    def backfill_null_option_labels(self, batch_code: str, subject_name: Optional[str] = None) -> Dict[str, Any]:
        """回填 questionnaire_option_distribution 表中 option_label 为空的记录。

        策略：
        - 优先使用科目级量表映射（instrument_type+scale_level 推断）
        - 若无法推断，回退为通用标签："选项{level}"
        - 按题目/等级逐条更新，避免跨科目误填
        """
        fixed = 0
        scanned = 0
        subjects: List[str]
        with get_db_context() as db:
            if subject_name:
                subjects = [subject_name]
            else:
                rows = db.execute(text(
                    """
                    SELECT DISTINCT subject_name
                    FROM questionnaire_option_distribution
                    WHERE batch_code = :batch
                    """
                ), {"batch": batch_code}).fetchall()
                subjects = [r[0] for r in rows]

            for subj in subjects:
                # 科目级兜底标签
                subject_label_map = self._get_scale_label_map(batch_code, subj, db)
                # 构建题目级标签映射
                question_label_maps = self._get_question_scale_label_maps(db, batch_code, subj)
                rows = db.execute(text(
                    """
                    SELECT school_id, question_id, option_level
                    FROM questionnaire_option_distribution
                    WHERE batch_code   COLLATE utf8mb4_unicode_ci = CAST(:batch   AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                      AND subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                      AND (option_label IS NULL OR option_label='')
                    """
                ), {"batch": batch_code, "subject": subj}).fetchall()
                for school_id, qid, level in rows:
                    scanned += 1
                    lvl = int(level)
                    q_map = question_label_maps.get(str(qid), {}) if isinstance(question_label_maps, dict) else {}
                    label = q_map.get(lvl) or (subject_label_map.get(lvl) if isinstance(subject_label_map, dict) else None) or f"选项{lvl}"
                    db.execute(text(
                        """
                        UPDATE questionnaire_option_distribution
                           SET option_label = :label
                         WHERE batch_code   COLLATE utf8mb4_unicode_ci = CAST(:batch   AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                           AND subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                           AND school_id    = :school
                           AND question_id  = :qid
                           AND option_level = :lvl
                           AND (option_label IS NULL OR option_label='')
                        """
                    ), {"label": label, "batch": batch_code, "subject": subj, "school": school_id, "qid": qid, "lvl": lvl})
                    fixed += 1
                db.commit()

        return {"batch_code": batch_code, "subject_name": subject_name, "scanned": scanned, "fixed": fixed}


# 便捷函数
def populate_questionnaire_distributions(batch_code: str, subject_name: str = None) -> Dict[str, Any]:
    """填充问卷题目选项分布数据的便捷函数
    
    Args:
        batch_code: 批次代码
        subject_name: 科目名称，如果为None则处理所有问卷科目
        
    Returns:
        填充结果
    """
    service = QuestionOptionDistributionService()
    
    if subject_name:
        return service.populate_school_option_distributions(batch_code, subject_name)
    else:
        # 处理所有问卷科目
        with get_db_context() as db:
            sql = text("""
                SELECT DISTINCT subject_name 
                FROM student_cleaned_scores 
                WHERE batch_code   COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
                  AND subject_type = 'questionnaire'
            """)
            rows = db.execute(sql, {'batch': batch_code}).fetchall()
            
        results = {}
        for row in rows:
            subject = row[0]
            try:
                results[subject] = service.populate_school_option_distributions(batch_code, subject)
            except Exception as e:
                logger.error(f"填充科目 {subject} 失败: {e}")
                results[subject] = {'error': str(e), 'success': False}
        
        return results
