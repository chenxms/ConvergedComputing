#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enhanced version with fixes for per-subject statistics at school level

修复版本：确保学校级每个科目有独立的增强统计
"""

from __future__ import annotations
import sys
import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np

from sqlalchemy import text
from sqlalchemy.orm import Session

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db
from app.services.subjects_builder import SubjectsBuilder
from app.services.calculation_service import CalculationService

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_enhanced_stats_for_regional(batch_code: str, db: Session) -> Optional[Dict[str, Any]]:
    """获取区域级的增强统计数据"""
    try:
        calc_service = CalculationService(db)
        
        # 获取学生分数数据
        logger.info(f"获取批次 {batch_code} 的学生分数数据...")
        scores_df = await calc_service._fetch_student_scores(batch_code)
        
        if scores_df.empty:
            logger.warning(f"批次 {batch_code} 没有找到学生分数数据")
            return None
        
        # 字段映射
        if 'total_score' in scores_df.columns and 'score' not in scores_df.columns:
            scores_df['score'] = scores_df['total_score']
        
        # 计算区域级统计
        logger.info(f"计算批次 {batch_code} 的区域级增强统计...")
        regional_stats = await calc_service._consolidate_multi_subject_results(
            batch_code=batch_code,
            scores_df=scores_df,
            validation_result={'is_valid': True, 'warnings': []}
        )
        
        # 转换区域级数据格式，提取区分度和其他增强字段
        if regional_stats:
            # 处理学业科目
            for subject_name, subject_data in regional_stats.get('academic_subjects', {}).items():
                # 提取区分度从statistical_indicators
                if 'statistical_indicators' in subject_data:
                    discrimination_index = subject_data['statistical_indicators'].get('discrimination_index', 0)
                    subject_data['discrimination'] = {'discrimination_index': discrimination_index}
                
                # 百分位数已经在正确位置
                # 等级分布已经在正确位置
            
            # 处理非学业科目
            for subject_name, subject_data in regional_stats.get('non_academic_subjects', {}).items():
                # 提取区分度从statistical_indicators
                if 'statistical_indicators' in subject_data:
                    discrimination_index = subject_data['statistical_indicators'].get('discrimination_index', 0)
                    subject_data['discrimination'] = {'discrimination_index': discrimination_index}
        
        return regional_stats
        
    except Exception as e:
        logger.error(f"计算区域级增强统计失败: {e}")
        return None


async def get_enhanced_stats_for_school(batch_code: str, school_id: str, db: Session) -> Optional[Dict[str, Any]]:
    """获取学校级的增强统计数据 - 按科目分别计算"""
    try:
        calc_service = CalculationService(db)
        
        # 获取学校数据
        all_data = await calc_service._fetch_school_scores(batch_code, school_id)
        if all_data.empty:
            logger.warning(f"学校 {school_id} 无数据")
            return None
        
        # 字段映射
        if 'total_score' in all_data.columns and 'score' not in all_data.columns:
            all_data['score'] = all_data['total_score']
        
        # 获取配置
        config = await calc_service._get_calculation_config(batch_code)
        grade_level = config.get('grade_level', '4th_grade')
        
        # 按科目分组计算
        logger.info(f"计算学校 {school_id} 的增强统计（按科目）...")
        subject_stats = {}
        
        # 获取所有科目
        subjects = all_data['subject_name'].unique() if 'subject_name' in all_data.columns else []
        
        for subject_name in subjects:
            # 筛选该科目的数据
            subject_data = all_data[all_data['subject_name'] == subject_name].copy()
            
            if subject_data.empty:
                continue
            
            logger.info(f"  处理科目 {subject_name}，数据量: {len(subject_data)}")
            
            # 获取该科目的满分
            max_score = calc_service._get_subject_max_score(batch_code, subject_name)
            
            # 为该科目准备配置
            subject_config = {
                'max_score': float(max_score),
                'grade_level': grade_level,
                'percentiles': [10, 25, 50, 75, 90],
                'required_columns': ['score']
            }
            
            # 确保数据有 grade_level 字段
            subject_data['grade_level'] = grade_level
            
            # 计算该科目的统计
            subject_results = {}
            
            try:
                # 基础统计
                basic_stats = calc_service.engine.calculate('basic_statistics', subject_data, subject_config)
                subject_results['basic_statistics'] = basic_stats
                
                # 教育指标
                educational_metrics = calc_service.engine.calculate('educational_metrics', subject_data, subject_config)
                subject_results['educational_metrics'] = educational_metrics
                
                # 百分位数
                percentiles = calc_service.engine.calculate('percentiles', subject_data, subject_config)
                subject_results['percentiles'] = percentiles
                
                # 区分度（如果数据足够）
                if len(subject_data) >= 30:
                    discrimination = calc_service.engine.calculate('discrimination', subject_data, subject_config)
                    subject_results['discrimination'] = discrimination
                else:
                    logger.info(f"    科目 {subject_name} 数据不足({len(subject_data)}条)，跳过区分度计算")
                    subject_results['discrimination'] = {'discrimination_index': 0, 'message': '数据不足'}
                
                # 等级分布
                grade_dist = calc_service.engine.calculate('grade_distribution', subject_data, subject_config)
                subject_results['grade_distribution'] = grade_dist
                
            except Exception as e:
                logger.error(f"    科目 {subject_name} 计算失败: {e}")
                # 使用默认值
                subject_results = {
                    'basic_statistics': {},
                    'educational_metrics': {},
                    'percentiles': {},
                    'discrimination': {'discrimination_index': 0},
                    'grade_distribution': {}
                }
            
            # 将该科目的统计结果保存
            subject_stats[subject_name] = {
                "basic_stats": subject_results.get('basic_statistics', {}),
                "educational_metrics": subject_results.get('educational_metrics', {}),
                "percentiles": subject_results.get('percentiles', {}),
                "grade_distribution": subject_results.get('grade_distribution', {}),
                "discrimination": subject_results.get('discrimination', {})
            }
        
        return subject_stats
        
    except Exception as e:
        logger.error(f"计算学校 {school_id} 增强统计失败: {e}")
        return None


async def rewrite_batch_async(batch_code: str) -> None:
    """异步重写批次数据，包含增强统计（修复版）"""
    sb = SubjectsBuilder()
    
    with next(get_db()) as db:
        # 设置数据库超时
        try:
            db.execute(text("SET SESSION net_write_timeout=600"))
            db.execute(text("SET SESSION net_read_timeout=600"))
            db.execute(text("SET SESSION wait_timeout=600"))
        except Exception:
            pass
        
        # 1. 处理区域级数据
        logger.info(f"处理批次 {batch_code} 的区域级数据...")
        
        # 获取区域级增强统计
        regional_enhanced_stats = await get_enhanced_stats_for_regional(batch_code, db)
        
        # 构建区域级subjects（传递增强统计）
        regional_subjects = sb.build_regional_subjects(
            batch_code, 
            enhanced_stats=regional_enhanced_stats
        )
        
        regional_json = {
            "schema_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": regional_subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # 保存区域级数据（确保仅保留一条：先清理再插入）
        try:
            db.execute(text("DELETE FROM statistical_aggregations WHERE batch_code=:b AND aggregation_level='REGIONAL'"), {"b": batch_code})
        except Exception:
            pass
        db.execute(
            text(
                "INSERT INTO statistical_aggregations (batch_code, aggregation_level, school_id, school_name, statistics_data, data_version, calculation_status, created_at, updated_at)"
                " VALUES (:b, 'REGIONAL', NULL, NULL, :d, 'v1.2', 'COMPLETED', NOW(), NOW())"
            ),
            {"b": batch_code, "d": json.dumps(regional_json, ensure_ascii=False)},
        )
        
        # 2. 处理学校级数据
        logger.info(f"处理批次 {batch_code} 的学校级数据...")
        
        schools = db.execute(
            text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code=:b ORDER BY school_code"),
            {"b": batch_code},
        ).fetchall()
        
        total_schools = len(schools)
        logger.info(f"共 {total_schools} 所学校需要处理")
        
        for idx, (school_code,) in enumerate(schools, 1):
            logger.info(f"处理学校 {school_code} ({idx}/{total_schools})...")
            
            # 获取学校级增强统计（按科目分别计算）
            school_enhanced_stats = await get_enhanced_stats_for_school(batch_code, school_code, db)
            
            # 构建学校级subjects（传递增强统计）
            school_subjects = sb.build_school_subjects(
                batch_code, 
                school_code,
                enhanced_stats=school_enhanced_stats
            )
            
            school_json = {
                "schema_version": "v1.2",
                "batch_code": batch_code,
                "aggregation_level": "SCHOOL",
                "school_code": school_code,
                "subjects": school_subjects,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            
            # 保存学校级数据
            db.execute(
                text(
                    "INSERT INTO statistical_aggregations (batch_code, aggregation_level, school_id, statistics_data, data_version, calculation_status, created_at, updated_at)"
                    " VALUES (:b, 'SCHOOL', :s, :d, 'v1.2', 'COMPLETED', NOW(), NOW())"
                    " ON DUPLICATE KEY UPDATE statistics_data=VALUES(statistics_data), updated_at=NOW()"
                ),
                {"b": batch_code, "s": school_code, "d": json.dumps(school_json, ensure_ascii=False)},
            )
            
            # 每处理10所学校提交一次
            if idx % 10 == 0:
                db.commit()
                logger.info(f"已处理 {idx}/{total_schools} 所学校")
        
        # 最终提交
        db.commit()
        logger.info(f"批次 {batch_code} 处理完成！")


def rewrite_batch(batch_code: str) -> None:
    """同步包装器"""
    asyncio.run(rewrite_batch_async(batch_code))


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/rewrite_subjects_v12_enhanced_fixed.py <BATCH> [<BATCH> ...]")
        print("\n此脚本将重新计算并更新statistical_aggregations表中的数据")
        print("修复版本：确保每个科目有独立的增强统计")
        return 1
    
    for batch in argv[1:]:
        print(f"\n{'='*60}")
        print(f"开始处理批次: {batch}")
        print(f"{'='*60}")
        
        try:
            rewrite_batch(batch)
            print(f"\n[成功] 批次 {batch} 处理成功！")
        except Exception as e:
            print(f"\n[失败] 批次 {batch} 处理失败: {e}")
            logger.exception(f"批次 {batch} 处理异常")
            return 1
    
    print(f"\n{'='*60}")
    print("所有批次处理完成！")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
