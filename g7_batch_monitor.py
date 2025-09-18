#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G7-2025全链路批次处理监控器
"""

import os
import time
import asyncio
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

class G7BatchMonitor:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL",
                               "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4")
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.start_time = None
        self.stages = {}

    def log_stage(self, stage_name: str, status: str = "START"):
        """记录阶段时间"""
        current_time = datetime.now()
        if stage_name not in self.stages:
            self.stages[stage_name] = {}

        if status == "START":
            self.stages[stage_name]['start'] = current_time
            print(f"[{current_time.strftime('%H:%M:%S')}] 🟢 {stage_name} 开始")
        elif status == "END":
            if 'start' in self.stages[stage_name]:
                duration = (current_time - self.stages[stage_name]['start']).total_seconds()
                self.stages[stage_name]['end'] = current_time
                self.stages[stage_name]['duration'] = duration
                print(f"[{current_time.strftime('%H:%M:%S')}] ✅ {stage_name} 完成 (耗时: {duration:.1f}秒)")
            else:
                print(f"[{current_time.strftime('%H:%M:%S')}] ⚠️ {stage_name} 结束 (未找到开始时间)")

    def get_system_metrics(self):
        """获取系统性能指标（简化版）"""
        return {
            'memory_used_mb': 0,
            'memory_percent': 0,
            'cpu_percent': 0,
            'timestamp': datetime.now()
        }

    def get_db_stats(self, batch_code: str):
        """获取数据库统计信息"""
        session = self.Session()
        try:
            # 检查statistical_aggregations记录数
            agg_count = session.execute(
                text("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = :batch"),
                {"batch": batch_code}
            ).scalar() or 0

            # 检查清洗数据记录数
            cleaned_count = session.execute(
                text("SELECT COUNT(*) FROM student_cleaned_scores WHERE batch_code = :batch"),
                {"batch": batch_code}
            ).scalar() or 0

            # 检查原始数据记录数
            raw_count = session.execute(
                text("SELECT COUNT(*) FROM student_score_detail WHERE batch_code = :batch"),
                {"batch": batch_code}
            ).scalar() or 0

            return {
                'aggregations_count': agg_count,
                'cleaned_count': cleaned_count,
                'raw_count': raw_count,
                'timestamp': datetime.now()
            }
        finally:
            session.close()

    def sample_statistics_data(self, batch_code: str, limit: int = 3):
        """抽样statistical_aggregations的statistics_data字段"""
        session = self.Session()
        try:
            result = session.execute(
                text("""
                SELECT school_id, aggregation_level, data_version,
                       JSON_EXTRACT(statistics_data, '$.schema_version') as schema_version,
                       JSON_LENGTH(JSON_EXTRACT(statistics_data, '$.subjects')) as subjects_count,
                       CHAR_LENGTH(statistics_data) as data_size
                FROM statistical_aggregations
                WHERE batch_code = :batch
                ORDER BY created_at DESC
                LIMIT :limit
                """),
                {"batch": batch_code, "limit": limit}
            ).fetchall()

            samples = []
            for row in result:
                samples.append({
                    'school_id': row[0],
                    'aggregation_level': row[1],
                    'data_version': row[2],
                    'schema_version': row[3],
                    'subjects_count': row[4],
                    'data_size_kb': round((row[5] or 0) / 1024, 1)
                })
            return samples
        finally:
            session.close()

    def print_summary_report(self, batch_code: str):
        """打印完整总结报告"""
        print("\n" + "="*80)
        print(f"G7-2025 全链路批次处理监控报告")
        print("="*80)

        # 时间统计
        if self.stages:
            print("\n📊 阶段耗时统计:")
            total_duration = 0
            for stage_name, stage_data in self.stages.items():
                if 'duration' in stage_data:
                    duration = stage_data['duration']
                    total_duration += duration
                    print(f"  • {stage_name}: {duration:.1f}秒")

            if total_duration > 0:
                print(f"  • 总耗时: {total_duration:.1f}秒 ({total_duration/60:.1f}分钟)")

        # 数据库统计
        db_stats = self.get_db_stats(batch_code)
        print(f"\n📈 数据统计:")
        print(f"  • 原始数据记录: {db_stats['raw_count']:,}")
        print(f"  • 清洗数据记录: {db_stats['cleaned_count']:,}")
        print(f"  • 汇聚结果记录: {db_stats['aggregations_count']:,}")

        # 抽样检查
        samples = self.sample_statistics_data(batch_code)
        if samples:
            print(f"\n🔍 汇聚数据抽样检查:")
            for sample in samples:
                print(f"  • {sample['school_id']} ({sample['aggregation_level']}): "
                      f"v{sample['data_version']}, schema={sample['schema_version']}, "
                      f"{sample['subjects_count']}科目, {sample['data_size_kb']}KB")

        # 系统资源
        metrics = self.get_system_metrics()
        print(f"\n🖥️ 系统资源状态:")
        print(f"  • 内存使用: {metrics['memory_used_mb']:.0f}MB ({metrics['memory_percent']:.1f}%)")
        print(f"  • CPU使用率: {metrics['cpu_percent']:.1f}%")

        print("\n" + "="*80)

# 全局监控器实例
monitor = G7BatchMonitor()