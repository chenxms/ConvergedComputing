#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量数据汇聚执行器
对G4-2025、G7-2025、G8-2025三个批次执行完整数据汇聚
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService
from app.database.repositories import DataAdapterRepository

class BatchAggregationRunner:
    """批量汇聚执行器"""
    
    def __init__(self):
        # 创建数据库连接
        self.DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        self.engine = create_engine(self.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        
        # 目标批次
        self.target_batches = ['G4-2025', 'G7-2025', 'G8-2025']
        self.results = {}
    
    async def check_batch_readiness(self, batch_code: str) -> dict:
        """检查批次数据准备状态"""
        session = self.Session()
        try:
            data_adapter = DataAdapterRepository(session)
            readiness = data_adapter.check_data_readiness(batch_code)
            
            subjects = data_adapter.get_subject_configurations(batch_code)
            exam_subjects = [s for s in subjects if s.get('question_type_enum') != 'questionnaire']
            questionnaire_subjects = [s for s in subjects if s.get('question_type_enum') == 'questionnaire']
            
            return {
                'overall_status': readiness['overall_status'],
                'cleaned_students': readiness['cleaned_students'],
                'original_students': readiness['original_students'],
                'questionnaire_students': readiness['questionnaire_students'],
                'completeness_ratio': readiness['completeness_ratio'],
                'data_source': readiness['data_sources']['primary_source'],
                'total_subjects': len(subjects),
                'exam_subjects': len(exam_subjects),
                'questionnaire_subjects': len(questionnaire_subjects),
                'is_ready': readiness['overall_status'] in ['READY', 'READY_WITH_WARNINGS']
            }
        finally:
            session.close()
    
    async def execute_batch_aggregation(self, batch_code: str) -> dict:
        """执行单个批次的汇聚计算"""
        print(f"\n{'='*60}")
        print(f"开始汇聚计算: {batch_code}")
        print(f"{'='*60}")
        
        start_time = datetime.now()
        session = self.Session()
        
        try:
            # 1. 检查数据准备状态
            print(f"[步骤1] 检查数据准备状态...")
            readiness = await self.check_batch_readiness(batch_code)
            
            print(f"   数据状态: {readiness['overall_status']}")
            print(f"   学生数量: {readiness['cleaned_students']:,}")
            print(f"   科目数量: {readiness['total_subjects']} (考试:{readiness['exam_subjects']}, 问卷:{readiness['questionnaire_subjects']})")
            print(f"   数据完整度: {readiness['completeness_ratio']:.1%}")
            
            if not readiness['is_ready']:
                error_msg = f"批次 {batch_code} 数据未准备就绪: {readiness['overall_status']}"
                print(f"[错误] {error_msg}")
                return {
                    'batch_code': batch_code,
                    'status': 'failed',
                    'error': error_msg,
                    'readiness': readiness
                }
            
            # 2. 创建计算服务
            print(f"[步骤2] 初始化计算服务...")
            calc_service = CalculationService(session)
            
            # 3. 获取批次学校列表
            print(f"[步骤3] 获取批次学校列表...")
            schools = await calc_service._get_batch_schools(batch_code)
            print(f"   找到学校数: {len(schools)}")
            
            # 4. 区域级汇聚计算
            print(f"[步骤4] 执行区域级汇聚计算...")
            try:
                regional_result = await calc_service.calculate_regional_statistics(batch_code)
                regional_success = True
                print(f"   区域级汇聚: 成功")
            except Exception as e:
                print(f"   区域级汇聚: 失败 - {str(e)}")
                regional_result = None
                regional_success = False
            
            # 5. 学校级汇聚计算
            print(f"[步骤5] 执行学校级汇聚计算...")
            school_results = {}
            school_success_count = 0
            school_fail_count = 0
            
            for i, school_id in enumerate(schools[:10], 1):  # 先处理前10个学校作为示例
                try:
                    print(f"   处理学校 {i}/{min(10, len(schools))}: {school_id}")
                    school_result = await calc_service.calculate_school_statistics(batch_code, school_id)
                    school_results[school_id] = {
                        'status': 'success',
                        'result': school_result
                    }
                    school_success_count += 1
                except Exception as e:
                    print(f"   学校 {school_id} 计算失败: {str(e)}")
                    school_results[school_id] = {
                        'status': 'failed',
                        'error': str(e)
                    }
                    school_fail_count += 1
            
            print(f"   学校级汇聚: 成功 {school_success_count}, 失败 {school_fail_count}")
            
            # 6. 统计结果汇总
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'batch_code': batch_code,
                'status': 'success' if (regional_success and school_success_count > 0) else 'partial',
                'readiness': readiness,
                'regional_aggregation': {
                    'status': 'success' if regional_success else 'failed',
                    'result': regional_result
                },
                'school_aggregations': {
                    'total_attempted': min(10, len(schools)),
                    'success_count': school_success_count,
                    'fail_count': school_fail_count,
                    'results': school_results
                },
                'performance': {
                    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'duration_seconds': duration,
                    'duration_formatted': f"{int(duration//60)}分{int(duration%60)}秒"
                }
            }
            
            print(f"[完成] 批次 {batch_code} 汇聚计算完成")
            print(f"   耗时: {result['performance']['duration_formatted']}")
            print(f"   区域级: {'成功' if regional_success else '失败'}")
            print(f"   学校级: 成功{school_success_count}个, 失败{school_fail_count}个")
            
            return result
            
        except Exception as e:
            error_msg = f"批次 {batch_code} 汇聚计算异常: {str(e)}"
            print(f"[错误] {error_msg}")
            import traceback
            traceback.print_exc()
            
            return {
                'batch_code': batch_code,
                'status': 'error',
                'error': error_msg,
                'performance': {
                    'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'duration_seconds': (datetime.now() - start_time).total_seconds()
                }
            }
        finally:
            session.close()
    
    async def run_batch_aggregation(self):
        """运行批量汇聚计算"""
        print("="*80)
        print("批量数据汇聚执行器")
        print("="*80)
        print(f"目标批次: {', '.join(self.target_batches)}")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        overall_start = datetime.now()
        
        # 逐个处理批次
        for batch_code in self.target_batches:
            result = await self.execute_batch_aggregation(batch_code)
            self.results[batch_code] = result
        
        # 生成汇总报告
        overall_end = datetime.now()
        overall_duration = (overall_end - overall_start).total_seconds()
        
        await self.generate_summary_report(overall_duration)
    
    async def generate_summary_report(self, overall_duration: float):
        """生成汇总报告"""
        print(f"\n{'='*80}")
        print("批量汇聚执行汇总报告")
        print(f"{'='*80}")
        
        total_batches = len(self.target_batches)
        success_batches = 0
        partial_batches = 0
        failed_batches = 0
        
        total_regional_success = 0
        total_school_success = 0
        total_school_attempted = 0
        
        print(f"总体执行时间: {int(overall_duration//60)}分{int(overall_duration%60)}秒")
        print(f"处理批次数: {total_batches}")
        print()
        
        for batch_code, result in self.results.items():
            print(f"批次: {batch_code}")
            print(f"   状态: {result['status']}")
            
            if result['status'] in ['success', 'partial']:
                if 'readiness' in result:
                    readiness = result['readiness']
                    print(f"   学生数: {readiness['cleaned_students']:,}")
                    print(f"   科目数: {readiness['total_subjects']}")
                    print(f"   完整度: {readiness['completeness_ratio']:.1%}")
                
                if 'regional_aggregation' in result:
                    regional_status = result['regional_aggregation']['status']
                    print(f"   区域级汇聚: {regional_status}")
                    if regional_status == 'success':
                        total_regional_success += 1
                
                if 'school_aggregations' in result:
                    school_agg = result['school_aggregations']
                    print(f"   学校级汇聚: 成功{school_agg['success_count']}/{school_agg['total_attempted']}")
                    total_school_success += school_agg['success_count']
                    total_school_attempted += school_agg['total_attempted']
                
                if 'performance' in result:
                    print(f"   耗时: {result['performance'].get('duration_formatted', 'N/A')}")
                
                if result['status'] == 'success':
                    success_batches += 1
                else:
                    partial_batches += 1
            else:
                failed_batches += 1
                if 'error' in result:
                    print(f"   错误: {result['error']}")
            
            print()
        
        # 汇总统计
        print("汇总统计:")
        print(f"   成功批次: {success_batches}/{total_batches}")
        print(f"   部分成功: {partial_batches}/{total_batches}")
        print(f"   失败批次: {failed_batches}/{total_batches}")
        print(f"   区域级汇聚成功: {total_regional_success}/{total_batches}")
        print(f"   学校级汇聚成功: {total_school_success}/{total_school_attempted}")
        
        success_rate = (success_batches + partial_batches) / total_batches * 100
        print(f"   总体成功率: {success_rate:.1f}%")
        
        if success_batches == total_batches:
            print(f"\n[SUCCESS] 所有批次汇聚计算完全成功！")
        elif success_batches + partial_batches == total_batches:
            print(f"\n[WARNING] 所有批次基本完成，部分功能有警告")
        else:
            print(f"\n[PARTIAL] 部分批次汇聚成功，请检查失败原因")

async def main():
    """主函数"""
    runner = BatchAggregationRunner()
    await runner.run_batch_aggregation()

if __name__ == "__main__":
    asyncio.run(main())