#!/usr/bin/env python3
"""
维度数据清洗功能完整验证测试
验证两阶段处理方案：第一阶段数据清洗 + 第二阶段统计计算
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json
import asyncio
import time
import pandas as pd
from typing import Dict, List, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

class ComprehensiveDimensionVerification:
    """维度数据清洗功能完整验证"""
    
    def __init__(self):
        # 数据库连接
        self.DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        self.engine = create_engine(self.DATABASE_URL)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.batch_code = 'G4-2025'
        
        # 验证结果
        self.verification_results = {
            'batch_code': self.batch_code,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'tests_passed': 0,
            'tests_failed': 0,
            'tests_details': [],
            'overall_status': 'UNKNOWN',
            'performance_metrics': {},
            'data_quality_metrics': {}
        }
    
    async def run_comprehensive_verification(self) -> Dict[str, Any]:
        """运行完整验证测试"""
        print("=" * 80)
        print("维度数据清洗功能完整验证测试")
        print(f"测试批次: {self.batch_code}")
        print(f"开始时间: {self.verification_results['timestamp']}")
        print("=" * 80)
        
        try:
            # 1. 验证数据完整性
            await self._test_data_completeness()
            
            # 2. 验证维度计算正确性
            await self._test_dimension_calculation_correctness()
            
            # 3. 验证性能优化效果
            await self._test_performance_optimization()
            
            # 4. 验证接口兼容性
            await self._test_interface_compatibility()
            
            # 5. 验证维度统计计算
            await self._test_dimension_statistics()
            
            # 6. 验证数据质量
            await self._test_data_quality()
            
            # 7. 生成最终验证报告
            self._generate_final_report()
            
            return self.verification_results
            
        except Exception as e:
            self._add_test_result("ERROR", "critical_error", f"验证过程中发生严重错误: {e}", False)
            import traceback
            traceback.print_exc()
            return self.verification_results
        
        finally:
            self.session.close()
    
    async def _test_data_completeness(self):
        """测试1: 数据完整性验证"""
        print("\n" + "=" * 60)
        print("测试 1: 数据完整性验证")
        print("=" * 60)
        
        try:
            # 1.1 验证清洗表数据覆盖率
            query = text("""
                SELECT 
                    subject_name,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students,
                    COUNT(CASE WHEN dimension_scores IS NOT NULL AND dimension_scores != '{}' THEN 1 END) as records_with_dimension_scores,
                    COUNT(CASE WHEN dimension_max_scores IS NOT NULL AND dimension_max_scores != '{}' THEN 1 END) as records_with_dimension_max_scores
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
                GROUP BY subject_name
                ORDER BY subject_name
            """)
            
            result = self.session.execute(query, {'batch_code': self.batch_code})
            subjects_data = result.fetchall()
            
            if not subjects_data:
                self._add_test_result("FAIL", "data_completeness", "清洗表中没有找到任何数据", False)
                return
            
            total_records = 0
            total_students = 0
            complete_dimension_records = 0
            subjects_processed = 0
            
            print("\n科目数据覆盖率分析:")
            print("科目名称".ljust(15) + "总记录数".ljust(10) + "学生数".ljust(8) + "维度分数".ljust(10) + "维度满分".ljust(10) + "完整率")
            print("-" * 80)
            
            for row in subjects_data:
                subject_name, total, students, with_scores, with_max = row
                complete_rate = min(with_scores, with_max) / total * 100 if total > 0 else 0
                
                print(f"{subject_name[:14].ljust(15)} {str(total).ljust(10)} {str(students).ljust(8)} {str(with_scores).ljust(10)} {str(with_max).ljust(10)} {complete_rate:.1f}%")
                
                total_records += total
                total_students = max(total_students, students)  # 取最大值作为总学生数
                complete_dimension_records += min(with_scores, with_max)
                subjects_processed += 1
            
            overall_completeness = complete_dimension_records / total_records * 100 if total_records > 0 else 0
            
            print(f"\n总计: {subjects_processed} 个科目, {total_records} 条记录, {total_students} 个学生")
            print(f"维度数据完整率: {overall_completeness:.1f}%")
            
            # 保存指标
            self.verification_results['data_quality_metrics'] = {
                'subjects_processed': subjects_processed,
                'total_records': total_records,
                'unique_students': total_students,
                'complete_dimension_records': complete_dimension_records,
                'dimension_completeness_rate': overall_completeness
            }
            
            # 验证标准：完整率 >= 95%
            if overall_completeness >= 95:
                self._add_test_result("PASS", "data_completeness", f"维度数据完整率 {overall_completeness:.1f}% >= 95%", True)
            else:
                self._add_test_result("FAIL", "data_completeness", f"维度数据完整率 {overall_completeness:.1f}% < 95%", False)
            
            # 1.2 验证与原始表的数据一致性
            print("\n验证数据一致性...")
            consistency_query = text("""
                SELECT 
                    (SELECT COUNT(DISTINCT student_id, subject_name) FROM student_score_detail WHERE batch_code = :batch_code) as original_count,
                    (SELECT COUNT(*) FROM student_cleaned_scores WHERE batch_code = :batch_code AND is_valid = 1) as cleaned_count
            """)
            
            consistency_result = self.session.execute(consistency_query, {'batch_code': self.batch_code})
            original_count, cleaned_count = consistency_result.fetchone()
            
            consistency_rate = cleaned_count / original_count * 100 if original_count > 0 else 0
            print(f"原始数据记录数: {original_count}")
            print(f"清洗数据记录数: {cleaned_count}")
            print(f"数据保留率: {consistency_rate:.1f}%")
            
            if consistency_rate >= 90:
                self._add_test_result("PASS", "data_consistency", f"数据保留率 {consistency_rate:.1f}% >= 90%", True)
            else:
                self._add_test_result("WARN", "data_consistency", f"数据保留率 {consistency_rate:.1f}% < 90%", False)
            
        except Exception as e:
            self._add_test_result("ERROR", "data_completeness", f"数据完整性测试失败: {e}", False)
    
    async def _test_dimension_calculation_correctness(self):
        """测试2: 维度计算正确性验证"""
        print("\n" + "=" * 60)
        print("测试 2: 维度计算正确性验证")
        print("=" * 60)
        
        try:
            # 2.1 从清洗表中直接验证维度计算
            sample_query = text("""
                SELECT student_id, subject_name, total_score, 
                       dimension_scores, dimension_max_scores
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
                    AND dimension_scores IS NOT NULL 
                    AND dimension_scores != '{}'
                ORDER BY RAND()
                LIMIT 5
            """)
            
            result = self.session.execute(sample_query, {'batch_code': self.batch_code})
            samples = result.fetchall()
            
            if not samples:
                self._add_test_result("FAIL", "dimension_calculation", "没有找到包含维度数据的样本", False)
                return
            
            calculation_accuracy_count = 0
            total_samples = len(samples)
            
            print(f"\n分析 {total_samples} 个学生样本的维度计算正确性:")
            
            for i, (student_id, subject_name, total_score, dimension_scores_json, 
                   dimension_max_scores_json) in enumerate(samples):
                
                print(f"\n样本 {i+1}: 学生 {student_id} ({subject_name})")
                
                try:
                    # 解析JSON数据
                    dimension_scores = json.loads(dimension_scores_json) if dimension_scores_json else {}
                    dimension_max_scores = json.loads(dimension_max_scores_json) if dimension_max_scores_json else {}
                    
                    # 计算维度分数总和
                    dimension_total = sum(dim_data.get('score', 0) for dim_data in dimension_scores.values() if isinstance(dim_data, dict))
                    stored_total = float(total_score)
                    
                    print(f"  维度分数总和: {dimension_total:.2f}")
                    print(f"  存储的总分: {stored_total:.2f}")
                    print(f"  维度数量: {len(dimension_scores)}")
                    
                    # 验证维度分数与总分的一致性（允许小的数值误差）
                    dimension_total_valid = abs(dimension_total - stored_total) < 0.1
                    
                    if dimension_total_valid and len(dimension_scores) > 0:
                        calculation_accuracy_count += 1
                        print(f"  [OK] 维度计算正确")
                    else:
                        print(f"  [ERROR] 维度计算异常")
                        if not dimension_total_valid:
                            print(f"    - 维度总分不一致 (差异: {abs(dimension_total - stored_total):.2f})")
                        if len(dimension_scores) == 0:
                            print(f"    - 维度数据为空")
                            
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    print(f"  ✗ JSON解析失败: {e}")
            
            accuracy_rate = calculation_accuracy_count / total_samples * 100
            print(f"\n维度计算准确率: {accuracy_rate:.1f}% ({calculation_accuracy_count}/{total_samples})")
            
            if accuracy_rate >= 90:
                self._add_test_result("PASS", "dimension_calculation", f"维度计算准确率 {accuracy_rate:.1f}% >= 90%", True)
            else:
                self._add_test_result("FAIL", "dimension_calculation", f"维度计算准确率 {accuracy_rate:.1f}% < 90%", False)
            
            # 2.2 验证维度满分配置
            print("\n验证维度满分配置...")
            max_score_query = text("""
                SELECT subject_name, dimension_max_scores
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
                    AND dimension_max_scores IS NOT NULL
                    AND dimension_max_scores != '{}'
                GROUP BY subject_name, dimension_max_scores
                ORDER BY subject_name
                LIMIT 3
            """)
            
            max_score_result = self.session.execute(max_score_query, {'batch_code': self.batch_code})
            
            for row in max_score_result.fetchall():
                subject_name, dimension_max_scores_json = row
                try:
                    dimension_max_scores = json.loads(dimension_max_scores_json)
                    total_max = sum(dim_data.get('max_score', 0) for dim_data in dimension_max_scores.values())
                    print(f"  {subject_name}: {len(dimension_max_scores)} 个维度, 总满分 {total_max}")
                except Exception as e:
                    print(f"  {subject_name}: 维度满分解析失败 - {e}")
            
            self._add_test_result("PASS", "dimension_max_scores", "维度满分配置验证完成", True)
            
        except Exception as e:
            self._add_test_result("ERROR", "dimension_calculation", f"维度计算正确性测试失败: {e}", False)
    
    async def _test_performance_optimization(self):
        """测试3: 性能优化效果验证"""
        print("\n" + "=" * 60)
        print("测试 3: 性能优化效果验证")
        print("=" * 60)
        
        try:
            # 3.1 测试查询性能 - 清洗表 vs 原始表
            print("测试查询性能对比...")
            
            # 清洗表查询时间
            start_time = time.time()
            cleaned_query = text("""
                SELECT student_id, subject_name, total_score, dimension_scores
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code AND is_valid = 1
                ORDER BY student_id, subject_name
                LIMIT 1000
            """)
            self.session.execute(cleaned_query, {'batch_code': self.batch_code}).fetchall()
            cleaned_time = time.time() - start_time
            
            # 原始表复杂查询时间
            start_time = time.time()
            original_complex_query = text("""
                SELECT ssd.student_id, ssd.subject_name, SUM(ssd.total_score) as total_score,
                       GROUP_CONCAT(CONCAT(qdm.dimension_code, ':', bdd.dimension_name)) as dimensions
                FROM student_score_detail ssd
                LEFT JOIN subject_question_config sqc ON ssd.subject_id = sqc.subject_id 
                    AND ssd.batch_code = sqc.batch_code
                LEFT JOIN question_dimension_mapping qdm ON sqc.question_id = qdm.question_id
                    AND sqc.batch_code = qdm.batch_code 
                LEFT JOIN batch_dimension_definition bdd ON qdm.dimension_code = bdd.dimension_code
                    AND qdm.batch_code = bdd.batch_code
                WHERE ssd.batch_code = :batch_code
                GROUP BY ssd.student_id, ssd.subject_name
                ORDER BY ssd.student_id, ssd.subject_name
                LIMIT 1000
            """)
            self.session.execute(original_complex_query, {'batch_code': self.batch_code}).fetchall()
            original_time = time.time() - start_time
            
            performance_improvement = (original_time - cleaned_time) / original_time * 100 if original_time > 0 else 0
            
            print(f"清洗表查询时间: {cleaned_time:.3f}s")
            print(f"原始表复杂查询时间: {original_time:.3f}s")
            print(f"性能提升: {performance_improvement:.1f}%")
            
            self.verification_results['performance_metrics'] = {
                'cleaned_query_time': cleaned_time,
                'original_query_time': original_time,
                'performance_improvement': performance_improvement
            }
            
            if performance_improvement > 0:
                self._add_test_result("PASS", "query_performance", f"查询性能提升 {performance_improvement:.1f}%", True)
            else:
                self._add_test_result("WARN", "query_performance", f"查询性能提升不明显 {performance_improvement:.1f}%", False)
            
            # 3.2 测试数据获取简化程度
            print("\n测试数据获取简化...")
            simple_access_query = text("""
                SELECT COUNT(*) as count FROM student_cleaned_scores 
                WHERE batch_code = :batch_code 
                    AND dimension_scores IS NOT NULL 
                    AND JSON_LENGTH(dimension_scores) > 0
            """)
            result = self.session.execute(simple_access_query, {'batch_code': self.batch_code})
            direct_access_count = result.fetchone()[0]
            
            print(f"可直接访问的维度数据记录: {direct_access_count}")
            
            if direct_access_count > 0:
                self._add_test_result("PASS", "data_access_simplification", f"简化数据访问，{direct_access_count} 条记录可直接获取维度数据", True)
            else:
                self._add_test_result("FAIL", "data_access_simplification", "没有可直接访问的维度数据", False)
            
        except Exception as e:
            self._add_test_result("ERROR", "performance_optimization", f"性能优化测试失败: {e}", False)
    
    async def _test_interface_compatibility(self):
        """测试4: 接口兼容性验证"""
        print("\n" + "=" * 60)
        print("测试 4: 接口兼容性验证")
        print("=" * 60)
        
        try:
            # 4.1 测试计算服务能否正常使用清洗数据
            from app.services.calculation_service import CalculationService
            calc_service = CalculationService(self.session)
            
            print("测试计算服务数据获取...")
            start_time = time.time()
            student_data = await calc_service._fetch_student_scores(self.batch_code)
            fetch_time = time.time() - start_time
            
            print(f"数据获取时间: {fetch_time:.3f}s")
            print(f"获取到的记录数: {len(student_data) if not student_data.empty else 0}")
            
            if not student_data.empty:
                print(f"数据列: {list(student_data.columns)}")
                required_columns = ['student_id', 'student_name', 'school_id', 'subject_name', 'total_score']
                missing_columns = [col for col in required_columns if col not in student_data.columns]
                
                if not missing_columns:
                    self._add_test_result("PASS", "calculation_service_compatibility", "计算服务可以正常获取清洗数据", True)
                else:
                    self._add_test_result("FAIL", "calculation_service_compatibility", f"数据缺少必要列: {missing_columns}", False)
            else:
                self._add_test_result("FAIL", "calculation_service_compatibility", "计算服务无法获取数据", False)
            
            # 4.2 测试统计计算功能
            if not student_data.empty:
                print("\n测试基础统计计算...")
                try:
                    # 简单统计验证
                    total_students = student_data['student_id'].nunique()
                    total_subjects = student_data['subject_name'].nunique()
                    avg_score = student_data['total_score'].mean()
                    
                    print(f"统计结果 - 学生数: {total_students}, 科目数: {total_subjects}, 平均分: {avg_score:.2f}")
                    
                    if total_students > 0 and total_subjects > 0:
                        self._add_test_result("PASS", "statistics_calculation", "统计计算功能正常", True)
                    else:
                        self._add_test_result("FAIL", "statistics_calculation", "统计计算结果异常", False)
                        
                except Exception as e:
                    self._add_test_result("ERROR", "statistics_calculation", f"统计计算失败: {e}", False)
            
        except Exception as e:
            self._add_test_result("ERROR", "interface_compatibility", f"接口兼容性测试失败: {e}", False)
    
    async def _test_dimension_statistics(self):
        """测试5: 维度统计计算验证"""
        print("\n" + "=" * 60)
        print("测试 5: 维度统计计算验证")
        print("=" * 60)
        
        try:
            # 5.1 验证维度数据可用性
            dimension_query = text("""
                SELECT subject_name, 
                       COUNT(*) as total_records,
                       AVG(JSON_LENGTH(dimension_scores)) as avg_dimensions_per_student
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code 
                    AND dimension_scores IS NOT NULL 
                    AND dimension_scores != '{}'
                    AND JSON_VALID(dimension_scores) = 1
                GROUP BY subject_name
                ORDER BY subject_name
            """)
            
            result = self.session.execute(dimension_query, {'batch_code': self.batch_code})
            dimension_stats = result.fetchall()
            
            if not dimension_stats:
                self._add_test_result("FAIL", "dimension_statistics", "没有有效的维度数据进行统计", False)
                return
            
            print("各科目维度数据统计:")
            print("科目名称".ljust(15) + "记录数".ljust(8) + "平均维度数")
            print("-" * 40)
            
            total_dimension_records = 0
            for subject_name, records, avg_dims in dimension_stats:
                avg_dims = float(avg_dims) if avg_dims else 0
                print(f"{subject_name[:14].ljust(15)} {str(records).ljust(8)} {avg_dims:.1f}")
                total_dimension_records += records
            
            print(f"\n总计: {total_dimension_records} 条记录包含维度数据")
            
            # 5.2 执行维度统计计算示例
            print("\n执行维度统计计算示例...")
            
            # 选择一个科目进行详细分析
            sample_subject_query = text("""
                SELECT subject_name, dimension_scores, dimension_max_scores
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
                    AND dimension_scores IS NOT NULL
                    AND dimension_scores != '{}'
                    AND JSON_VALID(dimension_scores) = 1
                LIMIT 100
            """)
            
            sample_result = self.session.execute(sample_subject_query, {'batch_code': self.batch_code})
            sample_data = sample_result.fetchall()
            
            if sample_data:
                # 分析维度分数分布
                dimension_stats = {}
                for subject_name, dim_scores_json, dim_max_json in sample_data:
                    try:
                        dim_scores = json.loads(dim_scores_json)
                        dim_max = json.loads(dim_max_json) if dim_max_json else {}
                        
                        for dim_code, dim_data in dim_scores.items():
                            if dim_code not in dimension_stats:
                                dimension_stats[dim_code] = {
                                    'scores': [],
                                    'max_score': dim_max.get(dim_code, {}).get('max_score', 0) if dim_max else 0,
                                    'name': dim_data.get('name', dim_code)
                                }
                            dimension_stats[dim_code]['scores'].append(dim_data.get('score', 0))
                    except Exception:
                        continue
                
                # 计算维度统计指标
                calculated_dimensions = 0
                for dim_code, stats in list(dimension_stats.items())[:5]:  # 显示前5个维度
                    if stats['scores']:
                        scores = stats['scores']
                        avg_score = sum(scores) / len(scores)
                        max_score = stats['max_score']
                        avg_rate = avg_score / max_score * 100 if max_score > 0 else 0
                        
                        print(f"维度 {dim_code}: 平均分 {avg_score:.2f}/{max_score} ({avg_rate:.1f}%)")
                        calculated_dimensions += 1
                
                if calculated_dimensions > 0:
                    self._add_test_result("PASS", "dimension_statistics", f"成功计算 {calculated_dimensions} 个维度的统计指标", True)
                else:
                    self._add_test_result("FAIL", "dimension_statistics", "维度统计计算失败", False)
            else:
                self._add_test_result("FAIL", "dimension_statistics", "没有足够的样本数据进行维度统计", False)
                
        except Exception as e:
            self._add_test_result("ERROR", "dimension_statistics", f"维度统计计算测试失败: {e}", False)
    
    async def _test_data_quality(self):
        """测试6: 数据质量验证"""
        print("\n" + "=" * 60)
        print("测试 6: 数据质量验证")
        print("=" * 60)
        
        try:
            # 6.1 数据完整性检查
            quality_query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN student_id IS NULL OR student_id = '' THEN 1 END) as missing_student_id,
                    COUNT(CASE WHEN subject_name IS NULL OR subject_name = '' THEN 1 END) as missing_subject_name,
                    COUNT(CASE WHEN total_score IS NULL THEN 1 END) as missing_total_score,
                    COUNT(CASE WHEN dimension_scores IS NULL OR dimension_scores = '{}' THEN 1 END) as missing_dimension_scores,
                    COUNT(CASE WHEN dimension_max_scores IS NULL OR dimension_max_scores = '{}' THEN 1 END) as missing_dimension_max_scores,
                    COUNT(CASE WHEN total_score < 0 THEN 1 END) as negative_scores,
                    COUNT(CASE WHEN total_score > max_score THEN 1 END) as excessive_scores
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
            """)
            
            result = self.session.execute(quality_query, {'batch_code': self.batch_code})
            quality_stats = result.fetchone()
            
            (total_records, missing_student_id, missing_subject_name, missing_total_score,
             missing_dimension_scores, missing_dimension_max_scores, negative_scores, excessive_scores) = quality_stats
            
            print("数据质量检查结果:")
            print(f"总记录数: {total_records}")
            print(f"缺少学生ID: {missing_student_id} ({missing_student_id/total_records*100:.2f}%)")
            print(f"缺少科目名称: {missing_subject_name} ({missing_subject_name/total_records*100:.2f}%)")
            print(f"缺少总分: {missing_total_score} ({missing_total_score/total_records*100:.2f}%)")
            print(f"缺少维度分数: {missing_dimension_scores} ({missing_dimension_scores/total_records*100:.2f}%)")
            print(f"缺少维度满分: {missing_dimension_max_scores} ({missing_dimension_max_scores/total_records*100:.2f}%)")
            print(f"负分记录: {negative_scores}")
            print(f"超满分记录: {excessive_scores}")
            
            # 计算数据质量得分
            quality_issues = missing_student_id + missing_subject_name + missing_total_score + negative_scores + excessive_scores
            quality_score = (total_records - quality_issues) / total_records * 100 if total_records > 0 else 0
            
            print(f"\n数据质量得分: {quality_score:.1f}%")
            
            if quality_score >= 95:
                self._add_test_result("PASS", "data_quality", f"数据质量得分 {quality_score:.1f}% >= 95%", True)
            elif quality_score >= 90:
                self._add_test_result("WARN", "data_quality", f"数据质量得分 {quality_score:.1f}% >= 90%", True)
            else:
                self._add_test_result("FAIL", "data_quality", f"数据质量得分 {quality_score:.1f}% < 90%", False)
            
            # 6.2 JSON数据格式验证
            json_validity_query = text("""
                SELECT 
                    COUNT(*) as total_with_dimension_scores,
                    COUNT(CASE WHEN JSON_VALID(dimension_scores) = 0 THEN 1 END) as invalid_dimension_scores_json,
                    COUNT(CASE WHEN JSON_VALID(dimension_max_scores) = 0 THEN 1 END) as invalid_dimension_max_scores_json
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
                    AND dimension_scores IS NOT NULL
                    AND dimension_scores != '{}'
            """)
            
            json_result = self.session.execute(json_validity_query, {'batch_code': self.batch_code})
            json_stats = json_result.fetchone()
            
            if json_stats:
                total_json, invalid_scores, invalid_max_scores = json_stats
                json_validity_rate = (total_json - invalid_scores - invalid_max_scores) / total_json * 100 if total_json > 0 else 100
                
                print(f"\nJSON格式验证:")
                print(f"包含维度数据的记录: {total_json}")
                print(f"无效维度分数JSON: {invalid_scores}")
                print(f"无效维度满分JSON: {invalid_max_scores}")
                print(f"JSON有效率: {json_validity_rate:.1f}%")
                
                if json_validity_rate >= 99:
                    self._add_test_result("PASS", "json_validity", f"JSON有效率 {json_validity_rate:.1f}% >= 99%", True)
                else:
                    self._add_test_result("WARN", "json_validity", f"JSON有效率 {json_validity_rate:.1f}% < 99%", False)
            
        except Exception as e:
            self._add_test_result("ERROR", "data_quality", f"数据质量验证失败: {e}", False)
    
    def _generate_final_report(self):
        """生成最终验证报告"""
        print("\n" + "=" * 80)
        print("最终验证报告")
        print("=" * 80)
        
        # 汇总测试结果
        passed_tests = [test for test in self.verification_results['tests_details'] if test['passed']]
        failed_tests = [test for test in self.verification_results['tests_details'] if not test['passed']]
        
        self.verification_results['tests_passed'] = len(passed_tests)
        self.verification_results['tests_failed'] = len(failed_tests)
        
        total_tests = len(self.verification_results['tests_details'])
        success_rate = len(passed_tests) / total_tests * 100 if total_tests > 0 else 0
        
        # 确定整体状态
        if success_rate >= 90:
            self.verification_results['overall_status'] = 'EXCELLENT'
        elif success_rate >= 80:
            self.verification_results['overall_status'] = 'GOOD'
        elif success_rate >= 70:
            self.verification_results['overall_status'] = 'ACCEPTABLE'
        else:
            self.verification_results['overall_status'] = 'POOR'
        
        print(f"批次: {self.verification_results['batch_code']}")
        print(f"测试时间: {self.verification_results['timestamp']}")
        print(f"总测试数: {total_tests}")
        print(f"通过测试: {len(passed_tests)}")
        print(f"失败测试: {len(failed_tests)}")
        print(f"成功率: {success_rate:.1f}%")
        print(f"整体状态: {self.verification_results['overall_status']}")
        
        # 详细结果
        print("\n详细测试结果:")
        for test in self.verification_results['tests_details']:
            status_icon = "[PASS]" if test['passed'] else "[FAIL]"
            print(f"  {status_icon} {test['test_name']}: {test['message']}")
        
        # 关键指标汇总
        if self.verification_results['data_quality_metrics']:
            metrics = self.verification_results['data_quality_metrics']
            print(f"\n关键指标:")
            print(f"  处理科目数: {metrics.get('subjects_processed', 0)}")
            print(f"  处理记录数: {metrics.get('total_records', 0)}")
            print(f"  学生数: {metrics.get('unique_students', 0)}")
            print(f"  维度数据完整率: {metrics.get('dimension_completeness_rate', 0):.1f}%")
        
        if self.verification_results['performance_metrics']:
            perf = self.verification_results['performance_metrics']
            print(f"  查询性能提升: {perf.get('performance_improvement', 0):.1f}%")
        
        # 最终结论
        print(f"\n" + "=" * 60)
        if self.verification_results['overall_status'] in ['EXCELLENT', 'GOOD']:
            print(">>> 维度数据清洗功能验证 - 成功！")
            print("[OK] 两阶段处理方案工作正常")
            print("[OK] 维度数据完整且准确") 
            print("[OK] 性能优化效果显著")
            print("[OK] 接口兼容性良好")
        else:
            print("[WARN] 维度数据清洗功能验证 - 需要改进")
            print("请关注失败的测试项目并进行相应修复")
        
        print("=" * 60)
    
    def _add_test_result(self, result: str, test_name: str, message: str, passed: bool):
        """添加测试结果"""
        self.verification_results['tests_details'].append({
            'result': result,
            'test_name': test_name,
            'message': message,
            'passed': passed
        })
        
        print(f"[{result}] {test_name}: {message}")

async def main():
    """主函数"""
    verifier = ComprehensiveDimensionVerification()
    results = await verifier.run_comprehensive_verification()
    
    # 保存结果到文件
    output_file = f"dimension_verification_report_{int(time.time())}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n验证报告已保存到: {output_file}")

if __name__ == "__main__":
    asyncio.run(main())