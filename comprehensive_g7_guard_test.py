#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7守卫系统综合测试脚本
包含所有功能的完整验证
"""

import sys
import os
import time
import uuid
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from app.database.connection import get_db


class ComprehensiveG7GuardTest:
    """G7守卫系统综合测试"""

    def __init__(self):
        self.db = next(get_db())
        self.test_id = uuid.uuid4().hex[:8]
        self.test_results = []

    def run_all_tests(self):
        """运行所有测试"""
        print("=" * 60)
        print("G7 Guard System Comprehensive Test")
        print("=" * 60)
        print(f"Test ID: {self.test_id}")
        print()

        try:
            self.test_installation()
            self.test_basic_blocking()
            self.test_batch_code_variants()
            self.test_maintenance_mode()
            self.test_whitelist_mechanism()
            self.test_logging_functionality()
            self.test_performance()
            self.test_recovery_scenarios()

            self.print_summary()

        except Exception as e:
            print(f"Test suite failed: {e}")
            raise
        finally:
            self.cleanup()

    def test_installation(self):
        """测试安装验证"""
        self.add_test_section("Installation Verification")

        # 检查触发器
        result = self.db.execute(text("SHOW TRIGGERS LIKE 'statistical_aggregations'"))
        triggers = result.fetchall()
        g7_triggers = [t for t in triggers if 'g7_enhanced_guard' in t[0]]

        if len(g7_triggers) == 2:
            self.add_result("Trigger Installation", True, f"Found {len(g7_triggers)} triggers")
        else:
            self.add_result("Trigger Installation", False, f"Expected 2 triggers, found {len(g7_triggers)}")

        # 检查表结构
        tables = ['g7_enhanced_guard_log', 'g7_guard_whitelist', 'g7_guard_config']
        for table in tables:
            try:
                result = self.db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                self.add_result(f"Table {table}", True, "Exists and accessible")
            except:
                self.add_result(f"Table {table}", False, "Not accessible")

    def test_basic_blocking(self):
        """测试基础阻断功能"""
        self.add_test_section("Basic Blocking Functionality")

        # 确保维护模式关闭
        self.set_maintenance_mode(False)

        # 测试G7-2025阻断
        school_id = f"TEST_BLOCK_{self.test_id}"
        try:
            self.db.execute(text("""
                INSERT INTO statistical_aggregations (
                    batch_code, aggregation_level, school_id,
                    statistics_data, data_version, calculation_status, created_at, updated_at
                ) VALUES (
                    'G7-2025', 'SCHOOL', :school_id,
                    '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                )
            """), {'school_id': school_id})

            self.db.commit()
            self.add_result("G7-2025 Blocking", False, "Write was not blocked")

            # 清理意外插入的数据
            self.db.execute(text("DELETE FROM statistical_aggregations WHERE school_id = :school_id"),
                          {'school_id': school_id})
            self.db.commit()

        except Exception as e:
            if "G7-2025 writes blocked" in str(e):
                self.add_result("G7-2025 Blocking", True, "Correctly blocked")
            else:
                self.add_result("G7-2025 Blocking", False, f"Unexpected error: {e}")

        # 测试非G7数据允许
        school_id = f"TEST_ALLOW_{self.test_id}"
        try:
            self.db.execute(text("""
                INSERT INTO statistical_aggregations (
                    batch_code, aggregation_level, school_id,
                    statistics_data, data_version, calculation_status, created_at, updated_at
                ) VALUES (
                    'TEST-BATCH', 'SCHOOL', :school_id,
                    '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                )
            """), {'school_id': school_id})

            self.db.commit()
            self.add_result("Non-G7 Data", True, "Write allowed")

            # 清理测试数据
            self.db.execute(text("DELETE FROM statistical_aggregations WHERE school_id = :school_id"),
                          {'school_id': school_id})
            self.db.commit()

        except Exception as e:
            self.add_result("Non-G7 Data", False, f"Write failed: {e}")

    def test_batch_code_variants(self):
        """测试批次代码变体识别"""
        self.add_test_section("Batch Code Variants")

        variants = [
            'G7-2025',   # 标准破折号 (测试用)
            'G7_2025',   # 下划线 (测试用)
            ' G7-2025 ', # 带空格
            'g7-2025',   # 小写 (测试用)
        ]

        blocked_count = 0
        for i, variant in enumerate(variants):
            school_id = f"TEST_VARIANT_{self.test_id}_{i}"
            try:
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        :batch_code, 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'batch_code': variant, 'school_id': school_id})

                self.db.commit()
                print(f"   Variant '{variant}' not blocked")

                # 清理意外插入的数据
                self.db.execute(text("DELETE FROM statistical_aggregations WHERE school_id = :school_id"),
                              {'school_id': school_id})
                self.db.commit()

            except Exception as e:
                if "G7-2025 writes blocked" in str(e):
                    blocked_count += 1
                    print(f"   Variant '{variant}' correctly blocked")

        if blocked_count == len(variants):
            self.add_result("Batch Code Normalization", True, "All variants correctly identified")
        else:
            self.add_result("Batch Code Normalization", False,
                          f"Only {blocked_count}/{len(variants)} variants blocked")

    def test_maintenance_mode(self):
        """测试维护模式"""
        self.add_test_section("Maintenance Mode")

        school_id = f"TEST_MAINT_{self.test_id}"

        # 启用维护模式
        self.set_maintenance_mode(True)

        try:
            self.db.execute(text("""
                INSERT INTO statistical_aggregations (
                    batch_code, aggregation_level, school_id,
                    statistics_data, data_version, calculation_status, created_at, updated_at
                ) VALUES (
                    'G7-2025', 'SCHOOL', :school_id,
                    '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                )
            """), {'school_id': school_id})

            self.db.commit()
            self.add_result("Maintenance Mode", True, "Write allowed in maintenance mode")

            # 清理测试数据
            self.db.execute(text("DELETE FROM statistical_aggregations WHERE school_id = :school_id"),
                          {'school_id': school_id})
            self.db.commit()

        except Exception as e:
            self.add_result("Maintenance Mode", False, f"Write failed in maintenance mode: {e}")

        # 关闭维护模式
        self.set_maintenance_mode(False)

    def test_whitelist_mechanism(self):
        """测试白名单机制"""
        self.add_test_section("Whitelist Mechanism")

        # 获取当前用户
        result = self.db.execute(text("SELECT USER()"))
        current_user = result.fetchone()[0]
        user_pattern = current_user.split('@')[0] + '%'

        school_id = f"TEST_WHITE_{self.test_id}"

        # 添加白名单
        self.db.execute(text("""
            INSERT INTO g7_guard_whitelist (user_pattern, added_by, notes)
            VALUES (:pattern, USER(), 'Comprehensive test whitelist')
            ON DUPLICATE KEY UPDATE is_active = TRUE, notes = 'Comprehensive test whitelist'
        """), {'pattern': user_pattern})
        self.db.commit()

        # 确保维护模式关闭
        self.set_maintenance_mode(False)

        try:
            self.db.execute(text("""
                INSERT INTO statistical_aggregations (
                    batch_code, aggregation_level, school_id,
                    statistics_data, data_version, calculation_status, created_at, updated_at
                ) VALUES (
                    'G7-2025', 'SCHOOL', :school_id,
                    '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                )
            """), {'school_id': school_id})

            self.db.commit()
            self.add_result("Whitelist Access", True, "Whitelisted user can write")

            # 清理测试数据
            self.db.execute(text("DELETE FROM statistical_aggregations WHERE school_id = :school_id"),
                          {'school_id': school_id})
            self.db.commit()

        except Exception as e:
            self.add_result("Whitelist Access", False, f"Whitelisted user write failed: {e}")

        # 移除白名单
        self.db.execute(text("""
            UPDATE g7_guard_whitelist
            SET is_active = FALSE
            WHERE user_pattern = :pattern
        """), {'pattern': user_pattern})
        self.db.commit()

    def test_logging_functionality(self):
        """测试日志功能"""
        self.add_test_section("Logging Functionality")

        # 获取测试前的日志数量
        result = self.db.execute(text("""
            SELECT COUNT(*) FROM g7_enhanced_guard_log
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
        """))
        initial_count = result.fetchone()[0]

        # 触发一些日志记录
        school_id = f"TEST_LOG_{self.test_id}"
        for i in range(3):
            try:
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': f'{school_id}_{i}'})
                self.db.commit()
            except:
                pass  # 预期会失败

        # 检查日志增加
        result = self.db.execute(text("""
            SELECT COUNT(*) FROM g7_enhanced_guard_log
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
        """))
        final_count = result.fetchone()[0]

        if final_count > initial_count:
            self.add_result("Logging System", True, f"Generated {final_count - initial_count} log entries")
        else:
            self.add_result("Logging System", False, "No log entries generated")

        # 检查日志内容质量
        result = self.db.execute(text("""
            SELECT decision, is_whitelisted, maintenance_mode, execution_time_ms
            FROM g7_enhanced_guard_log
            WHERE batch_code = 'G7-2025'
            ORDER BY created_at DESC
            LIMIT 5
        """))

        logs = result.fetchall()
        if logs:
            valid_logs = sum(1 for log in logs if log[0] in ['ALLOWED', 'BLOCKED'])
            if valid_logs == len(logs):
                self.add_result("Log Quality", True, "All logs have valid decisions")
            else:
                self.add_result("Log Quality", False, f"Only {valid_logs}/{len(logs)} logs valid")
        else:
            self.add_result("Log Quality", False, "No recent G7 logs found")

    def test_performance(self):
        """测试性能"""
        self.add_test_section("Performance Testing")

        # 测试非G7数据性能（不应受影响）
        start_time = time.time()
        for i in range(10):
            school_id = f"PERF_TEST_{self.test_id}_{i}"
            try:
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'PERF-TEST', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': school_id})
                self.db.commit()
            except Exception as e:
                print(f"Performance test insert failed: {e}")

        non_g7_time = time.time() - start_time

        # 清理性能测试数据
        self.db.execute(text("""
            DELETE FROM statistical_aggregations
            WHERE school_id LIKE :pattern
        """), {'pattern': f'PERF_TEST_{self.test_id}_%'})
        self.db.commit()

        # 测试G7阻断性能
        start_time = time.time()
        blocked_count = 0
        for i in range(10):
            school_id = f"G7_PERF_{self.test_id}_{i}"
            try:
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': school_id})
                self.db.commit()
            except:
                blocked_count += 1

        g7_time = time.time() - start_time

        if non_g7_time < 5.0 and g7_time < 5.0:
            self.add_result("Performance", True,
                          f"Non-G7: {non_g7_time:.3f}s, G7 blocking: {g7_time:.3f}s, Block rate: {blocked_count}/10")
        else:
            self.add_result("Performance", False,
                          f"Slow performance - Non-G7: {non_g7_time:.3f}s, G7: {g7_time:.3f}s")

    def test_recovery_scenarios(self):
        """测试恢复场景"""
        self.add_test_section("Recovery Scenarios")

        # 测试配置丢失恢复
        original_value = None
        try:
            result = self.db.execute(text("""
                SELECT config_value FROM g7_guard_config
                WHERE config_key = 'maintenance_mode'
            """))
            row = result.fetchone()
            original_value = row[0] if row else 'false'

            # 模拟配置丢失
            self.db.execute(text("""
                UPDATE g7_guard_config
                SET config_value = NULL
                WHERE config_key = 'maintenance_mode'
            """))
            self.db.commit()

            # 测试系统是否仍能工作（应该默认为阻断）
            school_id = f"TEST_RECOVERY_{self.test_id}"
            try:
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': school_id})
                self.db.commit()

                self.add_result("Config Recovery", False, "System allowed write with NULL config")

                # 清理意外数据
                self.db.execute(text("DELETE FROM statistical_aggregations WHERE school_id = :school_id"),
                              {'school_id': school_id})
                self.db.commit()

            except Exception as e:
                if "G7-2025 writes blocked" in str(e):
                    self.add_result("Config Recovery", True, "System blocked write with NULL config")
                else:
                    self.add_result("Config Recovery", False, f"Unexpected error: {e}")

            # 恢复配置
            self.db.execute(text("""
                UPDATE g7_guard_config
                SET config_value = :value
                WHERE config_key = 'maintenance_mode'
            """), {'value': original_value})
            self.db.commit()

        except Exception as e:
            self.add_result("Config Recovery", False, f"Recovery test failed: {e}")

    def set_maintenance_mode(self, enabled):
        """设置维护模式"""
        self.db.execute(text("""
            UPDATE g7_guard_config
            SET config_value = :value
            WHERE config_key = 'maintenance_mode'
        """), {'value': 'true' if enabled else 'false'})
        self.db.commit()

    def add_test_section(self, section_name):
        """添加测试章节"""
        print(f"\n{section_name}")
        print("-" * len(section_name))

    def add_result(self, test_name, success, message):
        """添加测试结果"""
        self.test_results.append({
            'name': test_name,
            'success': success,
            'message': message
        })
        status = "OK" if success else "FAIL"
        print(f"  [{status}] {test_name}: {message}")

    def print_summary(self):
        """打印测试总结"""
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)

        passed = sum(1 for r in self.test_results if r['success'])
        total = len(self.test_results)

        print(f"Total tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {total - passed}")
        print(f"Success rate: {passed/total*100:.1f}%")

        if total - passed > 0:
            print("\nFailed tests:")
            for result in self.test_results:
                if not result['success']:
                    print(f"  - {result['name']}: {result['message']}")

        if passed == total:
            print("\n✅ All tests passed! G7 Guard system is fully functional.")
        else:
            print("\n⚠️ Some tests failed. Please review the system configuration.")

    def cleanup(self):
        """清理测试数据"""
        try:
            # 清理测试数据
            self.db.execute(text("""
                DELETE FROM statistical_aggregations
                WHERE school_id LIKE :pattern
            """), {'pattern': f'%{self.test_id}%'})

            # 清理测试白名单
            self.db.execute(text("""
                DELETE FROM g7_guard_whitelist
                WHERE notes = 'Comprehensive test whitelist'
            """))

            # 确保维护模式关闭
            self.set_maintenance_mode(False)

            self.db.commit()
            print(f"\nTest cleanup completed for test ID: {self.test_id}")

        except Exception as e:
            print(f"Cleanup failed: {e}")

        finally:
            if hasattr(self, 'db'):
                self.db.close()


if __name__ == '__main__':
    try:
        test = ComprehensiveG7GuardTest()
        test.run_all_tests()
    except Exception as e:
        print(f"Test execution failed: {e}")
        sys.exit(1)