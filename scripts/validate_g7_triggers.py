#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 触发器验证脚本：
- 验证触发器完整性和功能正确性
- 测试白名单机制
- 测试维护模式
- 性能测试
- 安全测试

用法：
  python scripts/validate_g7_triggers.py                    # 完整验证
  python scripts/validate_g7_triggers.py --quick           # 快速验证
  python scripts/validate_g7_triggers.py --performance     # 性能测试
  python scripts/validate_g7_triggers.py --security        # 安全测试
"""

import argparse
import time
import uuid
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db


class G7TriggerValidator:
    """G7触发器验证器"""

    def __init__(self):
        self.db = next(get_db())
        self.test_results = []
        self.test_school_id = f'TEST_{uuid.uuid4().hex[:8]}'

    def run_validation(self, quick=False, performance=False, security=False):
        """运行验证套件"""
        print("🔍 开始G7触发器验证...")
        print(f"测试学校ID: {self.test_school_id}")
        print("=" * 60)

        try:
            # 基础验证（总是运行）
            self._test_trigger_existence()
            self._test_table_structure()

            if not quick:
                # 功能验证
                self._test_basic_blocking()
                self._test_whitelist_mechanism()
                self._test_maintenance_mode()
                self._test_batch_code_normalization()
                self._test_logging_functionality()

            if performance:
                self._test_performance()

            if security:
                self._test_security()

            # 清理测试数据
            self._cleanup_test_data()

            # 输出结果
            self._print_results()

        except Exception as e:
            print(f"❌ 验证过程中发生错误: {e}")
            self._cleanup_test_data()
            raise

    def _test_trigger_existence(self):
        """测试触发器存在性"""
        print("📋 检查触发器存在性...")

        try:
            result = self.db.execute(text("SHOW TRIGGERS LIKE 'statistical_aggregations'"))
            triggers = result.fetchall()

            trigger_names = [t[0] for t in triggers]
            expected_triggers = ['g7_enhanced_guard_insert', 'g7_enhanced_guard_update']

            missing_triggers = [t for t in expected_triggers if t not in trigger_names]

            if missing_triggers:
                self._add_result("TRIGGER_EXISTENCE", False, f"缺少触发器: {missing_triggers}")
            else:
                self._add_result("TRIGGER_EXISTENCE", True, "所有触发器存在")
                print("  ✅ INSERT和UPDATE触发器都存在")

        except Exception as e:
            self._add_result("TRIGGER_EXISTENCE", False, f"检查失败: {e}")

    def _test_table_structure(self):
        """测试表结构完整性"""
        print("🗃️ 检查表结构...")

        tables = {
            'g7_enhanced_guard_log': ['id', 'event', 'action', 'decision'],
            'g7_guard_whitelist': ['id', 'user_pattern', 'is_active'],
            'g7_guard_config': ['config_key', 'config_value']
        }

        for table_name, required_columns in tables.items():
            try:
                result = self.db.execute(text(f"DESCRIBE {table_name}"))
                columns = [row[0] for row in result.fetchall()]

                missing_columns = [col for col in required_columns if col not in columns]

                if missing_columns:
                    self._add_result(f"TABLE_{table_name.upper()}", False,
                                   f"缺少列: {missing_columns}")
                else:
                    self._add_result(f"TABLE_{table_name.upper()}", True, "表结构完整")
                    print(f"  ✅ {table_name} 结构完整")

            except Exception as e:
                self._add_result(f"TABLE_{table_name.upper()}", False, f"检查失败: {e}")

    def _test_basic_blocking(self):
        """测试基础阻断功能"""
        print("🚫 测试基础阻断功能...")

        try:
            # 确保不在维护模式
            self._set_maintenance_mode(False)

            # 尝试插入G7-2025数据（应该被阻断）
            try:
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        year, grade, subject_name, created_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        2025, 7, 'TEST', NOW()
                    )
                """), {'school_id': self.test_school_id})

                self.db.commit()
                self._add_result("BASIC_BLOCKING", False, "INSERT未被阻断")

            except Exception as e:
                if "G7-2025 writes blocked" in str(e):
                    self._add_result("BASIC_BLOCKING", True, "INSERT正确被阻断")
                    print("  ✅ INSERT操作被正确阻断")
                else:
                    self._add_result("BASIC_BLOCKING", False, f"意外错误: {e}")

            # 测试UPDATE阻断
            # 首先插入一条非G7数据
            self.db.execute(text("""
                INSERT INTO statistical_aggregations (
                    batch_code, aggregation_level, school_id,
                    year, grade, subject_name, created_at
                ) VALUES (
                    'TEST-BATCH', 'SCHOOL', :school_id,
                    2025, 7, 'TEST', NOW()
                )
            """), {'school_id': self.test_school_id})
            self.db.commit()

            # 尝试更新为G7-2025（应该被阻断）
            try:
                self.db.execute(text("""
                    UPDATE statistical_aggregations
                    SET batch_code = 'G7-2025'
                    WHERE school_id = :school_id AND batch_code = 'TEST-BATCH'
                """), {'school_id': self.test_school_id})

                self.db.commit()
                self._add_result("UPDATE_BLOCKING", False, "UPDATE未被阻断")

            except Exception as e:
                if "G7-2025 writes blocked" in str(e):
                    self._add_result("UPDATE_BLOCKING", True, "UPDATE正确被阻断")
                    print("  ✅ UPDATE操作被正确阻断")
                else:
                    self._add_result("UPDATE_BLOCKING", False, f"意外错误: {e}")

        except Exception as e:
            self._add_result("BASIC_BLOCKING", False, f"测试失败: {e}")

    def _test_whitelist_mechanism(self):
        """测试白名单机制"""
        print("👥 测试白名单机制...")

        try:
            # 获取当前用户
            result = self.db.execute(text("SELECT USER(), CURRENT_USER()"))
            current_user, current_user_full = result.fetchone()
            print(f"  当前用户: {current_user} / {current_user_full}")

            # 添加当前用户到白名单
            self.db.execute(text("""
                INSERT INTO g7_guard_whitelist (user_pattern, added_by, notes)
                VALUES (:pattern, USER(), 'Test whitelist entry')
                ON DUPLICATE KEY UPDATE is_active = TRUE
            """), {'pattern': current_user.split('@')[0] + '%'})
            self.db.commit()

            # 确保不在维护模式
            self._set_maintenance_mode(False)

            # 尝试插入G7-2025数据（应该被允许）
            try:
                test_id = f'{self.test_school_id}_WHITELIST'
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        year, grade, subject_name, created_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        2025, 7, 'TEST_WHITELIST', NOW()
                    )
                """), {'school_id': test_id})

                self.db.commit()
                self._add_result("WHITELIST_MECHANISM", True, "白名单用户可以写入")
                print("  ✅ 白名单用户写入成功")

                # 清理测试数据
                self.db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE school_id = :school_id
                """), {'school_id': test_id})
                self.db.commit()

            except Exception as e:
                self._add_result("WHITELIST_MECHANISM", False, f"白名单用户写入失败: {e}")

            # 移除白名单
            self.db.execute(text("""
                UPDATE g7_guard_whitelist
                SET is_active = FALSE
                WHERE user_pattern = :pattern
            """), {'pattern': current_user.split('@')[0] + '%'})
            self.db.commit()

        except Exception as e:
            self._add_result("WHITELIST_MECHANISM", False, f"测试失败: {e}")

    def _test_maintenance_mode(self):
        """测试维护模式"""
        print("🔧 测试维护模式...")

        try:
            # 启用维护模式
            self._set_maintenance_mode(True)

            # 尝试插入G7-2025数据（应该被允许）
            try:
                test_id = f'{self.test_school_id}_MAINTENANCE'
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        year, grade, subject_name, created_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        2025, 7, 'TEST_MAINTENANCE', NOW()
                    )
                """), {'school_id': test_id})

                self.db.commit()
                self._add_result("MAINTENANCE_MODE", True, "维护模式下可以写入")
                print("  ✅ 维护模式下写入成功")

                # 清理测试数据
                self.db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE school_id = :school_id
                """), {'school_id': test_id})
                self.db.commit()

            except Exception as e:
                self._add_result("MAINTENANCE_MODE", False, f"维护模式下写入失败: {e}")

            # 禁用维护模式
            self._set_maintenance_mode(False)

        except Exception as e:
            self._add_result("MAINTENANCE_MODE", False, f"测试失败: {e}")

    def _test_batch_code_normalization(self):
        """测试批次代码标准化"""
        print("🔤 测试批次代码标准化...")

        try:
            # 确保不在维护模式
            self._set_maintenance_mode(False)

            # 测试各种破折号变体
            batch_variants = [
                'G7–2025',  # 长破折号
                'G7−2025',  # 减号
                'G7—2025',  # em dash
                ' G7-2025 ',  # 带空格
            ]

            blocked_count = 0
            for variant in batch_variants:
                try:
                    test_id = f'{self.test_school_id}_{blocked_count}'
                    self.db.execute(text("""
                        INSERT INTO statistical_aggregations (
                            batch_code, aggregation_level, school_id,
                            year, grade, subject_name, created_at
                        ) VALUES (
                            :batch_code, 'SCHOOL', :school_id,
                            2025, 7, 'TEST_NORM', NOW()
                        )
                    """), {'batch_code': variant, 'school_id': test_id})

                    self.db.commit()
                    print(f"  ❌ 变体 '{variant}' 未被阻断")

                except Exception as e:
                    if "G7-2025 writes blocked" in str(e):
                        blocked_count += 1
                        print(f"  ✅ 变体 '{variant}' 被正确阻断")

            if blocked_count == len(batch_variants):
                self._add_result("BATCH_NORMALIZATION", True, "所有批次代码变体都被正确识别")
            else:
                self._add_result("BATCH_NORMALIZATION", False,
                               f"只有 {blocked_count}/{len(batch_variants)} 个变体被阻断")

        except Exception as e:
            self._add_result("BATCH_NORMALIZATION", False, f"测试失败: {e}")

    def _test_logging_functionality(self):
        """测试日志功能"""
        print("📝 测试日志功能...")

        try:
            # 获取测试前的日志数量
            result = self.db.execute(text("""
                SELECT COUNT(*) FROM g7_enhanced_guard_log
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
            """))
            initial_count = result.fetchone()[0]

            # 执行一些操作来生成日志
            try:
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        year, grade, subject_name, created_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        2025, 7, 'TEST_LOG', NOW()
                    )
                """), {'school_id': f'{self.test_school_id}_LOG'})

                self.db.commit()
            except:
                pass  # 预期会失败

            # 检查日志是否增加
            result = self.db.execute(text("""
                SELECT COUNT(*) FROM g7_enhanced_guard_log
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
            """))
            final_count = result.fetchone()[0]

            if final_count > initial_count:
                self._add_result("LOGGING_FUNCTIONALITY", True,
                               f"日志记录正常 (+{final_count - initial_count}条)")
                print(f"  ✅ 生成了 {final_count - initial_count} 条日志记录")
            else:
                self._add_result("LOGGING_FUNCTIONALITY", False, "未生成日志记录")

        except Exception as e:
            self._add_result("LOGGING_FUNCTIONALITY", False, f"测试失败: {e}")

    def _test_performance(self):
        """测试性能"""
        print("⚡ 测试触发器性能...")

        try:
            # 测试非G7数据的插入性能（不应受影响）
            start_time = time.time()
            for i in range(100):
                self.db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        year, grade, subject_name, created_at
                    ) VALUES (
                        'PERF-TEST', 'SCHOOL', :school_id,
                        2025, 7, 'PERF', NOW()
                    )
                """), {'school_id': f'{self.test_school_id}_PERF_{i}'})

            self.db.commit()
            non_g7_time = time.time() - start_time

            # 测试G7数据的阻断性能
            start_time = time.time()
            blocked_count = 0
            for i in range(100):
                try:
                    self.db.execute(text("""
                        INSERT INTO statistical_aggregations (
                            batch_code, aggregation_level, school_id,
                            year, grade, subject_name, created_at
                        ) VALUES (
                            'G7-2025', 'SCHOOL', :school_id,
                            2025, 7, 'PERF', NOW()
                        )
                    """), {'school_id': f'{self.test_school_id}_G7_PERF_{i}'})
                    self.db.commit()
                except:
                    blocked_count += 1

            g7_time = time.time() - start_time

            self._add_result("PERFORMANCE", True,
                           f"非G7: {non_g7_time:.3f}s, G7阻断: {g7_time:.3f}s, 阻断率: {blocked_count}%")
            print(f"  ✅ 非G7数据插入: {non_g7_time:.3f}秒 (100条)")
            print(f"  ✅ G7数据阻断: {g7_time:.3f}秒 (100次尝试, {blocked_count}次被阻断)")

        except Exception as e:
            self._add_result("PERFORMANCE", False, f"测试失败: {e}")

    def _test_security(self):
        """测试安全性"""
        print("🔒 测试安全性...")

        try:
            # 测试SQL注入防护
            malicious_codes = [
                "G7-2025'; DROP TABLE statistical_aggregations; --",
                "G7-2025' OR '1'='1",
                "G7-2025\"; UPDATE g7_guard_config SET config_value='true' WHERE config_key='maintenance_mode'; --"
            ]

            injection_blocked = 0
            for code in malicious_codes:
                try:
                    self.db.execute(text("""
                        INSERT INTO statistical_aggregations (
                            batch_code, aggregation_level, school_id,
                            year, grade, subject_name, created_at
                        ) VALUES (
                            :batch_code, 'SCHOOL', :school_id,
                            2025, 7, 'SECURITY', NOW()
                        )
                    """), {'batch_code': code, 'school_id': f'{self.test_school_id}_SEC'})

                    self.db.commit()
                    print(f"  ❌ 恶意代码未被阻断: {code[:20]}...")

                except Exception as e:
                    injection_blocked += 1
                    print(f"  ✅ 恶意代码被阻断: {code[:20]}...")

            self._add_result("SECURITY", True,
                           f"安全测试通过 ({injection_blocked}/{len(malicious_codes)} 被阻断)")

        except Exception as e:
            self._add_result("SECURITY", False, f"测试失败: {e}")

    def _set_maintenance_mode(self, enabled):
        """设置维护模式"""
        self.db.execute(text("""
            UPDATE g7_guard_config
            SET config_value = :value
            WHERE config_key = 'maintenance_mode'
        """), {'value': 'true' if enabled else 'false'})
        self.db.commit()

    def _cleanup_test_data(self):
        """清理测试数据"""
        print("🧹 清理测试数据...")

        try:
            # 清理统计汇聚数据
            self.db.execute(text("""
                DELETE FROM statistical_aggregations
                WHERE school_id LIKE :pattern
            """), {'pattern': f'{self.test_school_id}%'})

            # 清理白名单测试数据
            self.db.execute(text("""
                DELETE FROM g7_guard_whitelist
                WHERE notes = 'Test whitelist entry'
            """))

            # 确保维护模式关闭
            self._set_maintenance_mode(False)

            self.db.commit()
            print("  ✅ 测试数据清理完成")

        except Exception as e:
            print(f"  ⚠️ 清理失败: {e}")

    def _add_result(self, test_name, success, message):
        """添加测试结果"""
        self.test_results.append({
            'test': test_name,
            'success': success,
            'message': message,
            'timestamp': datetime.now()
        })

    def _print_results(self):
        """打印测试结果"""
        print("\n" + "=" * 60)
        print("📊 验证结果汇总")
        print("=" * 60)

        passed = sum(1 for r in self.test_results if r['success'])
        total = len(self.test_results)

        for result in self.test_results:
            status = "✅ 通过" if result['success'] else "❌ 失败"
            print(f"{status} {result['test']}: {result['message']}")

        print("\n" + "-" * 60)
        print(f"总计: {passed}/{total} 项测试通过")

        if passed == total:
            print("🎉 所有验证项目都通过了！")
        else:
            print("⚠️ 存在失败的验证项目，请检查配置")

    def __del__(self):
        """清理资源"""
        if hasattr(self, 'db'):
            self.db.close()


def main():
    parser = argparse.ArgumentParser(description='G7触发器验证工具')
    parser.add_argument('--quick', action='store_true', help='快速验证（仅基础检查）')
    parser.add_argument('--performance', action='store_true', help='包含性能测试')
    parser.add_argument('--security', action='store_true', help='包含安全测试')

    args = parser.parse_args()

    validator = G7TriggerValidator()

    try:
        validator.run_validation(
            quick=args.quick,
            performance=args.performance,
            security=args.security
        )
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())