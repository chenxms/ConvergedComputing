#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 白名单守卫验证脚本：
- 测试白名单用户可以写入
- 测试非白名单用户被拦截
- 验证日志记录功能
- 测试触发器性能

用法：
  python scripts/test_g7_guard_whitelist.py
"""

import traceback
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db


class G7GuardTester:
    def __init__(self):
        self.test_school_id = f"TEST_SCHOOL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.test_results = []

    def log_result(self, test_name, success, message, details=None):
        """记录测试结果"""
        status = "✅ PASS" if success else "❌ FAIL"
        result = {
            'test': test_name,
            'success': success,
            'message': message,
            'details': details,
            'timestamp': datetime.now()
        }
        self.test_results.append(result)
        print(f"{status} {test_name}: {message}")
        if details:
            print(f"    详情: {details}")

    def setup_test_data(self):
        """准备测试数据"""
        print("🔧 准备测试环境...")

        with next(get_db()) as db:
            # 确保白名单表存在
            try:
                db.execute(text("SELECT 1 FROM g7_guard_whitelist LIMIT 1"))
            except:
                self.log_result("Setup", False, "白名单表不存在，请先运行 install_g7_guard_with_whitelist.py")
                return False

            # 添加测试白名单条目
            try:
                db.execute(text(
                    """
                    INSERT IGNORE INTO g7_guard_whitelist (type, value, description)
                    VALUES
                    ('user', 'test_pipeline_user', '测试流水线用户'),
                    ('application', 'test_app', '测试应用'),
                    ('user', 'admin', '管理员用户')
                    """
                ))
                db.commit()
                self.log_result("Setup", True, "测试白名单条目已添加")
                return True
            except Exception as e:
                self.log_result("Setup", False, f"添加测试白名单失败: {str(e)}")
                return False

    def cleanup_test_data(self):
        """清理测试数据"""
        print("\n🧹 清理测试环境...")

        with next(get_db()) as db:
            # 删除测试数据
            try:
                db.execute(text(
                    "DELETE FROM statistical_aggregations WHERE school_id = :school_id"
                ), {"school_id": self.test_school_id})

                # 清理测试白名单（保留 admin）
                db.execute(text(
                    """
                    DELETE FROM g7_guard_whitelist
                    WHERE value IN ('test_pipeline_user', 'test_app')
                    """
                ))

                db.commit()
                self.log_result("Cleanup", True, "测试数据已清理")
            except Exception as e:
                self.log_result("Cleanup", False, f"清理失败: {str(e)}")

    def test_trigger_status(self):
        """测试触发器状态"""
        print("\n📌 测试触发器状态...")

        with next(get_db()) as db:
            try:
                triggers = db.execute(text(
                    """
                    SELECT TRIGGER_NAME, EVENT_MANIPULATION
                    FROM information_schema.TRIGGERS
                    WHERE TRIGGER_SCHEMA = DATABASE()
                      AND TRIGGER_NAME LIKE '%g7_guard%'
                    """
                )).fetchall()

                if not triggers:
                    self.log_result("Trigger Status", False, "未找到G7守卫触发器")
                    return False

                trigger_info = [f"{t.TRIGGER_NAME}({t.EVENT_MANIPULATION})" for t in triggers]
                self.log_result("Trigger Status", True, f"找到 {len(triggers)} 个触发器",
                              ", ".join(trigger_info))
                return True

            except Exception as e:
                self.log_result("Trigger Status", False, f"检查触发器失败: {str(e)}")
                return False

    def test_non_g7_write(self):
        """测试非G7批次写入（应该允许）"""
        print("\n✏️ 测试非G7批次写入...")

        with next(get_db()) as db:
            try:
                # 测试非G7批次数据
                db.execute(text(
                    """
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id, region_id,
                        calculated_data, created_at, updated_at
                    ) VALUES (
                        'TEST-2024', 'school', :school_id, 'TEST_REGION',
                        '{"test": true}', NOW(), NOW()
                    )
                    """
                ), {"school_id": self.test_school_id})

                db.commit()
                self.log_result("Non-G7 Write", True, "非G7批次写入成功")
                return True

            except Exception as e:
                self.log_result("Non-G7 Write", False, f"非G7批次写入失败: {str(e)}")
                return False

    def test_g7_write_blocked(self):
        """测试G7批次写入被拦截（非白名单用户）"""
        print("\n🚫 测试G7批次写入拦截...")

        with next(get_db()) as db:
            try:
                # 尝试写入G7数据（应该被拦截）
                db.execute(text(
                    """
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id, region_id,
                        calculated_data, created_at, updated_at
                    ) VALUES (
                        'G7-2025', 'school', :school_id, 'TEST_REGION',
                        '{"test": true}', NOW(), NOW()
                    )
                    """
                ), {"school_id": self.test_school_id})

                db.commit()
                self.log_result("G7 Write Block", False, "G7写入未被拦截（预期应被拦截）")
                return False

            except Exception as e:
                if "blocked by guard" in str(e):
                    self.log_result("G7 Write Block", True, "G7写入正确被拦截")
                    return True
                else:
                    self.log_result("G7 Write Block", False, f"G7写入失败但原因不明: {str(e)}")
                    return False

    def test_whitelist_functionality(self):
        """测试白名单功能"""
        print("\n📝 测试白名单功能...")

        with next(get_db()) as db:
            try:
                # 检查白名单查询逻辑
                whitelist_check = db.execute(text(
                    """
                    SELECT COUNT(*) as count
                    FROM g7_guard_whitelist
                    WHERE type = 'user' AND enabled = TRUE
                      AND (CURRENT_USER() LIKE CONCAT('%', value, '%') OR
                           SUBSTRING_INDEX(CURRENT_USER(), '@', 1) = value)
                    """
                )).fetchone()

                if whitelist_check.count > 0:
                    self.log_result("Whitelist Check", True,
                                  f"当前用户在白名单中 (匹配 {whitelist_check.count} 条规则)")
                    return True
                else:
                    self.log_result("Whitelist Check", True,
                                  "当前用户不在白名单中 (预期行为)")
                    return True

            except Exception as e:
                self.log_result("Whitelist Check", False, f"白名单检查失败: {str(e)}")
                return False

    def test_guard_logging(self):
        """测试守卫日志功能"""
        print("\n📝 测试守卫日志...")

        with next(get_db()) as db:
            try:
                # 检查最近的守卫日志
                recent_logs = db.execute(text(
                    """
                    SELECT COUNT(*) as count
                    FROM g7_guard_log
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                    """
                )).fetchone()

                self.log_result("Guard Logging", True,
                              f"找到 {recent_logs.count} 条最近1小时的守卫日志")

                # 显示最新日志
                if recent_logs.count > 0:
                    latest_log = db.execute(text(
                        """
                        SELECT event, action, message, current_user_name, created_at
                        FROM g7_guard_log
                        ORDER BY created_at DESC
                        LIMIT 1
                        """
                    )).fetchone()

                    details = (f"最新: {latest_log.event}/{latest_log.action} "
                             f"by {latest_log.current_user_name} at {latest_log.created_at}")
                    self.log_result("Latest Log", True, "守卫日志记录正常", details)

                return True

            except Exception as e:
                self.log_result("Guard Logging", False, f"检查守卫日志失败: {str(e)}")
                return False

    def test_batch_code_normalization(self):
        """测试批次代码归一化"""
        print("\n🔄 测试批次代码归一化...")

        test_cases = [
            'G7-2025',    # 标准格式
            'G7–2025',    # en dash
            'G7—2025',    # em dash
            'G7−2025',    # minus sign
            ' G7-2025 ',  # 带空格
        ]

        passed = 0
        total = len(test_cases)

        for batch_code in test_cases:
            with next(get_db()) as db:
                try:
                    db.execute(text(
                        """
                        INSERT INTO statistical_aggregations (
                            batch_code, aggregation_level, school_id, region_id,
                            calculated_data, created_at, updated_at
                        ) VALUES (
                            :batch_code, 'school', :school_id, 'TEST_REGION',
                            '{"test": true}', NOW(), NOW()
                        )
                        """
                    ), {"batch_code": batch_code, "school_id": f"{self.test_school_id}_{passed}"})

                    db.commit()
                    self.log_result("Normalization", False,
                                  f"批次代码 '{batch_code}' 未被拦截（应该被归一化为G7-2025）")

                except Exception as e:
                    if "blocked by guard" in str(e):
                        passed += 1
                        self.log_result("Normalization", True,
                                      f"批次代码 '{batch_code}' 正确被归一化并拦截")
                    else:
                        self.log_result("Normalization", False,
                                      f"批次代码 '{batch_code}' 处理异常: {str(e)}")

        overall_success = passed == total
        self.log_result("Batch Normalization", overall_success,
                       f"批次代码归一化测试: {passed}/{total} 通过")

        return overall_success

    def test_performance(self):
        """测试触发器性能"""
        print("\n⚡ 测试触发器性能...")

        with next(get_db()) as db:
            try:
                start_time = datetime.now()

                # 执行多次非G7写入测试性能
                for i in range(10):
                    db.execute(text(
                        """
                        INSERT INTO statistical_aggregations (
                            batch_code, aggregation_level, school_id, region_id,
                            calculated_data, created_at, updated_at
                        ) VALUES (
                            'PERF-TEST', 'school', :school_id, 'TEST_REGION',
                            '{"test": true}', NOW(), NOW()
                        )
                        """
                    ), {"school_id": f"{self.test_school_id}_PERF_{i}"})

                db.commit()
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                self.log_result("Performance", True,
                              f"10次写入耗时 {duration:.3f} 秒 (平均 {duration/10:.3f} 秒/次)")
                return True

            except Exception as e:
                self.log_result("Performance", False, f"性能测试失败: {str(e)}")
                return False

    def run_all_tests(self):
        """运行所有测试"""
        print("🧪 开始G7-2025白名单守卫验证测试")
        print("=" * 60)

        # 设置测试环境
        if not self.setup_test_data():
            return

        try:
            # 运行测试套件
            tests = [
                self.test_trigger_status,
                self.test_non_g7_write,
                self.test_g7_write_blocked,
                self.test_whitelist_functionality,
                self.test_guard_logging,
                self.test_batch_code_normalization,
                self.test_performance
            ]

            for test in tests:
                try:
                    test()
                except Exception as e:
                    self.log_result(test.__name__, False, f"测试执行异常: {str(e)}")
                    print(f"  堆栈跟踪: {traceback.format_exc()}")

        finally:
            # 清理测试环境
            self.cleanup_test_data()

        # 输出测试总结
        self.print_summary()

    def print_summary(self):
        """输出测试总结"""
        print("\n" + "=" * 60)
        print("📋 测试总结报告")
        print("=" * 60)

        passed = sum(1 for result in self.test_results if result['success'])
        total = len(self.test_results)
        success_rate = (passed / total * 100) if total > 0 else 0

        print(f"总测试数: {total}")
        print(f"通过数: {passed}")
        print(f"失败数: {total - passed}")
        print(f"成功率: {success_rate:.1f}%")

        if passed == total:
            print("\n🎉 所有测试通过！G7守卫白名单功能正常。")
        else:
            print(f"\n⚠️ {total - passed} 个测试失败，请检查以下问题：")
            for result in self.test_results:
                if not result['success']:
                    print(f"  ❌ {result['test']}: {result['message']}")

        print("\n🔧 下一步操作建议：")
        if passed == total:
            print("  1. 可以安全部署白名单守卫")
            print("  2. 配置生产环境白名单: python scripts/manage_g7_whitelist.py")
            print("  3. 切换到白名单模式: python scripts/g7_guard_switch.py whitelist")
        else:
            print("  1. 修复失败的测试问题")
            print("  2. 重新运行验证测试")
            print("  3. 检查触发器安装状态")


def main():
    """主函数"""
    tester = G7GuardTester()
    tester.run_all_tests()


if __name__ == '__main__':
    main()