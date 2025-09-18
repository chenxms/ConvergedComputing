# 等级阈值调整验证测试
import sys
import os
import logging
from datetime import datetime

# 添加app路径到sys.path  
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.calculation.calculators.grade_calculator import GradeLevelConfig, calculate_individual_grade

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_updated_thresholds():
    """测试更新后的等级阈值"""
    logger.info("测试等级阈值调整")
    
    # 验证配置
    logger.info("验证阈值配置:")
    logger.info(f"小学阈值: {GradeLevelConfig.ELEMENTARY_THRESHOLDS}")
    logger.info(f"初中阈值: {GradeLevelConfig.MIDDLE_SCHOOL_THRESHOLDS}")
    
    # 测试小学等级计算
    logger.info("\n测试小学等级计算:")
    elementary_cases = [
        (90, '3rd_grade', 100, 'excellent', '90分应该是优秀(≥85%)'),
        (85, '3rd_grade', 100, 'excellent', '85分应该是优秀(≥85%)'),
        (84, '3rd_grade', 100, 'good', '84分应该是良好(70-85%)'),
        (75, '3rd_grade', 100, 'good', '75分应该是良好(70-85%)'),
        (70, '3rd_grade', 100, 'good', '70分应该是良好(70-85%)'),
        (69, '3rd_grade', 100, 'pass', '69分应该是及格(60-69%)'),
        (65, '3rd_grade', 100, 'pass', '65分应该是及格(60-69%)'),
        (60, '3rd_grade', 100, 'pass', '60分应该是及格(60-69%)'),
        (59, '3rd_grade', 100, 'fail', '59分应该是不及格(<60%)'),
        (45, '3rd_grade', 100, 'fail', '45分应该是不及格(<60%)'),
    ]
    
    elementary_passed = 0
    for score, grade_level, max_score, expected, description in elementary_cases:
        result = calculate_individual_grade(score, grade_level, max_score)
        actual = result['grade']
        passed = actual == expected
        status = "✓" if passed else "✗"
        
        if passed:
            elementary_passed += 1
        
        logger.info(f"  {status} {description}: 实际={actual}, 期望={expected}")
        if not passed:
            logger.error(f"    分数={score}, 得分率={result['score_rate']}")
    
    # 测试初中等级计算
    logger.info("\n测试初中等级计算:")
    middle_school_cases = [
        (90, '7th_grade', 100, 'excellent', '90分应该是优秀(≥80%)'),
        (80, '7th_grade', 100, 'excellent', '80分应该是优秀(≥80%)'),
        (79, '7th_grade', 100, 'good', '79分应该是良好(70-80%)'),
        (75, '7th_grade', 100, 'good', '75分应该是良好(70-80%)'),
        (70, '7th_grade', 100, 'good', '70分应该是良好(70-80%)'),
        (69, '7th_grade', 100, 'pass', '69分应该是及格(60-69%)'),
        (65, '7th_grade', 100, 'pass', '65分应该是及格(60-69%)'),
        (60, '7th_grade', 100, 'pass', '60分应该是及格(60-69%)'),
        (59, '7th_grade', 100, 'fail', '59分应该是不及格(<60%)'),
        (40, '7th_grade', 100, 'fail', '40分应该是不及格(<60%)'),
    ]
    
    middle_school_passed = 0
    for score, grade_level, max_score, expected, description in middle_school_cases:
        result = calculate_individual_grade(score, grade_level, max_score)
        actual = result['grade']
        passed = actual == expected
        status = "✓" if passed else "✗"
        
        if passed:
            middle_school_passed += 1
        
        logger.info(f"  {status} {description}: 实际={actual}, 期望={expected}")
        if not passed:
            logger.error(f"    分数={score}, 得分率={result['score_rate']}")
    
    # 汇总结果
    total_elementary = len(elementary_cases)
    total_middle_school = len(middle_school_cases)
    total_passed = elementary_passed + middle_school_passed
    total_cases = total_elementary + total_middle_school
    
    logger.info(f"\n测试结果汇总:")
    logger.info(f"小学测试: {elementary_passed}/{total_elementary} 通过")
    logger.info(f"初中测试: {middle_school_passed}/{total_middle_school} 通过")
    logger.info(f"总计: {total_passed}/{total_cases} 通过")
    logger.info(f"成功率: {total_passed/total_cases*100:.1f}%")
    
    if total_passed == total_cases:
        logger.info("🎉 所有等级阈值测试通过!")
        return True
    else:
        logger.error("❌ 部分等级阈值测试失败!")
        return False


def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("等级阈值调整验证测试")
    logger.info("=" * 50)
    
    success = test_updated_thresholds()
    
    logger.info("=" * 50)
    if success:
        logger.info("等级阈值调整验证: 全部通过")
        return 0
    else:
        logger.error("等级阈值调整验证: 存在问题")
        return 1


if __name__ == "__main__":
    exit(main())