# 年级等级分布映射实现指南

## 数据库字段映射

### grade_aggregation_main.grade_level 字段值
```sql
-- 小学年级 (1-6年级)
'1st_grade'   -- 一年级
'2nd_grade'   -- 二年级  
'3rd_grade'   -- 三年级
'4th_grade'   -- 四年级
'5th_grade'   -- 五年级
'6th_grade'   -- 六年级

-- 初中年级 (7-9年级)
'7th_grade'   -- 七年级
'8th_grade'   -- 八年级
'9th_grade'   -- 九年级
```

## 等级分布阈值计算逻辑

### Python实现示例

```python
def get_grade_thresholds(grade_level: str) -> dict:
    """
    根据年级获取等级分布阈值
    
    Args:
        grade_level: 来自grade_aggregation_main.grade_level字段
        
    Returns:
        dict: 包含各等级阈值的字典
    """
    # 小学年级列表 (1-6年级)
    primary_grades = ['1st_grade', '2nd_grade', '3rd_grade', 
                     '4th_grade', '5th_grade', '6th_grade']
    
    # 初中年级列表 (7-9年级)  
    junior_grades = ['7th_grade', '8th_grade', '9th_grade']
    
    if grade_level in primary_grades:
        # 小学阈值标准
        return {
            'excellent': 0.85,  # 优秀：≥85%
            'good': 0.70,       # 良好：70%-85%
            'pass': 0.60,       # 及格：60%-70%
            'fail': 0.00        # 不及格：<60%
        }
    elif grade_level in junior_grades:
        # 初中阈值标准
        return {
            'excellent': 0.80,  # 优秀：≥80%
            'good': 0.70,       # 良好：70%-80%
            'pass': 0.60,       # 及格：60%-70%
            'fail': 0.00        # 不及格：<60%
        }
    else:
        raise ValueError(f"未识别的年级: {grade_level}")

def calculate_grade_distribution(student_scores: list, total_score: float, grade_level: str) -> dict:
    """
    计算等级分布
    
    Args:
        student_scores: 学生分数列表
        total_score: 科目总分
        grade_level: 年级标识
        
    Returns:
        dict: 等级分布统计结果
    """
    thresholds = get_grade_thresholds(grade_level)
    
    # 计算每个学生的得分率
    score_rates = [score / total_score for score in student_scores]
    
    # 统计各等级人数
    excellent_count = sum(1 for rate in score_rates if rate >= thresholds['excellent'])
    good_count = sum(1 for rate in score_rates if thresholds['good'] <= rate < thresholds['excellent'])  
    pass_count = sum(1 for rate in score_rates if thresholds['pass'] <= rate < thresholds['good'])
    fail_count = sum(1 for rate in score_rates if rate < thresholds['pass'])
    
    total_students = len(student_scores)
    
    return {
        'excellent': {
            'count': excellent_count,
            'percentage': round(excellent_count / total_students, 2)
        },
        'good': {
            'count': good_count,
            'percentage': round(good_count / total_students, 2)
        },
        'pass': {
            'count': pass_count,
            'percentage': round(pass_count / total_students, 2)
        },
        'fail': {
            'count': fail_count,
            'percentage': round(fail_count / total_students, 2)
        }
    }
```

### SQLAlchemy查询示例

```python
from sqlalchemy import select, case

def get_batch_grade_category(session, batch_code: str) -> str:
    """
    获取批次的学段分类（小学/初中）
    """
    query = select(GradeAggregationMain.grade_level).where(
        GradeAggregationMain.batch_code == batch_code
    )
    grade_level = session.scalar(query)
    
    primary_grades = ['1st_grade', '2nd_grade', '3rd_grade', 
                     '4th_grade', '5th_grade', '6th_grade']
    
    if grade_level in primary_grades:
        return '小学'
    else:
        return '初中'

def build_grade_distribution_case(grade_level_field) -> case:
    """
    构建SQL CASE语句用于等级分布计算
    """
    primary_grades = ['1st_grade', '2nd_grade', '3rd_grade', 
                     '4th_grade', '5th_grade', '6th_grade']
    
    return case(
        (
            grade_level_field.in_(primary_grades),
            case(
                (score_rate >= 0.85, 'excellent'),
                (score_rate >= 0.70, 'good'), 
                (score_rate >= 0.60, 'pass'),
                else_='fail'
            )
        ),
        else_=case(
            (score_rate >= 0.80, 'excellent'),
            (score_rate >= 0.70, 'good'),
            (score_rate >= 0.60, 'pass'), 
            else_='fail'
        )
    )
```

## 测试用例

### 单元测试示例

```python
import pytest

def test_get_grade_thresholds():
    """测试年级阈值获取"""
    # 测试小学年级
    primary_thresholds = get_grade_thresholds('3rd_grade')
    assert primary_thresholds['excellent'] == 0.85
    assert primary_thresholds['good'] == 0.70
    
    # 测试初中年级  
    junior_thresholds = get_grade_thresholds('8th_grade')
    assert junior_thresholds['excellent'] == 0.80
    assert junior_thresholds['good'] == 0.70
    
    # 测试无效年级
    with pytest.raises(ValueError):
        get_grade_thresholds('invalid_grade')

def test_calculate_grade_distribution():
    """测试等级分布计算"""
    # 模拟学生分数数据
    student_scores = [95, 88, 75, 65, 45]  # 对应得分率：0.95, 0.88, 0.75, 0.65, 0.45
    total_score = 100
    
    # 测试小学等级分布
    primary_distribution = calculate_grade_distribution(student_scores, total_score, '5th_grade')
    assert primary_distribution['excellent']['count'] == 2  # 95和88分 >= 85%
    assert primary_distribution['good']['count'] == 1       # 75分在70%-85%
    assert primary_distribution['pass']['count'] == 1       # 65分在60%-70%
    assert primary_distribution['fail']['count'] == 1       # 45分 < 60%
    
    # 测试初中等级分布
    junior_distribution = calculate_grade_distribution(student_scores, total_score, '8th_grade')
    assert junior_distribution['excellent']['count'] == 2   # 95和88分 >= 80%
    assert junior_distribution['good']['count'] == 1        # 75分在70%-80%
    assert junior_distribution['pass']['count'] == 1        # 65分在60%-70%
    assert junior_distribution['fail']['count'] == 1        # 45分 < 60%
```

## 注意事项

1. **数据验证**：在计算前必须验证grade_level字段的有效性
2. **边界处理**：确保等级分布的边界值处理准确（如正好70%应归入哪个等级）
3. **百分比精度**：等级分布百分比保留2位小数，四个等级总和应为1.0
4. **异常处理**：对于无效的grade_level值要有明确的错误处理
5. **性能考虑**：大批次数据处理时考虑使用数据库层面的聚合计算

## 配置管理建议

```python
# 建议将阈值配置外化管理
GRADE_THRESHOLDS_CONFIG = {
    'primary': {  # 小学 1-6年级
        'grades': ['1st_grade', '2nd_grade', '3rd_grade', '4th_grade', '5th_grade', '6th_grade'],
        'thresholds': {
            'excellent': 0.85,
            'good': 0.70, 
            'pass': 0.60,
            'fail': 0.00
        }
    },
    'junior': {   # 初中 7-9年级
        'grades': ['7th_grade', '8th_grade', '9th_grade'],
        'thresholds': {
            'excellent': 0.80,
            'good': 0.70,
            'pass': 0.60, 
            'fail': 0.00
        }
    }
}
```

这样可以方便未来调整阈值或新增学段。