# G4批次学校ID数据质量问题深度分析报告

## 执行摘要

通过深度分析发现，G4批次存在严重的数据质量问题：**汇聚系统生成的学校ID与标准学校主数据完全不匹配**。这导致前端报告系统无法正确显示学校信息，影响数据的准确性和可用性。

## 问题概述

### 核心问题
1. **数据源不一致**: 汇聚结果使用的是自动生成的SCH_xxxx格式ID，而不是school_master_data表中的标准学校ID
2. **学校名称缺失**: 汇聚结果表中school_name字段为NULL，无法提供有意义的学校识别信息
3. **数据映射断裂**: student_score_detail表中的school_id与school_master_data表无法正确关联

## 详细分析结果

### 1. school_master_data表结构分析

**表结构信息:**
- **记录总数**: 139条（包含G4和G8批次）
- **G4批次记录**: 56所学校
- **关键字段**: 
  - `school_id`: varchar(50) - 标准学校ID (5044-5099格式)
  - `standard_school_name`: varchar(128) - 标准学校名称
  - `school_type`: PRIMARY_SCHOOL
  - `data_source`: SYSTEM_AUTO_REGISTER

**G4批次学校ID示例:**
```
5044 - 柏杜(小学)
5045 - 百水(小学)
5046 - 滨河园(小学)
5047 - 草堂(小学)
5048 - 翠湖(小学)
...（共56所学校）
```

**数据质量评估:**
- ✅ school_id字段无重复值
- ✅ standard_school_name字段完整性100%
- ✅ 数据结构规范统一

### 2. student_score_detail表数据问题

**数据统计:**
- **G4批次总记录数**: 12,851条学生答题记录
- **涉及学校数**: 57所（包含1个空school_id）
- **数据问题**: 学校信息混乱，同一学校出现多个不同的school_name

**学校信息不一致示例:**
```sql
school_id: 5044
  - 成都市温江区柏杜学校(小学部) (49名学生)
  - 柏杜(小学) (22名学生)
  - 柏杜民族学校 (22名学生)

school_id: 5045  
  - 成都市温江区百水学校 (23名学生)
  - 成都市温江区百水学校(小学部) (50名学生)
  - 百水(小学) (47名学生)
```

**名称匹配分析:**
- **一致**: 32所学校
- **不一致**: 24所学校
- **匹配率**: 57.1%

### 3. statistical_aggregations表严重问题

**汇聚结果数据问题:**
- **学校级记录数**: 166条
- **使用的school_id格式**: SCH_0001, SCH_0002, ..., SCH_0164, SCH_UNKNOWN, INVALID-SCHOOL
- **school_name字段**: 全部为NULL
- **与master表重叠**: 0所学校

**关键发现:**
```sql
-- 汇聚表中的school_id完全是自动生成的代码
SCH_0001, SCH_0002, SCH_0003, ..., SCH_0164, SCH_UNKNOWN, INVALID-SCHOOL

-- school_master_data中的真实school_id
5044, 5045, 5046, 5047, 5048, 5049, ..., 5099
```

**零匹配问题:**
- 汇聚结果与标准学校数据**完全无法关联**
- 前端系统无法获取正确的学校名称
- 数据追溯链条完全断裂

## 根因分析

### 1. 数据处理流程缺陷

通过代码分析发现关键问题在于：

**SubjectsBuilder.py中的学校信息获取:**
```python
def _compute_school_rankings(self, batch_code: str, subject_name: str):
    sql = text("""
        SELECT school_code,
               MAX(school_name) AS school_name,
               ROUND(AVG(total_score), 2) AS avg
        FROM student_cleaned_scores  -- 问题：依赖清洗后的数据
        WHERE batch_code = :batch AND subject_name = :subject
        GROUP BY school_code  -- 问题：使用school_code而非标准school_id
    """)
```

**数据清洗服务的问题:**
```python
# data_cleaning_service.py中直接复制原始数据的学校信息
df = pd.DataFrame(raw_data, columns=[
    'student_id', 'student_name', 'school_id', 'school_code', 
    'school_name', 'class_name', 'subject_id', 'total_score', 'subject_scores'
])
```

### 2. 架构设计问题

1. **未使用标准数据源**: 系统没有将school_master_data作为学校信息的单一标准源
2. **数据清洗阶段缺乏标准化**: 清洗过程中未对学校信息进行标准化映射
3. **汇聚算法依赖错误数据**: 汇聚计算直接使用未标准化的学校信息
4. **缺乏数据一致性检查**: 没有验证学校ID与标准数据的一致性

### 3. 数据质量控制缺失

1. **没有主数据管理策略**: 学校信息散布在多个表中，缺乏统一管理
2. **缺乏数据验证规则**: 没有验证school_id是否在master表中存在
3. **没有数据质量监控**: 无法及时发现数据不一致问题

## 修复建议与优先级

### 高优先级修复（P0）

#### 1. 数据清洗服务改造
- **修改数据源查询**: 在清洗阶段与school_master_data表关联
- **实现学校信息标准化**: 使用标准school_id和standard_school_name
- **添加数据验证**: 拒绝处理不在master表中的学校数据

#### 2. 汇聚算法修复  
- **修改SubjectsBuilder**: 直接使用标准school_id进行汇聚
- **更新统计查询**: 将所有school_code查询改为school_id查询
- **修复JSON序列化**: 确保输出使用标准学校信息

#### 3. G4批次数据重新处理
- **清理错误数据**: 删除statistical_aggregations表中的G4错误记录
- **重新执行清洗**: 基于修复后的清洗服务重新处理G4数据
- **重新汇聚计算**: 生成正确的学校级和区域级统计数据

### 中优先级改进（P1）

#### 4. 数据一致性检查机制
- **实现学校ID验证**: 在数据导入时验证school_id有效性
- **添加质量检查报告**: 定期检查数据一致性
- **实现告警机制**: 发现不一致时及时通知

#### 5. API层修复
- **更新subjects_v12_api**: 确保返回标准学校信息
- **修复前端数据格式**: 统一学校信息字段结构
- **添加向后兼容**: 处理历史数据的兼容性问题

### 低优先级优化（P2）

#### 6. 架构改进
- **实现主数据管理**: 建立学校信息的单一真实源
- **设计数据治理策略**: 制定数据质量管理规范
- **建立数据血缘关系**: 追踪数据的来源和流向

## 技术实施方案

### 阶段1：紧急修复（1-2天）

1. **修改数据清洗SQL**:
```sql
-- 替换现有的学校信息查询
SELECT 
    ssd.student_id,
    ssd.student_name,
    COALESCE(smd.school_id, ssd.school_id) as school_id,
    smd.school_id as school_code,  -- 使用标准ID作为code
    COALESCE(smd.standard_school_name, ssd.school_name) as school_name,
    ssd.class_name,
    ssd.subject_id,
    ssd.total_score,
    ssd.subject_scores
FROM student_score_detail ssd
LEFT JOIN school_master_data smd 
    ON ssd.school_id = smd.school_id 
    AND ssd.batch_code = smd.batch_code
WHERE ssd.batch_code = :batch_code
```

2. **更新SubjectsBuilder查询**:
```python
def _compute_school_rankings(self, batch_code: str, subject_name: str):
    sql = text("""
        SELECT scs.school_id,
               smd.standard_school_name AS school_name,
               ROUND(AVG(scs.total_score), 2) AS avg,
               DENSE_RANK() OVER (ORDER BY AVG(scs.total_score) DESC, scs.school_id ASC) AS rnk
        FROM student_cleaned_scores scs
        JOIN school_master_data smd ON scs.school_id = smd.school_id 
                                    AND scs.batch_code = smd.batch_code
        WHERE scs.batch_code = :batch AND scs.subject_name = :subject
        GROUP BY scs.school_id, smd.standard_school_name
        ORDER BY avg DESC, scs.school_id ASC
    """)
```

### 阶段2：数据重建（2-3天）

1. **清理G4错误数据**
2. **重新执行数据清洗流程**
3. **重新生成汇聚统计**
4. **验证数据质量**

### 阶段3：质量保障（1周）

1. **实现数据验证检查**
2. **建立监控报告**
3. **文档更新**
4. **培训团队**

## 风险评估

### 高风险
- **数据不一致影响决策**: 错误的学校信息可能导致错误的教育决策
- **系统信任度下降**: 数据质量问题影响用户对系统的信任

### 中风险  
- **修复过程数据丢失**: 重新处理过程中可能出现数据丢失
- **性能影响**: 增加JOIN查询可能影响系统性能

### 低风险
- **向后兼容问题**: 修改数据结构可能影响现有集成

## 验证标准

### 数据质量验证
- [ ] G4批次所有学校ID均存在于school_master_data表中
- [ ] 汇聚结果表中school_name字段不为NULL
- [ ] 学校统计数据可以正确追溯到具体学校

### 功能验证
- [ ] 前端系统能正确显示学校名称
- [ ] API返回标准格式的学校信息
- [ ] 数据导出包含正确的学校标识

### 性能验证
- [ ] 汇聚计算性能未明显下降
- [ ] API响应时间在可接受范围内

## 结论

G4批次的学校ID数据质量问题是一个系统性问题，需要从数据清洗、汇聚算法、API输出等多个层面进行修复。建议立即启动紧急修复流程，确保数据的准确性和可用性。

**关键成功因素:**
1. 将school_master_data确立为学校信息的单一真实源
2. 在数据清洗阶段实现学校信息标准化
3. 建立数据质量监控和验证机制

**预期收益:**
1. 提高数据准确性和一致性
2. 改善用户体验和系统可信度
3. 为后续批次处理建立标准化流程

---

**报告生成时间**: 2025-09-10  
**分析批次**: G4-2025  
**分析范围**: 完整数据链路（原始数据→清洗→汇聚→API输出）  
**数据基准日期**: 2025-09-09  