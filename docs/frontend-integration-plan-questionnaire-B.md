# 前端接口对接实施方案（问卷增强B）

面向前端（含 AI 编码工程师）提供的对接说明：覆盖接口清单、字段映射、集成步骤、示例代码与验收要点，确保在 v1.2 下快速完成对接。

## 1. 接口清单
- 区域级数据：GET /api/v12/batch/{batch_code}/regional
- 学校级数据：GET /api/v12/batch/{batch_code}/school/{school_code}
- 物化生成：POST /api/v12/batch/{batch_code}/materialize

说明：全部返回 schema_version = "v1.2"，主体结构为 subjects: Subject[]。

## 2. 重要字段（问卷增强B）
- 区域级：
  - dimensions[].avg：维度区域平均分（两位小数）；
  - dimensions[].questions[]：维度→题目→选项分布（百分比 0–100 两位小数）；
  - 保留 subjects[].questions[]（兼容旧渲染）。
- 学校级：
  - dimensions[].regional_avg：维度区域对标均分；
  - （可选）subjects[].questions[]：学校级题目分布，若底表具备 school 粒度。

TS 类型参考：见 docs/typescript-interfaces.ts（已包含 egional_avg? 与 QuestionnaireDimension.questions?）。

## 3. 对接步骤
1) 拉取区域级数据：
   - 读取 subjects，筛选 	ype === 'questionnaire'；
   - 渲染“维度表现卡”：使用 dimensions[].name/avg；
   - 渲染“题目倾向分析”：优先用 dimensions[].questions[]（维度下题目→选项分布）。
2) 拉取学校级数据：
   - 对于问卷维度，展示 vg（本校）与 egional_avg（区域对标）的双指标条；
   - 若存在 subjects[].questions[]，用于学校层题目倾向分析卡片。
3) 兼容策略：
   - 若 dimensions[].questions 不存在，退化为使用顶层 subjects[].questions[]；
   - 百分比字段统一按 0–100 渲染（两位小数）。

## 4. 示例代码（Axios，TypeScript）
`	s
import axios from 'axios';

interface ApiResponse<T> { success: boolean; message: string; data: T; code: number; }

async function fetchRegional(batch: string) {
  const res = await axios.get<ApiResponse<any>>(/api/v12/batch//regional);
  const data = res.data.data;
  // 取问卷科目
  const qSubjects = (data.subjects || []).filter((s: any) => s.type === 'questionnaire');
  // 维度均分
  const dims = qSubjects[0]?.dimensions || [];
  // 维度下题目分布
  const dimQuestions = dims.map((d: any) => ({ code: d.code, avg: d.avg, questions: d.questions || [] }));
  return { dims, dimQuestions };
}

async function fetchSchool(batch: string, school: string) {
  const res = await axios.get<ApiResponse<any>>(/api/v12/batch//school/);
  const data = res.data.data;
  const qSubjects = (data.subjects || []).filter((s: any) => s.type === 'questionnaire');
  const dims = qSubjects[0]?.dimensions || [];
  // 学校对标区域
  const bars = dims.map((d: any) => ({ name: d.name, my: d.avg, region: d.regional_avg }));
  // 学校级题目分布（如存在）
  const questions = qSubjects[0]?.questions || [];
  return { bars, questions };
}
`

## 5. UI 与交互建议
- 维度表现卡：
  - 区域页：柱状/条形图展示 vg；
  - 学校页：双柱/对比条（vg vs egional_avg），支持排序；
- 题目倾向分析：
  - 使用堆叠条形展示各选项 pct；
  - 保留两位小数，显示百分号；
- 兼容处理：
  - 若 dimensions[].questions 缺失，回退使用顶层 subjects[].questions；
  - 缺失 option_label 时，用通用标签（如 1..5）降级展示。

## 6. 验收清单（关键断言）
- 返回 schema_version = v1.2；
- 问卷维度包含 vg（区域页）；
- 学校页每个问卷维度包含 egional_avg；
- 百分比 pct ∈ [0,100] 且两位小数；
- 老页面不依赖的字段不被破坏（兼容通过）。

## 7. 常见问题
- 学校级题目分布缺失？多为底表无 school 粒度 —— 仅区域页展示分布；
- 选项标签缺失？后端提供量表推断与通用兜底，前端可直接渲染 label 为空时的数值序号。

## 8. 里程碑建议
- D+0：对接区域页维度均分与题目分布；
- D+1：对接学校页对标（egional_avg）与题目分布；
- D+2：联调 & 验收；
- D+3：回归与灰度发布。
