# 前端分析 API 对接指南（v1.2）

面向前端/AI 开发工程师的对接文档，覆盖学科汇聚、维度、问卷题目选项分布等常用数据拉取方式与渲染要点。按本文即可直接编写页面与图表渲染逻辑。

---

## 1. 基本信息（部署与基址）

- 对外基址（默认）：`http://<host>:8000`
- Subjects v1.2 接口统一前缀：`/api/v12`
- 问卷题目分布接口前缀：`/api/v1/questionnaire-distributions`
- 单端口说明：运维仅开放 8000 端口是正常的。该端口运行主服务，已包含 v1.2 路由；无需单独再开 8001。
- 鉴权：当前无鉴权
- CORS：应用已启用 CORS。若使用网关/反向代理，对跨域预检（OPTIONS）需在网关回显相应头（见第 6 节）。

---

## 2. 统一 Subjects（v1.2）

统一返回区域级或学校级的“学科列表 + 指标 + 维度”，便于前端一次性渲染学科卡片、维度雷达、排名等核心视图。

### 2.1 端点一览（v1.2）
- 获取区域级 subjects（推荐）
  - `GET /api/v12/batch/{batch_code}/regional`
- 获取学校级 subjects（推荐）
  - `GET /api/v12/batch/{batch_code}/school/{school_id}`
- 触发物化（生成并写入 DB，通常用于预热/回填）
  - `POST /api/v12/batch/{batch_code}/materialize`
  - 联调一般不需要主动调用。

> 说明：以上端点均在主服务 8000 端口对外提供。

### 2.2 响应结构（区域/学校通用示例）

```json
{
  "success": true,
  "message": "v1.2 区域级 subjects 已生成: G4-2025",
  "code": 200,
  "data": {
    "schema_version": "v1.2",
    "data_version": "v1.2",
    "batch_code": "G4-2025",
    "aggregation_level": "REGIONAL", // 或 "SCHOOL"
    "subjects": [
      {
        "subject_name": "数学",
        "type": "exam", // 或 "questionnaire"
        "metrics": {
          "avg": 78.53,
          "stddev": 12.36,
          "min": 12.0,
          "max": 100.0,
          "difficulty": 0.79,
          "discrimination": 0.52,
          "percentiles": { "P10": 56.0, "P50": 80.0, "P90": 95.0 },
          "subject_full_score": 100.0,
          "rank": 8 // 学校级有效；区域级无 rank
        },
        "school_rankings": [
          { "school_id": "5044", "school_name": "一小", "avg": 85.12, "rank": 1 },
          { "school_id": "5068", "school_name": "二小", "avg": 83.05, "rank": 2 }
        ],
        "dimensions": [
          {
            "code": "D-CUR",
            "name": "好奇心",
            "avg": 82.35,
            "score_rate": 0.82,
            "rank": 5, // 学校级有效；区域级无 rank
            "option_distribution": [
              { "option_level": 1, "option_label": "非常不符合", "pct": 5.21 },
              { "option_level": 2, "option_label": "不符合",     "pct": 12.45 },
              { "option_level": 3, "option_label": "一般",       "pct": 30.29 },
              { "option_level": 4, "option_label": "符合",       "pct": 36.10 },
              { "option_level": 5, "option_label": "非常符合",   "pct": 15.95 }
            ]
          }
        ]
      }
    ]
  }
}
```

### 2.3 前端常用渲染映射

- 学科卡片（区域/学校）
  - 标题：`subject_name`
  - 类型徽标：`type === 'questionnaire' ? '问卷' : '考试'`
  - 关键指标：`metrics.avg`、`metrics.difficulty`、`metrics.stddev`、`metrics.min`/`metrics.max`
  - 百分位（区域级）：`metrics.percentiles.P10/P50/P90`
  - 学校名次（学校级）：`metrics.rank`
- 学校排名榜（区域级）：`school_rankings` 按 `rank` 升序渲染
- 维度雷达/条形图（考试/问卷）
  - 名称：`dimensions[].name`
  - 数值：`dimensions[].avg` 或 `dimensions[].score_rate * 100`
  - 学校级维度名次：`dimensions[].rank`
- 问卷维度选项分布（堆叠条/环图）
  - 数据源：`dimensions[].option_distribution`
  - 显示：`option_label`（若为空可回退为 `选项{option_level}`），占比：`pct`

> 提示：问卷“题目级”的选项分布不在 subjects 内嵌，需使用第 3 节的问卷分布 API。

---

## 3. 问卷题目选项分布 API

- 区域级：`GET /api/v1/questionnaire-distributions/{batch_code}/{subject_name}/regional`
- 学校级：`GET /api/v1/questionnaire-distributions/{batch_code}/{subject_name}/school/{school_id}`
- 按问卷科目列学校：`GET /api/v1/questionnaire-distributions/{batch_code}/{subject_name}/schools`

返回示例（题目级分布）：

```json
{
  "code": 200,
  "data": {
    "questions": [
      {
        "question_id": "Q01",
        "total_responses": 1234,
        "options": [
          { "option_level": 1, "option_label": "非常不符合", "pct": 5.2 },
          { "option_level": 2, "option_label": "不符合",     "pct": 12.4 },
          { "option_level": 3, "option_label": "一般",       "pct": 30.3 },
          { "option_level": 4, "option_label": "符合",       "pct": 36.1 },
          { "option_level": 5, "option_label": "非常符合",   "pct": 16.0 }
        ]
      }
    ]
  }
}
```

---

## 4. 常见组合查询与前端调用示例

```ts
// 建议使用环境变量配置基址（示例以 Vite 为例）
const BASE = (import.meta as any).env?.VITE_API_BASE || 'http://localhost:8000';

async function loadRegionalSubjects(batchCode: string) {
  const res = await fetch(`${BASE}/api/v12/batch/${batchCode}/regional`);
  const body = await res.json();
  if (!body?.success) throw new Error(body?.message || '请求失败');
  const subjects = body.data?.subjects || [];
  return subjects.map((s: any) => ({
    name: s.subject_name,
    type: s.type,
    avg: s.metrics?.avg,
    stddev: s.metrics?.stddev,
    difficulty: s.metrics?.difficulty,
    p10: s.metrics?.percentiles?.P10,
    p50: s.metrics?.percentiles?.P50,
    p90: s.metrics?.percentiles?.P90,
    rankings: s.school_rankings || [],
    dimensions: s.dimensions || []
  }));
}

async function loadSchoolSubjects(batchCode: string, schoolId: string) {
  const res = await fetch(`${BASE}/api/v12/batch/${batchCode}/school/${schoolId}`);
  const body = await res.json();
  if (!body?.success) throw new Error(body?.message || '请求失败');
  const subjects = body.data?.subjects || [];
  const first = subjects[0];
  const dims = (first?.dimensions || []).map((d: any) => ({
    name: d.name || d.code,
    value: d.avg ?? (d.score_rate ? d.score_rate * 100 : null),
    rank: d.rank
  }));
  const rank = first?.metrics?.rank; // 全科目在区域内的排名（学校级）
  return { subjects, dims, rank };
}

async function loadQuestionOptionDistribution(batch: string, subject: string, schoolId?: string) {
  const path = schoolId
    ? `/api/v1/questionnaire-distributions/${batch}/${subject}/school/${schoolId}`
    : `/api/v1/questionnaire-distributions/${batch}/${subject}/regional`;
  const res = await fetch(`${BASE}${path}`);
  const body = await res.json();
  if (body?.code !== 200) throw new Error(body?.message || '请求失败');
  const qs = body?.data?.questions || [];
  return qs.map((q: any) => ({
    id: q.question_id,
    total: q.total_responses,
    options: (q.options || []).map((o: any) => ({ label: o.option_label || `选项${o.option_level}`, pct: o.pct }))
  }));
}
```

---

## 5. 渲染建议与边界处理

- 数值精度：百分比/均值统一保留 2 位小数；前端可直接显示或格式化为 `%`。
- 问卷标签缺失：`option_label` 可能为空，前端应回退为 `选项{option_level}`。
- 维度缺失：若 `dimensions` 为空或仅返回 `code`，前端可降级显示 `code`。
- 排名相等：区域学校排名使用密集排名（DENSE_RANK）。
- 性能建议：页面首次进入读取区域级 subjects；学校切换时再拉学校级 subjects/问卷分布。

---

## 6. CORS 与联调策略

- 本地联调（推荐使用前端代理，避免浏览器跨域限制）
  - Vite：
    - `server.proxy['/api'] = { target: 'http://<host>:8000', changeOrigin: true }`
  - Webpack devServer：
    - `devServer.proxy = { '/api': { target: 'http://<host>:8000', changeOrigin: true } }`
- 生产网关（Nginx）要点（示例，需按实际调整 upstream）
  - 反向代理：`location /api/ { proxy_pass http://app_backend/; }`
  - 预检响应（OPTIONS）与回显 CORS 头：
    - `add_header Access-Control-Allow-Origin $http_origin always;`
    - `add_header Access-Control-Allow-Credentials true always;`
    - `add_header Access-Control-Allow-Methods 'GET,POST,PUT,PATCH,DELETE,OPTIONS' always;`
    - `add_header Access-Control-Allow-Headers $http_access_control_request_headers always;`
    - `add_header Access-Control-Max-Age 86400 always;`
    - `if ($request_method = OPTIONS) { return 204; }`
- 如前端会带凭证（Cookie/Authorization），建议在后端/网关使用域名白名单而非 `*`。

---

## 7. 快速自检（curl）

```bash
# 区域级 subjects
curl "http://localhost:8000/api/v12/batch/G4-2025/regional"

# 学校级 subjects
curl "http://localhost:8000/api/v12/batch/G4-2025/school/5044"

# 区域级问卷题目分布
curl "http://localhost:8000/api/v1/questionnaire-distributions/G4-2025/问卷/regional"

# 学校级问卷题目分布
curl "http://localhost:8000/api/v1/questionnaire-distributions/G4-2025/问卷/school/5044"

# 按问卷科目列学校（是否已有分布）
curl "http://localhost:8000/api/v1/questionnaire-distributions/G4-2025/问卷/schools"
```

---

## 8. 变更记录

- 2025-09-13：统一对外端口为 8000（可选 subjects 独立服务为 8001，仅在需要时启用）；修复中文乱码；补充 CORS 与前端代理指引。

