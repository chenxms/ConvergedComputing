# 前端分析 API 对接指南（v1.2 增补）

本增补文档在不改变原有接口路径的前提下，补充“科目顶层字段”“区域层级图表映射”“问卷维度/题目分布对接”与“批次物化脚本”说明，确保前端顺利完成数据展示与图表输出。

---

## 1. 顶层字段（物化后可直接读取）

- 区域级科目顶层：`grade_distribution`（精简结构，含各等级 `counts/percentages`）。
- 学校级科目顶层：`grade_distribution`（同上，可选）。
- 统一读取：
  - 百分位：仅保留 `metrics.percentiles.{P10,P50,P90}`，不再输出顶层 `p10/p50/p90`。
  - 区分度：仅保留 `metrics.discrimination`，不再输出顶层 `discrimination`。
  - 学校级排名：仅保留 `metrics.rank`，不再输出顶层 `rank`。
  - 等级占比：`metrics.{rate_excellent, rate_good, rate_pass, rate_fail}`；如需原始分布读取顶层 `grade_distribution`。

兼容读取建议：

- 已按上面收敛为单一来源；历史批次若仍带顶层同名字段，请忽略顶层，仅从 `metrics` 读取相应字段。

> 注意：示例代码中若仍使用 `subj.p10/p50/p90` 或顶层 `rank/discrimination`，请替换为 `subj.metrics.percentiles.Pxx`、`subj.metrics.rank`、`subj.metrics.discrimination`。

---

## 2. 区域层级图表与数据映射

调用：`GET /api/v12/batch/{batch}/regional`

1) 学校排名榜（按科目）
- 源：`data.subjects[].school_rankings`
- 字段：`school_name`、`avg`、`rank`（升序）

2) 科目总体统计卡片（区域级）
- 源：`data.subjects[]`
- 显示：`subject_name`、`type`、`metrics.avg`、`metrics.stddev`、顶层 `p10/p50/p90`、顶层 `discrimination`、`metrics.rate_*`（或顶层 `grade_distribution`）

3) 维度雷达/条形（考试/问卷）
- 源：`data.subjects[].dimensions[]`
- 显示：`name`（缺失回退 `code`）+ `avg`（问卷可用 `score_rate` 0–100）

4) 问卷维度内题目选项占比（堆叠条/环图）
- 区域级一次拿齐：问卷科目 `dimensions[].questions[].option_distribution`（`pct` 为 0–100）
- 学校级两步：
  - A：`GET /api/v12/batch/{batch}/school/{schoolId}` 获取维度 `avg/score_rate/rank`
  - B：`GET /api/v1/questionnaire-distributions/{batch}/{subject}/school/{schoolId}` 获取题目级分布
  - 用“区域级 subjects”构建 `question_id -> dimension_code` 映射，将学校级题目分布归组到维度

更多细节与示例代码：`docs/问卷维度与选项占比对接说明_v1.2.md`

---

## 3. 物化脚本（写入顶层字段）

为确保区域与学校“科目顶层”包含全部字段，请对目标批次物化：

```bash
# 单批次
python scripts/rewrite_subjects_v12.py G7-2025

# 多批次
python scripts/rewrite_subjects_v12.py G4-2025 G7-2025 G8-2025
```

物化完成后直接调用：
- 区域级：`GET /api/v12/batch/{batch}/regional`
- 学校级：`GET /api/v12/batch/{batch}/school/{schoolId}`

在线即时生成且需要区域顶层细节：
- `GET /api/v12/batch/{batch}/regional?include_detail=true`

---

参考原文档：`docs/前端分析API对接指南_v1.2.md`

---

## 4. 区域层级可视化方案（以 G4-2025 为例）

本节给出三类图的标准取数与 ECharts 绘制要点。所有取数均基于：

- 区域 subjects：`GET /api/v12/batch/{batch}/regional`
- 学校列表：`GET /api/batch/{batch}/schools`
- 学校 subjects：`GET /api/v12/batch/{batch}/school/{schoolId}`（用于按学校统计）

统一约定：

- 科目选择用 `subject` 变量（如“数学”“科学”“问卷”等）
- 维度字段优先读 `dimensions[].score_rate`（0–100）；若不存在则回退 `dimensions[].avg`
- 百分位优先顶层 `p10/p50/p90`；若不存在回退 `metrics.percentiles.{P10,P50,P90}`

### 4.1 雷达图：各学科核心素养维度得分率（区域）

- 数据源：`GET /api/v12/batch/G4-2025/regional`
- 取数：在问卷/目标学科的 `dimensions[]` 上取 `score_rate`（%）；无 `score_rate` 则取 `avg`，并将指示器最大值设为各维度 max(avg) 或 100。

示例代码（要点）：

```ts
// 拉取区域维度
const regional = await axios.get(`${BASE}/api/v12/batch/${batch}/regional`).then(r=>r.data.data);
const subj = regional.subjects.find((s:any)=>s.subject_name===subject);
const dims = (subj?.dimensions||[]).map((d:any)=>({
  name: d.name || d.code,
  value: d.score_rate ?? d.avg
}));
const maxVal = Math.max(100, ...dims.map(d=>d.value||0));
const option = {
  radar: {indicator: dims.map(d=>({ name: d.name, max: maxVal }))},
  series: [{ type: 'radar', data: [{ value: dims.map(d=>d.value||0) }]}]
};
```

### 4.2 散点图：学校为单位，X=标准差，Y=平均分（含两条基准线）

- 步骤：
  1) 学校列表：`GET /api/batch/{batch}/schools` → `school_code[]`
  2) 并发拉取学校 subjects：`GET /api/v12/batch/{batch}/school/{schoolId}`，提取目标科目 `metrics.stddev` 和 `metrics.avg`
  3) 基准线：
     - Y 轴“全区学科总平均分”＝区域 subjects 中该科目 `metrics.avg`
     - X 轴“全区学科标准差均值”＝步骤 2 收集的各校 `metrics.stddev` 的平均值

示例代码（限流并发建议 8~12）：

```ts
const schools = await axios.get(`${BASE}/api/batch/${batch}/schools`).then(r=>r.data.data.schools);
const codes = schools.map((s:any)=>s.school_code);
const fetchSchool = (code:string)=> axios.get(`${BASE}/api/v12/batch/${batch}/school/${code}`).then(r=>({code, data:r.data.data}));
// 简易限流
const chunks = (arr:any[],size:number)=> arr.reduce((a,c,i)=>(i%size?a[a.length-1].push(c):a.push([c]),a),[] as any[]);
const out:any[]=[];
for (const group of chunks(codes,10)){
  const res = await Promise.all(group.map(fetchSchool));
  out.push(...res);
}
const points = out.map(({code,data})=>{
  const subj = data.subjects.find((s:any)=>s.subject_name===subject) || {};
  const m = subj.metrics || {};
  return { name: code, x: m.stddev || 0, y: m.avg || 0 };
});
const xMean = points.reduce((a,p)=>a+p.x,0)/Math.max(1,points.length);
// 区域均值（Y 基准）
const regional = await axios.get(`${BASE}/api/v12/batch/${batch}/regional`).then(r=>r.data.data);
const regionalAvg = (regional.subjects.find((s:any)=>s.subject_name===subject)?.metrics?.avg)||0;
const option = {
  xAxis: {}, yAxis: {},
  series: [{ type:'scatter', data: points.map(p=>[p.x,p.y]) }],
  markLine: {
    data: [
      { xAxis: xMean, name: '全区std均值' },
      { yAxis: regionalAvg, name: '全区平均分' }
    ]
  }
};
```

### 4.3 分布图：学校为单位，线段表示 P10–P90，点表示 P50

- 步骤与 4.2 类似，取学校科目顶层或 metrics 的百分位：`p10/p50/p90`。
- 方案：用 ECharts custom 系列绘制每所学校一条水平线段（P10→P90），在线段中点绘制散点（P50）。

示例代码（渲染数据组织）：

```ts
const items = out.map(({code,data})=>{
  const subj = data.subjects.find((s:any)=>s.subject_name===subject)||{};
  const p10 = subj.p10 ?? subj.metrics?.percentiles?.P10 || 0;
  const p50 = subj.p50 ?? subj.metrics?.percentiles?.P50 || 0;
  const p90 = subj.p90 ?? subj.metrics?.percentiles?.P90 || 0;
  return { school: code, p10, p50, p90 };
}).sort((a,b)=>a.p50-b.p50);
const yCats = items.map(i=>i.school);
const option = {
  grid: { left: 80 },
  xAxis: { type:'value' }, yAxis: { type:'category', data: yCats },
  series: [
    {
      type: 'custom', renderItem: (params:any, api:any)=>{
        const idx = params.dataIndex;
        const i = items[idx];
        const y = api.coord([0, idx])[1];
        const x1 = api.coord([i.p10, idx])[0];
        const x2 = api.coord([i.p90, idx])[0];
        const xMid = api.coord([i.p50, idx])[0];
        return {
          type: 'group', children: [
            { type:'line', shape:{ x1, y1:y, x2, y2:y }, style:{ stroke:'#409EFF', lineWidth:2 }},
            { type:'circle', shape:{ cx:xMid, cy:y, r:4 }, style:{ fill:'#E6A23C' } }
          ]
        };
      }, data: items
    }
  ]
};
```

---

## 5. 学校层级可视化方案

页面参数：`batch`、`schoolId`，以及学科映射配置（用于“智性/审美总分”计算）。

### 5.1 二维散点：智性发展总分 vs 审美素养发展总分（各校）

- 定义（G4-2025 示例）：
  - 智性发展总分：数学平均分 + 科学平均分
  - 审美素养总分：音乐平均分 + 美术平均分
- 实现：与 4.2 相同按学校批量获取，但改为计算两个坐标：

```ts
const cognitive = ['数学','科学'];
const aesthetic = ['音乐','美术'];
const pt = ({code,data}:any)=>{
  const sx = (names:string[])=> names.reduce((a,n)=> a + ((data.subjects.find((s:any)=>s.subject_name===n)?.metrics?.avg)||0), 0);
  return { name: code, x: sx(cognitive), y: sx(aesthetic) };
};
const points = out.map(pt);
const option = { xAxis:{}, yAxis:{}, series:[{ type:'scatter', data: points.map(p=>[p.x,p.y]) }] };
```

> 其他批次请通过配置替换学科名数组。

### 5.2 雷达图：本校 vs 区域 维度得分率对比

- 学校数据：`GET /api/v12/batch/{batch}/school/{schoolId}` → 目标学科 `dimensions[].score_rate`（或回退 `avg`）
- 区域数据：`GET /api/v12/batch/{batch}/regional` → 区域学科 `dimensions[]` 同字段
- 维度按 code 对齐，名称用 `name`（缺失回退 `code`）

```ts
const school = await axios.get(`${BASE}/api/v12/batch/${batch}/school/${schoolId}`).then(r=>r.data.data);
const reg = await axios.get(`${BASE}/api/v12/batch/${batch}/regional`).then(r=>r.data.data);
const sSub = school.subjects.find((s:any)=>s.subject_name===subject);
const rSub = reg.subjects.find((s:any)=>s.subject_name===subject);
const mapDims = (arr:any[])=> Object.fromEntries((arr||[]).map(d=>[d.code, {name:d.name||d.code, v:d.score_rate ?? d.avg}]));
const sd = mapDims(sSub?.dimensions||[]), rd = mapDims(rSub?.dimensions||[]);
const codes = Array.from(new Set([...Object.keys(sd),...Object.keys(rd)]));
const indicators = codes.map(c=>({ name: sd[c]?.name || rd[c]?.name || c, max: 100 }));
const option = {
  radar:{ indicator: indicators },
  series:[{ type:'radar', data:[ { name:'本校', value: codes.map(c=>sd[c]?.v||0) }, { name:'区域', value: codes.map(c=>rd[c]?.v||0) } ] }]
};
```

### 5.3 分布图：本校 P10/P50/P90，并含区域对标线

- 学校百分位：学校科目顶层 `p10/p50/p90`（或回退 metrics.percentiles）
- 区域对标：区域科目顶层 `p10/p50/p90`（或回退 metrics.percentiles）
- 绘制：同 4.3 的线段+点；再用 `markLine` 或额外 custom 画出区域的三条竖线。

```ts
const sSubj = sSub || {};
const sp10 = sSubj.p10 ?? sSubj.metrics?.percentiles?.P10 || 0;
const sp50 = sSubj.p50 ?? sSubj.metrics?.percentiles?.P50 || 0;
const sp90 = sSubj.p90 ?? sSubj.metrics?.percentiles?.P90 || 0;
const rSubj = rSub || {};
const rp10 = rSubj.p10 ?? rSubj.metrics?.percentiles?.P10 || 0;
const rp50 = rSubj.p50 ?? rSubj.metrics?.percentiles?.P50 || 0;
const rp90 = rSubj.p90 ?? rSubj.metrics?.percentiles?.P90 || 0;
const option = {
  xAxis:{}, yAxis:{ type:'category', data:['本校']},
  series:[{ type:'custom', renderItem: /* 复用 4.3 逻辑绘一条线段与P50点 */ }],
  markLine:{ data:[ {xAxis:rp10,name:'区域P10'}, {xAxis:rp50,name:'区域P50'}, {xAxis:rp90,name:'区域P90'} ] }
};
```

---

## 6. 前端实现模板（Vue2 + Axios + ECharts）

项目栈：Vue 2.6 + Element UI + Vue CLI 4 + Vuex + Axios。

- API 工具：`/src/api/subjects.ts`

```ts
import axios from 'axios';
const BASE = process.env.VUE_APP_API_BASE || 'http://localhost:8000';
export const getRegional = (batch:string)=> axios.get(`${BASE}/api/v12/batch/${batch}/regional`).then(r=>r.data.data);
export const getSchools = (batch:string)=> axios.get(`${BASE}/api/batch/${batch}/schools`).then(r=>r.data.data.schools);
export const getSchool = (batch:string, sid:string)=> axios.get(`${BASE}/api/v12/batch/${batch}/school/${sid}`).then(r=>r.data.data);
```

- 组件建议：每个图封装成独立组件，接收 `batch`、`subject`、`schoolId`、`mappings`（学科映射）等 props；在 `mounted` 中拉取数据，`loading`/`error` 用 Element UI 处理；ECharts 用官方 Vue 组件或手动 init。

- 性能：
  - 学校聚合图（4.2/4.3/5.1）需多次请求，建议并发限流（10）+ 结果缓存（Vuex/内存 Map）。
  - 可在后端侧增加专用聚合端点以减少请求数（可选）。

---

## 7. 可视化与富文本栈集成（ECharts 5.4.0 + Quill 2.0.2 + highlight.js）

### 7.1 安装与版本

```bash
# 可视化
npm i echarts@5.4.0

# 富文本（Quill v2）与代码高亮
npm i quill@2.0.2 highlight.js@11.9.0
```

注意：Vue2 常见的 `vue-quill-editor` 多绑定 Quill v1，不适配 v2。建议直接使用 Quill 原生 API 在组件中初始化。

### 7.2 ECharts 全局封装（按需、可复用）

创建 `src/plugins/echarts.ts`：

```ts
import * as echarts from 'echarts/core';
import { BarChart, LineChart, ScatterChart, RadarChart, CustomChart } from 'echarts/charts';
import { GridComponent, TooltipComponent, LegendComponent, TitleComponent, RadarComponent, MarkLineComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

echarts.use([
  BarChart, LineChart, ScatterChart, RadarChart, CustomChart,
  GridComponent, TooltipComponent, LegendComponent, TitleComponent, RadarComponent, MarkLineComponent,
  CanvasRenderer,
]);

export default echarts;
```

基础组件 `src/components/BaseChart.vue`：

```vue
<template>
  <div ref="el" :style="{width, height}"></div>
  </template>
<script>
import echarts from '@/plugins/echarts';
export default {
  name: 'BaseChart',
  props: { option: Object, width: { default: '100%' }, height: { default: '360px' } },
  data: () => ({ chart: null }),
  mounted() {
    this.chart = echarts.init(this.$refs.el);
    this.option && this.chart.setOption(this.option);
    window.addEventListener('resize', this.resize);
  },
  watch: {
    option: { deep: true, handler(v){ if(this.chart && v){ this.chart.setOption(v, true); } } }
  },
  beforeDestroy(){ window.removeEventListener('resize', this.resize); this.chart && this.chart.dispose(); },
  methods: {
    resize(){ this.chart && this.chart.resize(); },
    toPng(){ return this.chart ? this.chart.getDataURL({ type: 'png', pixelRatio: 2 }) : null; }
  }
}
</script>
```

使用示例：

```vue
<BaseChart :option="option" height="420px" />
```

### 7.3 Quill v2 富文本（含代码高亮）

在 `src/main.js` 引入高亮样式（任选主题）：

```js
import 'highlight.js/styles/github.css';
```

富文本组件 `src/components/RichEditor.vue`：

```vue
<template>
  <div ref="editor" class="ql-container ql-snow" style="min-height:220px"></div>
</template>
<script>
import Quill from 'quill';
import hljs from 'highlight.js';
export default {
  name: 'RichEditor',
  props: { value: String },
  data:()=>({ q:null }),
  mounted(){
    this.q = new Quill(this.$refs.editor, {
      theme: 'snow',
      modules: {
        toolbar: [
          [{ header: [1, 2, 3, false] }],
          ['bold', 'italic', 'underline', 'strike'],
          [{ list: 'ordered'}, { list: 'bullet' }],
          ['code-block', 'blockquote', 'link'],
          ['clean']
        ],
        syntax: { highlight: text => hljs.highlightAuto(text).value }
      }
    });
    if (this.value) this.q.root.innerHTML = this.value;
    this.q.on('text-change', ()=> this.$emit('input', this.q.root.innerHTML));
  },
  beforeDestroy(){ this.q = null; }
}
</script>
```

要点：

- 传参/双向绑定：使用 `v-model` 绑定 HTML（或自行处理 Quill Delta）
- 语法高亮：开启 `modules.syntax`，并加载 highlight.js CSS 主题
- 只读展示：`new Quill(el, { readOnly: true, modules: { toolbar: false, syntax: true } })`

### 7.4 常见实践与注意

- 响应式：图表容器进入可视区域后再 init，避免隐藏容器初始化尺寸为 0（使用 `v-if` 或 `IntersectionObserver`）
- 性能：大数据散点请设置 `progressiveThreshold` 与 `animation: false`；分批渲染点数据
- 导出：用 `chart.getDataURL()` 生成 PNG，用于报告/打印
- 主题：统一在 `plugins/echarts.ts` 引入自定义主题（可选）
- 富文本安全：后端存储时建议保留 Delta（富文本结构化数据）与 HTML 双份，渲染时对外部来源 HTML 做 XSS 处理

---

## 8. ECharts Option 模板函数（可直接拷贝）

以下函数接收已整理好的数据，直接返回 ECharts 5.4.0 的 option。可放在 `src/utils/chart-options.ts` 并按需导入。

```ts
// 8.1 区域雷达：维度得分率
export function buildRadarOption(labelsValues: { name: string; value: number }[], max = 100, title?: string) {
  const indicator = labelsValues.map(d => ({ name: d.name, max }));
  return {
    title: title ? { text: title } : undefined,
    tooltip: { trigger: 'item' },
    radar: { indicator },
    series: [
      {
        type: 'radar',
        areaStyle: { opacity: 0.15 },
        data: [{ value: labelsValues.map(d => d.value) }]
      }
    ]
  };
}

// 8.2 学校 vs 区域维度雷达对比
export function buildRadarCompareOption(
  indicators: { name: string; max?: number }[],
  school: number[],
  regional: number[],
  title?: string
) {
  return {
    title: title ? { text: title } : undefined,
    tooltip: { trigger: 'item' },
    legend: { data: ['本校', '区域'] },
    radar: { indicator: indicators.map(i => ({ name: i.name, max: i.max ?? 100 })) },
    series: [
      { type: 'radar', name: '对比', data: [ { name: '本校', value: school }, { name: '区域', value: regional } ] }
    ]
  };
}

// 8.3 散点：X=标准差，Y=平均分，含两条基准线
export function buildScatterStdAvgOption(
  points: { name: string; x: number; y: number }[],
  xMean: number,
  yBaseline: number,
  title?: string
) {
  return {
    title: title ? { text: title } : undefined,
    tooltip: { trigger: 'item', formatter: (p:any) => `${points[p.dataIndex].name}<br/>std: ${p.value[0]}<br/>avg: ${p.value[1]}` },
    xAxis: { name: '标准差' },
    yAxis: { name: '平均分' },
    series: [ { type: 'scatter', data: points.map(p => [p.x, p.y]) } ],
    markLine: {
      symbol: 'none',
      label: { formatter: (p:any)=> p.name || '' },
      data: [ { xAxis: xMean, name: '全区std均值' }, { yAxis: yBaseline, name: '全区平均分' } ]
    }
  };
}

// 8.4 分布图：学校线段（P10→P90）+ 中点（P50）
// items 将按 P50 升序排列，并作为 y 轴类目；若需要保持原顺序，请移除 sort
export function buildDistributionSegmentOption(items: { label: string; p10: number; p50: number; p90: number }[], title?: string) {
  const sorted = [...items].sort((a,b)=> (a.p50 - b.p50));
  const yCats = sorted.map(i=> i.label);
  const data = sorted.map(i=> [i.p10, i.p50, i.p90]); // [p10,p50,p90]
  return {
    title: title ? { text: title } : undefined,
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 80, right: 24 },
    xAxis: { type: 'value', name: '分数' },
    yAxis: { type: 'category', data: yCats },
    series: [{
      type: 'custom',
      renderItem: (params:any, api:any) => {
        const idx = params.dataIndex;
        const y = api.coord([0, idx])[1];
        const x1 = api.coord([api.value(0), idx])[0];
        const x2 = api.coord([api.value(2), idx])[0];
        const xm = api.coord([api.value(1), idx])[0];
        return {
          type: 'group', children: [
            { type: 'line', shape: { x1, y1: y, x2, y2: y }, style: { stroke: '#409EFF', lineWidth: 2 } },
            { type: 'circle', shape: { cx: xm, cy: y, r: 4 }, style: { fill: '#E6A23C' } }
          ]
        };
      },
      encode: { x: [0,1,2], y: 3 },
      data
    }]
  };
}

// 8.5 单校分布 + 区域对标线
export function buildSingleDistributionWithBenchmarkOption(
  item: { p10: number; p50: number; p90: number; label?: string },
  benchmark: { p10: number; p50: number; p90: number },
  title?: string
) {
  return {
    title: title ? { text: title } : undefined,
    grid: { left: 80, right: 24 },
    xAxis: { type: 'value', name: '分数' },
    yAxis: { type: 'category', data: [item.label || '本校'] },
    series: [{
      type: 'custom',
      renderItem: (params:any, api:any) => {
        const y = api.coord([0, 0])[1];
        const x1 = api.coord([item.p10, 0])[0];
        const x2 = api.coord([item.p90, 0])[0];
        const xm = api.coord([item.p50, 0])[0];
        return {
          type: 'group', children: [
            { type: 'line', shape: { x1, y1: y, x2, y2: y }, style: { stroke: '#409EFF', lineWidth: 2 } },
            { type: 'circle', shape: { cx: xm, cy: y, r: 5 }, style: { fill: '#E6A23C' } }
          ]
        };
      }
    }],
    markLine: {
      symbol: 'none',
      data: [
        { xAxis: benchmark.p10, name: '区域P10' },
        { xAxis: benchmark.p50, name: '区域P50' },
        { xAxis: benchmark.p90, name: '区域P90' }
      ]
    }
  };
}

// 8.6 二维散点：智性（如 数学+科学） vs 审美（如 音乐+美术）
export function buildCognitiveAestheticScatterOption(points: { name: string; x: number; y: number }[], title?: string) {
  return {
    title: title ? { text: title } : undefined,
    tooltip: { trigger: 'item', formatter: (p:any)=> `${points[p.dataIndex].name}<br/>智性: ${p.value[0]}<br/>审美: ${p.value[1]}` },
    xAxis: { name: '智性发展总分' },
    yAxis: { name: '审美素养总分' },
    series: [{ type: 'scatter', data: points.map(p=>[p.x,p.y]) }]
  };
}
```

提示：上述模板仅关注图形层配置；数据获取与整理请复用第 4/5 节的步骤与示例。若需统一主题、字体或导出图片等，请参考第 7 节实践建议。
