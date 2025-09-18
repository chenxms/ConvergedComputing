<template>
  <div class="questionnaire-dimension-chart" :style="{ padding: '12px' }">
    <div class="toolbar" style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
      <span class="title" style="font-size:14px;color:#606266;">
        {{ titleText }}
      </span>
      <el-button size="mini" type="primary" @click="exportPng">导出PNG</el-button>
    </div>
    <div ref="chart" :style="{ width: '100%', height: computedHeight }"></div>
    <el-empty v-if="!hasData" description="暂无数据" />
  </div>
</template>

<script>
import * as echarts from 'echarts'
import axios from 'axios'

export default {
  name: 'QuestionnaireDimensionChart',
  props: {
    batchCode: { type: String, required: true },
    subjectName: { type: String, required: true },
    schoolId: { type: [String, Number], default: null },
    // 该组件以“一个维度一个图表”为原则，维度通过 dimensionCode 指定
    dimensionCode: { type: String, required: true },
    dimensionName: { type: String, default: '' },
    // 值域：'percentage_0_100' 或 'ratio_0_1'
    valueFormat: { type: String, default: 'percentage_0_100' },
    height: { type: [String, Number], default: 560 },
    // 后端服务地址
    baseUrl: { type: String, default: 'http://117.72.14.166:8000' }
  },
  data() {
    return {
      chart: null,
      loadingInstance: null,
      questions: [], // 当前维度下的问题（含选项占比）
      optionLabels: [], // 统一后的选项标签顺序
      LIKERT5_COLORS: ['#f56c6c', '#e6a23c', '#f4e06d', '#95d475', '#67c23a']
    }
  },
  computed: {
    computedHeight() {
      return typeof this.height === 'number' ? this.height + 'px' : this.height
    },
    hasData() {
      return Array.isArray(this.questions) && this.questions.length > 0
    },
    titleText() {
      const dn = this.dimensionName || this.dimensionCode
      return `${dn} — 题目选项占比（分组）`
    }
  },
  mounted() {
    this.initChart()
    this.loadAndRender()
    window.addEventListener('resize', this.handleResize)
  },
  beforeDestroy() {
    window.removeEventListener('resize', this.handleResize)
    if (this.chart) this.chart.dispose()
    this.chart = null
  },
  methods: {
    async loadAndRender() {
      try {
        this.loadingInstance = this.$loading?.({ lock: true, text: '加载中...', spinner: 'el-icon-loading' })
        // 真实 API 请求
        const b = encodeURIComponent(this.batchCode)
        const s = encodeURIComponent(this.subjectName)
        const statsUrl = this.schoolId
          ? `${this.baseUrl}/api/v12/batch/${b}/school/${this.schoolId}`
          : `${this.baseUrl}/api/v12/batch/${b}/regional`
        const distUrl = this.schoolId
          ? `${this.baseUrl}/api/v1/questionnaire-distributions/${b}/${s}/school/${this.schoolId}`
          : `${this.baseUrl}/api/v1/questionnaire-distributions/${b}/${s}/regional`

        const [mainRes, distRes] = await Promise.all([
          axios.get(statsUrl),
          axios.get(distUrl)
        ])

        // 维度校验（可选）：确保 dimensionCode 在该 subject 的维度列表中
        const subjects = mainRes?.data?.data?.subjects || []
        const questionnaire = subjects.find(x => x.type === 'questionnaire' && x.subject_name === this.subjectName)
        const dims = questionnaire?.dimensions || []
        const exists = dims.some(d => d.code === this.dimensionCode)
        if (!exists) {
          console.warn(`[QuestionnaireDimensionChart] 维度不存在: ${this.dimensionCode}`)
        }

        const allQuestions = (distRes?.data?.data?.questions || [])
          .filter(q => q.dimension_code === this.dimensionCode)
        this.questions = allQuestions

        this.render()
      } catch (e) {
        console.error('[QuestionnaireDimensionChart] 加载失败:', e)
        this.$message?.error?.('数据加载失败，请稍后重试')
      } finally {
        this.loadingInstance && this.loadingInstance.close?.()
      }
    },

    initChart() {
      const dom = this.$refs.chart
      if (!dom) return
      this.chart = echarts.init(dom)
    },

    exportPng() {
      if (!this.chart) return
      const url = this.chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: '#fff' })
      const a = document.createElement('a')
      a.href = url
      a.download = `${this.dimensionName || this.dimensionCode}-题目选项分布.png`
      a.click()
    },

    render() {
      if (!this.chart) this.initChart()
      if (!this.chart) return

      // 统一选项标签顺序（若为 5 级李克特）
      const likertLabels = ['非常不同意','不同意','一般','同意','非常同意']

      // 收集所有出现过的标签（如题目选项集不一致，则并集填0）
      const labelsSet = new Set(likertLabels)
      this.questions.forEach(q => (q.options || []).forEach(o => labelsSet.add(o.option_label || `选项${o.option_level}`)))
      const optionLabels = Array.from(labelsSet)
      // 若正好包含 5 级李克特标签，则按语义顺序排序，否则保持字典序
      const exactlyLikert5 = likertLabels.every(l => optionLabels.includes(l)) && optionLabels.length === 5
      const finalLabels = exactlyLikert5 ? likertLabels.slice() : optionLabels.sort()
      this.optionLabels = finalLabels

      // 构造 dataset.source（分组柱：x轴=题目ID；series=选项）
      const header = ['题目', ...finalLabels]
      const source = [header]
      const normalize = v => {
        if (v == null) return 0
        const num = +v
        if (this.valueFormat === 'ratio_0_1') return Math.round(num * 10000) / 100
        return Math.round(num * 100) / 100
      }

      this.questions.forEach(q => {
        const map = {}
        ;(q.options || []).forEach(o => {
          const label = o.option_label || `选项${o.option_level}`
          map[label] = normalize(o.percentage)
        })
        // 使用题目ID作为类目
        const row = [String(q.question_id)]
        finalLabels.forEach(lbl => row.push(map[lbl] || 0))
        source.push(row)
      })

      const option = {
        backgroundColor: '#fff',
        title: { text: this.titleText, left: 'center', textStyle: { fontSize: 14 } },
        color: this.LIKERT5_COLORS,
        legend: { type: 'scroll', top: 28 },
        tooltip: {
          trigger: 'axis', axisPointer: { type: 'shadow' },
          formatter: params => {
            const name = params[0] && params[0].name ? params[0].name : ''
            const lines = params
              .filter(p => p.seriesType === 'bar')
              .map(p => `${p.marker}${p.seriesName}: ${p.value != null ? p.value : 0}%`)
            const total = params.reduce((s, p) => s + (p.value || 0), 0)
            lines.push(`合计: ${Math.round(total)}%`)
            return `${name}<br/>${lines.join('<br/>')}`
          }
        },
        grid: { left: 12, right: 14, top: 70, bottom: 58, containLabel: true },
        dataset: { source, sourceHeader: true },
        xAxis: {
          type: 'category',
          axisLabel: {
            interval: 0,
            rotate: source.length > 9 ? 30 : 0
          }
        },
        yAxis: { type: 'value', max: 100, axisLabel: { formatter: '{value}%' } },
        dataZoom: [
          { type: 'inside', start: 0, end: 100 },
          { type: 'slider', start: 0, end: 100, height: 14 }
        ],
        series: finalLabels.map(() => ({
          type: 'bar', barMaxWidth: 36, barGap: '10%', barCategoryGap: '30%',
          label: { show: true, position: 'top', fontSize: 10, formatter: p => (p.value && p.value >= 6 ? p.value + '%' : '') },
          emphasis: { focus: 'series' }
        }))
      }

      this.chart.setOption(option, true)
    },

    truncate(t, n) { return t && t.length > n ? t.slice(0, n) + '…' : t }
  }
}
</script>

<style scoped>
.questionnaire-dimension-chart { background: #fff; border: 1px solid #ebeef5; border-radius: 4px; }
</style>
