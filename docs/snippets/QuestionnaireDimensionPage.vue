<template>
  <div class="page" style="padding:12px;">
    <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;">
      <el-select v-model="selectedDimension" placeholder="选择维度" filterable style="width: 300px" @change="reload">
        <el-option v-for="d in dimensions" :key="d.code" :label="d.name" :value="d.code" />
      </el-select>
    </div>
    <questionnaire-dimension-chart
      v-if="selectedDimension"
      :base-url="baseUrl"
      :batch-code="batchCode"
      :subject-name="subjectName"
      :school-id="schoolId"
      :dimension-code="selectedDimension"
      :dimension-name="dimensionNameMap[selectedDimension]"
      :height="600"
      value-format="percentage_0_100"
    />
    <el-empty v-else description="未选择维度" />
  </div>
</template>

<script>
import axios from 'axios'
import QuestionnaireDimensionChart from './QuestionnaireChart.vue'

export default {
  name: 'QuestionnaireDimensionPage',
  components: { QuestionnaireDimensionChart },
  props: {
    baseUrl: { type: String, default: 'http://117.72.14.166:8000' },
    batchCode: { type: String, required: true },
    subjectName: { type: String, required: true },
    schoolId: { type: [String, Number], default: null }
  },
  data() {
    return {
      loading: false,
      dimensions: [], // { code, name }
      dimensionNameMap: {},
      selectedDimension: ''
    }
  },
  mounted() {
    this.loadDimensions()
  },
  methods: {
    async loadDimensions() {
      try {
        this.loading = true
        const b = encodeURIComponent(this.batchCode)
        const url = this.schoolId
          ? `${this.baseUrl}/api/v12/batch/${b}/school/${this.schoolId}`
          : `${this.baseUrl}/api/v12/batch/${b}/regional`
        const res = await axios.get(url)
        const subjects = res?.data?.data?.subjects || []
        const questionnaire = subjects.find(x => x.type === 'questionnaire' && x.subject_name === this.subjectName)
        const dims = questionnaire?.dimensions || []
        this.dimensions = dims
        this.dimensionNameMap = dims.reduce((m, d) => (m[d.code] = d.name, m), {})
        if (dims.length > 0) this.selectedDimension = dims[0].code
      } catch (e) {
        console.error('[QuestionnaireDimensionPage] 加载维度失败:', e)
        this.$message?.error?.('加载维度失败')
      } finally {
        this.loading = false
      }
    },
    reload() { /* 交由子组件根据 props 变化自行加载 */ }
  }
}
</script>

<style scoped>
.page { background: #fff; border: 1px solid #ebeef5; border-radius: 4px; }
</style>

