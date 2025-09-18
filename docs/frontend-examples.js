/**
 * 前端集成实用工具和示例代码
 * 
 * 提供完整的JavaScript/React组件示例，帮助前端开发者快速集成API
 * 支持多种前端框架：React、Vue、原生JavaScript
 * 
 * @version 1.2
 * @author 统计分析服务团队
 */

// ================================
// API 请求工具函数
// ================================

/**
 * 统一的API请求工具
 * 支持超时、重试、错误处理
 */
class ApiClient {
  constructor(baseURL = 'http://localhost:8000', timeout = 30000) {
    this.baseURL = baseURL;
    this.timeout = timeout;
  }

  /**
   * 发送HTTP请求
   * @param {string} url 请求URL
   * @param {object} options 请求选项
   * @returns {Promise<any>} 响应数据
   */
  async request(url, options = {}) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const response = await fetch(`${this.baseURL}${url}`, {
        ...options,
        headers: {
          'Accept': 'application/json; charset=utf-8',
          'Content-Type': 'application/json; charset=utf-8',
          ...options.headers
        },
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const result = await response.json();
      
      if (!result.success) {
        throw new Error(result.message || '请求失败');
      }

      return result.data;
    } catch (error) {
      clearTimeout(timeoutId);
      
      if (error.name === 'AbortError') {
        throw new Error('请求超时，请稍后重试');
      }
      
      throw error;
    }
  }

  /**
   * 获取区域数据
   * @param {string} batchCode 批次代码
   * @returns {Promise<RegionalData>}
   */
  async getRegionalData(batchCode) {
    return this.request(`/api/v12/batch/${batchCode}/regional`);
  }

  /**
   * 获取学校数据
   * @param {string} batchCode 批次代码
   * @param {string} schoolCode 学校代码
   * @returns {Promise<SchoolData>}
   */
  async getSchoolData(batchCode, schoolCode) {
    return this.request(`/api/v12/batch/${batchCode}/school/${schoolCode}`);
  }

  /**
   * 物化批次数据
   * @param {string} batchCode 批次代码
   * @returns {Promise<MaterializeResult>}
   */
  async materializeBatch(batchCode) {
    return this.request(`/api/v12/batch/${batchCode}/materialize`, {
      method: 'POST'
    });
  }
}

// 全局API客户端实例
const apiClient = new ApiClient();

// ================================
// 数据处理工具函数
// ================================

/**
 * 格式化数值显示
 * @param {number} value 数值
 * @param {number} precision 小数位数
 * @returns {string}
 */
const formatNumber = (value, precision = 2) => {
  if (typeof value !== 'number' || isNaN(value)) return '--';
  return value.toFixed(precision);
};

/**
 * 格式化百分比显示
 * @param {number} percentage 百分比 (0-100)
 * @returns {string}
 */
const formatPercentage = (percentage) => {
  if (typeof percentage !== 'number' || isNaN(percentage)) return '--';
  return `${percentage.toFixed(2)}%`;
};

/**
 * 获取满意度标签
 * @param {number} level 满意度等级
 * @param {string} scaleType 量表类型 '5_scale' | '10_scale'
 * @returns {string}
 */
const getSatisfactionLabel = (level, scaleType = '5_scale') => {
  const labels5Scale = {
    5: '非常满意', 4: '满意', 3: '一般', 2: '不满意', 1: '非常不满意'
  };
  
  const labels10Scale = {
    10: '非常满意', 9: '很满意', 8: '满意', 7: '比较满意',
    6: '略微满意', 5: '一般', 4: '略微不满意', 3: '比较不满意',
    2: '不满意', 1: '非常不满意'
  };
  
  const labelMap = scaleType === '10_scale' ? labels10Scale : labels5Scale;
  return labelMap[level] || `选项${level}`;
};

/**
 * 获取满意度颜色
 * @param {number} level 满意度等级
 * @returns {string} 颜色值
 */
const getSatisfactionColor = (level) => {
  const colors = {
    5: '#52c41a', 4: '#73d13d', 3: '#faad14', 2: '#ff7875', 1: '#f5222d'
  };
  return colors[level] || '#d9d9d9';
};

/**
 * 获取排名颜色
 * @param {number} rank 排名
 * @param {number} total 总数
 * @returns {string} 颜色值
 */
const getRankColor = (rank, total) => {
  const percentile = (total - rank + 1) / total;
  if (percentile >= 0.8) return '#52c41a';      // 绿色 - 优秀
  if (percentile >= 0.6) return '#1890ff';      // 蓝色 - 良好  
  if (percentile >= 0.4) return '#faad14';      // 橙色 - 中等
  return '#f5222d';                             // 红色 - 需改进
};

/**
 * 处理并列排名
 * @param {SchoolRanking[]} rankings 排名数组
 * @returns {SchoolRanking[]} 处理后的排名数组
 */
const processSharedRankings = (rankings) => {
  return rankings.map((school, index) => {
    const prevSchool = rankings[index - 1];
    const isShared = prevSchool && Math.abs(prevSchool.avg - school.avg) < 0.01;
    
    return {
      ...school,
      displayRank: isShared ? prevSchool.displayRank || prevSchool.rank : school.rank,
      isShared
    };
  });
};

/**
 * 计算满意度汇总
 * @param {OptionDistribution[]} distribution 选项分布
 * @returns {object} 满意度汇总
 */
const calculateSatisfactionSummary = (distribution) => {
  const satisfied = distribution
    .filter(opt => opt.option_level >= 4)
    .reduce((sum, opt) => sum + opt.pct, 0);
    
  const neutral = distribution
    .filter(opt => opt.option_level === 3)
    .reduce((sum, opt) => sum + opt.pct, 0);
    
  const dissatisfied = distribution
    .filter(opt => opt.option_level <= 2)
    .reduce((sum, opt) => sum + opt.pct, 0);
    
  return { satisfied, neutral, dissatisfied };
};

// ================================
// ECharts 图表配置
// ================================

/**
 * 创建科目雷达图配置
 * @param {ExamSubject[]} examSubjects 考试科目数据
 * @returns {object} ECharts配置
 */
const createRadarChartConfig = (examSubjects) => {
  return {
    title: {
      text: '各科目成绩雷达图',
      left: 'center'
    },
    tooltip: {
      trigger: 'item'
    },
    radar: {
      indicator: examSubjects.map(subject => ({
        name: subject.subject_name,
        max: 100,
        min: 0
      })),
      radius: '60%'
    },
    series: [{
      name: '成绩分析',
      type: 'radar',
      data: [
        {
          value: examSubjects.map(s => s.metrics.avg),
          name: '平均分',
          areaStyle: {
            opacity: 0.3,
            color: '#1890ff'
          },
          lineStyle: {
            color: '#1890ff'
          }
        },
        {
          value: examSubjects.map(s => s.metrics.difficulty * 100),
          name: '难度系数',
          areaStyle: {
            opacity: 0.2,
            color: '#52c41a'
          },
          lineStyle: {
            color: '#52c41a'
          }
        }
      ]
    }]
  };
};

/**
 * 创建学校排名柱状图配置
 * @param {SchoolRanking[]} rankings 学校排名数据
 * @param {number} topN 显示前N名
 * @returns {object} ECharts配置
 */
const createRankingBarConfig = (rankings, topN = 10) => {
  const topRankings = rankings.slice(0, topN);
  
  return {
    title: {
      text: `学校排名 TOP ${topN}`,
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      formatter: (params) => {
        const data = params[0];
        return `${data.name}<br/>平均分: ${data.value}<br/>排名: 第${data.dataIndex + 1}名`;
      }
    },
    xAxis: {
      type: 'category',
      data: topRankings.map(s => s.school_name),
      axisLabel: {
        rotate: 45,
        interval: 0
      }
    },
    yAxis: {
      type: 'value',
      name: '平均分',
      min: 0,
      max: 100
    },
    series: [{
      type: 'bar',
      data: topRankings.map((school, index) => ({
        value: school.avg,
        itemStyle: {
          color: index < 3 ? ['#FFD700', '#C0C0C0', '#CD7F32'][index] : '#1890ff'
        }
      })),
      label: {
        show: true,
        position: 'top',
        formatter: '{c}'
      }
    }]
  };
};

/**
 * 创建满意度饼图配置
 * @param {OptionDistribution[]} distribution 选项分布
 * @returns {object} ECharts配置
 */
const createSatisfactionPieConfig = (distribution) => {
  const sortedData = distribution
    .sort((a, b) => b.option_level - a.option_level)
    .map(opt => ({
      value: opt.pct,
      name: opt.option_label || getSatisfactionLabel(opt.option_level),
      itemStyle: {
        color: getSatisfactionColor(opt.option_level)
      }
    }));
  
  return {
    title: {
      text: '满意度分布',
      left: 'center'
    },
    tooltip: {
      trigger: 'item',
      formatter: '{b}: {c}% ({d}%)'
    },
    legend: {
      orient: 'vertical',
      left: 'left',
      data: sortedData.map(item => item.name)
    },
    series: [{
      type: 'pie',
      radius: ['40%', '70%'],
      center: ['60%', '50%'],
      data: sortedData,
      emphasis: {
        itemStyle: {
          shadowBlur: 10,
          shadowOffsetX: 0,
          shadowColor: 'rgba(0, 0, 0, 0.5)'
        }
      },
      label: {
        formatter: '{b}\n{c}%'
      }
    }]
  };
};

// ================================
// React 组件示例
// ================================

/**
 * 统计数据主页面组件
 */
const StatisticsPage = ({ batchCode, schoolCode }) => {
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);

  React.useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        
        const result = schoolCode 
          ? await apiClient.getSchoolData(batchCode, schoolCode)
          : await apiClient.getRegionalData(batchCode);
          
        setData(result);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [batchCode, schoolCode]);

  if (loading) {
    return (
      <div className="loading-container">
        <div className="spinner"></div>
        <p>正在加载统计数据...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="error-container">
        <div className="error-message">
          <h3>数据加载失败</h3>
          <p>{error}</p>
          <button onClick={() => window.location.reload()}>重新加载</button>
        </div>
      </div>
    );
  }

  const examSubjects = data.subjects.filter(s => s.type === 'exam');
  const questionnaireSubjects = data.subjects.filter(s => s.type === 'questionnaire');

  return (
    <div className="statistics-page">
      <div className="page-header">
        <h1>{data.batch_code} 统计数据</h1>
        <div className="data-info">
          <span>数据版本: {data.schema_version}</span>
          <span>层级: {data.aggregation_level}</span>
          {data.school_code && <span>学校: {data.school_code}</span>}
        </div>
      </div>

      <div className="content-tabs">
        <div className="tab-nav">
          <button className="tab-btn active">考试成绩</button>
          <button className="tab-btn">问卷调查</button>
        </div>

        <div className="tab-content">
          <ExamSubjectsPanel subjects={examSubjects} />
          <QuestionnaireSubjectsPanel subjects={questionnaireSubjects} />
        </div>
      </div>
    </div>
  );
};

/**
 * 考试科目面板组件
 */
const ExamSubjectsPanel = ({ subjects }) => {
  return (
    <div className="exam-subjects-panel">
      <div className="subjects-overview">
        <div className="metrics-cards">
          {subjects.map((subject, index) => (
            <SubjectCard key={index} subject={subject} />
          ))}
        </div>
      </div>

      <div className="charts-section">
        <div className="chart-container">
          <RadarChart subjects={subjects} />
        </div>
        
        {subjects[0]?.school_rankings && (
          <div className="chart-container">
            <RankingChart rankings={subjects[0].school_rankings} />
          </div>
        )}
      </div>
    </div>
  );
};

/**
 * 科目卡片组件
 */
const SubjectCard = ({ subject }) => {
  const { subject_name, type, metrics, region_rank, total_schools } = subject;
  
  return (
    <div className="subject-card">
      <div className="card-header">
        <h3>{subject_name}</h3>
        <span className={`type-badge ${type}`}>
          {type === 'exam' ? '考试' : '问卷'}
        </span>
      </div>
      
      <div className="card-body">
        <div className="main-metric">
          <span className="value">{formatNumber(metrics.avg)}</span>
          <span className="label">平均分</span>
        </div>
        
        <div className="sub-metrics">
          <div className="metric-item">
            <span className="label">难度系数</span>
            <span className="value">{formatNumber(metrics.difficulty)}</span>
          </div>
          <div className="metric-item">
            <span className="label">标准差</span>
            <span className="value">{formatNumber(metrics.stddev)}</span>
          </div>
        </div>
        
        {region_rank && total_schools && (
          <div className="rank-info">
            <span className="rank-text">
              区域排名: 第{region_rank}名 / {total_schools}校
            </span>
            <div 
              className="rank-bar"
              style={{
                backgroundColor: getRankColor(region_rank, total_schools),
                width: `${((total_schools - region_rank + 1) / total_schools) * 100}%`
              }}
            />
          </div>
        )}
      </div>
    </div>
  );
};

/**
 * 雷达图组件
 */
const RadarChart = ({ subjects }) => {
  const chartRef = React.useRef(null);
  const chartInstanceRef = React.useRef(null);

  React.useEffect(() => {
    if (!chartRef.current || subjects.length === 0) return;

    // 初始化图表
    if (!chartInstanceRef.current) {
      chartInstanceRef.current = echarts.init(chartRef.current);
    }

    const option = createRadarChartConfig(subjects);
    chartInstanceRef.current.setOption(option);

    // 清理函数
    return () => {
      if (chartInstanceRef.current) {
        chartInstanceRef.current.dispose();
        chartInstanceRef.current = null;
      }
    };
  }, [subjects]);

  return <div ref={chartRef} className="chart" style={{ height: '400px' }} />;
};

/**
 * 排名图表组件
 */
const RankingChart = ({ rankings }) => {
  const chartRef = React.useRef(null);
  const chartInstanceRef = React.useRef(null);

  React.useEffect(() => {
    if (!chartRef.current || rankings.length === 0) return;

    if (!chartInstanceRef.current) {
      chartInstanceRef.current = echarts.init(chartRef.current);
    }

    const option = createRankingBarConfig(rankings);
    chartInstanceRef.current.setOption(option);

    return () => {
      if (chartInstanceRef.current) {
        chartInstanceRef.current.dispose();
        chartInstanceRef.current = null;
      }
    };
  }, [rankings]);

  return <div ref={chartRef} className="chart" style={{ height: '400px' }} />;
};

/**
 * 问卷科目面板组件
 */
const QuestionnaireSubjectsPanel = ({ subjects }) => {
  return (
    <div className="questionnaire-subjects-panel">
      {subjects.map((subject, index) => (
        <QuestionnaireSubjectCard key={index} subject={subject} />
      ))}
    </div>
  );
};

/**
 * 问卷科目卡片组件
 */
const QuestionnaireSubjectCard = ({ subject }) => {
  return (
    <div className="questionnaire-card">
      <div className="card-header">
        <h3>{subject.subject_name}</h3>
      </div>
      
      <div className="card-body">
        <div className="basic-info">
          <SubjectCard subject={subject} />
        </div>
        
        <div className="dimensions-section">
          <h4>维度分析</h4>
          {subject.dimensions.map((dimension, index) => (
            <DimensionCard key={index} dimension={dimension} />
          ))}
        </div>
      </div>
    </div>
  );
};

/**
 * 维度卡片组件
 */
const DimensionCard = ({ dimension }) => {
  const { name, avg, score_rate, option_distribution } = dimension;
  
  return (
    <div className="dimension-card">
      <div className="dimension-header">
        <h5>{name}</h5>
        <div className="dimension-metrics">
          <span>平均分: {formatNumber(avg)}</span>
          <span>得分率: {formatPercentage(score_rate)}</span>
        </div>
      </div>
      
      {option_distribution && (
        <div className="distribution-chart">
          <SatisfactionChart distribution={option_distribution} />
        </div>
      )}
    </div>
  );
};

/**
 * 满意度图表组件
 */
const SatisfactionChart = ({ distribution }) => {
  const chartRef = React.useRef(null);
  const chartInstanceRef = React.useRef(null);

  React.useEffect(() => {
    if (!chartRef.current || !distribution || distribution.length === 0) return;

    if (!chartInstanceRef.current) {
      chartInstanceRef.current = echarts.init(chartRef.current);
    }

    const option = createSatisfactionPieConfig(distribution);
    chartInstanceRef.current.setOption(option);

    return () => {
      if (chartInstanceRef.current) {
        chartInstanceRef.current.dispose();
        chartInstanceRef.current = null;
      }
    };
  }, [distribution]);

  return <div ref={chartRef} className="chart" style={{ height: '300px' }} />;
};

// ================================
// Vue.js 组件示例 (Vue 3 Composition API)
// ================================

const VueStatisticsPage = {
  setup(props) {
    const data = Vue.ref(null);
    const loading = Vue.ref(true);
    const error = Vue.ref(null);

    const fetchData = async () => {
      try {
        loading.value = true;
        error.value = null;
        
        const result = props.schoolCode 
          ? await apiClient.getSchoolData(props.batchCode, props.schoolCode)
          : await apiClient.getRegionalData(props.batchCode);
          
        data.value = result;
      } catch (err) {
        error.value = err.message;
      } finally {
        loading.value = false;
      }
    };

    Vue.onMounted(fetchData);
    Vue.watch(() => [props.batchCode, props.schoolCode], fetchData);

    const examSubjects = Vue.computed(() => 
      data.value ? data.value.subjects.filter(s => s.type === 'exam') : []
    );

    const questionnaireSubjects = Vue.computed(() => 
      data.value ? data.value.subjects.filter(s => s.type === 'questionnaire') : []
    );

    return {
      data,
      loading,
      error,
      examSubjects,
      questionnaireSubjects,
      fetchData
    };
  },

  template: `
    <div class="statistics-page" v-if="!loading && !error">
      <div class="page-header">
        <h1>{{ data.batch_code }} 统计数据</h1>
        <div class="data-info">
          <span>数据版本: {{ data.schema_version }}</span>
          <span>层级: {{ data.aggregation_level }}</span>
          <span v-if="data.school_code">学校: {{ data.school_code }}</span>
        </div>
      </div>

      <div class="content-section">
        <div v-for="subject in examSubjects" :key="subject.subject_name" class="subject-section">
          <h2>{{ subject.subject_name }}</h2>
          <div class="metrics">
            <span>平均分: {{ formatNumber(subject.metrics.avg) }}</span>
            <span>难度系数: {{ formatNumber(subject.metrics.difficulty) }}</span>
          </div>
        </div>
      </div>
    </div>
    
    <div v-else-if="loading" class="loading">
      <p>正在加载...</p>
    </div>
    
    <div v-else-if="error" class="error">
      <p>{{ error }}</p>
      <button @click="fetchData">重试</button>
    </div>
  `
};

// ================================
// 原生 JavaScript 示例
// ================================

/**
 * 原生JavaScript实现的统计页面
 */
class StatisticsPageVanilla {
  constructor(containerId, batchCode, schoolCode = null) {
    this.container = document.getElementById(containerId);
    this.batchCode = batchCode;
    this.schoolCode = schoolCode;
    this.data = null;
    
    this.init();
  }

  async init() {
    this.showLoading();
    
    try {
      this.data = this.schoolCode 
        ? await apiClient.getSchoolData(this.batchCode, this.schoolCode)
        : await apiClient.getRegionalData(this.batchCode);
      
      this.render();
    } catch (error) {
      this.showError(error.message);
    }
  }

  showLoading() {
    this.container.innerHTML = `
      <div class="loading-container">
        <div class="spinner"></div>
        <p>正在加载统计数据...</p>
      </div>
    `;
  }

  showError(message) {
    this.container.innerHTML = `
      <div class="error-container">
        <h3>数据加载失败</h3>
        <p>${message}</p>
        <button onclick="location.reload()">重新加载</button>
      </div>
    `;
  }

  render() {
    if (!this.data) return;

    const examSubjects = this.data.subjects.filter(s => s.type === 'exam');
    const questionnaireSubjects = this.data.subjects.filter(s => s.type === 'questionnaire');

    this.container.innerHTML = `
      <div class="statistics-page">
        <div class="page-header">
          <h1>${this.data.batch_code} 统计数据</h1>
          <div class="data-info">
            <span>数据版本: ${this.data.schema_version}</span>
            <span>层级: ${this.data.aggregation_level}</span>
            ${this.data.school_code ? `<span>学校: ${this.data.school_code}</span>` : ''}
          </div>
        </div>

        <div class="subjects-section">
          <h2>考试科目 (${examSubjects.length})</h2>
          <div class="subjects-grid">
            ${examSubjects.map(subject => this.renderSubjectCard(subject)).join('')}
          </div>
        </div>

        <div class="questionnaires-section">
          <h2>问卷调查 (${questionnaireSubjects.length})</h2>
          <div class="subjects-grid">
            ${questionnaireSubjects.map(subject => this.renderSubjectCard(subject)).join('')}
          </div>
        </div>
      </div>
    `;

    // 初始化图表
    this.initCharts();
  }

  renderSubjectCard(subject) {
    const rankInfo = subject.region_rank && subject.total_schools 
      ? `<div class="rank-info">排名: ${subject.region_rank}/${subject.total_schools}</div>`
      : '';

    return `
      <div class="subject-card">
        <div class="card-header">
          <h3>${subject.subject_name}</h3>
          <span class="type-badge ${subject.type}">${subject.type === 'exam' ? '考试' : '问卷'}</span>
        </div>
        <div class="card-body">
          <div class="main-metric">
            <span class="value">${formatNumber(subject.metrics.avg)}</span>
            <span class="label">平均分</span>
          </div>
          <div class="sub-metrics">
            <div>难度系数: ${formatNumber(subject.metrics.difficulty)}</div>
            <div>标准差: ${formatNumber(subject.metrics.stddev)}</div>
          </div>
          ${rankInfo}
        </div>
      </div>
    `;
  }

  initCharts() {
    // 这里可以初始化各种图表
    const examSubjects = this.data.subjects.filter(s => s.type === 'exam');
    if (examSubjects.length > 0) {
      this.initRadarChart(examSubjects);
    }
  }

  initRadarChart(subjects) {
    const chartContainer = document.createElement('div');
    chartContainer.id = 'radar-chart';
    chartContainer.style.height = '400px';
    this.container.appendChild(chartContainer);

    const chart = echarts.init(chartContainer);
    const option = createRadarChartConfig(subjects);
    chart.setOption(option);
  }
}

// ================================
// CSS 样式建议
// ================================

const recommendedCSS = `
/* 基础样式 */
.statistics-page {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  line-height: 1.6;
  color: #333;
}

.page-header {
  padding: 24px 0;
  border-bottom: 1px solid #e8e8e8;
  margin-bottom: 24px;
}

.page-header h1 {
  margin: 0 0 8px 0;
  font-size: 28px;
  font-weight: 600;
}

.data-info {
  display: flex;
  gap: 16px;
  font-size: 14px;
  color: #666;
}

/* 加载和错误状态 */
.loading-container {
  text-align: center;
  padding: 60px 20px;
}

.spinner {
  width: 40px;
  height: 40px;
  border: 4px solid #f3f3f3;
  border-top: 4px solid #1890ff;
  border-radius: 50%;
  animation: spin 1s linear infinite;
  margin: 0 auto 16px;
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

.error-container {
  text-align: center;
  padding: 60px 20px;
  color: #f5222d;
}

/* 科目卡片 */
.subjects-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 16px;
  margin-bottom: 32px;
}

.subject-card {
  background: #fff;
  border: 1px solid #e8e8e8;
  border-radius: 8px;
  padding: 16px;
  transition: box-shadow 0.3s;
}

.subject-card:hover {
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.card-header h3 {
  margin: 0;
  font-size: 18px;
  font-weight: 500;
}

.type-badge {
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 500;
}

.type-badge.exam {
  background: #e6f7ff;
  color: #1890ff;
}

.type-badge.questionnaire {
  background: #f6ffed;
  color: #52c41a;
}

.main-metric {
  text-align: center;
  margin-bottom: 16px;
}

.main-metric .value {
  display: block;
  font-size: 32px;
  font-weight: 600;
  color: #1890ff;
}

.main-metric .label {
  font-size: 14px;
  color: #666;
}

.sub-metrics {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
  font-size: 14px;
}

.rank-info {
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid #e8e8e8;
}

.rank-bar {
  height: 4px;
  background: #52c41a;
  border-radius: 2px;
  margin-top: 4px;
}

/* 图表容器 */
.chart {
  background: #fff;
  border: 1px solid #e8e8e8;
  border-radius: 8px;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .subjects-grid {
    grid-template-columns: 1fr;
  }
  
  .data-info {
    flex-direction: column;
    gap: 8px;
  }
  
  .main-metric .value {
    font-size: 24px;
  }
}
`;

// ================================
// 导出工具
// ================================

if (typeof module !== 'undefined' && module.exports) {
  // Node.js 环境
  module.exports = {
    ApiClient,
    formatNumber,
    formatPercentage,
    getSatisfactionLabel,
    getSatisfactionColor,
    getRankColor,
    processSharedRankings,
    calculateSatisfactionSummary,
    createRadarChartConfig,
    createRankingBarConfig,
    createSatisfactionPieConfig,
    StatisticsPageVanilla,
    recommendedCSS
  };
} else {
  // 浏览器环境
  window.StatisticsAPI = {
    ApiClient,
    formatNumber,
    formatPercentage,
    getSatisfactionLabel,
    getSatisfactionColor,
    getRankColor,
    processSharedRankings,
    calculateSatisfactionSummary,
    createRadarChartConfig,
    createRankingBarConfig,
    createSatisfactionPieConfig,
    StatisticsPageVanilla,
    recommendedCSS
  };
}