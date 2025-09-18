/**
 * v1.2 API TypeScript 接口定义
 * 
 * 为前端开发提供完整的类型支持，确保类型安全和智能提示
 * 
 * @version 1.2
 * @author 统计分析服务团队
 */

// ================================
// 基础类型定义
// ================================

/** API 响应基础结构 */
interface ApiResponse<T = any> {
  /** 请求是否成功 */
  success: boolean;
  /** 响应消息 */
  message: string;
  /** 响应数据 */
  data: T;
  /** HTTP状态码 */
  code: number;
}

/** 统计指标 */
interface Metrics {
  /** 平均分 (两位小数) */
  avg: number;
  /** 标准差 (两位小数) */
  stddev: number;
  /** 最高分 (两位小数) */
  max: number;
  /** 最低分 (两位小数) */
  min: number;
  /** 难度系数 0-1 (两位小数) */
  difficulty: number;
}

/** 学校排名信息 */
interface SchoolRanking {
  /** 学校代码 */
  school_code: string;
  /** 学校名称 */
  school_name: string;
  /** 平均分 (两位小数) */
  avg: number;
  /** 排名 (1-based) */
  rank: number;
}

/** 选项分布 (问卷专用) */
interface OptionDistribution {
  /** 选项等级 (1-5 或 1-10) */
  option_level: number;
  /** 选项标签 (可选，如"非常满意") */
  option_label?: string;
  /** 百分比 0-100 (两位小数) */
  pct: number;
}

// ================================
// 维度相关类型
// ================================

/** 考试维度基础结构 */
interface BaseDimension {
  /** 维度代码 */
  code: string;
  /** 维度名称 */
  name: string;
  /** 平均分 (两位小数) */
  avg: number;
  /** 得分率 0-100 (两位小数) */
  score_rate: number;
  /** 排名 (可选，学校层/区域层可能有) */
  rank?: number;
  /** 区域均分(学校维度对标，可选) */
  regional_avg?: number;
}

/** 考试维度 */
interface ExamDimension extends BaseDimension {
  /** 满分 (考试维度特有) */
  max_score?: number;
}

/** 问卷维度 */
interface QuestionnaireDimension extends BaseDimension {
  /** 选项分布 (问卷维度特有) */
  option_distribution?: OptionDistribution[];
  /** 维度下的题目选项分布（可选，方案B增强） */
  questions?: QuestionData[];
}

// ================================
// 题目相关类型
// ================================

/** 问卷题目数据 */
interface QuestionData {
  /** 题目ID */
  question_id: string;
  /** 选项分布 */
  option_distribution: OptionDistribution[];
}

// ================================
// 科目相关类型
// ================================

/** 科目基础结构 */
interface BaseSubject {
  /** 科目/问卷名称 */
  subject_name: string;
  /** 统计指标 */
  metrics: Metrics;
  
  // 区域层特有字段
  /** 学校排名列表 (区域层) */
  school_rankings?: SchoolRanking[];
  
  // 学校层特有字段
  /** 在区域内的排名 (学校层) */
  region_rank?: number;
  /** 参与排名的学校总数 (学校层) */
  total_schools?: number;
}

/** 考试科目 */
interface ExamSubject extends BaseSubject {
  /** 科目类型：考试 */
  type: "exam";
  /** 维度数据 */
  dimensions: ExamDimension[];
}

/** 问卷科目 */
interface QuestionnaireSubject extends BaseSubject {
  /** 科目类型：问卷 */
  type: "questionnaire";
  /** 维度数据 */
  dimensions: QuestionnaireDimension[];
  /** 题目级数据 (可选) */
  questions?: QuestionData[];
}

/** 科目联合类型 */
type Subject = ExamSubject | QuestionnaireSubject;

// ================================
// 数据层级类型
// ================================

/** 数据基础结构 */
interface BaseData {
  /** 数据格式版本 */
  schema_version: string;
  /** 批次代码 */
  batch_code: string;
  /** 科目列表 */
  subjects: Subject[];
}

/** 区域层数据 */
interface RegionalData extends BaseData {
  /** 聚合层级：区域 */
  aggregation_level: "REGIONAL";
}

/** 学校层数据 */
interface SchoolData extends BaseData {
  /** 聚合层级：学校 */
  aggregation_level: "SCHOOL";
  /** 学校代码 */
  school_code: string;
}

// ================================
// API 响应类型
// ================================

/** 区域数据API响应 */
type RegionalResponse = ApiResponse<RegionalData>;

/** 学校数据API响应 */
type SchoolResponse = ApiResponse<SchoolData>;

/** 批次物化结果 */
interface MaterializeResult {
  /** 批次代码 */
  batch_code: string;
  /** 已物化的学校数量 */
  schools_materialized: number;
}

/** 批次物化API响应 */
type MaterializeResponse = ApiResponse<MaterializeResult>;

// ================================
// 工具类型
// ================================

/** 提取考试科目类型 */
type ExamSubjects = Extract<Subject, { type: "exam" }>;

/** 提取问卷科目类型 */
type QuestionnaireSubjects = Extract<Subject, { type: "questionnaire" }>;

/** 聚合层级类型 */
type AggregationLevel = "REGIONAL" | "SCHOOL";

/** 科目类型 */
type SubjectType = "exam" | "questionnaire";

// ================================
// 前端组件Props类型
// ================================

/** 统计页面组件Props */
interface StatisticsPageProps {
  /** 批次代码 */
  batchCode: string;
  /** 学校代码 (可选，区域层时不传) */
  schoolCode?: string;
}

/** 科目卡片组件Props */
interface SubjectCardProps {
  /** 科目数据 */
  subject: Subject;
  /** 是否显示排名信息 */
  showRank?: boolean;
  /** 点击回调 */
  onClick?: (subject: Subject) => void;
}

/** 排名表格组件Props */
interface RankingTableProps {
  /** 学校排名数据 */
  rankings: SchoolRanking[];
  /** 表格高度 */
  height?: number;
  /** 是否显示分页 */
  showPagination?: boolean;
}

/** 雷达图组件Props */
interface RadarChartProps {
  /** 考试科目数据 */
  examSubjects: ExamSubjects[];
  /** 图表高度 */
  height?: number;
  /** 图表配置选项 */
  options?: any;
}

/** 满意度图表组件Props */
interface SatisfactionChartProps {
  /** 选项分布数据 */
  distribution: OptionDistribution[];
  /** 图表类型 */
  chartType?: 'pie' | 'bar' | 'doughnut';
  /** 图表高度 */
  height?: number;
}

// ================================
// 错误处理类型
// ================================

/** API错误信息 */
interface ApiError {
  /** 错误消息 */
  message: string;
  /** 错误代码 */
  code?: string | number;
  /** 错误详情 */
  details?: any;
}

/** 错误边界组件Props */
interface ErrorBoundaryProps {
  /** 错误信息 */
  error: ApiError;
  /** 重试回调 */
  onRetry?: () => void;
  /** 返回首页回调 */
  onGoHome?: () => void;
}

// ================================
// 状态管理类型
// ================================

/** 加载状态 */
type LoadingState = 'idle' | 'loading' | 'success' | 'error';

/** 统计数据状态 */
interface StatisticsState {
  /** 加载状态 */
  status: LoadingState;
  /** 区域数据 */
  regionalData: RegionalData | null;
  /** 学校数据映射 */
  schoolDataMap: Map<string, SchoolData>;
  /** 错误信息 */
  error: ApiError | null;
  /** 最后更新时间 */
  lastUpdated: Date | null;
}

// ================================
// 筛选和排序类型
// ================================

/** 排序方向 */
type SortDirection = 'asc' | 'desc';

/** 排序字段 */
type SortField = 'avg' | 'rank' | 'difficulty' | 'school_name';

/** 排序配置 */
interface SortConfig {
  /** 排序字段 */
  field: SortField;
  /** 排序方向 */
  direction: SortDirection;
}

/** 筛选配置 */
interface FilterConfig {
  /** 科目类型筛选 */
  subjectType?: SubjectType[];
  /** 分数范围筛选 */
  scoreRange?: [number, number];
  /** 学校名称搜索 */
  schoolNameKeyword?: string;
}

// ================================
// 图表配置类型
// ================================

/** ECharts 配置类型 (简化) */
interface ChartOption {
  title?: {
    text?: string;
    subtext?: string;
  };
  tooltip?: any;
  legend?: any;
  xAxis?: any;
  yAxis?: any;
  series?: any[];
  color?: string[];
  [key: string]: any;
}

/** 图表数据点 */
interface ChartDataPoint {
  /** 名称 */
  name: string;
  /** 数值 */
  value: number;
  /** 额外属性 */
  [key: string]: any;
}

/** 雷达图数据 */
interface RadarData {
  /** 指示器配置 */
  indicators: Array<{
    name: string;
    max: number;
    min?: number;
  }>;
  /** 数据系列 */
  series: Array<{
    name: string;
    value: number[];
    [key: string]: any;
  }>;
}

// ================================
// 导出所有类型
// ================================

export type {
  // 基础类型
  ApiResponse,
  Metrics,
  SchoolRanking,
  OptionDistribution,
  
  // 维度类型
  BaseDimension,
  ExamDimension,
  QuestionnaireDimension,
  
  // 题目类型
  QuestionData,
  
  // 科目类型
  BaseSubject,
  ExamSubject,
  QuestionnaireSubject,
  Subject,
  ExamSubjects,
  QuestionnaireSubjects,
  
  // 数据层级类型
  BaseData,
  RegionalData,
  SchoolData,
  
  // API响应类型
  RegionalResponse,
  SchoolResponse,
  MaterializeResult,
  MaterializeResponse,
  
  // 工具类型
  AggregationLevel,
  SubjectType,
  
  // 组件Props类型
  StatisticsPageProps,
  SubjectCardProps,
  RankingTableProps,
  RadarChartProps,
  SatisfactionChartProps,
  
  // 错误处理类型
  ApiError,
  ErrorBoundaryProps,
  
  // 状态管理类型
  LoadingState,
  StatisticsState,
  
  // 筛选排序类型
  SortDirection,
  SortField,
  SortConfig,
  FilterConfig,
  
  // 图表类型
  ChartOption,
  ChartDataPoint,
  RadarData,
};

// ================================
// 类型守卫函数
// ================================

/** 检查是否为考试科目 */
export const isExamSubject = (subject: Subject): subject is ExamSubject => {
  return subject.type === 'exam';
};

/** 检查是否为问卷科目 */
export const isQuestionnaireSubject = (subject: Subject): subject is QuestionnaireSubject => {
  return subject.type === 'questionnaire';
};

/** 检查是否为区域数据 */
export const isRegionalData = (data: RegionalData | SchoolData): data is RegionalData => {
  return data.aggregation_level === 'REGIONAL';
};

/** 检查是否为学校数据 */
export const isSchoolData = (data: RegionalData | SchoolData): data is SchoolData => {
  return data.aggregation_level === 'SCHOOL';
};

/** 检查API响应是否成功 */
export const isSuccessResponse = <T>(response: ApiResponse<T>): response is ApiResponse<T> & { success: true } => {
  return response.success === true;
};

// ================================
// 常量定义
// ================================

/** 支持的数据版本 */
export const SUPPORTED_SCHEMA_VERSIONS = ['v1.2'] as const;

/** 科目类型映射 */
export const SUBJECT_TYPE_LABELS = {
  exam: '考试',
  questionnaire: '问卷'
} as const;

/** 满意度等级标签 (5级量表) */
export const SATISFACTION_LABELS_5_SCALE = {
  5: '非常满意',
  4: '满意', 
  3: '一般',
  2: '不满意',
  1: '非常不满意'
} as const;

/** 满意度等级标签 (10级量表) */
export const SATISFACTION_LABELS_10_SCALE = {
  10: '非常满意', 9: '很满意',
  8: '满意', 7: '比较满意',
  6: '略微满意', 5: '一般',
  4: '略微不满意', 3: '比较不满意',
  2: '不满意', 1: '非常不满意'
} as const;

/** 默认图表颜色 */
export const DEFAULT_CHART_COLORS = [
  '#1890ff', '#52c41a', '#faad14', '#f5222d',
  '#722ed1', '#fa8c16', '#13c2c2', '#eb2f96',
  '#a0d911', '#fadb14'
] as const;

/** 满意度颜色映射 */
export const SATISFACTION_COLORS = {
  5: '#52c41a',    // 非常满意 - 绿色
  4: '#73d13d',    // 满意 - 浅绿
  3: '#faad14',    // 一般 - 橙色
  2: '#ff7875',    // 不满意 - 浅红
  1: '#f5222d'     // 很不满意 - 红色
} as const;
