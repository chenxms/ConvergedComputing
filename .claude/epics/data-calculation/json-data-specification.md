# 数据汇聚JSON格式规范文档

> **版本**: v1.0  
> **创建时间**: 2025-09-04  
> **适用范围**: data-calculation统计汇聚功能

## 1. 数据库字段定义

### 1.1 regional_statistics_summary 表

```sql
CREATE TABLE `regional_statistics_summary` (
  `batch_code` VARCHAR(50) NOT NULL COMMENT '批次代码',
  `statistics_data` JSON NOT NULL COMMENT '区域级统计数据',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`batch_code`),
  INDEX `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='区域统计汇聚表';
```

### 1.2 school_statistics_summary 表

```sql
CREATE TABLE `school_statistics_summary` (
  `batch_code` VARCHAR(50) NOT NULL COMMENT '批次代码',
  `school_id` VARCHAR(50) NOT NULL COMMENT '学校ID',
  `school_name` VARCHAR(100) NOT NULL COMMENT '学校名称',
  `statistics_data` JSON NOT NULL COMMENT '学校级统计数据',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`batch_code`, `school_id`),
  INDEX `idx_school_id` (`school_id`),
  INDEX `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='学校统计汇聚表';
```

### 1.3 statistics_task_status 表

```sql
CREATE TABLE `statistics_task_status` (
  `task_id` VARCHAR(36) NOT NULL COMMENT '任务ID (UUID)',
  `batch_code` VARCHAR(50) NOT NULL COMMENT '批次代码',
  `status` ENUM('pending', 'processing', 'completed', 'failed') NOT NULL DEFAULT 'pending' COMMENT '任务状态',
  `progress` DECIMAL(5,2) DEFAULT 0.00 COMMENT '进度百分比 (0.00-100.00)',
  `error_message` TEXT COMMENT '错误信息',
  `started_at` DATETIME COMMENT '开始时间',
  `completed_at` DATETIME COMMENT '完成时间',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`task_id`),
  INDEX `idx_batch_code` (`batch_code`),
  INDEX `idx_status` (`status`),
  INDEX `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='统计任务状态表';
```

## 2. JSON数据结构规范

### 2.1 区域级统计数据 (regional_statistics_summary.statistics_data)

```json
{
  "batch_info": {
    "batch_code": "BATCH_2025_001",           // 批次代码，唯一标识
    "grade_level": "初中",                    // 学段：小学(1-6th_grade)/初中(7-9th_grade)（影响等级分布阈值）
    "total_schools": 25,                      // 参与此批次的学校总数
    "total_students": 8500,                   // 参与此批次的学生总数
    "calculation_time": "2025-09-04T18:30:00Z"  // 统计计算完成时间
  },
  "academic_subjects": {
    "数学": {
      "subject_id": "MATH_001",               // 科目唯一标识ID
      "subject_type": "考试类",               // 科目类型：考试类/人机交互类/问卷类
      "total_score": 100,                     // 科目满分，从subject_question_config.max_score汇总
      "regional_stats": {
        "avg_score": 78.5,                   // 全区该科目平均分
        "score_rate": 0.785,                 // 全区得分率 = 平均分/满分 = 78.5/100
        "difficulty": 0.785,                 // 难度系数 = 全区平均分/满分，0-1之间，越高越简单
        "discrimination": 0.65,              // 区分度 = (前27%学生平均分-后27%学生平均分)/满分，0-1之间，越高区分能力越强
        "std_dev": 12.3,                     // 标准差，反映分数离散程度
        "max_score": 98,                     // 全区该科目最高分
        "min_score": 32                      // 全区该科目最低分
      },
      "grade_distribution": {                 // 等级分布统计，基于学生个人得分率划分
        "excellent": {                       // 优秀等级：初中(7-9th_grade)≥80%，小学(1-6th_grade)≥85%
          "count": 2550,                     // 优秀等级学生人数
          "percentage": 0.30                 // 优秀等级学生占比 = 2550/8500
        },
        "good": {                           // 良好等级：初中(7-9th_grade)70%-80%，小学(1-6th_grade)70%-85%
          "count": 3400,                     // 良好等级学生人数
          "percentage": 0.40                 // 良好等级学生占比
        },
        "pass": {                           // 及格等级：60%-70%（1-9th_grade统一标准）
          "count": 1700,                     // 及格等级学生人数
          "percentage": 0.20                 // 及格等级学生占比
        },
        "fail": {                           // 不及格等级：<60%（1-9th_grade统一标准）
          "count": 850,                      // 不及格等级学生人数
          "percentage": 0.10                 // 不及格等级学生占比
        }
      },
      "school_rankings": [                     // 学校在该科目的全区排名列表（按平均分降序）
        {
          "school_id": "SCH_001",             // 学校唯一标识
          "school_name": "第一中学",          // 学校名称
          "avg_score": 85.2,                 // 该学校该科目平均分
          "score_rate": 0.852,               // 该学校该科目得分率
          "ranking": 1                       // 该学校在全区的排名（1=第一名）
        }
        // ... 其他学校排名数据
      ],
      "dimensions": {                         // 该科目下的能力维度统计
        "数学运算": {                         // 维度名称（从batch_dimension_definition获取）
          "dimension_id": "MATH_CALC",       // 维度唯一标识
          "dimension_name": "数学运算",      // 维度显示名称
          "total_score": 40,                 // 维度满分，从该维度下所有题目max_score汇总
          "avg_score": 32.5,                 // 全区该维度平均分
          "score_rate": 0.8125,              // 全区该维度得分率 = 32.5/40
          "regional_ranking_avg": 0.8125     // 区域平均得分率（供学校对比使用）
        },
        "逻辑推理": {
          "dimension_id": "MATH_LOGIC",
          "dimension_name": "逻辑推理", 
          "total_score": 35,                 // 逻辑推理维度满分
          "avg_score": 26.8,                 // 全区逻辑推理平均分
          "score_rate": 0.766,               // 全区逻辑推理得分率 = 26.8/35
          "regional_ranking_avg": 0.766      // 区域平均得分率
        }
      }
    },
    "语文": {
      "subject_id": "CHINESE_001",
      "subject_type": "考试类",
      "total_score": 120,
      "regional_stats": {
        "avg_score": 92.4,
        "score_rate": 0.77,
        "difficulty": 0.77,
        "discrimination": 0.58,
        "std_dev": 15.2,
        "max_score": 118,
        "min_score": 45
      },
      "grade_distribution": {
        "excellent": {
          "count": 2210,
          "percentage": 0.26
        },
        "good": {
          "count": 3570,
          "percentage": 0.42
        },
        "pass": {
          "count": 2125,
          "percentage": 0.25
        },
        "fail": {
          "count": 595,
          "percentage": 0.07
        }
      },
      "school_rankings": [
        {
          "school_id": "SCH_003", 
          "school_name": "第三中学",
          "avg_score": 98.7,
          "score_rate": 0.822,
          "ranking": 1
        }
      ],
      "dimensions": {
        "阅读理解": {
          "dimension_id": "CN_READ",
          "dimension_name": "阅读理解",
          "total_score": 50,
          "avg_score": 38.2,
          "score_rate": 0.764,
          "regional_ranking_avg": 0.764
        }
      }
    }
  },
  "non_academic_subjects": {                  // 非学业类科目统计
    "创新思维": {
      "subject_id": "INNOVATION_001",         // 非学业科目唯一标识
      "subject_type": "问卷类",               // 科目类型：问卷类（需要处理正向/反向量表）
      "total_schools_participated": 23,      // 参与该科目测评的学校数量
      "total_students_participated": 7890,   // 参与该科目测评的学生数量
      "dimensions": {
        "好奇心": {                           // 问卷类维度名称
          "dimension_id": "CURIOSITY",       // 维度唯一标识
          "dimension_name": "好奇心",        // 维度显示名称
          "total_score": 25,                 // 好奇心维度满分（基于量表满分）
          "avg_score": 20.5,                 // 全区好奇心维度平均分
          "score_rate": 0.82,                // 好奇心维度得分率 = 20.5/25
          "question_analysis": [             // 该维度下每道题目的选项分布分析
            {
              "question_id": "Q001",         // 题目唯一标识
              "question_text": "我经常对新事物感到好奇",  // 题目内容
              "scale_type": "正向",          // 量表类型：正向量表（分数越高越好）或反向量表
              "option_distribution": {       // 各选项的选择频率统计
                "非常同意": {
                  "count": 3156,             // 选择"非常同意"的学生数量
                  "percentage": 0.40         // 选择"非常同意"的学生比例 = 3156/7890
                },
                "同意": {
                  "count": 2841,             // 选择"同意"的学生数量
                  "percentage": 0.36         // 选择"同意"的学生比例
                },
                "中立": {
                  "count": 1262,             // 选择"中立"的学生数量
                  "percentage": 0.16         // 选择"中立"的学生比例
                },
                "不同意": {
                  "count": 473,              // 选择"不同意"的学生数量
                  "percentage": 0.06         // 选择"不同意"的学生比例
                },
                "非常不同意": {
                  "count": 158,              // 选择"非常不同意"的学生数量
                  "percentage": 0.02         // 选择"非常不同意"的学生比例
                }
              }
            }
          ]
        }
      }
    },
    "科学探究": {
      "subject_id": "SCIENCE_INQUIRY_001",   // 人机交互类科目唯一标识
      "subject_type": "人机交互类",           // 科目类型：人机交互类（计算机评测）
      "total_schools_participated": 25,      // 参与该科目的学校总数
      "total_students_participated": 8500,   // 参与该科目的学生总数
      "regional_stats": {                    // 人机交互类科目的区域统计
        "avg_score": 45.2,                  // 全区该科目平均分
        "score_rate": 0.752,                // 全区得分率 = 45.2/60
        "total_score": 60,                  // 该科目满分
        "std_dev": 8.7                      // 标准差
      },
      "dimensions": {                        // 人机交互类科目的子能力维度
        "观察能力": {
          "dimension_id": "OBSERVE",        // 观察能力维度ID
          "dimension_name": "观察能力",     // 维度显示名称
          "total_score": 20,                // 观察能力维度满分
          "avg_score": 15.1,                // 全区观察能力平均分
          "score_rate": 0.755,              // 观察能力得分率 = 15.1/20
          "regional_ranking_avg": 0.755     // 区域平均得分率（供学校对比）
        },
        "假设验证": {
          "dimension_id": "HYPOTHESIS",     // 假设验证维度ID
          "dimension_name": "假设验证",     // 维度显示名称
          "total_score": 25,                // 假设验证维度满分
          "avg_score": 18.8,                // 全区假设验证平均分
          "score_rate": 0.752,              // 假设验证得分率 = 18.8/25
          "regional_ranking_avg": 0.752     // 区域平均得分率
        }
      }
    }
  },
  "radar_chart_data": {                       // 专门为前端雷达图准备的数据格式
    "academic_dimensions": [                 // 学业类维度数据（用于雷达图的一个分类）
      {
        "dimension_name": "数学运算",        // 维度名称（雷达图上的标签）
        "score_rate": 0.8125,               // 区域该维度得分率（雷达图上的数值）
        "max_rate": 1.0                     // 雷达图最大值（固定为1.0，表示100%）
      },
      {
        "dimension_name": "逻辑推理", 
        "score_rate": 0.766,                // 区域逻辑推理得分率
        "max_rate": 1.0
      },
      {
        "dimension_name": "阅读理解",
        "score_rate": 0.764,                // 区域阅读理解得分率
        "max_rate": 1.0
      }
    ],
    "non_academic_dimensions": [             // 非学业类维度数据（用于雷达图的另一个分类）
      {
        "dimension_name": "好奇心",          // 非学业维度名称
        "score_rate": 0.82,                 // 区域好奇心得分率
        "max_rate": 1.0
      },
      {
        "dimension_name": "观察能力",        // 人机交互类维度
        "score_rate": 0.755,                // 区域观察能力得分率
        "max_rate": 1.0
      },
      {
        "dimension_name": "假设验证",        // 人机交互类维度
        "score_rate": 0.752,                // 区域假设验证得分率
        "max_rate": 1.0
      }
    ]
  }
}
```

### 2.2 学校级统计数据 (school_statistics_summary.statistics_data)

```json
{
  "school_info": {
    "school_id": "SCH_001",                   // 学校唯一标识ID
    "school_name": "第一中学",                // 学校显示名称
    "batch_code": "BATCH_2025_001",          // 所属批次代码
    "total_students": 340,                    // 该学校参与测评的学生总数
    "calculation_time": "2025-09-04T18:35:00Z"  // 该学校统计计算完成时间
  },
  "academic_subjects": {
    "数学": {
      "subject_id": "MATH_001",
      "subject_type": "考试类",
      "total_score": 100,
      "school_stats": {
        "avg_score": 85.2,
        "score_rate": 0.852,
        "std_dev": 10.5,
        "max_score": 98,
        "min_score": 58,
        "regional_ranking": 1
      },
      "percentiles": {                        // 学校内学生成绩百分位数（分数从高到低排序）
        "P10": 95,                           // P10百分位：成绩排名前10%位置的学生分数（第34名学生的分数）
        "P50": 86,                           // P50百分位：成绩中位数，50%位置的学生分数（第170名学生的分数）  
        "P90": 68                            // P90百分位：成绩排名90%位置的学生分数（第306名学生的分数）
      },
      "grade_distribution": {
        "excellent": {
          "count": 136,
          "percentage": 0.40
        },
        "good": {
          "count": 136,
          "percentage": 0.40  
        },
        "pass": {
          "count": 51,
          "percentage": 0.15
        },
        "fail": {
          "count": 17,
          "percentage": 0.05
        }
      },
      "regional_comparison": {               // 该学校与区域平均水平的对比分析
        "regional_avg_score": 78.5,         // 全区该科目平均分（用于对比）
        "regional_score_rate": 0.785,       // 全区该科目得分率（用于对比）  
        "difference": 6.7,                  // 学校平均分与区域平均分的差值 = 85.2 - 78.5
        "rate_difference": 0.067,           // 学校得分率与区域得分率的差值 = 0.852 - 0.785
        "performance_level": "优秀"          // 根据差值判定的表现水平：优秀/良好/一般/待提升
      },
      "dimensions": {
        "数学运算": {
          "dimension_id": "MATH_CALC",
          "dimension_name": "数学运算",
          "total_score": 40,
          "school_avg_score": 34.8,
          "school_score_rate": 0.87,
          "regional_avg_score": 32.5,
          "regional_score_rate": 0.8125,
          "difference": 2.3,
          "rate_difference": 0.0575,
          "regional_ranking": 2
        },
        "逻辑推理": {
          "dimension_id": "MATH_LOGIC", 
          "dimension_name": "逻辑推理",
          "total_score": 35,
          "school_avg_score": 28.2,
          "school_score_rate": 0.806,
          "regional_avg_score": 26.8,
          "regional_score_rate": 0.766,
          "difference": 1.4,
          "rate_difference": 0.04,
          "regional_ranking": 3
        }
      }
    },
    "语文": {
      "subject_id": "CHINESE_001",
      "subject_type": "考试类",
      "total_score": 120,
      "school_stats": {
        "avg_score": 95.8,
        "score_rate": 0.798,
        "std_dev": 13.2,
        "max_score": 115,
        "min_score": 62,
        "regional_ranking": 5
      },
      "percentiles": {
        "P10": 112,
        "P50": 96,
        "P90": 75
      },
      "grade_distribution": {
        "excellent": {
          "count": 102,
          "percentage": 0.30
        },
        "good": {
          "count": 153,
          "percentage": 0.45
        },
        "pass": {
          "count": 68,
          "percentage": 0.20
        },
        "fail": {
          "count": 17,
          "percentage": 0.05
        }
      },
      "regional_comparison": {
        "regional_avg_score": 92.4,
        "regional_score_rate": 0.77,
        "difference": 3.4,
        "rate_difference": 0.028,
        "performance_level": "良好"
      },
      "dimensions": {
        "阅读理解": {
          "dimension_id": "CN_READ",
          "dimension_name": "阅读理解",
          "total_score": 50,
          "school_avg_score": 39.5,
          "school_score_rate": 0.79,
          "regional_avg_score": 38.2,
          "regional_score_rate": 0.764,
          "difference": 1.3,
          "rate_difference": 0.026,
          "regional_ranking": 8
        }
      }
    }
  },
  "non_academic_subjects": {
    "创新思维": {
      "subject_id": "INNOVATION_001",
      "subject_type": "问卷类",
      "participated_students": 328,
      "dimensions": {
        "好奇心": {
          "dimension_id": "CURIOSITY",
          "dimension_name": "好奇心",
          "total_score": 25,
          "school_avg_score": 21.2,
          "school_score_rate": 0.848,
          "regional_avg_score": 20.5,
          "regional_score_rate": 0.82,
          "difference": 0.7,
          "rate_difference": 0.028,
          "regional_ranking": 3
        }
      }
    },
    "科学探究": {
      "subject_id": "SCIENCE_INQUIRY_001", 
      "subject_type": "人机交互类",
      "participated_students": 340,
      "school_stats": {
        "avg_score": 48.5,
        "score_rate": 0.808,
        "std_dev": 7.2,
        "max_score": 58,
        "min_score": 32,
        "regional_ranking": 2
      },
      "percentiles": {
        "P10": 56,
        "P50": 49,
        "P90": 38
      },
      "regional_comparison": {
        "regional_avg_score": 45.2,
        "regional_score_rate": 0.752,
        "difference": 3.3,
        "rate_difference": 0.056,
        "performance_level": "优秀"
      },
      "dimensions": {
        "观察能力": {
          "dimension_id": "OBSERVE",
          "dimension_name": "观察能力",
          "total_score": 20,
          "school_avg_score": 16.2,
          "school_score_rate": 0.81,
          "regional_avg_score": 15.1,
          "regional_score_rate": 0.755,
          "difference": 1.1,
          "rate_difference": 0.055,
          "regional_ranking": 2
        },
        "假设验证": {
          "dimension_id": "HYPOTHESIS",
          "dimension_name": "假设验证",
          "total_score": 25,
          "school_avg_score": 20.1,
          "school_score_rate": 0.804,
          "regional_avg_score": 18.8,
          "regional_score_rate": 0.752,
          "difference": 1.3,
          "rate_difference": 0.052,
          "regional_ranking": 3
        }
      }
    }
  },
  "radar_chart_data": {                      // 学校雷达图数据（包含学校与区域对比）
    "academic_dimensions": [                // 学业类维度雷达图数据
      {
        "dimension_name": "数学运算",        // 维度名称（雷达图标签）
        "school_score_rate": 0.87,          // 该学校该维度得分率（雷达图内圈数值）
        "regional_score_rate": 0.8125,      // 区域该维度得分率（雷达图外圈参考线）
        "max_rate": 1.0                     // 雷达图最大刻度（100%）
      },
      {
        "dimension_name": "逻辑推理",
        "school_score_rate": 0.806,         // 学校逻辑推理得分率
        "regional_score_rate": 0.766,       // 区域逻辑推理得分率（用于对比）
        "max_rate": 1.0
      },
      {
        "dimension_name": "阅读理解",
        "school_score_rate": 0.79,          // 学校阅读理解得分率
        "regional_score_rate": 0.764,       // 区域阅读理解得分率（用于对比）
        "max_rate": 1.0
      }
    ],
    "non_academic_dimensions": [            // 非学业类维度雷达图数据
      {
        "dimension_name": "好奇心",          // 问卷类维度
        "school_score_rate": 0.848,         // 该学校好奇心得分率
        "regional_score_rate": 0.82,        // 区域好奇心得分率（对比基线）
        "max_rate": 1.0
      },
      {
        "dimension_name": "观察能力",        // 人机交互类维度
        "school_score_rate": 0.81,          // 该学校观察能力得分率
        "regional_score_rate": 0.755,       // 区域观察能力得分率（对比基线）
        "max_rate": 1.0
      },
      {
        "dimension_name": "假设验证",        // 人机交互类维度
        "school_score_rate": 0.804,         // 该学校假设验证得分率
        "regional_score_rate": 0.752,       // 区域假设验证得分率（对比基线）
        "max_rate": 1.0
      }
    ]
  }
}
```

## 3. API响应格式规范

### 3.1 获取区域统计报告 API

**请求**: `GET /api/v1/reports/regional/{batch_code}`

**响应格式**:
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "batch_code": "BATCH_2025_001",
    "statistics": {
      // 完整的regional_statistics_summary.statistics_data内容
    }
  },
  "timestamp": "2025-09-04T18:30:00Z"
}
```

### 3.2 获取学校统计报告 API

**请求**: `GET /api/v1/reports/school/{batch_code}/{school_id}`

**响应格式**:
```json
{
  "code": 200,
  "message": "success", 
  "data": {
    "batch_code": "BATCH_2025_001",
    "school_id": "SCH_001",
    "statistics": {
      // 完整的school_statistics_summary.statistics_data内容
    }
  },
  "timestamp": "2025-09-04T18:30:00Z"
}
```

### 3.3 触发统计计算 API

**请求**: `POST /api/v1/calculations/batches/{batch_code}/trigger`

**响应格式**:
```json
{
  "code": 200,
  "message": "计算任务已启动",
  "data": {
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "batch_code": "BATCH_2025_001",
    "status": "pending",
    "estimated_duration": "25-30分钟"
  },
  "timestamp": "2025-09-04T18:30:00Z"
}
```

### 3.4 查询任务状态 API

**请求**: `GET /api/v1/calculations/tasks/{task_id}/status`

**响应格式**:
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "task_id": "550e8400-e29b-41d4-a716-446655440000", 
    "batch_code": "BATCH_2025_001",
    "status": "processing",
    "progress": 65.5,
    "started_at": "2025-09-04T18:30:00Z",
    "estimated_completion": "2025-09-04T18:55:00Z",
    "current_phase": "正在计算学校级统计..."
  },
  "timestamp": "2025-09-04T18:42:30Z"
}
```

## 4. 关键业务规则

### 4.1 年级划分标准

**重要**：年级划分基于`grade_aggregation_main.grade_level`字段，影响等级分布阈值计算

```sql
-- 年级划分逻辑
CASE 
  WHEN grade_level IN ('1st_grade', '2nd_grade', '3rd_grade', '4th_grade', '5th_grade', '6th_grade') 
  THEN '小学'
  WHEN grade_level IN ('7th_grade', '8th_grade', '9th_grade') 
  THEN '初中'
END
```

**等级分布阈值对应关系**：
- **小学阶段 (1-6年级)**：
  - 优秀：得分率 ≥ 85%
  - 良好：70% ≤ 得分率 < 85%  
  - 及格：60% ≤ 得分率 < 70%
  - 不及格：得分率 < 60%

- **初中阶段 (7-9年级)**：
  - 优秀：得分率 ≥ 80%
  - 良好：70% ≤ 得分率 < 80%
  - 及格：60% ≤ 得分率 < 70%  
  - 不及格：得分率 < 60%

### 4.2 数值精度要求
- 所有分数保留1位小数 (如: 85.2)
- 所有得分率保留3位小数 (如: 0.852)
- 所有百分比保留2位小数 (如: 0.40 = 40%)
- 进度百分比保留2位小数 (如: 65.50%)

### 4.2 数据一致性验证
- 等级分布的百分比之和必须等于1.0
- 维度得分率 = 维度平均分 / 维度总分
- 学校排名必须连续且不重复
- 所有JSON必须通过格式验证

### 4.3 必填字段要求
- 所有score_rate字段必填
- 所有total_score字段必填  
- 雷达图数据必须包含所有维度
- 排名数据必须完整且准确

## 5. 版本控制和迁移

### 5.1 版本标识
每个JSON数据结构应包含版本标识:
```json
{
  "data_version": "1.0",
  "schema_version": "2025-09-04",
  // ... 其他数据
}
```

### 5.2 向后兼容性
- 新增字段时保持现有字段结构不变
- 废弃字段时先标记为deprecated，下个版本删除
- 关键字段名称变更需要提供映射规则

## 6. 使用建议

1. **开发阶段**: 严格按照此规范创建数据结构，使用JSON Schema验证
2. **测试阶段**: 用示例数据验证前端渲染效果，特别是雷达图
3. **生产部署**: 建立数据质量监控，确保JSON格式一致性
4. **维护升级**: 任何格式变更都需要更新此文档并做好版本管理

---

**备注**: 此规范文档应在开发开始前与前端团队确认，确保数据格式满足所有消费端的需求。