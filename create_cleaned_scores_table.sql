-- 创建清洗后的学生分数表
-- 每个学生每个科目只有一条记录，包含该科目的总分

CREATE TABLE IF NOT EXISTS `student_cleaned_scores` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `batch_code` varchar(50) NOT NULL COMMENT '批次代码',
  `student_id` varchar(100) NOT NULL COMMENT '学生ID', 
  `student_name` varchar(50) DEFAULT NULL COMMENT '学生姓名',
  `school_id` varchar(50) DEFAULT NULL COMMENT '学校ID',
  `school_code` varchar(50) DEFAULT NULL COMMENT '学校代码', 
  `school_name` varchar(100) DEFAULT NULL COMMENT '学校名称',
  `class_name` varchar(50) DEFAULT NULL COMMENT '班级名称',
  `subject_id` varchar(64) NOT NULL COMMENT '科目ID',
  `subject_name` varchar(100) NOT NULL COMMENT '科目名称',
  `total_score` decimal(8,2) NOT NULL DEFAULT 0 COMMENT '科目总分',
  `max_score` decimal(8,2) NOT NULL DEFAULT 0 COMMENT '科目满分',
  `question_count` int DEFAULT 0 COMMENT '题目数量',
  `is_valid` tinyint(1) DEFAULT 1 COMMENT '数据是否有效（过滤异常分数）',
  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_batch_student_subject` (`batch_code`, `student_id`, `subject_name`),
  KEY `idx_batch_code` (`batch_code`),
  KEY `idx_student_id` (`student_id`),
  KEY `idx_subject_name` (`subject_name`),
  KEY `idx_school_code` (`school_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='清洗后的学生分数表';