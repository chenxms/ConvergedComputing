-- 创建问卷题目分数详情表
-- 用于存储问卷类型科目的题目级别分数数据和选项占比分析

CREATE TABLE IF NOT EXISTS questionnaire_question_scores (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    student_id BIGINT NOT NULL COMMENT '学生ID',
    subject_name VARCHAR(100) NOT NULL COMMENT '科目名称',
    batch_code VARCHAR(50) NOT NULL COMMENT '批次代码',
    dimension_code VARCHAR(20) COMMENT '维度代码',
    dimension_name VARCHAR(100) COMMENT '维度名称',
    question_id BIGINT NOT NULL COMMENT '题目ID',
    question_name VARCHAR(500) COMMENT '题目名称',
    original_score DECIMAL(10,2) NOT NULL COMMENT '原始分数',
    scale_level INT NOT NULL COMMENT '量表级别(4分位/5分位/7分位等)',
    instrument_type VARCHAR(50) NOT NULL COMMENT '量表类型(LIKERT_4_POSITIV等)',
    is_reverse BOOLEAN DEFAULT FALSE COMMENT '是否反向计分',
    option_label VARCHAR(50) COMMENT '选项标签(非常同意/同意等)',
    option_level INT COMMENT '选项等级(1-4, 1-5, 1-7等)',
    max_score DECIMAL(10,2) COMMENT '该题目满分',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    -- 创建索引以优化查询性能
    INDEX idx_batch_subject (batch_code, subject_name),
    INDEX idx_student_subject (student_id, subject_name),
    INDEX idx_dimension (dimension_code),
    INDEX idx_question (question_id),
    INDEX idx_scale_type (scale_level, instrument_type),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='问卷题目分数详情表-支持选项占比分析';

-- 创建量表选项映射表
CREATE TABLE IF NOT EXISTS questionnaire_scale_options (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    instrument_type VARCHAR(50) NOT NULL COMMENT '量表类型',
    scale_level INT NOT NULL COMMENT '量表级别',
    option_level INT NOT NULL COMMENT '选项等级',
    option_label VARCHAR(50) NOT NULL COMMENT '选项标签',
    option_description VARCHAR(200) COMMENT '选项描述',
    is_reverse BOOLEAN DEFAULT FALSE COMMENT '是否为反向题目的映射',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_scale_option (instrument_type, scale_level, option_level, is_reverse),
    INDEX idx_instrument (instrument_type, scale_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='问卷量表选项映射表';

-- 插入常用量表选项映射数据
INSERT INTO questionnaire_scale_options 
(instrument_type, scale_level, option_level, option_label, option_description, is_reverse) VALUES
-- 4分位正向量表
('LIKERT_4_POSITIV', 4, 4, '非常同意', '完全赞同该观点', FALSE),
('LIKERT_4_POSITIV', 4, 3, '同意', '基本赞同该观点', FALSE),
('LIKERT_4_POSITIV', 4, 2, '不同意', '基本不赞同该观点', FALSE),
('LIKERT_4_POSITIV', 4, 1, '非常不同意', '完全不赞同该观点', FALSE),

-- 4分位负向量表（反向计分）
('LIKERT_4_NEGATIVE', 4, 1, '非常同意', '完全赞同该观点（反向计分）', TRUE),
('LIKERT_4_NEGATIVE', 4, 2, '同意', '基本赞同该观点（反向计分）', TRUE),
('LIKERT_4_NEGATIVE', 4, 3, '不同意', '基本不赞同该观点（反向计分）', TRUE),
('LIKERT_4_NEGATIVE', 4, 4, '非常不同意', '完全不赞同该观点（反向计分）', TRUE),

-- 5分位正向量表
('LIKERT_5_POSITIV', 5, 5, '非常同意', '完全赞同该观点', FALSE),
('LIKERT_5_POSITIV', 5, 4, '同意', '基本赞同该观点', FALSE),
('LIKERT_5_POSITIV', 5, 3, '中立', '对该观点保持中性', FALSE),
('LIKERT_5_POSITIV', 5, 2, '不同意', '基本不赞同该观点', FALSE),
('LIKERT_5_POSITIV', 5, 1, '非常不同意', '完全不赞同该观点', FALSE),

-- 5分位负向量表（反向计分）
('LIKERT_5_NEGATIVE', 5, 1, '非常同意', '完全赞同该观点（反向计分）', TRUE),
('LIKERT_5_NEGATIVE', 5, 2, '同意', '基本赞同该观点（反向计分）', TRUE),
('LIKERT_5_NEGATIVE', 5, 3, '中立', '对该观点保持中性（反向计分）', TRUE),
('LIKERT_5_NEGATIVE', 5, 4, '不同意', '基本不赞同该观点（反向计分）', TRUE),
('LIKERT_5_NEGATIVE', 5, 5, '非常不同意', '完全不赞同该观点（反向计分）', TRUE);

-- 添加其他常用量表类型
INSERT INTO questionnaire_scale_options 
(instrument_type, scale_level, option_level, option_label, option_description, is_reverse) VALUES
-- 7分位满意度量表
('SATISFACTION_7', 7, 7, '非常满意', '极其满意', FALSE),
('SATISFACTION_7', 7, 6, '满意', '比较满意', FALSE),
('SATISFACTION_7', 7, 5, '略微满意', '稍微满意', FALSE),
('SATISFACTION_7', 7, 4, '一般', '既不满意也不不满意', FALSE),
('SATISFACTION_7', 7, 3, '略微不满意', '稍微不满意', FALSE),
('SATISFACTION_7', 7, 2, '不满意', '比较不满意', FALSE),
('SATISFACTION_7', 7, 1, '非常不满意', '极其不满意', FALSE),

-- 10分位满意度量表
('SATISFACTION_10', 10, 10, '10分', '极其满意', FALSE),
('SATISFACTION_10', 10, 9, '9分', '很满意', FALSE),
('SATISFACTION_10', 10, 8, '8分', '满意', FALSE),
('SATISFACTION_10', 10, 7, '7分', '比较满意', FALSE),
('SATISFACTION_10', 10, 6, '6分', '略微满意', FALSE),
('SATISFACTION_10', 10, 5, '5分', '一般', FALSE),
('SATISFACTION_10', 10, 4, '4分', '略微不满意', FALSE),
('SATISFACTION_10', 10, 3, '3分', '比较不满意', FALSE),
('SATISFACTION_10', 10, 2, '2分', '不满意', FALSE),
('SATISFACTION_10', 10, 1, '1分', '极其不满意', FALSE);