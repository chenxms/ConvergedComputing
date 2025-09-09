/*
 Navicat Premium Dump SQL

 Source Server         : 测评分析
 Source Server Type    : MySQL
 Source Server Version : 80406 (8.4.6)
 Source Host           : 117.72.14.166:23506
 Source Schema         : appraisal_test

 Target Server Type    : MySQL
 Target Server Version : 80406 (8.4.6)
 File Encoding         : 65001

 Date: 08/09/2025 10:09:58
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for questionnaire_question_scores
-- ----------------------------
DROP TABLE IF EXISTS `questionnaire_question_scores`;
CREATE TABLE `questionnaire_question_scores`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `student_id` bigint NOT NULL COMMENT '学生ID',
  `subject_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '科目名称',
  `batch_code` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '批次代码',
  `dimension_code` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '维度代码',
  `dimension_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '维度名称',
  `question_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '题目ID',
  `question_name` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '题目名称',
  `original_score` decimal(10, 2) NOT NULL COMMENT '原始分数',
  `scale_level` int NOT NULL COMMENT '量表级别(4分位/5分位/7分位等)',
  `instrument_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '量表类型(LIKERT_4_POSITIV等)',
  `is_reverse` tinyint(1) NULL DEFAULT 0 COMMENT '是否反向计分',
  `option_label` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '选项标签(非常同意/同意等)',
  `option_level` int NULL DEFAULT NULL COMMENT '选项等级(1-4, 1-5, 1-7等)',
  `max_score` decimal(10, 2) NULL DEFAULT NULL COMMENT '该题目满分',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_batch_subject`(`batch_code` ASC, `subject_name` ASC) USING BTREE,
  INDEX `idx_student_subject`(`student_id` ASC, `subject_name` ASC) USING BTREE,
  INDEX `idx_dimension`(`dimension_code` ASC) USING BTREE,
  INDEX `idx_question`(`question_id` ASC) USING BTREE,
  INDEX `idx_scale_type`(`scale_level` ASC, `instrument_type` ASC) USING BTREE,
  INDEX `idx_created_at`(`created_at` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1318234 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '问卷题目分数详情表-支持选项占比分析' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
