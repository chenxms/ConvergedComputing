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

 Date: 08/09/2025 10:12:28
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for questionnaire_scale_options
-- ----------------------------
DROP TABLE IF EXISTS `questionnaire_scale_options`;
CREATE TABLE `questionnaire_scale_options`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `instrument_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '量表类型',
  `scale_level` int NOT NULL COMMENT '量表级别',
  `option_level` int NOT NULL COMMENT '选项等级',
  `option_label` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '选项标签',
  `option_description` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '选项描述',
  `is_reverse` tinyint(1) NULL DEFAULT 0 COMMENT '是否为反向题目的映射',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_scale_option`(`instrument_type` ASC, `scale_level` ASC, `option_level` ASC, `is_reverse` ASC) USING BTREE,
  INDEX `idx_instrument`(`instrument_type` ASC, `scale_level` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 36 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '问卷量表选项映射表' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
