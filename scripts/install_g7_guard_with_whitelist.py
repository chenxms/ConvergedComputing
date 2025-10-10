#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
兼容包装：将“白名单守卫安装”统一到增强守卫实现。

作用：
- 安装增强版 G7 守卫（含维护模式与白名单表）
- 按默认规则写入若干白名单用户模式（user_pattern）

用法：
  python scripts/install_g7_guard_with_whitelist.py

说明：
- 本脚本不再创建自定义触发器或自定义表结构，统一依赖增强守卫 enhanced_g7_guard。
"""

from sqlalchemy import text
from app.database.connection import get_db
from scripts.enhanced_g7_guard import EnhancedG7Guard


DEFAULT_USER_PATTERNS = [
    'g7_pipeline_user%','system_batch_user%','admin%'
]


def install_whitelist_guard():
    guard = EnhancedG7Guard()
    # 1) 安装增强守卫（如已安装会保持幂等）
    guard.install()

    # 2) 写入默认白名单（基于增强守卫的 g7_guard_whitelist 表结构：user_pattern + is_active）
    with next(get_db()) as db:
        db.execute(text(
            """
            CREATE TABLE IF NOT EXISTS g7_guard_whitelist (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_pattern VARCHAR(128) NOT NULL UNIQUE,
                added_by VARCHAR(128) NOT NULL,
                notes TEXT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at DATETIME NOT NULL DEFAULT NOW(),
                INDEX idx_user_pattern (user_pattern),
                INDEX idx_is_active (is_active)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        ))

        for pattern in DEFAULT_USER_PATTERNS:
            db.execute(text(
                """
                INSERT IGNORE INTO g7_guard_whitelist (user_pattern, added_by, notes)
                VALUES (:pattern, USER(), 'Default whitelist (compat wrapper)')
                """
            ), {"pattern": pattern})

        db.commit()

    print('✅ 增强G7守卫已安装并添加默认白名单（user_pattern）')


if __name__ == '__main__':
    install_whitelist_guard()
