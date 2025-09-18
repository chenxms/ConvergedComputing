#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
安装 G7-2025 写入守卫：
- 在 statistical_aggregations 上创建 BEFORE INSERT/UPDATE 触发器
- 拦截 batch_code='G7-2025' 的写入并记录到 g7_guard_log 表

用法：
  python scripts/install_g7_guard.py

说明：
- 触发器会抛出 45000，自此任何来源的 INSERT/UPDATE（包括外部系统）都会被阻断并记录。
- 卸载请运行 scripts/uninstall_g7_guard.py
"""

from sqlalchemy import text
from app.database.connection import get_db


def install_guard():
    with next(get_db()) as db:
        # 1) 守卫日志表
        db.execute(text(
            """
            CREATE TABLE IF NOT EXISTS g7_guard_log (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                event VARCHAR(10) NOT NULL,
                message VARCHAR(255) NULL,
                batch_code VARCHAR(50) NOT NULL,
                aggregation_level VARCHAR(30) NULL,
                school_id VARCHAR(60) NULL,
                user_host VARCHAR(128) NULL,
                current_user_name VARCHAR(128) NULL,
                connection_id BIGINT NULL,
                created_at DATETIME NOT NULL DEFAULT NOW(),
                INDEX idx_created_at (created_at),
                INDEX idx_event (event)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        ))

        # 2) 清理旧触发器
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_insert"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_update"))

        # 3) 安装 INSERT 守卫（批次代码归一化：去空格 + 统一破折号为 '-'）
        db.execute(text(
            """
            CREATE TRIGGER g7_guard_insert
            BEFORE INSERT ON statistical_aggregations
            FOR EACH ROW
            BEGIN
                DECLARE nb VARCHAR(64);
                SET nb = TRIM(REPLACE(REPLACE(REPLACE(NEW.batch_code, '–','-'),'−','-'),'—','-'));
                IF nb = 'G7-2025' THEN
                    INSERT INTO g7_guard_log(
                        event, message, batch_code, aggregation_level, school_id,
                        user_host, current_user_name, connection_id
                    ) VALUES (
                        'INSERT', 'blocked by guard', nb, NEW.aggregation_level,
                        NEW.school_id, USER(), CURRENT_USER(), CONNECTION_ID()
                    );
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by guard';
                END IF;
            END
            """
        ))

        # 4) 安装 UPDATE 守卫（同样做批次代码归一化）
        db.execute(text(
            """
            CREATE TRIGGER g7_guard_update
            BEFORE UPDATE ON statistical_aggregations
            FOR EACH ROW
            BEGIN
                DECLARE nb VARCHAR(64);
                SET nb = TRIM(REPLACE(REPLACE(REPLACE(NEW.batch_code, '–','-'),'−','-'),'—','-'));
                IF nb = 'G7-2025' THEN
                    INSERT INTO g7_guard_log(
                        event, message, batch_code, aggregation_level, school_id,
                        user_host, current_user_name, connection_id
                    ) VALUES (
                        'UPDATE', 'blocked by guard', nb, NEW.aggregation_level,
                        NEW.school_id, USER(), CURRENT_USER(), CONNECTION_ID()
                    );
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by guard';
                END IF;
            END
            """
        ))

        db.commit()
        print('✅ G7-2025 写入守卫已安装（触发器 + 日志表）')


if __name__ == '__main__':
    install_guard()
