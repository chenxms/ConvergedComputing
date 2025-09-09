#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为Task表添加updated_at字段
"""
import pymysql
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_updated_at_field():
    """为tasks表添加updated_at字段"""
    
    # 数据库连接配置
    config = {
        'host': '117.72.14.166',
        'port': 23506,
        'user': 'root',
        'password': 'mysql_Lujing2022',
        'database': 'appraisal_test',
        'charset': 'utf8mb4'
    }
    
    try:
        # 连接数据库
        connection = pymysql.connect(**config)
        cursor = connection.cursor()
        
        # 检查字段是否已存在
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'appraisal_test' 
            AND TABLE_NAME = 'tasks' 
            AND COLUMN_NAME = 'updated_at'
        """)
        
        if cursor.fetchone():
            logger.info("updated_at字段已存在，无需添加")
            return True
        
        # 添加updated_at字段
        alter_sql = """
            ALTER TABLE tasks 
            ADD COLUMN updated_at DATETIME 
            DEFAULT CURRENT_TIMESTAMP 
            ON UPDATE CURRENT_TIMESTAMP 
            COMMENT '更新时间'
        """
        
        logger.info("正在添加updated_at字段...")
        cursor.execute(alter_sql)
        connection.commit()
        
        logger.info("updated_at字段添加成功")
        
        # 验证字段是否添加成功
        cursor.execute("DESC tasks")
        columns = cursor.fetchall()
        
        logger.info("tasks表结构:")
        for col in columns:
            logger.info(f"  {col[0]} | {col[1]} | {col[2]} | {col[3]} | {col[4]} | {col[5]}")
        
        return True
        
    except Exception as e:
        logger.error(f"添加字段失败: {e}")
        return False
        
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    success = add_updated_at_field()
    if success:
        print("✅ updated_at字段添加成功!")
    else:
        print("❌ 字段添加失败")