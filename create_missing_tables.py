#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建系统需要的数据表
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.connection import engine, create_tables, Base
from app.database.models import *  # 导入所有模型
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    print("=" * 60)
    print("创建系统数据表")
    print("=" * 60)
    
    try:
        print("正在创建数据表...")
        
        # 创建所有表
        Base.metadata.create_all(bind=engine)
        
        print("数据表创建完成！")
        
        # 验证表是否创建成功
        from sqlalchemy import text
        with engine.connect() as connection:
            result = connection.execute(text("SHOW TABLES LIKE 'statistical_%'"))
            created_tables = [row[0] for row in result.fetchall()]
            
            print(f"\n创建的系统表:")
            for table in created_tables:
                print(f"  - {table}")
            
            if created_tables:
                print(f"\n成功创建了 {len(created_tables)} 个系统表")
                print("FastAPI服务现在应该可以正常工作了")
                return True
            else:
                print("未检测到新创建的表")
                return False
        
    except Exception as e:
        logger.error(f"创建表失败: {e}")
        print(f"创建表时出错: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)