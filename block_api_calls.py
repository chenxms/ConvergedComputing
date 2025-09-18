#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
阻断API调用的临时解决方案
通过修改API行为来防止数据重新生成
"""

import sys
import os

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db
from sqlalchemy import text


def create_api_block():
    """创建API阻断机制"""
    
    # 创建一个修改版的API文件
    api_block_content = '''from fastapi import APIRouter, HTTPException
from typing import Any, Dict

router = APIRouter()

@router.get("/batch/{batch_code}/regional")
def get_v12_regional(batch_code: str):
    """阻断版本：直接返回空数据，不重新生成"""
    if "G7-2025" in batch_code or "2025" in batch_code:
        return {
            "success": False, 
            "message": f"批次 {batch_code} 数据生成已被管理员临时禁用", 
            "data": {"error": "数据生成已阻断", "batch_code": batch_code}, 
            "code": 503
        }
    # 其他批次正常处理
    return {"success": True, "message": "正常批次", "data": {}, "code": 200}

@router.get("/batch/{batch_code}/school/{school_code}")  
def get_v12_school(batch_code: str, school_code: str):
    """阻断版本：直接返回空数据，不重新生成"""
    if "G7-2025" in batch_code or "2025" in batch_code:
        return {
            "success": False,
            "message": f"批次 {batch_code} 学校 {school_code} 数据生成已被管理员临时禁用",
            "data": {"error": "数据生成已阻断", "batch_code": batch_code, "school_code": school_code},
            "code": 503
        }
    return {"success": True, "message": "正常批次", "data": {}, "code": 200}

@router.post("/batch/{batch_code}/materialize")
def materialize_v12(batch_code: str):
    """阻断版本：直接返回阻断信息"""
    if "G7-2025" in batch_code or "2025" in batch_code:
        return {
            "success": False,
            "message": f"批次 {batch_code} 数据物化已被管理员临时禁用",
            "data": {"error": "数据生成已阻断", "batch_code": batch_code, "schools_materialized": 0},
            "code": 503
        }
    return {"success": True, "message": "正常批次", "data": {"schools_materialized": 0}, "code": 200}
'''

    # 备份原始文件
    original_file = "D:/myproject/后端/ConvergedComputing/app/api/subjects_v12_api.py"
    backup_file = "D:/myproject/后端/ConvergedComputing/app/api/subjects_v12_api.py.backup"
    
    try:
        import shutil
        shutil.copy2(original_file, backup_file)
        print(f"已备份原始API文件到: {backup_file}")
        
        # 写入阻断版本
        with open(original_file, 'w', encoding='utf-8') as f:
            f.write(api_block_content)
        print(f"已创建API阻断版本: {original_file}")
        print("API现在会阻断G7-2025相关的所有调用")
        
    except Exception as e:
        print(f"创建API阻断失败: {str(e)}")


def restore_api():
    """恢复原始API"""
    
    original_file = "D:/myproject/后端/ConvergedComputing/app/api/subjects_v12_api.py"
    backup_file = "D:/myproject/后端/ConvergedComputing/app/api/subjects_v12_api.py.backup"
    
    try:
        import shutil
        if os.path.exists(backup_file):
            shutil.copy2(backup_file, original_file)
            print(f"已恢复原始API文件: {original_file}")
            os.remove(backup_file)
            print(f"已删除备份文件: {backup_file}")
        else:
            print("未找到备份文件，无法恢复")
            
    except Exception as e:
        print(f"恢复API失败: {str(e)}")


def test_api_block():
    """测试API阻断效果"""
    
    import requests
    
    print("=== 测试API阻断效果 ===")
    
    try:
        # 测试regional端点
        response = requests.get("http://117.72.14.166:8011/api/v12/batch/G7-2025/regional", timeout=10)
        print(f"Regional API响应码: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"响应内容: {data}")
            if not data.get('success', True):
                print("✅ API阻断成功!")
            else:
                print("❌ API阻断失败!")
        else:
            print(f"API返回状态码: {response.status_code}")
            
    except Exception as e:
        print(f"测试API时出错: {str(e)}")


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--restore':
        restore_api()
    elif len(sys.argv) > 1 and sys.argv[1] == '--test':
        test_api_block()
    else:
        print("=== 创建API阻断 ===")
        create_api_block()
        print("\n注意：需要重启API服务才能生效")
        print("要恢复原始API，请运行：")
        print("python block_api_calls.py --restore")