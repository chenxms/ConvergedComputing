#!/usr/bin/env python3
"""
修复Pydantic V2配置警告
将schema_extra替换为json_schema_extra
"""
import os
import re
from pathlib import Path

def fix_pydantic_config_in_file(file_path: Path):
    """修复单个文件中的Pydantic配置"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 替换schema_extra为json_schema_extra
        original_content = content
        content = re.sub(r'\bschema_extra\s*=', 'json_schema_extra =', content)
        
        # 如果有修改，写回文件
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"修复文件: {file_path}")
            return True
        return False
    except Exception as e:
        print(f"处理文件 {file_path} 失败: {e}")
        return False

def main():
    """主函数"""
    print("开始修复Pydantic V2配置警告...")
    
    # 需要检查的目录
    schema_dirs = [
        "app/schemas",
    ]
    
    fixed_count = 0
    total_files = 0
    
    for schema_dir in schema_dirs:
        schema_path = Path(schema_dir)
        if not schema_path.exists():
            print(f"目录不存在: {schema_path}")
            continue
            
        # 查找所有Python文件
        for py_file in schema_path.rglob("*.py"):
            total_files += 1
            if fix_pydantic_config_in_file(py_file):
                fixed_count += 1
    
    print(f"\n修复完成:")
    print(f"  检查文件: {total_files} 个")
    print(f"  修复文件: {fixed_count} 个")
    
    if fixed_count > 0:
        print("\n注意: 修复后请重新测试应用启动，确保没有其他问题。")

if __name__ == "__main__":
    main()