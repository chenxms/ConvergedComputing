#!/usr/bin/env python3
"""
Check actual table structures for key tables
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.connection import engine

def check_table_structure(table_name):
    """Check the structure of a specific table"""
    print(f"\nTable: {table_name}")
    print("-" * 50)
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"DESCRIBE {table_name}"))
            columns = result.fetchall()
            
            print("Column Name         | Type             | Null | Key  | Default")
            print("-" * 70)
            
            for col in columns:
                field = str(col[0])[:18].ljust(18)
                col_type = str(col[1])[:15].ljust(15)
                null_val = str(col[2])[:4].ljust(4)
                key = str(col[3])[:4].ljust(4)
                default = str(col[4] or "")[:10]
                print(f"{field} | {col_type} | {null_val} | {key} | {default}")
            
            # Show sample data
            result = connection.execute(text(f"SELECT * FROM {table_name} LIMIT 2"))
            rows = result.fetchall()
            
            if rows:
                print(f"\nSample data (first 2 rows):")
                columns_list = [desc[0] for desc in result.description]
                print(" | ".join([col[:12].ljust(12) for col in columns_list]))
                print("-" * (len(columns_list) * 15))
                
                for row in rows:
                    print(" | ".join([str(val)[:12].ljust(12) if val is not None else "NULL".ljust(12) for val in row]))
            
            return True
            
    except Exception as e:
        print(f"Error checking table {table_name}: {e}")
        return False

def main():
    print("=" * 70)
    print("Table Structure Check")
    print("=" * 70)
    
    # Key tables to check
    tables = [
        'grade_aggregation_main',
        'student_score_detail', 
        'subject_question_config',
        'question_dimension_mapping'
    ]
    
    for table in tables:
        success = check_table_structure(table)
        if not success:
            break
    
    print("\n" + "=" * 70)
    print("Structure check complete")
    print("=" * 70)

if __name__ == "__main__":
    main()