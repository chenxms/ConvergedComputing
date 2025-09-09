#!/usr/bin/env python3
"""
Simple database connection test without Unicode characters
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.connection import engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection_and_tables():
    """Test database connection and check for expected tables"""
    print("Testing database connection to appraisal_test...")
    
    # Expected tables from documentation
    expected_tables = [
        'student_score_detail',
        'subject_question_config', 
        'question_dimension_mapping',
        'grade_aggregation_main',
        'batch_dimension_definition',
        'school_statistics_summary',
        'regional_statistics_summary'
    ]
    
    try:
        with engine.connect() as connection:
            # Test basic connection
            result = connection.execute(text("SELECT DATABASE()"))
            current_db = result.fetchone()[0]
            print(f"Connected to database: {current_db}")
            
            # Get all tables
            result = connection.execute(text("SHOW TABLES"))
            all_tables = [row[0] for row in result.fetchall()]
            print(f"Total tables found: {len(all_tables)}")
            
            # Check for expected tables
            found_tables = []
            missing_tables = []
            
            for table in expected_tables:
                if table in all_tables:
                    found_tables.append(table)
                    # Get row count
                    try:
                        count_result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        count = count_result.fetchone()[0]
                        print(f"  Table {table}: {count:,} records")
                    except Exception as e:
                        print(f"  Table {table}: Error getting count - {e}")
                else:
                    missing_tables.append(table)
            
            print(f"\nSummary:")
            print(f"Found expected tables: {len(found_tables)}/{len(expected_tables)}")
            
            if found_tables:
                print("Available tables:")
                for table in found_tables:
                    print(f"  - {table}")
            
            if missing_tables:
                print("Missing tables:")
                for table in missing_tables:
                    print(f"  - {table}")
            
            # Show some other tables that might be relevant
            other_relevant = [t for t in all_tables 
                            if any(keyword in t.lower() 
                                 for keyword in ['score', 'student', 'subject', 'grade', 'batch'])
                            and t not in found_tables]
            
            if other_relevant:
                print("Other potentially relevant tables:")
                for table in other_relevant:
                    print(f"  - {table}")
            
            return len(found_tables) > 0
            
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

def main():
    print("=" * 60)
    print("Database Connection Test")
    print("=" * 60)
    
    success = test_connection_and_tables()
    
    if success:
        print("\nConnection test successful!")
        print("Ready to test data aggregation computation.")
    else:
        print("\nConnection test failed!")
        print("Please check database configuration.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)