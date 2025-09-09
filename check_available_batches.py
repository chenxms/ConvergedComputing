#!/usr/bin/env python3
"""
Check available batches in the database for testing aggregation
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.connection import engine
import json

def check_available_batches():
    """Check what batches are available for testing"""
    print("Checking available batches for aggregation testing...")
    
    try:
        with engine.connect() as connection:
            # Check grade_aggregation_main for available batches
            result = connection.execute(text("""
                SELECT DISTINCT batch_code, COUNT(*) as student_count
                FROM grade_aggregation_main 
                GROUP BY batch_code
                ORDER BY student_count DESC
            """))
            
            batches = result.fetchall()
            
            if not batches:
                print("No batches found in grade_aggregation_main")
                return None
                
            print(f"Found {len(batches)} batches:")
            for batch_code, count in batches:
                print(f"  - {batch_code}: {count} students")
            
            # Pick the batch with most students
            test_batch = batches[0][0]
            print(f"\nSelected batch for testing: {test_batch}")
            
            # Check what subjects are available for this batch
            result = connection.execute(text("""
                SELECT DISTINCT ssd.subject_id, COUNT(*) as question_count
                FROM student_score_detail ssd
                JOIN grade_aggregation_main gam ON gam.student_id = ssd.student_id 
                    AND gam.batch_code = ssd.batch_code
                WHERE ssd.batch_code = :batch_code
                GROUP BY ssd.subject_id
                ORDER BY question_count DESC
            """), {"batch_code": test_batch})
            
            subjects = result.fetchall()
            
            if subjects:
                print(f"\nAvailable subjects in batch {test_batch}:")
                for subject_id, q_count in subjects:
                    print(f"  - Subject {subject_id}: {q_count} questions")
            
            # Check schools in this batch
            result = connection.execute(text("""
                SELECT DISTINCT school_id, school_name, COUNT(*) as student_count
                FROM grade_aggregation_main
                WHERE batch_code = :batch_code
                GROUP BY school_id, school_name
                ORDER BY student_count DESC
                LIMIT 5
            """), {"batch_code": test_batch})
            
            schools = result.fetchall()
            
            if schools:
                print(f"\nTop schools in batch {test_batch}:")
                for school_id, school_name, s_count in schools:
                    print(f"  - {school_id} ({school_name}): {s_count} students")
            
            return {
                "batch_code": test_batch,
                "total_students": batches[0][1],
                "subjects": [(s[0], s[1]) for s in subjects] if subjects else [],
                "schools": [(s[0], s[1], s[2]) for s in schools] if schools else []
            }
            
    except Exception as e:
        print(f"Error checking batches: {e}")
        return None

def main():
    print("=" * 60)
    print("Available Batches Check")  
    print("=" * 60)
    
    batch_info = check_available_batches()
    
    if batch_info:
        print("\n" + "=" * 60)
        print("READY FOR AGGREGATION TESTING")
        print("=" * 60)
        print(f"Batch: {batch_info['batch_code']}")
        print(f"Students: {batch_info['total_students']}")
        print(f"Subjects: {len(batch_info['subjects'])}")
        print(f"Schools: {len(batch_info['schools'])}")
        
        if batch_info['schools']:
            test_school = batch_info['schools'][0]
            print(f"\nSuggested test parameters:")
            print(f"  - Batch Code: {batch_info['batch_code']}")
            print(f"  - School ID: {test_school[0]}")
            print(f"  - School Name: {test_school[1]}")
            print(f"  - Expected Students: {test_school[2]}")
        
        return True
    else:
        print("\nNo batch data available for testing")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)