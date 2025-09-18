#!/usr/bin/env python3
"""
Get real API response examples for frontend integration guide
"""

import sys
import os
import json
import requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database.connection import engine

def get_available_batches():
    """Get available batches and select the best one for examples"""
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
                
            # Pick the batch with most students
            test_batch = batches[0][0]
            
            # Check schools in this batch
            result = connection.execute(text("""
                SELECT DISTINCT school_id, school_name, COUNT(*) as student_count
                FROM grade_aggregation_main
                WHERE batch_code = :batch_code
                GROUP BY school_id, school_name
                ORDER BY student_count DESC
                LIMIT 3
            """), {"batch_code": test_batch})
            
            schools = result.fetchall()
            
            return {
                "batch_code": test_batch,
                "schools": [(s[0], s[1], s[2]) for s in schools] if schools else []
            }
            
    except Exception as e:
        print(f"Error checking batches: {e}")
        return None

def fetch_api_response(url, description):
    """Fetch API response and format it nicely"""
    print(f"\n🔄 Fetching {description}...")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Success: {description}")
            return data
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching {description}: {e}")
        return None

def main():
    print("=" * 60)
    print("API Response Examples for Frontend Guide")
    print("=" * 60)
    
    # Get available batches
    batch_info = get_available_batches()
    if not batch_info:
        print("No batch data available")
        return False
        
    batch_code = batch_info['batch_code']
    schools = batch_info['schools']
    
    print(f"Selected batch: {batch_code}")
    print(f"Available schools: {len(schools)}")
    
    # API base URL (assuming local development)
    base_url = "http://localhost:8000"
    
    examples = {}
    
    # 1. Get regional data
    regional_url = f"{base_url}/api/v12/batch/{batch_code}/regional"
    regional_data = fetch_api_response(regional_url, "Regional Data")
    
    if regional_data:
        examples["regional"] = regional_data
    
    # 2. Get school data (from the first school with most students)
    if schools:
        school_code = schools[0][0]
        school_name = schools[0][1]
        school_url = f"{base_url}/api/v12/batch/{batch_code}/school/{school_code}"
        school_data = fetch_api_response(school_url, f"School Data ({school_name})")
        
        if school_data:
            examples["school"] = school_data
    
    # Save examples to JSON file
    if examples:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"api_examples_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(examples, f, ensure_ascii=False, indent=2)
        
        print(f"\n📁 Examples saved to: {filename}")
        
        # Print summary
        print(f"\n📊 Example Data Summary:")
        if "regional" in examples:
            regional_subjects = examples["regional"]["data"]["subjects"]
            print(f"  - Regional subjects: {len(regional_subjects)}")
            for subj in regional_subjects:
                print(f"    • {subj['subject_name']} ({subj['type']})")
                
        if "school" in examples:
            school_subjects = examples["school"]["data"]["subjects"]  
            print(f"  - School subjects: {len(school_subjects)}")
            for subj in school_subjects:
                rank = subj.get('region_rank', 'N/A')
                total = subj.get('total_schools', 'N/A')
                print(f"    • {subj['subject_name']} (rank {rank}/{total})")
        
        return True
    else:
        print("❌ No API examples collected")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)