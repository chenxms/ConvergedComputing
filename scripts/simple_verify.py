#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from app.database.connection import engine
from app.database.models import StatisticalMetadata

def main():
    print("Database verification started...")
    
    # Check tables
    with engine.connect() as connection:
        result = connection.execute(text("SHOW TABLES"))
        tables = [row[0] for row in result.fetchall()]
        print(f"Tables found: {len(tables)}")
        for table in sorted(tables):
            print(f"  - {table}")
    
    # Check metadata
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        configs = session.query(StatisticalMetadata).all()
        print(f"\nMetadata configurations: {len(configs)}")
        
        for config in configs:
            print(f"  {config.id}: {config.metadata_type.value} - {config.metadata_key}")
    
    except Exception as e:
        print(f"Error: {str(e)}")
    
    finally:
        session.close()
    
    print("\nDatabase verification completed!")

if __name__ == "__main__":
    main()