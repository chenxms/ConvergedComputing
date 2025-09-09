#!/usr/bin/env python3
"""
Run cleaning for a given batch and print verification summary.

Usage:
  python clean_batch.py G4-2025

Requires DATABASE_URL env to be set (or adjust to your connection method).
"""
import os
import sys
import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_cleaning_service import DataCleaningService, verify_cleaning_result


async def main():
    if len(sys.argv) < 2:
        print("Usage: python clean_batch.py <batch_code>")
        sys.exit(1)

    batch_code = sys.argv[1]
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL env var is not set.")
        sys.exit(2)

    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        cleaner = DataCleaningService(session)
        result = await cleaner.clean_batch_scores(batch_code)
        print("\nCleaning complete. Summary:")
        print(result)

        print("\nVerifying cleaned data...")
        verification = await verify_cleaning_result(session, batch_code)
        print(verification)
    finally:
        session.close()


if __name__ == "__main__":
    asyncio.run(main())

