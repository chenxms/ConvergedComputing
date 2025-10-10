import asyncio
from data_cleaning_service import DataCleaningService
from app.database.connection import get_db

async def main():
    gen = get_db()
    db = next(gen)
    try:
        service = DataCleaningService(db)
        await service.clean_batch_scores('G7-2025')
    finally:
        db.close()
        try:
            next(gen)
        except StopIteration:
            pass

asyncio.run(main())
