import asyncio
from types import MethodType

from app.services.calculation_service import CalculationService
from app.services.subjects_builder import SubjectsBuilder
from app.database.connection import get_db

captured = {}

async def fake_save_regional(self, batch_code, statistics_data, total_students, calculation_duration):
    builder = SubjectsBuilder()
    subjects = builder.build_regional_subjects_v12(batch_code, enhanced_stats=statistics_data)
    captured['regional'] = subjects
    captured['regional_raw'] = statistics_data
    print(f"Captured regional subjects: {len(subjects)}")

async def fake_save_school(self, batch_code, school_id, school_name, statistics_data, total_students, calculation_duration):
    # Skip school-level persistence to save time
    pass

async def fake_calculate_batch_all_schools(self, batch_code, config=None, progress_callback=None):
    return {
        'total_schools': 0,
        'successful_schools': 0,
        'failed_schools': [],
        'school_results': []
    }

async def main():
    gen = get_db()
    db = next(gen)
    try:
        service = CalculationService(db)
        service._save_regional_statistics = MethodType(fake_save_regional, service)
        service._save_school_statistics = MethodType(fake_save_school, service)
        service.calculate_batch_all_schools = MethodType(fake_calculate_batch_all_schools, service)
        result = await service.calculate_batch_statistics('G7-2025')
        import json
        with open('temp\\captured_regional_subjects.json', 'w', encoding='utf-8') as f:
            json.dump(captured.get('regional'), f, ensure_ascii=False, indent=2)
        with open('temp\\captured_regional_raw.json', 'w', encoding='utf-8') as f:
            json.dump(captured.get('regional_raw'), f, ensure_ascii=False, indent=2)
        print('Captured files written.')
    finally:
        db.close()
        try:
            next(gen)
        except StopIteration:
            pass

asyncio.run(main())
