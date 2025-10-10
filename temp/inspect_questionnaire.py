import asyncio
from app.services.calculation_service import CalculationService
from app.database.connection import get_db

TARGET_SUBJECT = '问卷'

async def main():
    gen = get_db()
    db = next(gen)
    try:
        service = CalculationService(db)
        data = await service._fetch_student_scores('G7-2025')
        if data.empty:
            print('no data')
            return
        data = data[data['subject_name'] == TARGET_SUBJECT].copy()
        print('filtered rows', len(data))
        if 'total_score' in data.columns:
            data = data.rename(columns={'total_score': 'score'})
        consolidated = await service._consolidate_multi_subject_results('G7-2025', data)
        nq = consolidated['non_academic_subjects']
        target = nq.get(TARGET_SUBJECT)
        import json
        print(json.dumps(target, ensure_ascii=False, indent=2)[:4000])
    finally:
        db.close()
        try:
            next(gen)
        except StopIteration:
            pass

asyncio.run(main())
