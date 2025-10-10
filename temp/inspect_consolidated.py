import asyncio
from app.services.calculation_service import CalculationService
from app.database.connection import get_db

async def main():
    gen = get_db()
    db = next(gen)
    try:
        service = CalculationService(db)
        data = await service._fetch_student_scores('G7-2025')
        if data.empty:
            print('no data')
            return
        if 'total_score' in data.columns:
            data = data.rename(columns={'total_score': 'score'})
        consolidated = await service._consolidate_multi_subject_results('G7-2025', data)
        print('keys', consolidated.keys())
        nq = consolidated['non_academic_subjects']
        print('non-academic subjects:', list(nq.keys())[:3])
        target = nq.get('问卷')
        if not target:
            target = next(iter(nq.values()))
        import json
        print(json.dumps(target, ensure_ascii=False)[:2000])
    finally:
        db.close()
        try:
            next(gen)
        except StopIteration:
            pass

asyncio.run(main())
