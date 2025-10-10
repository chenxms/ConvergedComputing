import asyncio
import pandas as pd
from app.services.calculation_service import CalculationService
from app.database.connection import get_db

TARGET_SUBJECT = '问卷'

async def main():
    gen = get_db()
    db = next(gen)
    try:
        service = CalculationService(db)
        df = await service._fetch_student_scores('G7-2025')
        df = df[df['subject_name'] == TARGET_SUBJECT].copy()
        if df.empty:
            print('no data for subject')
            return
        if 'total_score' in df.columns:
            df = df.rename(columns={'total_score': 'score'})
        max_score = service._get_subject_max_score('G7-2025', TARGET_SUBJECT)
        config = {
            'max_score': float(max_score),
            'grade_level': service._get_batch_grade_level('G7-2025'),
            'percentiles': [10,25,50,75,90],
            'required_columns': ['score']
        }
        basic, edu, percentile, discrim, dims = await service._calculate_questionnaire_statistics(
            'G7-2025', TARGET_SUBJECT, max_score, config, subject_data_df=df
        )
        import json
        print('basic:', basic)
        print('edu keys:', edu.keys())
        print('percentiles:', percentile)
        print('discrimination:', discrim)
        print('dims sample:', list(dims.keys())[:3])
        with open('temp\\questionnaire_dims.json', 'w', encoding='utf-8') as f:
            json.dump(dims, f, ensure_ascii=False, indent=2)
    finally:
        db.close()
        try:
            next(gen)
        except StopIteration:
            pass

asyncio.run(main())
