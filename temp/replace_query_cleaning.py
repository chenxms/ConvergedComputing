from pathlib import Path
path = Path('data_cleaning_service.py')
text = path.read_text(encoding='utf-8')
old_query = "                SELECT \n                    subject_name,\n                    SUM(CASE WHEN question_type_enum IN ('exam','interaction') THEN max_score ELSE 0 END) as total_max_score,\n                    SUM(CASE WHEN question_type_enum IN ('exam','interaction') THEN 1 ELSE 0 END) as question_count,\n                    MAX(question_type_enum) as subject_type,\n                    MAX(instrument_id) as instrument_id\n                FROM subject_question_config \n                WHERE batch_code = :batch_code\n                GROUP BY subject_name\n                ORDER BY subject_name\n            \"\"\"\n"
new_query = "                SELECT \n                    subject_name,\n                    SUM(COALESCE(max_score, 0)) as total_max_score,\n                    COUNT(*) as question_count,\n                    MAX(question_type_enum) as question_type_enum,\n                    MAX(instrument_id) as instrument_id\n                FROM subject_question_config \n                WHERE batch_code = :batch_code\n                GROUP BY subject_name\n                ORDER BY subject_name\n            \"\"\"\n"
if old_query not in text:
    raise SystemExit('old query block not found')
text = text.replace(old_query, new_query, 1)
path.write_text(text, encoding='utf-8')
