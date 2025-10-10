from pathlib import Path
path = Path('data_cleaning_service.py')
text = path.read_text(encoding='utf-8')
old = "                SELECT \r\n                    subject_name,\r\n                    SUM(CASE WHEN question_type_enum IN ('exam','interaction') THEN max_score ELSE 0 END) as total_max_score,\r\n                    SUM(CASE WHEN question_type_enum IN ('exam','interaction') THEN 1 ELSE 0 END) as question_count,\r\n                    MAX(question_type_enum) as subject_type,\r\n                    MAX(instrument_id) as instrument_id\r\n                FROM subject_question_config \r\n                WHERE batch_code = :batch_code\r\n                GROUP BY subject_name\r\n                ORDER BY subject_name\r\n            """"
new = "                SELECT \r\n                    subject_name,\r\n                    SUM(COALESCE(max_score, 0)) as total_max_score,\r\n                    COUNT(*) as question_count,\r\n                    MAX(question_type_enum) as question_type_enum,\r\n                    MAX(instrument_id) as instrument_id\r\n                FROM subject_question_config \r\n                WHERE batch_code = :batch_code\r\n                GROUP BY subject_name\r\n                ORDER BY subject_name\r\n            """"
if old not in text:
    raise SystemExit('query block not found')
text = text.replace(old, new, 1)
path.write_text(text, encoding='utf-8')
