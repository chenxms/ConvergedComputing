from pathlib import Path
path = Path('data_cleaning_service.py')
text = path.read_text(encoding='utf-8')
text = text.replace("SUM(CASE WHEN question_type_enum IN ('exam','interaction') THEN max_score ELSE 0 END)", "SUM(COALESCE(max_score, 0))", 1)
text = text.replace("SUM(CASE WHEN question_type_enum IN ('exam','interaction') THEN 1 ELSE 0 END)", "COUNT(*)", 1)
text = text.replace("MAX(question_type_enum) as subject_type", "MAX(question_type_enum) as question_type_enum", 1)
path.write_text(text, encoding='utf-8')
