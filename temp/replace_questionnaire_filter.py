from pathlib import Path
path = Path('data_cleaning_service.py')
text = path.read_text(encoding='utf-8')
text = text.replace("question_type_enum = 'questionnaire'", "question_type_enum IN ('questionnaire','exam')")
text = text.replace("question_type_enum IN ('questionnaire','exam') IN", "question_type_enum IN ('questionnaire','exam')")
path.write_text(text, encoding='utf-8')
