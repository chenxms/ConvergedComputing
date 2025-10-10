from pathlib import Path
path = Path('app/database/repositories.py')
text = path.read_text(encoding='utf-8')
text = text.replace('MAX(max_score) as single_question_max_score\n                FROM subject_question_config', "MAX(max_score) as single_question_max_score,\n                    MAX(instrument_id) as instrument_id\n                FROM subject_question_config", 1)
text = text.replace('MAX(max_score) as single_question_max_score\n                    FROM subject_question_config', "MAX(max_score) as single_question_max_score,\n                        MAX(instrument_id) as instrument_id\n                    FROM subject_question_config", 1)
path.write_text(text, encoding='utf-8')
