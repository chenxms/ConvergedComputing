from pathlib import Path
path = Path('data_cleaning_service.py')
text = path.read_text(encoding='utf-8')
text = text.replace("sqc.instrument_id AS instrument_type", "COALESCE(sqc.instrument_id, 'LIKERT_AUTO') AS instrument_type", 1)
path.write_text(text, encoding='utf-8')
