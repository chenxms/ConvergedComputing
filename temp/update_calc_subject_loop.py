from pathlib import Path
path = Path('app/services/calculation_service.py')
lines = path.read_text(encoding='utf-8').splitlines()
start = None
end = None
for idx, line in enumerate(lines):
    if line.strip() == 'subjects = []':
        start = idx
    if start is not None and line.strip().startswith('logger.info('):
        end = idx
        break
if start is None or end is None:
    raise SystemExit('block not found')
new_block = [
"            subjects = []",
"            for config in subject_configs:",
"                normalized_type = self._normalize_subject_type(config)",
"                subjects.append({",
"                    'subject_name': config.get('subject_name'),",
"                    'max_score': config.get('max_score'),",
"                    'question_count': config.get('question_count'),",
"                    'subject_type': normalized_type,",
"                    'question_type_enum': config.get('question_type_enum'),",
"                    'instrument_id': config.get('instrument_id'),",
"                })"
]
lines[start:end] = new_block
path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
