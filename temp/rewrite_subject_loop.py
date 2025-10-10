from pathlib import Path
path = Path('data_cleaning_service.py')
lines = path.read_text(encoding='utf-8').splitlines()
start_idx = None
end_idx = None
for i, line in enumerate(lines):
    if line.strip() == 'for row in result.fetchall():' and start_idx is None:
        start_idx = i
    if start_idx is not None and line.strip().startswith('return subjects'):
        end_idx = i
        break
if start_idx is None or end_idx is None:
    raise SystemExit('target block not found')
new_block = [
'            subjects = []',
'            for row in result.fetchall():',
'                subject_name = row[0]',
'                total_max = float(row[1]) if row[1] else 0.0',
'                question_total = int(row[2]) if row[2] else 0',
"                question_type_enum = (row[3] or '').strip().lower() if row[3] else ''",
'                instrument_id = row[4] if row[4] else None',
'                subject_type = self._normalize_subject_type(subject_name, question_type_enum, instrument_id)',
'                is_questionnaire = subject_type == \"questionnaire\"',
'                subjects.append({',
'                    \"subject_name\": subject_name,',
'                    \"max_score\": total_max,',
'                    \"question_count\": question_total,',
'                    \"subject_type\": subject_type,',
'                    \"question_type_enum\": question_type_enum,',
'                    \"is_questionnaire\": is_questionnaire,',
'                    \"instrument_id\": instrument_id',
'                })',
"                if is_questionnaire:",
"                    print(f\"  问卷科目: {subject_name} (量表ID: {instrument_id or 'N/A'})\")",
'                else:',
"                    print(f\"  考试科目: {subject_name} (满分: {total_max})\")"
]
lines[start_idx:end_idx] = new_block
path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
