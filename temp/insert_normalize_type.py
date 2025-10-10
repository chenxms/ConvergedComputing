from pathlib import Path
path = Path('data_cleaning_service.py')
lines = path.read_text(encoding='utf-8').splitlines()
search = "    async def clean_batch_scores"
for idx, line in enumerate(lines):
    if line.startswith(search):
        insert_pos = idx
        break
else:
    raise SystemExit('clean_batch_scores not found')
method = [
"    def _normalize_subject_type(self, subject_name: str, subject_type: str, instrument_id: Optional[str]) -> str:",
"        subject_type_value = (subject_type or '').strip().lower() if subject_type else ''",
"        if subject_type_value == 'questionnaire':",
"            return 'questionnaire'",
"        instrument_value = ''",
"        if instrument_id:",
"            instrument_value = str(instrument_id).strip().lower()",
"            if any(token in instrument_value for token in ('likert', 'survey', 'questionnaire', '问卷')):",
"                return 'questionnaire'",
"        name_value = (subject_name or '').strip()",
"        name_lower = name_value.lower()",
"        for kw in self._questionnaire_keywords:",
"            if kw and kw in name_value:",
"                return 'questionnaire'",
"        for kw in getattr(self, '_questionnaire_normalized', ()):",
"            if kw and kw in name_lower:",
"                return 'questionnaire'",
"        return subject_type_value if subject_type_value else 'exam'",
""]
lines[insert_pos:insert_pos] = method
path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
