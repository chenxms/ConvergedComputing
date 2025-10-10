from pathlib import Path
path = Path('data_cleaning_service.py')
lines = path.read_text(encoding='utf-8').splitlines()
injected = False
for idx in range(len(lines)):
    if lines[idx].strip().startswith('self.exclude_zero_total_scores = bool(exclude_zero_total_scores)'):
        insert_pos = idx + 1
        lines.insert(insert_pos, "        self._questionnaire_keywords = ('问卷', '调查', '满意度', '测评', 'survey', 'questionnaire')")
        lines.insert(insert_pos + 1, "        self._questionnaire_normalized = {kw.lower() for kw in self._questionnaire_keywords}")
        injected = True
        break
if not injected:
    raise SystemExit('insertion point not found')
path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
