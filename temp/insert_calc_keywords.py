from pathlib import Path
path = Path('app/services/calculation_service.py')
lines = path.read_text(encoding='utf-8').splitlines()
injected = False
for idx, line in enumerate(lines):
    if line.strip().startswith('self._dimension_statistics_cache = {}'):
        insert_pos = idx + 1
        lines.insert(insert_pos, "        self._questionnaire_keywords = ('问卷', '调查', '满意度', '测评', 'survey', 'questionnaire')")
        lines.insert(insert_pos + 1, "        self._questionnaire_normalized = {kw.lower() for kw in self._questionnaire_keywords}")
        injected = True
        break
if not injected:
    raise SystemExit('insertion point not found')
path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
