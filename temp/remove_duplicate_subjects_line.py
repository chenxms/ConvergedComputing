from pathlib import Path
path = Path('data_cleaning_service.py')
lines = path.read_text(encoding='utf-8').splitlines()
for idx, line in enumerate(lines):
    if idx > 0 and lines[idx-1].strip().startswith('result =') and line.strip() == 'subjects = []':
        del lines[idx]
        break
path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
