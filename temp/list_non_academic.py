import json
with open('temp/captured_regional_raw.json', encoding='utf-8') as f:
    data = json.load(f)
print('non-academic keys:', list(data.get('non_academic_subjects', {}).keys()))
