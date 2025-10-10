import json
with open('temp/captured_regional_subjects.json', encoding='utf-8') as f:
    data = json.load(f)
for subj in data:
    dims = subj.get('dimensions')
    if dims:
        print('Subject', subj['subject_name'], 'dimensions count', len(dims))
    else:
        print('Subject', subj['subject_name'], 'has no dimensions field')
