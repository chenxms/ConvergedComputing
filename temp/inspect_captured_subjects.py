import json
with open('temp/captured_regional_subjects.json', encoding='utf-8') as f:
    data = json.load(f)
for subj in data:
    dims = subj.get('dimensions')
    print(subj['subject_name'], 'dims' if dims else 'no dims', 'type', subj.get('type'))
    if subj['subject_name'] == '问卷' and dims:
        print('dimension sample count:', len(dims))
        print('first dimension:', dims[0])
        break
