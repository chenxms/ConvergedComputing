import json
with open('temp/captured_regional_raw.json', encoding='utf-8') as f:
    data = json.load(f)
question = data['non_academic_subjects']['问卷']
dims = question.get('dimensions', {})
print('dimension keys sample:', list(dims.keys())[:5])
print('has _option_distributions:', '_option_distributions' in dims)
if '_option_distributions' in dims:
    sample_items = list(dims['_option_distributions'].items())[:1]
    for qid, payload in sample_items:
        print('option distribution for', qid, '=>', payload)
