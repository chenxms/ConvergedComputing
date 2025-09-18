from pathlib import Path

path = Path('app/schemas/request_schemas.py')
text = path.read_text(encoding='utf-8')
replacements = {
    "max_length=50)": "max_length=255)",
    "max_length=100)": "max_length=255)",
    "max_length=10)": "max_length=255)",
}
for old, new in replacements.items():
    text = text.replace(old, new)
text = text.replace("max_length=50)", "max_length=64)", 1)
path.write_text(text, encoding='utf-8')
