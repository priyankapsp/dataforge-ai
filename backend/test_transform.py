
import requests

r = requests.post('http://127.0.0.1:8000/transform/run')
data = r.json()

print('Status:', data.get('status'))
print('Succeeded:', data.get('models_succeeded'))
print('Failed:', data.get('models_failed'))
print()
print('SILVER TABLES:')
for t in data.get('silver_tables', []):
    print(f"  {t['table']}: {t['records']} records")
print()
print('GOLD TABLES:')
for t in data.get('gold_tables', []):
    print(f"  {t['table']}: {t['records']} records")
print()
print('ERRORS:')
for e in data.get('errors', []):
    print(f"  {e['table']}: {e.get('error', '')[:100]}")
print()
print('AI Summary:', data.get('ai_summary'))