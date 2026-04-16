
import requests

questions = [
    "Which store has highest revenue?",
    "Show me products with low stock",
    "What is daily revenue trend?",
    "Which products have negative margin?",
    "Show me store rankings by revenue",
    "Which stores need inventory reorder?",
    "What is total revenue across all stores?",
    "Show me top 3 products by units sold",
]

for q in questions:
    print(f"\nQ: {q}")
    r = requests.post(
        "http://127.0.0.1:8000/query/ask",
        json={"question": q}
    )
    data = r.json()
    if data.get("status") == "success":
        print(f"Table used: {data.get('generated_sql', '')[:60]}...")
        print(f"Rows: {data.get('total_rows')}")
        print(f"Answer: {data.get('explanation')}")
    else:
        print(f"Error: {data.get('message')}")
    print("-" * 50)