
import requests

questions = [
    "Show me store performance ranked by revenue from GOLD_STORE_PERFORMANCE",
    "Which products have low stock from GOLD_INVENTORY_STATUS",
    "Show me daily revenue trend from GOLD_DAILY_REVENUE",
    "Which products have negative margin from GOLD_PRODUCT_ANALYSIS",
    "Which store achieved highest target percentage",
]

for q in questions:
    print(f"\nQ: {q}")
    r = requests.post(
        "http://127.0.0.1:8000/query/ask",
        json={"question": q}
    )
    data = r.json()
    if data.get("status") == "success":
        print(f"Answer: {data.get('explanation')}")
        print(f"Rows: {data.get('total_rows')}")
    else:
        print(f"Error: {data.get('message')}")
    print("-" * 50)