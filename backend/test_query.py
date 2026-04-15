
import requests
import json

questions = [
    "Which store has the most orders?",
    "Show me all orders with negative amounts",
    "Find top 3 products by revenue with running total",
    "Show me stores ranked by number of orders with rank number",
    "Find all orders where total amount is more than average order value",
    "Show me total revenue per store joined with store name",
    "Which products have low inventory stock?",
    "Show me month over month order count trend",
]

for q in questions:
    print(f"\nQ: {q}")
    r = requests.post(
        "http://127.0.0.1:8000/query/ask",
        json={"question": q}
    )
    data = r.json()
    if data.get("status") == "success":
        print(f"SQL: {data.get('generated_sql')}")
        print(f"Rows: {data.get('total_rows')}")
        print(f"Answer: {data.get('explanation')}")
    else:
        print(f"Error: {data.get('message')}")
    print("=" * 60)