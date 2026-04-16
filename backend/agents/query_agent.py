
import snowflake.connector
from dotenv import load_dotenv
import os
import uuid
from datetime import datetime
from groq import Groq

load_dotenv()

groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def get_available_tables() -> list:
    """Get all available tables with their columns"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'PUBLIC'
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY
                CASE
                    WHEN TABLE_NAME LIKE 'GOLD%' THEN 1
                    WHEN TABLE_NAME LIKE 'SLV%' THEN 2
                    WHEN TABLE_NAME LIKE 'BRZ%' THEN 3
                    ELSE 4
                END,
                TABLE_NAME
        """)
        tables = [row[0] for row in cursor.fetchall()]

        table_info = []
        for table in tables:
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table}'
                AND TABLE_SCHEMA = 'PUBLIC'
                ORDER BY ORDINAL_POSITION
            """)
            columns = cursor.fetchall()
            col_str = ", ".join([
                f"{c[0]}({c[1]})" for c in columns[:10]
            ])
            table_info.append(f"{table}: [{col_str}]")

        cursor.close()
        conn.close()
        return table_info
    except Exception as e:
        return []

def generate_sql_from_question(question: str, table_info: list) -> str:
    """
    Groq AI converts ANY plain English question to SQL
    Always prefers Gold tables for accurate clean data
    Handles: JOINs, CTEs, Window functions, MERGE,
    Subqueries, Complex aggregations, Rankings, Trends
    """
    tables_context = "\n".join(table_info)

    prompt = f"""You are DataForge AI — expert Snowflake SQL engineer for E.L.F Beauty.

Available tables:
{tables_context}

IMPORTANT TABLE PRIORITY RULES:
1. ALWAYS prefer GOLD tables for business questions:
   GOLD_STORE_PERFORMANCE — store revenue, orders, rankings, target achievement
   GOLD_PRODUCT_ANALYSIS — product revenue, margin, stock status, units sold
   GOLD_DAILY_REVENUE — daily trends, growth percentages, cumulative revenue
   GOLD_INVENTORY_STATUS — stock alerts, reorder warnings by store

2. Use SILVER tables when Gold does not have needed detail:
   SLV_ELF_ORDERS — individual clean orders
   SLV_ELF_INVENTORY — individual clean inventory records
   SLV_ELF_STORES — store master data

3. ONLY use BRZ Bronze tables if Silver and Gold cannot answer

You can write ANY Snowflake SQL:
- Simple SELECT with WHERE GROUP BY ORDER BY
- INNER JOIN LEFT JOIN RIGHT JOIN FULL OUTER JOIN
- Subqueries in SELECT WHERE FROM
- CTEs using WITH clause
- Window functions: ROW_NUMBER RANK DENSE_RANK LAG LEAD SUM OVER AVG OVER
- MERGE INTO for upsert
- CASE WHEN for conditional logic
- PIVOT and UNPIVOT
- Date functions: DATEADD DATEDIFF DATE_TRUNC TO_DATE
- String functions: UPPER LOWER TRIM REGEXP_LIKE
- Aggregate: COUNT SUM AVG MIN MAX STDDEV

User question: {question}

Rules:
1. Generate ONLY valid Snowflake SQL
2. ALWAYS prefer GOLD tables first
3. Add LIMIT 100 for SELECT unless user asks for all
4. Use CTEs for complex multi-step logic
5. Use window functions for rankings and trends
6. Use JOINs when data spans multiple tables
7. Return ONLY the SQL — no explanation no markdown no backticks
8. If question needs ranking use RANK() or ROW_NUMBER()
9. If question needs trends use LAG() or DATE_TRUNC()
10. If question needs running totals use SUM() OVER()

Generate the most accurate complete SQL:"""

    response = groq_client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="llama-3.3-70b-versatile",
        max_tokens=1000,
        temperature=0.1
    )

    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql

def explain_results(question: str, sql: str, results: list, columns: list) -> str:
    """
    Groq AI explains query results in plain English
    Business focused — written for CEO
    """
    try:
        results_preview = str(results[:5]) if results else "No results"

        prompt = f"""You are DataForge AI — business intelligence analyst for E.L.F Beauty.

CEO asked: "{question}"

SQL executed:
{sql}

Results (first 5 rows):
Columns: {columns}
Data: {results_preview}
Total rows: {len(results)}

Write a clear business answer in 2-3 sentences.
Include specific numbers from results.
End with one actionable recommendation.
Do not mention SQL or technical terms.
Write directly to the CEO."""

        response = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile",
            max_tokens=200
        )
        return response.choices[0].message.content.strip()
    except:
        return f"Query returned {len(results)} results successfully."

def determine_chart_type(question: str, columns: list) -> str:
    """AI determines best chart type"""
    q = question.lower()
    if any(w in q for w in ['trend', 'over time', 'by month', 'by day', 'daily', 'weekly']):
        return "line"
    elif any(w in q for w in ['distribution', 'breakdown', 'percentage', 'share', 'pie']):
        return "pie"
    else:
        return "bar"

def run_ai_query(question: str) -> dict:
    """
    Complete AI Query Engine:
    Question → SQL → Execute → Explain → Chart
    All automatic. Zero SQL knowledge needed.
    Always queries cleanest available data.
    """
    query_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    sql = "SQL generation failed"

    try:
        # STEP 1 — Get available tables
        table_info = get_available_tables()
        if not table_info:
            return {
                "status": "error",
                "message": "No tables found in Snowflake"
            }

        # STEP 2 — Generate SQL from question
        sql = generate_sql_from_question(question, table_info)

        # STEP 3 — Execute on Snowflake
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        exec_start = datetime.now()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        exec_time = (datetime.now() - exec_start).microseconds // 1000

        # STEP 4 — Format results
        formatted_results = [
            dict(zip(columns, [
                str(v) if v is not None else "N/A"
                for v in row
            ]))
            for row in results
        ]

        # STEP 5 — AI explains in plain English
        explanation = explain_results(question, sql, results, columns)

        # STEP 6 — Determine chart type
        chart_type = determine_chart_type(question, columns)

        # STEP 7 — Log to query history
        duration = (datetime.now() - start_time).seconds
        cursor.execute("""
            INSERT INTO QUERY_HISTORY
            (QUERY_ID, QUESTION, GENERATED_SQL,
             EXECUTION_TIME_MS, ROWS_RETURNED, ASKED_AT)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            query_id, question, sql,
            exec_time, len(results), datetime.now()
        ))

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "status": "success",
            "query_id": query_id,
            "question": question,
            "generated_sql": sql,
            "columns": columns,
            "results": formatted_results[:50],
            "total_rows": len(results),
            "execution_time_ms": exec_time,
            "explanation": explanation,
            "chart_type": chart_type,
            "chart_data": formatted_results[:20]
        }

    except Exception as e:
        return {
            "status": "error",
            "query_id": query_id,
            "question": question,
            "message": str(e),
            "generated_sql": sql
        }

def get_query_history() -> list:
    """Get past AI queries"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT QUERY_ID, QUESTION, GENERATED_SQL,
                   EXECUTION_TIME_MS, ROWS_RETURNED, ASKED_AT
            FROM QUERY_HISTORY
            ORDER BY ASKED_AT DESC
            LIMIT 20
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [
            {
                "query_id": r[0],
                "question": r[1],
                "generated_sql": r[2],
                "execution_time_ms": r[3],
                "rows_returned": r[4],
                "asked_at": str(r[5])
            }
            for r in rows
        ]
    except:
        return []