
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
            ORDER BY TABLE_NAME
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
            col_str = ", ".join([f"{c[0]}({c[1]})" for c in columns[:10]])
            table_info.append(f"{table}: [{col_str}]")

        cursor.close()
        conn.close()
        return table_info
    except Exception as e:
        return []

def generate_sql_from_question(question: str, table_info: list) -> str:
    """
    Groq AI converts ANY plain English question to SQL
    Handles: simple queries, JOINs, subqueries, CTEs,
    window functions, MERGE, stored procedures, complex analytics
    """
    tables_context = "\n".join(table_info)

    prompt = f"""You are DataForge AI — an expert Snowflake SQL engineer for E.L.F Beauty.

Available Snowflake tables:
{tables_context}

User question: {question}

You can write ANY type of Snowflake SQL:
- Simple SELECT with WHERE, GROUP BY, ORDER BY
- INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN
- Subqueries in SELECT, WHERE, FROM clauses
- CTEs using WITH clause for complex logic
- Window functions: ROW_NUMBER(), RANK(), DENSE_RANK(),
  LAG(), LEAD(), SUM() OVER(), AVG() OVER()
- MERGE INTO for upsert operations
- CASE WHEN for conditional logic
- PIVOT and UNPIVOT for data transformation
- Date functions: DATEADD, DATEDIFF, DATE_TRUNC, TO_DATE
- String functions: UPPER, LOWER, TRIM, REGEXP_LIKE
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX, STDDEV
- Analytical queries with multiple aggregation levels
- Recursive CTEs for hierarchical data
- Stored procedures using Snowflake JavaScript

Rules:
1. Generate ONLY valid Snowflake SQL
2. Use only tables listed above
3. Add LIMIT 100 for SELECT queries unless user asks for all
4. For complex analytics use CTEs for readability
5. Use window functions when comparing rows or ranking
6. Return ONLY the SQL — no explanation no markdown no backticks
7. If question needs a JOIN write it — do not avoid complexity
8. If question asks for trends use LAG() or DATE_TRUNC()
9. If question asks for ranking use RANK() or ROW_NUMBER()
10. If question asks for running totals use SUM() OVER()

Generate the most accurate and complete SQL for:
{question}"""

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
    """
    try:
        results_preview = str(results[:5]) if results else "No results"

        prompt = f"""You are DataForge AI — a business intelligence analyst for E.L.F Beauty.

The CEO asked: "{question}"

SQL that was run:
{sql}

Results (first 5 rows):
Columns: {columns}
Data: {results_preview}
Total rows returned: {len(results)}

Write a clear business-focused answer in 2-3 sentences.
Include specific numbers from the results.
End with one actionable recommendation.
Do not mention SQL or technical terms.
Write as if talking directly to the CEO."""

        response = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile",
            max_tokens=200
        )
        return response.choices[0].message.content.strip()
    except:
        return f"Query returned {len(results)} results successfully."

def determine_chart_type(question: str, columns: list) -> str:
    """AI determines best chart type for the data"""
    question_lower = question.lower()
    if any(w in question_lower for w in ['trend', 'over time', 'by month', 'by day', 'by week']):
        return "line"
    elif any(w in question_lower for w in ['compare', 'vs', 'top', 'best', 'worst', 'highest', 'lowest']):
        return "bar"
    elif any(w in question_lower for w in ['distribution', 'breakdown', 'percentage', 'share']):
        return "pie"
    else:
        return "bar"

def run_ai_query(question: str) -> dict:
    """
    Main AI Query Engine function
    Question → SQL → Execute → Explain → Chart
    All automatic. Zero SQL knowledge needed.
    """
    query_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()

    try:
        # ── STEP 1: Get available tables ───────────────────
        table_info = get_available_tables()
        if not table_info:
            return {
                "status": "error",
                "message": "No tables found in Snowflake"
            }

        # ── STEP 2: Generate SQL from question ─────────────
        sql = generate_sql_from_question(question, table_info)

        # ── STEP 3: Execute SQL on Snowflake ───────────────
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        exec_start = datetime.now()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        exec_time = (datetime.now() - exec_start).microseconds // 1000

        # ── STEP 4: Format results ──────────────────────────
        formatted_results = [
            dict(zip(columns, [str(v) if v is not None else "N/A" for v in row]))
            for row in results
        ]

        # ── STEP 5: AI explains results ────────────────────
        explanation = explain_results(question, sql, results, columns)

        # ── STEP 6: Determine chart type ───────────────────
        chart_type = determine_chart_type(question, columns)

        # ── STEP 7: Log to query history ───────────────────
        duration = (datetime.now() - start_time).seconds
        cursor.execute("""
            INSERT INTO QUERY_HISTORY
            (QUERY_ID, QUESTION, GENERATED_SQL,
             EXECUTION_TIME_MS, ROWS_RETURNED, ASKED_AT)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (query_id, question, sql, exec_time, len(results), datetime.now()))

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
            "generated_sql": sql if 'sql' in locals() else "SQL generation failed"
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