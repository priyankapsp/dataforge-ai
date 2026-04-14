
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

def run_quality_checks(bronze_table: str, source_name: str) -> dict:
    run_id = str(uuid.uuid4())[:8]
    issues_found = []
    bad_records = 0

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {bronze_table}")
        total_records = cursor.fetchone()[0]

        if total_records == 0:
            return {"status": "success", "message": "Table is empty", "health_score": 100}

        cursor.execute(f"""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{bronze_table}'
            AND TABLE_SCHEMA = 'PUBLIC'
            ORDER BY ORDINAL_POSITION
        """)
        all_cols = [r[0] for r in cursor.fetchall()]
        columns = [c for c in all_cols if not c.startswith('_')]

        # CHECK 1 — NULL values
        for col in columns:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {bronze_table} WHERE {col} IS NULL")
                n = cursor.fetchone()[0]
                if n > 0:
                    pct = round(n/total_records*100, 1)
                    issues_found.append({
                        "issue_type": "NULL_VALUE", "column": col,
                        "count": n, "percentage": pct,
                        "severity": "HIGH" if pct > 20 else "MEDIUM" if pct > 5 else "LOW",
                        "description": f"{n} NULL values ({pct}%) in {col}"
                    })
                    bad_records += n
            except:
                pass

        # CHECK 2 — Negative numbers
        numeric_kw = ['quantity', 'amount', 'price', 'cost', 'total', 'stock', 'unit', 'reorder', 'hand']
        for col in columns:
            if any(k in col.lower() for k in numeric_kw):
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {bronze_table} WHERE {col} < 0")
                    n = cursor.fetchone()[0]
                    if n > 0:
                        issues_found.append({
                            "issue_type": "NEGATIVE_VALUE", "column": col,
                            "count": n, "percentage": round(n/total_records*100, 1),
                            "severity": "HIGH",
                            "description": f"{n} negative values in {col} — business rule violation"
                        })
                        bad_records += n
                except:
                    pass

        # CHECK 3 — Future dates
        date_kw = ['date', 'time', 'created', 'updated']
        for col in columns:
            if any(k in col.lower() for k in date_kw):
                try:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM {bronze_table}
                        WHERE {col} > CURRENT_TIMESTAMP()
                        AND {col} IS NOT NULL
                    """)
                    n = cursor.fetchone()[0]
                    if n > 0:
                        issues_found.append({
                            "issue_type": "FUTURE_DATE", "column": col,
                            "count": n, "percentage": round(n/total_records*100, 1),
                            "severity": "MEDIUM",
                            "description": f"{n} future dates in {col} — impossible dates"
                        })
                        bad_records += n
                except:
                    pass

        # CHECK 4 — Duplicates
        if columns:
            pk = columns[0]
            try:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {pk}, COUNT(*) cnt FROM {bronze_table}
                        GROUP BY {pk} HAVING cnt > 1
                    ) d
                """)
                n = cursor.fetchone()[0]
                if n > 0:
                    issues_found.append({
                        "issue_type": "DUPLICATE_RECORDS", "column": pk,
                        "count": n, "percentage": round(n/total_records*100, 1),
                        "severity": "HIGH",
                        "description": f"{n} duplicate values in {pk}"
                    })
                    bad_records += n
            except:
                pass

        # CHECK 5 — Invalid SKU
        for col in columns:
            if 'sku' in col.lower():
                try:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM {bronze_table}
                        WHERE {col} IS NOT NULL
                        AND NOT REGEXP_LIKE({col}, '^ELF-[0-9]{{5}}$')
                    """)
                    n = cursor.fetchone()[0]
                    if n > 0:
                        issues_found.append({
                            "issue_type": "INVALID_SKU_FORMAT", "column": col,
                            "count": n, "percentage": round(n/total_records*100, 1),
                            "severity": "HIGH",
                            "description": f"{n} SKUs don't match ELF-XXXXX format"
                        })
                        bad_records += n
                except:
                    pass

        # Health score
        health_score = round(max(0, (total_records - min(bad_records, total_records)) / total_records * 100), 1)

        # Log alerts
        if issues_found:
            for issue in issues_found:
                cursor.execute(
                    "INSERT INTO ALERT_LOG (ALERT_ID, ALERT_TYPE, MESSAGE, SEVERITY, CREATED_AT) VALUES (%s, %s, %s, %s, %s)",
                    (str(uuid.uuid4())[:8], issue['issue_type'],
                     f"{bronze_table}: {issue['description']}",
                     issue['severity'], datetime.now())
                )
            conn.commit()

        ai_diagnosis = generate_ai_diagnosis(
            bronze_table, total_records, bad_records, health_score, issues_found
        )

        cursor.close()
        conn.close()

        return {
            "status": "success",
            "run_id": run_id,
            "table": bronze_table,
            "total_records": total_records,
            "issues_found": len(issues_found),
            "bad_records": bad_records,
            "health_score": health_score,
            "quality_status": "HEALTHY" if health_score >= 90 else "WARNING" if health_score >= 70 else "CRITICAL",
            "issues": issues_found,
            "ai_diagnosis": ai_diagnosis
        }

    except Exception as e:
        return {"status": "error", "run_id": run_id, "message": str(e)}

def generate_ai_diagnosis(table_name, total_records, bad_records, health_score, issues) -> str:
    try:
        issues_text = "\n".join([
            f"- {i['issue_type']} in {i['column']}: {i['description']}"
            for i in issues
        ]) if issues else "No issues found"

        prompt = f"""You are DataForge AI Quality Agent for E.L.F Beauty.
Table: {table_name}
Records: {total_records} total, {bad_records} with issues
Health Score: {health_score}/100
Issues:
{issues_text}
Write 3 sentences explaining business impact.
End with STATUS: HEALTHY / WARNING / CRITICAL"""

        response = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile",
            max_tokens=200
        )
        return response.choices[0].message.content.strip()
    except:
        return f"Quality check complete. {len(issues)} issues. Health: {health_score}/100."
    