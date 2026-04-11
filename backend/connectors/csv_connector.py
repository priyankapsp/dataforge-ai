
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import uuid
from datetime import datetime
import io
from groq import Groq

load_dotenv()

# Initialize Groq AI
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

def create_bronze_table(cursor, table_name: str, df: pd.DataFrame):
    """Create Bronze table automatically based on CSV columns"""
    type_map = {
        'int64':          'NUMBER',
        'float64':        'FLOAT',
        'bool':           'BOOLEAN',
        'object':         'VARCHAR(1000)',
        'datetime64[ns]': 'TIMESTAMP'
    }
    columns = []
    for col in df.columns:
        clean_col = col.upper().strip().replace(' ', '_').replace('-', '_')
        dtype = str(df[col].dtype)
        sf_type = type_map.get(dtype, 'VARCHAR(1000)')
        columns.append(f"{clean_col} {sf_type}")

    columns.append("_SOURCE VARCHAR(100)")
    columns.append("_LOADED_AT TIMESTAMP")
    columns.append("_RUN_ID VARCHAR(50)")
    columns.append("_FILE_NAME VARCHAR(500)")

    cols_sql = ",\n    ".join(columns)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS BRZ_{table_name.upper()} (
        {cols_sql}
    )
    """
    cursor.execute(create_sql)

def validate_record(row: dict) -> tuple:
    """Validate a single record"""
    non_null_count = sum(1 for v in row.values() if pd.notna(v) and str(v).strip() != '')
    if non_null_count == 0:
        return False, "COMPLETELY_EMPTY_ROW"
    null_count = sum(1 for v in row.values() if pd.isna(v) or str(v).strip() == '')
    if len(row) > 0 and null_count / len(row) > 0.8:
        return False, f"TOO_MANY_NULLS_{null_count}_of_{len(row)}_columns"
    return True, None

def verify_bronze_load(cursor, table_name: str, expected_count: int) -> dict:
    """
    AI Agent automatically verifies data loaded correctly
    Compares source count vs Snowflake count
    """
    try:
        # Count rows in Snowflake
        cursor.execute(f"SELECT COUNT(*) FROM BRZ_{table_name.upper()}")
        snowflake_count = cursor.fetchone()[0]

        # Count nulls per column
        cursor.execute(f"SELECT * FROM BRZ_{table_name.upper()} LIMIT 1")
        columns = [desc[0] for desc in cursor.description
                   if not desc[0].startswith('_')]

        null_summary = {}
        for col in columns[:10]:  # Check first 10 columns
            cursor.execute(f"""
                SELECT COUNT(*) FROM BRZ_{table_name.upper()}
                WHERE {col} IS NULL
            """)
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                null_summary[col] = null_count

        # Get column count
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'BRZ_{table_name.upper()}'
            AND TABLE_SCHEMA = 'PUBLIC'
        """)
        col_count = cursor.fetchone()[0]

        # Verify match
        match = snowflake_count >= expected_count
        missing = expected_count - snowflake_count if not match else 0

        return {
            "verified": match,
            "expected_count": expected_count,
            "snowflake_count": snowflake_count,
            "missing_records": missing,
            "column_count": col_count,
            "null_summary": null_summary
        }
    except Exception as e:
        return {
            "verified": False,
            "error": str(e)
        }

def generate_ai_summary(
    table_name: str,
    records_fetched: int,
    records_loaded: int,
    records_quarantined: int,
    health_score: float,
    verification: dict,
    null_summary: dict,
    duration: int
) -> str:
    """
    Groq AI automatically writes plain English summary
    of what happened in the pipeline
    """
    try:
        prompt = f"""
You are DataForge AI — an intelligent data pipeline agent.

A CSV file was just loaded into the Bronze layer of Snowflake.
Write a short, clear, plain English summary of what happened.

Pipeline Results:
- Table: BRZ_{table_name.upper()}
- Records fetched from source: {records_fetched}
- Records loaded to Snowflake Bronze: {records_loaded}
- Records quarantined (bad data): {records_quarantined}
- Health score: {health_score}/100
- Verification passed: {verification.get('verified', False)}
- Columns loaded: {verification.get('column_count', 0)}
- Null columns found: {null_summary if null_summary else 'None'}
- Duration: {duration} seconds

Write 3-4 sentences maximum.
Be specific with numbers.
End with pipeline status: HEALTHY or WARNING or CRITICAL.
Do not use bullet points. Plain paragraph only.
"""
        response = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile",
            max_tokens=200
        )
        return response.choices[0].message.content.strip()

    except Exception as e:
        return f"Pipeline completed. {records_loaded} records loaded to BRZ_{table_name.upper()}. Health score: {health_score}/100."

def load_csv_to_bronze(
    file_content: bytes,
    file_name: str,
    table_name: str
) -> dict:
    """
    Main function: reads CSV/Excel → validates → loads to Bronze
    → verifies → AI explains what happened
    All automatic. Zero human involvement.
    """
    run_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()

    records_fetched = 0
    records_loaded = 0
    records_quarantined = 0
    quarantine_records = []

    try:
        # ── STEP 1: Read file ──────────────────────────────
        if file_name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_content))
        elif file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(file_content))
        else:
            return {"status": "error", "message": "Only CSV and Excel files supported"}

        records_fetched = len(df)

        # ── STEP 2: Clean column names ─────────────────────
        df.columns = [
            col.upper().strip()
               .replace(' ', '_').replace('-', '_')
               .replace('/', '_').replace('(', '').replace(')', '')
            for col in df.columns
        ]

        # ── STEP 3: Connect to Snowflake ───────────────────
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # ── STEP 4: Create Bronze table automatically ──────
        create_bronze_table(cursor, table_name, df)

        # ── STEP 5: Validate every record ──────────────────
        clean_rows = []
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            is_valid, reason = validate_record(row_dict)
            if not is_valid:
                quarantine_records.append({
                    "quarantine_id": f"Q-{run_id}-{idx}",
                    "run_id": run_id,
                    "source_name": f"CSV_{table_name.upper()}",
                    "rejection_reason": reason,
                    "quarantined_at": datetime.now()
                })
                records_quarantined += 1
            else:
                row_dict['_SOURCE'] = f"CSV_{table_name.upper()}"
                row_dict['_LOADED_AT'] = datetime.now()
                row_dict['_RUN_ID'] = run_id
                row_dict['_FILE_NAME'] = file_name
                clean_rows.append(row_dict)

        # ── STEP 6: Batch insert to Bronze ─────────────────
        if clean_rows:
            cols = list(clean_rows[0].keys())
            cols_str = ", ".join(cols)
            placeholders = ", ".join(["%s"] * len(cols))
            insert_sql = f"INSERT INTO BRZ_{table_name.upper()} ({cols_str}) VALUES ({placeholders})"

            batch_size = 1000
            for i in range(0, len(clean_rows), batch_size):
                batch = clean_rows[i:i + batch_size]
                values = [
                    tuple(None if pd.isna(v) else v for v in row.values())
                    for row in batch
                ]
                cursor.executemany(insert_sql, values)

            records_loaded = len(clean_rows)

        # ── STEP 7: Save quarantine records ────────────────
        if quarantine_records:
            for q in quarantine_records:
                cursor.execute("""
                    INSERT INTO QUARANTINE_RECORDS
                    (QUARANTINE_ID, RUN_ID, SOURCE_NAME, REJECTION_REASON, QUARANTINED_AT)
                    VALUES (%s, %s, %s, %s, %s)
                """, (q['quarantine_id'], q['run_id'], q['source_name'],
                      q['rejection_reason'], q['quarantined_at']))

        # ── STEP 8: AI Agent verifies data automatically ───
        verification = verify_bronze_load(cursor, table_name, records_loaded)

        # ── STEP 9: Log pipeline run ────────────────────────
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        health_score = round((records_loaded / records_fetched * 100), 1) if records_fetched > 0 else 0

        cursor.execute("""
            INSERT INTO PIPELINE_RUNS
            (RUN_ID, SOURCE_NAME, START_TIME, END_TIME, STATUS,
             RECORDS_FETCHED, RECORDS_LOADED, RECORDS_QUARANTINED, DURATION_SECONDS)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (run_id, f"CSV_{table_name.upper()}", start_time, end_time,
              "SUCCESS", records_fetched, records_loaded, records_quarantined, duration))

        conn.commit()
        cursor.close()
        conn.close()

        # ── STEP 10: AI writes plain English summary ───────
        ai_summary = generate_ai_summary(
            table_name=table_name,
            records_fetched=records_fetched,
            records_loaded=records_loaded,
            records_quarantined=records_quarantined,
            health_score=health_score,
            verification=verification,
            null_summary=verification.get('null_summary', {}),
            duration=duration
        )

        return {
            "status": "success",
            "run_id": run_id,
            "file_name": file_name,
            "table_created": f"BRZ_{table_name.upper()}",
            "records_fetched": records_fetched,
            "records_loaded": records_loaded,
            "records_quarantined": records_quarantined,
            "health_score": health_score,
            "verification": verification,
            "duration_seconds": duration,
            "ai_summary": ai_summary,
            "message": f"Successfully loaded {records_loaded} records to BRZ_{table_name.upper()}"
        }

    except Exception as e:
        try:
            conn = get_snowflake_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO PIPELINE_RUNS
                (RUN_ID, SOURCE_NAME, START_TIME, END_TIME, STATUS,
                 RECORDS_FETCHED, RECORDS_LOADED, RECORDS_QUARANTINED,
                 ERROR_MESSAGE, DURATION_SECONDS)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (run_id, f"CSV_{table_name.upper()}", start_time, datetime.now(),
                  "FAILED", records_fetched, 0, 0, str(e)[:1000],
                  (datetime.now() - start_time).seconds))
            conn.commit()
            cursor.close()
            conn.close()
        except:
            pass

        return {"status": "error", "run_id": run_id, "message": str(e)}