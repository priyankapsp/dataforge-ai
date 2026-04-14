
import mysql.connector
import snowflake.connector
from dotenv import load_dotenv
import os
import uuid
from datetime import datetime
from groq import Groq

load_dotenv()

groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def get_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def get_mysql_tables() -> list:
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables

def sync_mysql_table_to_bronze(table_name: str) -> dict:
    run_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    records_fetched = 0
    records_loaded = 0
    records_quarantined = 0

    try:
        # ── STEP 1: Extract from MySQL ─────────────────────
        mysql_conn = get_mysql_connection()
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # Get column info
        mysql_cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns_info = mysql_cursor.fetchall()

        # Get all data
        mysql_cursor.execute(f"SELECT * FROM {table_name}")
        rows = mysql_cursor.fetchall()
        records_fetched = len(rows)
        mysql_cursor.close()
        mysql_conn.close()

        if records_fetched == 0:
            return {
                "status": "success",
                "run_id": run_id,
                "source": f"MySQL_{table_name}",
                "bronze_table": f"BRZ_MYSQL_{table_name.upper()}",
                "records_fetched": 0,
                "records_loaded": 0,
                "message": "Table is empty"
            }

        # ── STEP 2: Build Snowflake column definitions ─────
        type_map = {
            'int':       'NUMBER',
            'bigint':    'NUMBER',
            'smallint':  'NUMBER',
            'tinyint':   'NUMBER',
            'float':     'FLOAT',
            'double':    'FLOAT',
            'decimal':   'FLOAT',
            'varchar':   'VARCHAR(1000)',
            'char':      'VARCHAR(500)',
            'text':      'VARCHAR(5000)',
            'longtext':  'VARCHAR(10000)',
            'datetime':  'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'date':      'DATE',
            'boolean':   'BOOLEAN',
        }

        col_defs = []
        col_names = []
        for col in columns_info:
            col_name = col['Field'].upper()
            col_type_raw = col['Type'].lower()
            col_type = col_type_raw.split('(')[0]
            sf_type = type_map.get(col_type, 'VARCHAR(1000)')
            col_defs.append(f"{col_name} {sf_type}")
            col_names.append(col['Field'])

        col_defs.append("_SOURCE VARCHAR(100)")
        col_defs.append("_LOADED_AT TIMESTAMP")
        col_defs.append("_RUN_ID VARCHAR(50)")

        # ── STEP 3: Connect to Snowflake ───────────────────
        sf_conn = get_snowflake_connection()
        sf_cursor = sf_conn.cursor()

        # ── STEP 4: Create Bronze table ────────────────────
        bronze_table = f"BRZ_MYSQL_{table_name.upper()}"
        cols_sql = ",\n    ".join(col_defs)
        sf_cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {bronze_table} (
                {cols_sql}
            )
        """)

        # ── STEP 5: Insert records ─────────────────────────
        sf_col_names = [c.upper() for c in col_names] + \
                       ['_SOURCE', '_LOADED_AT', '_RUN_ID']
        cols_str = ", ".join(sf_col_names)
        placeholders = ", ".join(["%s"] * len(sf_col_names))
        insert_sql = f"""
            INSERT INTO {bronze_table} ({cols_str})
            VALUES ({placeholders})
        """

        batch_values = []
        for row in rows:
            # Skip completely empty rows
            values = list(row.values())
            non_null = sum(1 for v in values if v is not None)
            if non_null == 0:
                records_quarantined += 1
                continue

            # Convert types for Snowflake compatibility
            clean_values = []
            for v in values:
                if isinstance(v, bytearray):
                    clean_values.append(int.from_bytes(v, 'big'))
                elif hasattr(v, 'isoformat'):
                    clean_values.append(str(v))
                else:
                    clean_values.append(v)

            clean_values.append(f"MYSQL_{table_name.upper()}")
            clean_values.append(datetime.now())
            clean_values.append(run_id)
            batch_values.append(tuple(clean_values))

        # Batch insert
        if batch_values:
            batch_size = 1000
            for i in range(0, len(batch_values), batch_size):
                batch = batch_values[i:i + batch_size]
                sf_cursor.executemany(insert_sql, batch)
            records_loaded = len(batch_values)

        # ── STEP 6: Log pipeline run ────────────────────────
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        health_score = round(
            (records_loaded / records_fetched * 100), 1
        ) if records_fetched > 0 else 0

        sf_cursor.execute("""
            INSERT INTO PIPELINE_RUNS
            (RUN_ID, SOURCE_NAME, START_TIME, END_TIME, STATUS,
             RECORDS_FETCHED, RECORDS_LOADED, RECORDS_QUARANTINED,
             DURATION_SECONDS)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id, f"MYSQL_{table_name.upper()}",
            start_time, end_time, "SUCCESS",
            records_fetched, records_loaded,
            records_quarantined, duration
        ))

        sf_conn.commit()
        sf_cursor.close()
        sf_conn.close()

        # ── STEP 7: AI Summary ──────────────────────────────
        ai_summary = generate_ai_summary(
            table_name, records_fetched,
            records_loaded, health_score, duration
        )

        return {
            "status": "success",
            "run_id": run_id,
            "source": f"MySQL → {table_name}",
            "bronze_table": bronze_table,
            "records_fetched": records_fetched,
            "records_loaded": records_loaded,
            "records_quarantined": records_quarantined,
            "health_score": health_score,
            "duration_seconds": duration,
            "ai_summary": ai_summary
        }

    except Exception as e:
        try:
            sf_conn = get_snowflake_connection()
            sf_cursor = sf_conn.cursor()
            sf_cursor.execute("""
                INSERT INTO PIPELINE_RUNS
                (RUN_ID, SOURCE_NAME, START_TIME, END_TIME,
                 STATUS, RECORDS_FETCHED, RECORDS_LOADED,
                 RECORDS_QUARANTINED, ERROR_MESSAGE, DURATION_SECONDS)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id, f"MYSQL_{table_name.upper()}",
                start_time, datetime.now(), "FAILED",
                records_fetched, 0, 0, str(e)[:1000],
                (datetime.now() - start_time).seconds
            ))
            sf_conn.commit()
            sf_cursor.close()
            sf_conn.close()
        except:
            pass

        return {
            "status": "error",
            "run_id": run_id,
            "table": table_name,
            "message": str(e)
        }

def generate_ai_summary(
    table_name, records_fetched,
    records_loaded, health_score, duration
) -> str:
    try:
        prompt = f"""
You are DataForge AI. A MySQL table was synced to Snowflake Bronze.
Write 2-3 sentences summary.
Table: {table_name}
Records: {records_fetched} fetched, {records_loaded} loaded
Health: {health_score}/100
Duration: {duration}s
End with: HEALTHY, WARNING, or CRITICAL.
"""
        response = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile",
            max_tokens=150
        )
        return response.choices[0].message.content.strip()
    except:
        return f"MySQL {table_name} synced. {records_loaded}/{records_fetched} records loaded. Health: {health_score}/100."