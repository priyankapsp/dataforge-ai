
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import snowflake.connector
import os
from datetime import datetime
from connectors.csv_connector import load_csv_to_bronze
from connectors.mysql_connector import sync_mysql_table_to_bronze, get_mysql_tables
from agents.quality_agent import run_quality_checks
from agents.query_agent import run_ai_query, get_query_history, get_available_tables

load_dotenv()

app = FastAPI(
    title="DataForge AI",
    description="AI-powered end-to-end data pipeline for E.L.F Beauty",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

# ─────────────────────────────────────────
# BASIC ROUTES
# ─────────────────────────────────────────

@app.get("/")
def root():
    return {
        "product": "DataForge AI",
        "version": "2.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
def health_check():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return {
            "status": "healthy",
            "snowflake": "connected",
            "user": result[0],
            "database": result[1],
            "warehouse": result[2],
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/setup")
def setup_database():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        tables = [
            "CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (RUN_ID VARCHAR(50), SOURCE_NAME VARCHAR(100), START_TIME TIMESTAMP, END_TIME TIMESTAMP, STATUS VARCHAR(20), RECORDS_FETCHED NUMBER, RECORDS_LOADED NUMBER, RECORDS_QUARANTINED NUMBER, ERROR_MESSAGE VARCHAR(1000), DURATION_SECONDS NUMBER)",
            "CREATE TABLE IF NOT EXISTS QUARANTINE_RECORDS (QUARANTINE_ID VARCHAR(50), RUN_ID VARCHAR(50), SOURCE_NAME VARCHAR(100), RAW_DATA VARIANT, REJECTION_REASON VARCHAR(500), QUARANTINED_AT TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS SCHEMA_CHANGES (CHANGE_ID VARCHAR(50), SOURCE_NAME VARCHAR(100), CHANGE_TYPE VARCHAR(50), COLUMN_NAME VARCHAR(100), OLD_VALUE VARCHAR(500), NEW_VALUE VARCHAR(500), DETECTED_AT TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS VOLUME_HISTORY (HISTORY_ID VARCHAR(50), SOURCE_NAME VARCHAR(100), RECORD_COUNT NUMBER, RECORDED_AT TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS QUERY_HISTORY (QUERY_ID VARCHAR(50), QUESTION VARCHAR(1000), GENERATED_SQL TEXT, EXECUTION_TIME_MS NUMBER, ROWS_RETURNED NUMBER, ASKED_AT TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS ALERT_LOG (ALERT_ID VARCHAR(50), ALERT_TYPE VARCHAR(100), MESSAGE VARCHAR(1000), SEVERITY VARCHAR(20), CREATED_AT TIMESTAMP, RESOLVED_AT TIMESTAMP)"
        ]
        for t in tables:
            cursor.execute(t)
        cursor.close()
        conn.close()
        return {
            "status": "success",
            "message": "All tables created",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status")
def pipeline_status():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN STATUS='SUCCESS' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN STATUS='FAILED' THEN 1 ELSE 0 END),
                   SUM(RECORDS_LOADED)
            FROM PIPELINE_RUNS
        """)
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return {
            "total_pipeline_runs": result[0] or 0,
            "successful_runs": result[1] or 0,
            "failed_runs": result[2] or 0,
            "total_records_loaded": result[3] or 0,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────
# CSV CONNECTOR ROUTES
# ─────────────────────────────────────────

@app.post("/upload/csv")
async def upload_csv(
    file: UploadFile = File(...),
    table_name: str = "ELF_DATA"
):
    try:
        contents = await file.read()
        result = load_csv_to_bronze(
            file_content=contents,
            file_name=file.filename,
            table_name=table_name
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/bronze/tables")
def list_bronze_tables():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME, ROW_COUNT, CREATED
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'PUBLIC'
            AND TABLE_NAME LIKE 'BRZ_%'
            ORDER BY CREATED DESC
        """)
        tables = cursor.fetchall()
        cursor.close()
        conn.close()
        return {
            "bronze_tables": [
                {"table_name": t[0], "row_count": t[1], "created": str(t[2])}
                for t in tables
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipeline/runs")
def get_pipeline_runs():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT RUN_ID, SOURCE_NAME, START_TIME, STATUS,
                   RECORDS_FETCHED, RECORDS_LOADED, RECORDS_QUARANTINED,
                   DURATION_SECONDS, ERROR_MESSAGE
            FROM PIPELINE_RUNS
            ORDER BY START_TIME DESC
            LIMIT 20
        """)
        runs = cursor.fetchall()
        cursor.close()
        conn.close()
        return {
            "pipeline_runs": [
                {
                    "run_id": r[0], "source_name": r[1],
                    "start_time": str(r[2]), "status": r[3],
                    "records_fetched": r[4], "records_loaded": r[5],
                    "records_quarantined": r[6], "duration_seconds": r[7],
                    "error_message": r[8]
                }
                for r in runs
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/quarantine")
def get_quarantine_records():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT QUARANTINE_ID, RUN_ID, SOURCE_NAME,
                   REJECTION_REASON, QUARANTINED_AT
            FROM QUARANTINE_RECORDS
            ORDER BY QUARANTINED_AT DESC
            LIMIT 50
        """)
        records = cursor.fetchall()
        cursor.close()
        conn.close()
        return {
            "quarantine_records": [
                {
                    "quarantine_id": r[0], "run_id": r[1],
                    "source_name": r[2], "rejection_reason": r[3],
                    "quarantined_at": str(r[4])
                }
                for r in records
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────
# MYSQL CONNECTOR ROUTES
# ─────────────────────────────────────────

@app.get("/mysql/tables")
def list_mysql_tables():
    try:
        tables = get_mysql_tables()
        return {"status": "success", "mysql_tables": tables, "count": len(tables)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mysql/sync/{table_name}")
def sync_mysql_table(table_name: str):
    try:
        result = sync_mysql_table_to_bronze(table_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mysql/sync-all")
def sync_all_mysql_tables():
    try:
        tables = get_mysql_tables()
        results = [sync_mysql_table_to_bronze(t) for t in tables]
        return {
            "status": "success",
            "tables_synced": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────
# QUALITY AGENT ROUTES
# ─────────────────────────────────────────

@app.get("/quality/check/{bronze_table}")
def quality_check_table(bronze_table: str):
    try:
        result = run_quality_checks(bronze_table.upper(), bronze_table)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/quality/check-all")
def quality_check_all():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'PUBLIC'
            AND TABLE_NAME LIKE 'BRZ_%'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        results = [run_quality_checks(t, t) for t in tables]
        return {
            "status": "success",
            "tables_checked": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/alerts")
def get_alerts():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ALERT_ID, ALERT_TYPE, MESSAGE, SEVERITY, CREATED_AT
            FROM ALERT_LOG
            ORDER BY CREATED_AT DESC
            LIMIT 50
        """)
        alerts = cursor.fetchall()
        cursor.close()
        conn.close()
        return {
            "alerts": [
                {
                    "alert_id": a[0], "alert_type": a[1],
                    "message": a[2], "severity": a[3],
                    "created_at": str(a[4])
                }
                for a in alerts
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────
# AI QUERY ENGINE ROUTES
# ─────────────────────────────────────────

@app.post("/query/ask")
def ask_question(payload: dict):
    """CEO asks any question in plain English — AI answers automatically"""
    try:
        question = payload.get("question", "")
        if not question:
            raise HTTPException(status_code=400, detail="Question is required")
        result = run_ai_query(question)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/history")
def query_history():
    """Get past AI queries"""
    try:
        return {"queries": get_query_history()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/tables")
def available_tables():
    """Get all available tables for querying"""
    try:
        tables = get_available_tables()
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))