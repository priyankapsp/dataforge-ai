
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import snowflake.connector
import os
from datetime import datetime
from connectors.csv_connector import load_csv_to_bronze

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="DataForge AI",
    description="AI-powered end-to-end data pipeline for E.L.F Beauty",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Snowflake connection
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
        raise HTTPException(status_code=500, detail=f"Snowflake connection failed: {str(e)}")

@app.get("/setup")
def setup_database():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        tables = [
            """CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
                RUN_ID VARCHAR(50),
                SOURCE_NAME VARCHAR(100),
                START_TIME TIMESTAMP,
                END_TIME TIMESTAMP,
                STATUS VARCHAR(20),
                RECORDS_FETCHED NUMBER,
                RECORDS_LOADED NUMBER,
                RECORDS_QUARANTINED NUMBER,
                ERROR_MESSAGE VARCHAR(1000),
                DURATION_SECONDS NUMBER
            )""",

            """CREATE TABLE IF NOT EXISTS QUARANTINE_RECORDS (
                QUARANTINE_ID VARCHAR(50),
                RUN_ID VARCHAR(50),
                SOURCE_NAME VARCHAR(100),
                RAW_DATA VARIANT,
                REJECTION_REASON VARCHAR(500),
                QUARANTINED_AT TIMESTAMP
            )""",

            """CREATE TABLE IF NOT EXISTS SCHEMA_CHANGES (
                CHANGE_ID VARCHAR(50),
                SOURCE_NAME VARCHAR(100),
                CHANGE_TYPE VARCHAR(50),
                COLUMN_NAME VARCHAR(100),
                OLD_VALUE VARCHAR(500),
                NEW_VALUE VARCHAR(500),
                DETECTED_AT TIMESTAMP
            )""",

            """CREATE TABLE IF NOT EXISTS VOLUME_HISTORY (
                HISTORY_ID VARCHAR(50),
                SOURCE_NAME VARCHAR(100),
                RECORD_COUNT NUMBER,
                RECORDED_AT TIMESTAMP
            )""",

            """CREATE TABLE IF NOT EXISTS QUERY_HISTORY (
                QUERY_ID VARCHAR(50),
                QUESTION VARCHAR(1000),
                GENERATED_SQL TEXT,
                EXECUTION_TIME_MS NUMBER,
                ROWS_RETURNED NUMBER,
                ASKED_AT TIMESTAMP
            )""",

            """CREATE TABLE IF NOT EXISTS ALERT_LOG (
                ALERT_ID VARCHAR(50),
                ALERT_TYPE VARCHAR(100),
                MESSAGE VARCHAR(1000),
                SEVERITY VARCHAR(20),
                CREATED_AT TIMESTAMP,
                RESOLVED_AT TIMESTAMP
            )"""
        ]

        for table_sql in tables:
            cursor.execute(table_sql)

        cursor.close()
        conn.close()

        return {
            "status": "success",
            "message": "All DataForge AI tables created successfully",
            "tables_created": [
                "PIPELINE_RUNS",
                "QUARANTINE_RECORDS",
                "SCHEMA_CHANGES",
                "VOLUME_HISTORY",
                "QUERY_HISTORY",
                "ALERT_LOG"
            ],
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Setup failed: {str(e)}")

@app.get("/status")
def pipeline_status():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) as total_runs,
                   SUM(CASE WHEN STATUS='SUCCESS' THEN 1 ELSE 0 END) as successful,
                   SUM(CASE WHEN STATUS='FAILED' THEN 1 ELSE 0 END) as failed,
                   SUM(RECORDS_LOADED) as total_records
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
    """Upload CSV or Excel file and load to Snowflake Bronze automatically"""
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
    """List all Bronze tables in Snowflake"""
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
                {
                    "table_name": t[0],
                    "row_count": t[1],
                    "created": str(t[2])
                }
                for t in tables
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipeline/runs")
def get_pipeline_runs():
    """Get last 20 pipeline runs"""
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
                    "run_id": r[0],
                    "source_name": r[1],
                    "start_time": str(r[2]),
                    "status": r[3],
                    "records_fetched": r[4],
                    "records_loaded": r[5],
                    "records_quarantined": r[6],
                    "duration_seconds": r[7],
                    "error_message": r[8]
                }
                for r in runs
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/quarantine")
def get_quarantine_records():
    """Get quarantined records"""
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
                    "quarantine_id": r[0],
                    "run_id": r[1],
                    "source_name": r[2],
                    "rejection_reason": r[3],
                    "quarantined_at": str(r[4])
                }
                for r in records
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))