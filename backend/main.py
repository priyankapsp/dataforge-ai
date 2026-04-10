
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import snowflake.connector
import os
from datetime import datetime

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
# ROUTES
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

        # Create all tracking tables
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