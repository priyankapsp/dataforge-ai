
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
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        login_timeout=10,
        network_timeout=30,
    )

def run_model(cursor, table_name: str, sql: str) -> dict:
    """Run a single SQL model and return results"""
    start = datetime.now()
    try:
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        for stmt in statements:
            cursor.execute(stmt)
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        duration = (datetime.now() - start).seconds
        print(f"✅ {table_name} — {count} records ({duration}s)")
        return {
            "table": table_name,
            "status": "SUCCESS",
            "records": count,
            "duration_seconds": duration
        }
    except Exception as e:
        duration = (datetime.now() - start).seconds
        print(f"❌ {table_name} — {str(e)[:100]}")
        return {
            "table": table_name,
            "status": "FAILED",
            "records": 0,
            "duration_seconds": duration,
            "error": str(e)[:500]
        }

# ─────────────────────────────────────────
# SILVER LAYER
# ─────────────────────────────────────────

SILVER_ORDERS = """
CREATE OR REPLACE TABLE SLV_ELF_ORDERS AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ORDER_ID
            ORDER BY _LOADED_AT DESC
        ) as rn
    FROM BRZ_MYSQL_ELF_ORDERS
    WHERE ORDER_ID IS NOT NULL
)
SELECT
    ORDER_ID, STORE_ID, CUSTOMER_ID,
    ORDER_DATE, PRODUCT_SKU, QUANTITY,
    UNIT_PRICE, TOTAL_AMOUNT, ORDER_STATUS,
    _SOURCE, _LOADED_AT, _RUN_ID,
    CURRENT_TIMESTAMP() as _SILVER_LOADED_AT
FROM deduped
WHERE rn = 1
  AND STORE_ID IS NOT NULL
  AND CUSTOMER_ID IS NOT NULL
  AND ORDER_DATE IS NOT NULL
  AND PRODUCT_SKU IS NOT NULL
  AND QUANTITY > 0
  AND UNIT_PRICE > 0
  AND TOTAL_AMOUNT > 0
  AND ORDER_DATE <= CURRENT_TIMESTAMP()
  AND REGEXP_LIKE(PRODUCT_SKU, '^ELF-[0-9]{5}$')
"""

SILVER_INVENTORY = """
CREATE OR REPLACE TABLE SLV_ELF_INVENTORY AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY INVENTORY_ID
            ORDER BY _LOADED_AT DESC
        ) as rn
    FROM BRZ_MYSQL_ELF_INVENTORY
    WHERE INVENTORY_ID IS NOT NULL
)
SELECT
    INVENTORY_ID, STORE_ID, PRODUCT_SKU,
    PRODUCT_NAME, QUANTITY_ON_HAND,
    REORDER_POINT, UNIT_COST,
    _SOURCE, _LOADED_AT, _RUN_ID,
    CURRENT_TIMESTAMP() as _SILVER_LOADED_AT
FROM deduped
WHERE rn = 1
  AND PRODUCT_SKU IS NOT NULL
  AND TRIM(COALESCE(PRODUCT_NAME,'')) != ''
  AND QUANTITY_ON_HAND >= 0
  AND UNIT_COST > 0
  AND STORE_ID IS NOT NULL
"""

SILVER_STORES = """
CREATE OR REPLACE TABLE SLV_ELF_STORES AS
SELECT
    STORE_ID, STORE_NAME, CITY,
    STATE, COUNTRY, STORE_TYPE,
    MONTHLY_TARGET, IS_ACTIVE,
    _SOURCE, _LOADED_AT,
    CURRENT_TIMESTAMP() as _SILVER_LOADED_AT
FROM BRZ_MYSQL_ELF_STORES
WHERE STORE_ID IS NOT NULL
  AND STORE_NAME IS NOT NULL
  AND TRIM(COALESCE(STORE_NAME,'')) != ''
"""

# ─────────────────────────────────────────
# GOLD LAYER — EXISTING MODELS
# ─────────────────────────────────────────

GOLD_STORE_PERFORMANCE = """
CREATE OR REPLACE TABLE GOLD_STORE_PERFORMANCE AS
WITH store_metrics AS (
    SELECT
        o.STORE_ID,
        s.STORE_NAME,
        s.CITY,
        s.STATE,
        s.STORE_TYPE,
        s.MONTHLY_TARGET,
        COUNT(o.ORDER_ID)             as TOTAL_ORDERS,
        SUM(o.TOTAL_AMOUNT)           as TOTAL_REVENUE,
        AVG(o.TOTAL_AMOUNT)           as AVG_ORDER_VALUE,
        COUNT(DISTINCT o.CUSTOMER_ID) as UNIQUE_CUSTOMERS,
        SUM(o.QUANTITY)               as TOTAL_UNITS_SOLD,
        MIN(o.ORDER_DATE)             as FIRST_ORDER_DATE,
        MAX(o.ORDER_DATE)             as LAST_ORDER_DATE
    FROM SLV_ELF_ORDERS o
    JOIN SLV_ELF_STORES s ON o.STORE_ID = s.STORE_ID
    GROUP BY
        o.STORE_ID, s.STORE_NAME, s.CITY,
        s.STATE, s.STORE_TYPE, s.MONTHLY_TARGET
)
SELECT
    STORE_ID, STORE_NAME, CITY, STATE,
    STORE_TYPE, MONTHLY_TARGET,
    TOTAL_ORDERS, ROUND(TOTAL_REVENUE,2) as TOTAL_REVENUE,
    ROUND(AVG_ORDER_VALUE,2)    as AVG_ORDER_VALUE,
    UNIQUE_CUSTOMERS, TOTAL_UNITS_SOLD,
    FIRST_ORDER_DATE, LAST_ORDER_DATE,
    ROUND(TOTAL_REVENUE / NULLIF(MONTHLY_TARGET,0) * 100, 1) as TARGET_PCT,
    RANK() OVER (ORDER BY TOTAL_REVENUE DESC) as REVENUE_RANK,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM store_metrics
ORDER BY TOTAL_REVENUE DESC
"""

GOLD_PRODUCT_ANALYSIS = """
CREATE OR REPLACE TABLE GOLD_PRODUCT_ANALYSIS AS
WITH order_metrics AS (
    SELECT
        PRODUCT_SKU,
        COUNT(ORDER_ID)     as TOTAL_ORDERS,
        SUM(QUANTITY)       as TOTAL_UNITS_SOLD,
        SUM(TOTAL_AMOUNT)   as TOTAL_REVENUE,
        AVG(UNIT_PRICE)     as AVG_SELLING_PRICE
    FROM SLV_ELF_ORDERS
    GROUP BY PRODUCT_SKU
),
inventory_metrics AS (
    SELECT
        PRODUCT_SKU,
        MAX(PRODUCT_NAME)   as PRODUCT_NAME,
        SUM(QUANTITY_ON_HAND) as TOTAL_STOCK,
        AVG(UNIT_COST)      as AVG_UNIT_COST,
        MIN(REORDER_POINT)  as REORDER_POINT
    FROM SLV_ELF_INVENTORY
    GROUP BY PRODUCT_SKU
)
SELECT
    COALESCE(o.PRODUCT_SKU, i.PRODUCT_SKU)  as PRODUCT_SKU,
    i.PRODUCT_NAME,
    COALESCE(o.TOTAL_ORDERS, 0)             as TOTAL_ORDERS,
    COALESCE(o.TOTAL_UNITS_SOLD, 0)         as TOTAL_UNITS_SOLD,
    ROUND(COALESCE(o.TOTAL_REVENUE, 0), 2)  as TOTAL_REVENUE,
    ROUND(COALESCE(o.AVG_SELLING_PRICE,0),2) as AVG_SELLING_PRICE,
    COALESCE(i.TOTAL_STOCK, 0)              as CURRENT_STOCK,
    ROUND(COALESCE(i.AVG_UNIT_COST, 0), 2) as AVG_UNIT_COST,
    COALESCE(i.REORDER_POINT, 0)            as REORDER_POINT,
    ROUND(COALESCE(o.AVG_SELLING_PRICE,0) -
          COALESCE(i.AVG_UNIT_COST,0), 2)   as UNIT_MARGIN,
    ROUND((COALESCE(o.AVG_SELLING_PRICE,0) -
           COALESCE(i.AVG_UNIT_COST,0)) /
           NULLIF(o.AVG_SELLING_PRICE,0)*100,1) as MARGIN_PCT,
    CASE
        WHEN COALESCE(i.TOTAL_STOCK,0) = 0 THEN 'OUT_OF_STOCK'
        WHEN COALESCE(i.TOTAL_STOCK,0) <= COALESCE(i.REORDER_POINT,0) THEN 'LOW_STOCK'
        ELSE 'IN_STOCK'
    END as STOCK_STATUS,
    RANK() OVER (ORDER BY COALESCE(o.TOTAL_REVENUE,0) DESC) as REVENUE_RANK,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM order_metrics o
FULL OUTER JOIN inventory_metrics i ON o.PRODUCT_SKU = i.PRODUCT_SKU
ORDER BY TOTAL_REVENUE DESC
"""

GOLD_DAILY_REVENUE = """
CREATE OR REPLACE TABLE GOLD_DAILY_REVENUE AS
WITH daily AS (
    SELECT
        DATE_TRUNC('DAY', ORDER_DATE)   as ORDER_DAY,
        COUNT(ORDER_ID)                 as TOTAL_ORDERS,
        SUM(TOTAL_AMOUNT)               as DAILY_REVENUE,
        AVG(TOTAL_AMOUNT)               as AVG_ORDER_VALUE,
        COUNT(DISTINCT STORE_ID)        as ACTIVE_STORES,
        COUNT(DISTINCT CUSTOMER_ID)     as UNIQUE_CUSTOMERS,
        SUM(QUANTITY)                   as TOTAL_UNITS
    FROM SLV_ELF_ORDERS
    GROUP BY DATE_TRUNC('DAY', ORDER_DATE)
)
SELECT
    ORDER_DAY,
    TOTAL_ORDERS,
    ROUND(DAILY_REVENUE, 2)             as DAILY_REVENUE,
    ROUND(AVG_ORDER_VALUE, 2)           as AVG_ORDER_VALUE,
    ACTIVE_STORES,
    UNIQUE_CUSTOMERS,
    TOTAL_UNITS,
    ROUND(SUM(DAILY_REVENUE) OVER (
        ORDER BY ORDER_DAY
        ROWS UNBOUNDED PRECEDING
    ), 2)                               as CUMULATIVE_REVENUE,
    ROUND(LAG(DAILY_REVENUE,1) OVER (
        ORDER BY ORDER_DAY
    ), 2)                               as PREV_DAY_REVENUE,
    ROUND((DAILY_REVENUE - LAG(DAILY_REVENUE,1) OVER (
        ORDER BY ORDER_DAY)) /
        NULLIF(LAG(DAILY_REVENUE,1) OVER (
        ORDER BY ORDER_DAY),0)*100, 1)  as DAY_OVER_DAY_GROWTH_PCT,
    CURRENT_TIMESTAMP()                 as _GOLD_LOADED_AT
FROM daily
ORDER BY ORDER_DAY DESC
"""

GOLD_INVENTORY_STATUS = """
CREATE OR REPLACE TABLE GOLD_INVENTORY_STATUS AS
SELECT
    i.INVENTORY_ID,
    i.STORE_ID,
    s.STORE_NAME,
    s.CITY,
    i.PRODUCT_SKU,
    i.PRODUCT_NAME,
    i.QUANTITY_ON_HAND,
    i.REORDER_POINT,
    i.UNIT_COST,
    ROUND(i.QUANTITY_ON_HAND * i.UNIT_COST, 2) as STOCK_VALUE,
    CASE
        WHEN i.QUANTITY_ON_HAND = 0 THEN 'OUT_OF_STOCK'
        WHEN i.QUANTITY_ON_HAND <= i.REORDER_POINT THEN 'REORDER_NOW'
        WHEN i.QUANTITY_ON_HAND <= i.REORDER_POINT * 1.5 THEN 'LOW_STOCK'
        ELSE 'HEALTHY'
    END as STOCK_ALERT,
    CASE
        WHEN i.QUANTITY_ON_HAND = 0 THEN 'CRITICAL'
        WHEN i.QUANTITY_ON_HAND <= i.REORDER_POINT THEN 'HIGH'
        WHEN i.QUANTITY_ON_HAND <= i.REORDER_POINT * 1.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END as ALERT_SEVERITY,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM SLV_ELF_INVENTORY i
JOIN SLV_ELF_STORES s ON i.STORE_ID = s.STORE_ID
ORDER BY
    CASE WHEN i.QUANTITY_ON_HAND = 0 THEN 1
         WHEN i.QUANTITY_ON_HAND <= i.REORDER_POINT THEN 2
         ELSE 3 END,
    i.QUANTITY_ON_HAND ASC
"""

# SCD Type 2 for product price history
SCD2_PRODUCTS_SETUP = """
CREATE TABLE IF NOT EXISTS SCD2_ELF_PRODUCTS (
    SURROGATE_KEY   VARCHAR(50),
    PRODUCT_SKU     VARCHAR(50),
    PRODUCT_NAME    VARCHAR(200),
    CATEGORY        VARCHAR(100),
    PRICE           FLOAT,
    STOCK_QUANTITY  NUMBER,
    VALID_FROM      TIMESTAMP,
    VALID_TO        TIMESTAMP,
    IS_CURRENT      BOOLEAN DEFAULT TRUE,
    _SOURCE         VARCHAR(100),
    _LOADED_AT      TIMESTAMP
)
"""

# ─────────────────────────────────────────
# GOLD LAYER — ELF BEAUTY SPECIFIC MODELS
# ─────────────────────────────────────────

GOLD_ELF_REVENUE_BY_CHANNEL = """
CREATE OR REPLACE TABLE GOLD_ELF_REVENUE_BY_CHANNEL AS
SELECT
    s.STORE_TYPE as CHANNEL,
    COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS,
    SUM(o.TOTAL_AMOUNT) as TOTAL_REVENUE,
    AVG(o.TOTAL_AMOUNT) as AVG_ORDER_VALUE,
    COUNT(DISTINCT o.CUSTOMER_ID) as UNIQUE_CUSTOMERS,
    SUM(o.QUANTITY) as TOTAL_UNITS_SOLD,
    ROUND(SUM(o.TOTAL_AMOUNT) / NULLIF(
        SUM(SUM(o.TOTAL_AMOUNT)) OVER (), 0
    ) * 100, 2) as REVENUE_SHARE_PCT,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM SLV_ELF_ORDERS o
JOIN SLV_ELF_STORES s ON o.STORE_ID = s.STORE_ID
GROUP BY s.STORE_TYPE
ORDER BY TOTAL_REVENUE DESC
"""

GOLD_ELF_TARIFF_IMPACT = """
CREATE OR REPLACE TABLE GOLD_ELF_TARIFF_IMPACT AS
SELECT
    s.COUNTRY,
    s.STORE_TYPE,
    COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS,
    SUM(o.TOTAL_AMOUNT) as TOTAL_REVENUE,
    AVG(o.TOTAL_AMOUNT) as AVG_ORDER_VALUE,
    ROUND(SUM(o.TOTAL_AMOUNT) * 0.15, 2) as ESTIMATED_TARIFF_COST,
    ROUND(SUM(o.TOTAL_AMOUNT) * 0.85, 2) as REVENUE_AFTER_TARIFF,
    ROUND(SUM(o.TOTAL_AMOUNT) * 0.60, 2) as HIGH_TARIFF_SCENARIO,
    ROUND(SUM(o.TOTAL_AMOUNT) * 0.40, 2) as REVENUE_AFTER_HIGH_TARIFF,
    CASE
        WHEN s.COUNTRY = 'USA' THEN 'HIGH_RISK'
        WHEN s.COUNTRY = 'CHINA' THEN 'CRITICAL_RISK'
        ELSE 'LOW_RISK'
    END as TARIFF_RISK_LEVEL,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM SLV_ELF_ORDERS o
JOIN SLV_ELF_STORES s ON o.STORE_ID = s.STORE_ID
GROUP BY s.COUNTRY, s.STORE_TYPE
ORDER BY TOTAL_REVENUE DESC
"""

GOLD_ELF_STORE_PERFORMANCE = """
CREATE OR REPLACE TABLE GOLD_ELF_STORE_PERFORMANCE AS
SELECT
    s.STORE_ID,
    s.STORE_NAME,
    s.CITY,
    s.STATE,
    s.COUNTRY,
    s.STORE_TYPE,
    s.MONTHLY_TARGET,
    COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS,
    SUM(o.TOTAL_AMOUNT) as ACTUAL_REVENUE,
    ROUND(s.MONTHLY_TARGET - SUM(o.TOTAL_AMOUNT), 2) as TARGET_GAP,
    ROUND(SUM(o.TOTAL_AMOUNT) / NULLIF(s.MONTHLY_TARGET, 0) * 100, 2) as TARGET_ACHIEVEMENT_PCT,
    COUNT(DISTINCT o.CUSTOMER_ID) as UNIQUE_CUSTOMERS,
    AVG(o.TOTAL_AMOUNT) as AVG_ORDER_VALUE,
    RANK() OVER (ORDER BY SUM(o.TOTAL_AMOUNT) DESC) as REVENUE_RANK,
    CASE
        WHEN SUM(o.TOTAL_AMOUNT) >= s.MONTHLY_TARGET THEN 'ON_TRACK'
        WHEN SUM(o.TOTAL_AMOUNT) >= s.MONTHLY_TARGET * 0.8 THEN 'AT_RISK'
        ELSE 'BELOW_TARGET'
    END as PERFORMANCE_STATUS,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM SLV_ELF_STORES s
LEFT JOIN SLV_ELF_ORDERS o ON s.STORE_ID = o.STORE_ID
GROUP BY
    s.STORE_ID, s.STORE_NAME, s.CITY,
    s.STATE, s.COUNTRY, s.STORE_TYPE,
    s.MONTHLY_TARGET
ORDER BY ACTUAL_REVENUE DESC NULLS LAST
"""

GOLD_ELF_MARKETING_ROI = """
CREATE OR REPLACE TABLE GOLD_ELF_MARKETING_ROI AS
SELECT
    s.STORE_TYPE as CHANNEL,
    s.STATE as REGION,
    s.COUNTRY,
    COUNT(DISTINCT o.CUSTOMER_ID) as TOTAL_CUSTOMERS,
    COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS,
    SUM(o.TOTAL_AMOUNT) as TOTAL_REVENUE,
    AVG(o.TOTAL_AMOUNT) as AVG_ORDER_VALUE,
    ROUND(COUNT(DISTINCT o.ORDER_ID) /
        NULLIF(COUNT(DISTINCT o.CUSTOMER_ID), 0), 2) as ORDERS_PER_CUSTOMER,
    ROUND(SUM(o.TOTAL_AMOUNT) /
        NULLIF(COUNT(DISTINCT o.CUSTOMER_ID), 0), 2) as REVENUE_PER_CUSTOMER,
    CASE
        WHEN AVG(o.TOTAL_AMOUNT) >= 100 THEN 'HIGH_VALUE_CHANNEL'
        WHEN AVG(o.TOTAL_AMOUNT) >= 50 THEN 'MEDIUM_VALUE_CHANNEL'
        ELSE 'LOW_VALUE_CHANNEL'
    END as CHANNEL_TIER,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM SLV_ELF_ORDERS o
JOIN SLV_ELF_STORES s ON o.STORE_ID = s.STORE_ID
GROUP BY s.STORE_TYPE, s.STATE, s.COUNTRY
ORDER BY TOTAL_REVENUE DESC
"""

GOLD_ELF_PRODUCT_MARGINS = """
CREATE OR REPLACE TABLE GOLD_ELF_PRODUCT_MARGINS AS
SELECT
    o.PRODUCT_SKU,
    MAX(i.PRODUCT_NAME) as PRODUCT_NAME,
    COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS,
    SUM(o.QUANTITY) as TOTAL_UNITS_SOLD,
    AVG(o.UNIT_PRICE) as AVG_SELLING_PRICE,
    AVG(i.UNIT_COST) as AVG_UNIT_COST,
    SUM(o.TOTAL_AMOUNT) as TOTAL_REVENUE,
    ROUND(AVG(o.UNIT_PRICE) - AVG(i.UNIT_COST), 2) as GROSS_MARGIN_PER_UNIT,
    ROUND((AVG(o.UNIT_PRICE) - AVG(i.UNIT_COST)) /
        NULLIF(AVG(o.UNIT_PRICE), 0) * 100, 2) as GROSS_MARGIN_PCT,
    SUM(o.QUANTITY * (o.UNIT_PRICE - COALESCE(i.UNIT_COST, 0))) as TOTAL_GROSS_PROFIT,
    CASE
        WHEN SUM(o.TOTAL_AMOUNT) >= 10000 THEN 'HERO_PRODUCT'
        WHEN SUM(o.TOTAL_AMOUNT) >= 5000 THEN 'STRONG_PRODUCT'
        WHEN SUM(o.TOTAL_AMOUNT) >= 1000 THEN 'GROWING_PRODUCT'
        ELSE 'WEAK_PRODUCT'
    END as PRODUCT_TIER,
    RANK() OVER (ORDER BY SUM(o.TOTAL_AMOUNT) DESC) as REVENUE_RANK,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM SLV_ELF_ORDERS o
LEFT JOIN SLV_ELF_INVENTORY i ON o.PRODUCT_SKU = i.PRODUCT_SKU
GROUP BY o.PRODUCT_SKU
ORDER BY TOTAL_REVENUE DESC
"""

GOLD_ELF_CUSTOMER_360 = """
CREATE OR REPLACE TABLE GOLD_ELF_CUSTOMER_360 AS
SELECT
    o.CUSTOMER_ID,
    COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS,
    SUM(o.TOTAL_AMOUNT) as LIFETIME_VALUE,
    AVG(o.TOTAL_AMOUNT) as AVG_ORDER_VALUE,
    MIN(o.ORDER_DATE) as FIRST_ORDER_DATE,
    MAX(o.ORDER_DATE) as LAST_ORDER_DATE,
    DATEDIFF('day', MIN(o.ORDER_DATE), MAX(o.ORDER_DATE)) as CUSTOMER_AGE_DAYS,
    COUNT(DISTINCT o.PRODUCT_SKU) as UNIQUE_PRODUCTS_BOUGHT,
    COUNT(DISTINCT o.STORE_ID) as STORES_VISITED,
    SUM(o.QUANTITY) as TOTAL_UNITS_PURCHASED,
    CASE
        WHEN SUM(o.TOTAL_AMOUNT) >= 10000 THEN 'VIP'
        WHEN SUM(o.TOTAL_AMOUNT) >= 5000 THEN 'LOYAL'
        WHEN SUM(o.TOTAL_AMOUNT) >= 1000 THEN 'REGULAR'
        ELSE 'NEW'
    END as CUSTOMER_SEGMENT,
    CASE
        WHEN DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_DATE()) <= 30
            THEN 'ACTIVE'
        WHEN DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_DATE()) <= 90
            THEN 'AT_RISK'
        ELSE 'CHURNED'
    END as ACTIVITY_STATUS,
    RANK() OVER (ORDER BY SUM(o.TOTAL_AMOUNT) DESC) as VALUE_RANK,
    CURRENT_TIMESTAMP() as _GOLD_LOADED_AT
FROM SLV_ELF_ORDERS o
GROUP BY o.CUSTOMER_ID
ORDER BY LIFETIME_VALUE DESC
"""

# ─────────────────────────────────────────
# MAIN ORCHESTRATION
# ─────────────────────────────────────────

def run_all_transformations() -> dict:
    """
    Run complete Bronze → Silver → Gold pipeline
    Includes all ELF Beauty specific Gold models
    AI explains every step
    """
    run_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    results = []
    errors = []

    # Models in correct dependency order
    # Silver first, then existing Gold, then ELF Gold
    models = [
        # ── SILVER LAYER ──
        ("SILVER", "SLV_ELF_STORES",                SILVER_STORES),
        ("SILVER", "SLV_ELF_ORDERS",                SILVER_ORDERS),
        ("SILVER", "SLV_ELF_INVENTORY",             SILVER_INVENTORY),

        # ── EXISTING GOLD LAYER ──
        ("GOLD",   "GOLD_STORE_PERFORMANCE",        GOLD_STORE_PERFORMANCE),
        ("GOLD",   "GOLD_PRODUCT_ANALYSIS",         GOLD_PRODUCT_ANALYSIS),
        ("GOLD",   "GOLD_DAILY_REVENUE",            GOLD_DAILY_REVENUE),
        ("GOLD",   "GOLD_INVENTORY_STATUS",         GOLD_INVENTORY_STATUS),

        # ── ELF BEAUTY GOLD LAYER ──
        ("GOLD",   "GOLD_ELF_REVENUE_BY_CHANNEL",   GOLD_ELF_REVENUE_BY_CHANNEL),
        ("GOLD",   "GOLD_ELF_TARIFF_IMPACT",        GOLD_ELF_TARIFF_IMPACT),
        ("GOLD",   "GOLD_ELF_STORE_PERFORMANCE",    GOLD_ELF_STORE_PERFORMANCE),
        ("GOLD",   "GOLD_ELF_MARKETING_ROI",        GOLD_ELF_MARKETING_ROI),
        ("GOLD",   "GOLD_ELF_PRODUCT_MARGINS",      GOLD_ELF_PRODUCT_MARGINS),
        ("GOLD",   "GOLD_ELF_CUSTOMER_360",         GOLD_ELF_CUSTOMER_360),
    ]

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Setup SCD2 table if not exists
        cursor.execute(SCD2_PRODUCTS_SETUP)

        print(f"\n🚀 DataForge AI Transform Pipeline — Run {run_id}")
        print("=" * 60)
        print(f"Running {len(models)} models (3 Silver + 4 Gold + 6 ELF Gold)")
        print("=" * 60)

        for layer, table_name, sql in models:
            result = run_model(cursor, table_name, sql)
            result["layer"] = layer
            if result["status"] == "SUCCESS":
                results.append(result)
            else:
                errors.append(result)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        return {"status": "error", "run_id": run_id, "message": str(e)}

    total_duration = (datetime.now() - start_time).seconds
    total_records = sum(r["records"] for r in results)

    print(f"\n{'='*60}")
    print(f"✅ Complete: {len(results)} models | {total_records} records | {total_duration}s")

    ai_summary = generate_summary(results, errors, total_duration)

    return {
        "status": "success" if not errors else "partial",
        "run_id": run_id,
        "total_duration_seconds": total_duration,
        "models_succeeded": len(results),
        "models_failed": len(errors),
        "silver_tables": [r for r in results if r["layer"] == "SILVER"],
        "gold_tables": [r for r in results if r["layer"] == "GOLD"],
        "errors": errors,
        "ai_summary": ai_summary
    }

def get_transformation_status() -> dict:
    """Get current status of all Silver and Gold tables"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        tables = [
            # Silver
            ("SILVER", "SLV_ELF_STORES"),
            ("SILVER", "SLV_ELF_ORDERS"),
            ("SILVER", "SLV_ELF_INVENTORY"),
            # Existing Gold
            ("GOLD",   "GOLD_STORE_PERFORMANCE"),
            ("GOLD",   "GOLD_PRODUCT_ANALYSIS"),
            ("GOLD",   "GOLD_DAILY_REVENUE"),
            ("GOLD",   "GOLD_INVENTORY_STATUS"),
            # ELF Gold
            ("GOLD",   "GOLD_ELF_REVENUE_BY_CHANNEL"),
            ("GOLD",   "GOLD_ELF_TARIFF_IMPACT"),
            ("GOLD",   "GOLD_ELF_STORE_PERFORMANCE"),
            ("GOLD",   "GOLD_ELF_MARKETING_ROI"),
            ("GOLD",   "GOLD_ELF_PRODUCT_MARGINS"),
            ("GOLD",   "GOLD_ELF_CUSTOMER_360"),
        ]

        status = []
        for layer, table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                status.append({
                    "table": table,
                    "layer": layer,
                    "exists": True,
                    "record_count": count
                })
            except:
                status.append({
                    "table": table,
                    "layer": layer,
                    "exists": False,
                    "record_count": 0
                })

        cursor.close()
        conn.close()
        return {"tables": status}

    except Exception as e:
        return {"error": str(e)}

def generate_summary(results, errors, duration) -> str:
    """AI explains what transformations ran"""
    try:
        success_tables = [r["table"] for r in results]
        total_records = sum(r["records"] for r in results)
        elf_gold = [r for r in results if "ELF" in r["table"]]

        prompt = f"""You are DataForge AI transform engine for E.L.F Beauty.

Pipeline completed:
- Models succeeded: {len(results)} — {success_tables}
- Models failed: {len(errors)}
- ELF Gold models built: {len(elf_gold)}
- Total records across all tables: {total_records}
- Duration: {duration} seconds

Write 4 sentences:
1. What Silver cleaned (bad records removed)
2. What existing Gold models built
3. What new ELF Gold models built (Revenue by Channel, Tariff Impact,
   Store Performance, Marketing ROI, Product Margins, Customer 360)
4. What ELF Beauty CEO can now query and decide

Be specific about E.L.F Beauty business value.
End with: STATUS: COMPLETE or PARTIAL"""

        response = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile",
            max_tokens=250
        )
        return response.choices[0].message.content.strip()
    except:
        return (
            f"Transform complete. {len(results)} models built including "
            f"6 ELF Beauty Gold models. "
            f"{sum(r['records'] for r in results)} total records processed."
        )