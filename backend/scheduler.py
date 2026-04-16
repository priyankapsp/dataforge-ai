
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataForge Scheduler")

API_BASE = "http://127.0.0.1:8000"

def run_full_pipeline():
    """
    Complete automated pipeline:
    Step 1: Sync all MySQL sources
    Step 2: Run quality checks
    Step 3: Run Silver + Gold transformations
    Step 4: Log completion
    Zero human involvement
    """
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"\n{'='*50}")
    logger.info(f"🚀 Pipeline starting — {run_time}")

    try:
        # STEP 1 — Sync MySQL sources
        logger.info("Step 1: Syncing MySQL sources...")
        r = requests.post(f"{API_BASE}/mysql/sync-all", timeout=120)
        data = r.json()
        tables_synced = data.get("tables_synced", 0)
        logger.info(f"✅ Synced {tables_synced} tables")

        # STEP 2 — Run quality checks
        logger.info("Step 2: Running quality checks...")
        r = requests.get(f"{API_BASE}/quality/check-all", timeout=120)
        data = r.json()
        tables_checked = data.get("tables_checked", 0)
        logger.info(f"✅ Quality checked {tables_checked} tables")

        # STEP 3 — Run transformations
        logger.info("Step 3: Running Silver + Gold transformations...")
        r = requests.post(f"{API_BASE}/transform/run", timeout=120)
        data = r.json()
        succeeded = data.get("models_succeeded", 0)
        failed = data.get("models_failed", 0)
        logger.info(f"✅ Transformations: {succeeded} succeeded, {failed} failed")

        logger.info(f"🎉 Pipeline complete — {datetime.now().strftime('%H:%M:%S')}")
        logger.info("="*50)

    except Exception as e:
        logger.error(f"❌ Pipeline error: {str(e)}")

def start_scheduler():
    """Start the automated pipeline scheduler"""
    scheduler = BackgroundScheduler()

    # Run every 15 minutes
    scheduler.add_job(
        func=run_full_pipeline,
        trigger=IntervalTrigger(minutes=15),
        id="full_pipeline",
        name="DataForge Full Pipeline",
        replace_existing=True
    )

    scheduler.start()
    logger.info("✅ Scheduler started — pipeline runs every 15 minutes")
    logger.info(f"Next run: {scheduler.get_jobs()[0].next_run_time}")
    return scheduler