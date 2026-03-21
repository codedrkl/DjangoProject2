import os
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime

# Logging setup
logging.basicConfig(
    filename='scheduler.log',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

def run_download():
    logging.info("=== Starting scheduled ES option chain download ===")
    try:
        os.system('python manage.py download_es_eod')
        logging.info("✅ Scheduled download completed successfully")
    except Exception as e:
        logging.error(f"❌ Scheduled download failed: {e}")

if __name__ == '__main__':
    scheduler = BlockingScheduler(timezone="America/Vancouver")   # ← Your city

    scheduler.add_job(
        run_download,
        trigger=CronTrigger(hour=2, minute=0),   # Every day at 2:00 AM AEST
        id='es_daily_download',
        name='Daily ES Options + Black-76 Download',
        replace_existing=True
    )

    logging.info(f"🚀 ES Option Chain Scheduler STARTED at {datetime.now()}")
    logging.info("   → Will run every day at 2:00 AM AEST")
    logging.info("   → Logs saved in scheduler.log")

    print("✅ Scheduler is now running!")
    print("   → Automatic download every night at 2:00 AM AEST")
    print("   → Close this window to stop (or minimize it)")
    print("   → Check scheduler.log for history")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped by user")
        print("\nScheduler stopped.")