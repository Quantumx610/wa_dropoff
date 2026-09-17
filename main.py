import logging
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from kafka_cosmos_v1 import process_event
from sarvam_service import SarvamService
from scheduler import calling_job


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # logging.FileHandler("logs/scheduler.log")
    ]
)
logger = logging.getLogger("APSchedulerMain")


def start_scheduler():
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")

    # Base Data Transfer
    scheduler.add_job(
        calling_job,
        trigger=IntervalTrigger(minutes=1),
        id="calling_job",
        name="calling_job",
        replace_existing=True,
        max_instances=1
    )


    logger.info("Starting APScheduler service...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down APScheduler...")

if __name__ == "__main__":
    start_scheduler()
    # process_event()
