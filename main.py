import logging
import threading
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from utils.kafka_cosmos_v1 import process_event
from services import KafkaConsumer
from jobs import calling_eligible, get_interactions, push_rechurn_queue


def setup_logger_suppression():
    """Globally suppresses verbose Cosmos DB, Azure Core, and HTTP transport logs."""
    suppressed_loggers = [
        "azure",
        "azure.cosmos",
        "azure.cosmos._cosmos_http_logging_policy",
        "azure.core",
        "azure.core.pipeline.policies.http_logging_policy",
        "urllib3",
        "requests"
    ]
    for logger_name in suppressed_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def start_scheduler():
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")

    scheduler.add_job(
        calling_eligible,
        trigger=IntervalTrigger(seconds=12),
        id="calling_job",
        name="calling_job",
        replace_existing=True,
        max_instances=1
    )

    scheduler.add_job(
        get_interactions,
        trigger=IntervalTrigger(seconds=30),
        id="get_interactions",
        name="get_interactions",
        replace_existing=True,
        max_instances=1
    )

    # Fixed Duplicate ID and Name bug
    scheduler.add_job(
        push_rechurn_queue,
        trigger=IntervalTrigger(seconds=30),
        id="push_rechurn_queue",
        name="push_rechurn_queue",
        replace_existing=True,
        max_instances=1
    )

    logger.info("Starting APScheduler service...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down APScheduler...")


# ==========================================
# MAIN EXECUTION ENTRYPOINT
# ==========================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # 1. Mute verbose Cosmos DB / Azure SDK logs globally once
    setup_logger_suppression()

    logger = logging.getLogger("MainFile")

    # 2. Run Kafka Consumer in a background thread so BlockingScheduler can run
    consumer_service = KafkaConsumer()
    consumer_thread = threading.Thread(
        target=consumer_service.start_listening,
        kwargs={"handler_func": process_event},
        daemon=True
    )
    consumer_thread.start()
    logger.info("Kafka consumer started in background thread.")

    # 3. Start Scheduler loop
    start_scheduler()