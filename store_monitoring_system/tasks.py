"""Handles celery background task"""
from typing import Any, Union
from time import time
from celery import Celery
from celery.schedules import crontab
from sqlalchemy.orm import sessionmaker
from store_monitoring_system.repositories.models import (
    DB_ENGINE,
    StoreReport,
)
from store_monitoring_system.logger import LOGGER
from store_monitoring_system.utils.report_task_utils import (
    fetch_store_details,
    calculate_store_report,
)

celery: Celery = Celery(
    __name__,
    broker="pyamqp://guest:guest@localhost//",
    backend="rpc://",
    include=["store_monitoring_system.tasks"],
)

celery.conf.beat_schedule: dict[str, Any] = {
    "add-every-1-hour": {
        "task": "tasks.polling_server",
        "schedule": crontab(hour="*/1"),
    },
}


@celery.task
def polling_server() -> None:
    """Handles polling server every one hour"""
    LOGGER.info("task triggered")


@celery.task
def generate_report(report_id: str) -> None:
    """Task to handle report generation
    1. Fetch last 7 days store status
    2. Fetch timezone and buisness hours for each store
    3. Calculate uptime and downtime for each store
    """
    start: float = time()
    LOGGER.info("Report generation initiated at %s", start)
    Session = sessionmaker(bind=DB_ENGINE)
    session = Session()
    report_data: list[dict[str, Union[str, float]]] = []
    store_datas, max_timestamp = fetch_store_details(session)
    for store_data in store_datas:
        store_specific_report =calculate_store_report(store_data, max_timestamp)
        report_data.append(store_specific_report)
    (
        session.query(StoreReport)
        .filter(StoreReport.id == report_id)
        .update(
            {
                "report_data": report_data,
                "status": "success",
            }
        )
    )
    session.commit()
    end_time: float = time()
    LOGGER.info("Report generation took %s", (end_time - start))
