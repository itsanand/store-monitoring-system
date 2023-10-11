"""Handles util methods for report generation task"""
from typing import Final, Union
from datetime import datetime, timedelta, time as dt
from pytz import timezone as tz
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy import func
from sqlalchemy.engine.row import Row
from store_monitoring_system.repositories.models import (
    StoreStatus,
    Timezone,
    BusinessHours,
)


_ACTIVE: Final[str] = "active"
_HOUR_MINUTES: Final[int] = 60 * 60
_MINUTES: Final[int] = 60
_TOTAL_WEEK_DAYS: Final[int] = 7


def fetch_store_details(session) -> tuple[list[Row], datetime]:
    """Fetch store related data
    NOTE: timestamp and status are from last 7 days only
    """

    max_timestamp = (
        session.query(func.max(StoreStatus.timestamp_utc).label("max_timestamp"))
    ).scalar()
    status_subquery = (
        session.query(
            StoreStatus.store_id,
            func.array_agg(
                aggregate_order_by(StoreStatus.status, StoreStatus.timestamp_utc).desc()
            )
            .filter(StoreStatus.timestamp_utc >= (max_timestamp - timedelta(days=7)))
            .label("status_array"),
            func.array_agg(
                aggregate_order_by(
                    StoreStatus.timestamp_utc, StoreStatus.timestamp_utc
                ).desc()
            )
            .filter(StoreStatus.timestamp_utc >= (max_timestamp - timedelta(days=7)))
            .label("timestamp_array"),
            func.coalesce(Timezone.timezone_str, "America/Chicago").label(
                "timezone_str"
            ),
        )
        .join(Timezone, Timezone.store_id == StoreStatus.store_id, isouter=True)
        .group_by(
            StoreStatus.store_id,
            func.coalesce(Timezone.timezone_str, "America/Chicago"),
        )
        .subquery()
    )
    query = (
        session.query(
            status_subquery.c.store_id,
            status_subquery.c.timezone_str,
            status_subquery.c.status_array,
            status_subquery.c.timestamp_array,
            func.array_agg(BusinessHours.day_of_week).label("week_days"),
            func.array_agg(BusinessHours.start_time_local).label("opens"),
            func.array_agg(BusinessHours.end_time_local).label("closes"),
        )
        .join(
            BusinessHours,
            BusinessHours.store_id == status_subquery.c.store_id,
            isouter=True,
        )
        .group_by(
            status_subquery.c.store_id,
            status_subquery.c.timezone_str,
            status_subquery.c.status_array,
            status_subquery.c.timestamp_array,
        )
    )
    return query.all(), max_timestamp

def calculate_store_report(store_data, max_timestamp):
    """If timestamps for current store is not empty
    then check and fill the business hours of the store
    1. If business hours are not available then assumption
    is that it is open 24*7
    Then for each week calculate the store report
    """

    timezone_str = (
        tz(store_data.timezone_str)
        if isinstance(store_data.timezone_str, str)
        else store_data.timezone_str
    )
    store_specific_report: dict[str, Union[str, float]] = {
        "uptime_last_hour": 0,
        "uptime_last_day": 0,
        "uptime_last_week": 0,
        "downtime_last_hour": 0,
        "downtime_last_day": 0,
        "downtime_last_week": 0,
    }
    if store_data.timestamp_array is not None:
        week_days: list[int] = store_data.week_days
        is_always_open: bool = False
        if store_data.week_days[0] is None:
            is_always_open = True
            week_days = list(range(_TOTAL_WEEK_DAYS))
        for index in range(len(week_days)):
            buisness_hours = (
                week_days[index],
                (dt(0, 0) if is_always_open else store_data.opens[index]),
                (dt(23, 59, 59) if is_always_open else store_data.closes[index]),
            )
            store_specific_report = _calculate_interpolated_uptime_downtime(
                store_data,
                timezone_str,
                buisness_hours,
                store_specific_report,
                max_timestamp
            )
    return {
        "store_id": store_data.store_id,
        "uptime_last_hour": store_specific_report["uptime_last_hour"]
        / _MINUTES,
        "uptime_last_day": store_specific_report["uptime_last_day"] / _HOUR_MINUTES,
        "uptime_last_week": store_specific_report["uptime_last_week"]
        / _HOUR_MINUTES,
        "downtime_last_hour": store_specific_report["downtime_last_hour"]
        / _MINUTES,
        "downtime_last_day": store_specific_report["downtime_last_day"]
        / _HOUR_MINUTES,
        "downtime_last_week": store_specific_report["downtime_last_week"]
        / _HOUR_MINUTES,
    }

def _calculate_interpolated_uptime_downtime(
    store_data: StoreStatus,
    store_timezone: tz,
    buisness_hours: tuple,
    store_specific_report: dict[str, Union[str, float]],
    max_timestamp: datetime
) -> dict[str, Union[str, float]]:
    """Calculate uptime and downtime based on status and
    time difference.
    1. If current status is active and current timestamp
    is under 1 hour then add difference  b/w curr stamp and
    next stamp to uptime_last_hour or vice-versa for downtime.
    2. Based on 1. uptime and downtime for last 1 day and 1 week
    will be calculated.
    NOTE: only timestamp b/w business hours will be added for report
    """

    max_timestamp = max_timestamp.replace(
        tzinfo=store_timezone
    )
    for index in range(len(store_data.timestamp_array) - 1):
        curr_stamp: datetime = store_data.timestamp_array[index].replace(
            tzinfo=store_timezone
        )
        next_stamp: datetime = store_data.timestamp_array[index + 1].replace(
            tzinfo=store_timezone
        )
        curr_status: str = store_data.status_array[index]
        if (
            curr_stamp.weekday() > buisness_hours[0]
            or curr_stamp.time() > buisness_hours[2]
        ):
            break
        if (
            curr_stamp.weekday() != buisness_hours[0]
            or curr_stamp.time() < buisness_hours[1]
        ):
            continue
        interval: float = (next_stamp - curr_stamp).total_seconds()
        if (max_timestamp - curr_stamp) <= timedelta(hours=1):
            if curr_status != _ACTIVE:
                store_specific_report["uptime_last_hour"] += interval
            else:
                store_specific_report["downtime_last_hour"] += interval
        if (max_timestamp - curr_stamp) <= timedelta(days=1):
            if store_data.status_array[index] == _ACTIVE:
                store_specific_report["uptime_last_day"] += interval
            else:
                store_specific_report["uptime_last_day"] += interval
        if (max_timestamp - curr_stamp) <= timedelta(weeks=1):
            if store_data.status_array[index] == _ACTIVE:
                store_specific_report["uptime_last_week"] += interval
            else:
                store_specific_report["downtime_last_week"] += interval
    return store_specific_report
