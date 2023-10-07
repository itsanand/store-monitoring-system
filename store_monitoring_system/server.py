"""Handles Store Monitoring System"""
from starlette.applications import Starlette
from starlette.routing import Route
from store_monitoring_system.endpoints.store_reports import (
    report_generation,
    fetch_reports,
)

routes: list[Route] = [
    Route("/trigger_report", report_generation, methods=["POST"]),
    Route("/get_report", fetch_reports, methods=["GET"]),
]
app: Starlette = Starlette(routes=routes)
