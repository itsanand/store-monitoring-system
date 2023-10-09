"""Handles Store Monitoring System"""
from starlette.applications import Starlette
from starlette.routing import Route
from store_monitoring_system.endpoints.store_reports import (
    fetch_reports,
    report_generation,
)


routes: list[Route] = [
    Route("/generate_report", report_generation, methods=["POST"]),
    Route("/get_report/{report_id}", fetch_reports, methods=["GET"]),
]

app: Starlette = Starlette(routes=routes)
