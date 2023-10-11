"""Handles endpoints of Report Generation and Retrieval"""
from uuid import uuid4
from starlette.responses import JSONResponse
from starlette.requests import Request
from store_monitoring_system.services.report_generation import ReportService
from store_monitoring_system.repositories.models import StoreStatus


async def report_generation(request: Request) -> JSONResponse:
    """Handles report generation endpoint"""

    report_id: str = str(uuid4())
    await ReportService(report_id).trigger_report_generation(request)
    return JSONResponse({"report_id": report_id})


async def fetch_reports(request: Request) -> JSONResponse:
    """Handles report retrieval endpoint"""

    report_id: str = request.path_params["report_id"]
    report: StoreStatus = await ReportService(report_id).fetch_report()
    return JSONResponse(
        {
            "report_id": report.id,
            "status": report.status,
            "report_data": report.report_data,
        }
    )
