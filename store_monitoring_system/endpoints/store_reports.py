"""Handles endpoints of Report Generation and Retrieval"""
from starlette.responses import Response
from starlette.requests import Request


def report_generation(_: Request) -> Response:
    """Handles report generation endpoint"""

    raise NotImplementedError()


def fetch_reports(_: Request) -> Response:
    """Handles report retrieval endpoint"""

    raise NotImplementedError()
