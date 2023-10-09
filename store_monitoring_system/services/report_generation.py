"""Service to handles report generation"""
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request
from store_monitoring_system.repositories.models import (
    StoreReport,
    DB_ENGINE,
)
from store_monitoring_system.tasks import generate_report


class ReportService:
    """Class to handle store report generation."""

    triggered_reports: dict[str, int] = {}

    def __init__(self, report_id: str) -> None:
        """Initialize the ReportService with the database engine and session."""
        self.Session = sessionmaker(DB_ENGINE)
        self.report_id = report_id

    async def trigger_report_generation(self, _: Request) -> None:
        """create report entry in database and trigger
        report generation task.
        """

        session = self.Session()
        store_report: StoreReport = StoreReport(id=self.report_id)
        session.add(store_report)
        session.commit()
        generate_report.delay(self.report_id)

    async def fetch_report(self) -> StoreReport:
        """Fetch report data based on report_id"""

        session = self.Session()
        reports: StoreReport = (
            session.query(StoreReport).filter(StoreReport.id == self.report_id)
        ).first()
        return reports
