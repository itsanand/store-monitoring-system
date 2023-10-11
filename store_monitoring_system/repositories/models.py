"""Handles database connection and tables and
handles Initial setup Service"""
import asyncio
import psycopg2
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.engine.base import Engine
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Time,
    BigInteger,
    create_engine,
)
from sqlalchemy.orm import declarative_base
import store_monitoring_system.settings as Config
from store_monitoring_system.logger import LOGGER

# Database Configuration
DB_ENGINE: Engine = create_engine(Config.DATABASE_URL)
BASE = declarative_base()


# Define SQLAlchemy Models
class StoreReport(BASE):
    """Model to store report of each store"""

    __tablename__ = "store_report"
    id = Column(String, primary_key=True)
    report_data = Column(JSON, nullable=True)
    status = Column(String, default="Running")


class StoreStatus(BASE):
    """Model to store status of each Store"""

    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(BigInteger)
    timestamp_utc = Column(DateTime(timezone=True), index=True)
    status = Column(String)


class BusinessHours(BASE):
    """Model to store status of each Store"""

    __tablename__ = "business_hours"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(BigInteger)
    day_of_week = Column(Integer)
    start_time_local = Column(Time)
    end_time_local = Column(Time)


class Timezone(BASE):
    """Model to store timezone of stores"""

    __tablename__ = "timezones"
    store_id = Column(BigInteger, primary_key=True)
    timezone_str = Column(String)


class InitialSetup:
    """InitialSetup class to handle hourly polling
    and initial data setup to tables
    """

    def __init__(self) -> None:
        self.conn = psycopg2.connect(
            host=Config.DB_HOST,
            dbname=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
        )
        self.cursor = self.conn.cursor()

    async def _load_csv_into_db(
        self, file_name: str, table_name: str, columns: tuple
    ) -> None:
        """Method will load CSV file data into respective
        table using psycopg2 export_expert method
        """
        try:
            column_names: str = ",".join(columns)
            copy_sql: str = f"""COPY {table_name}({column_names})
                FROM stdin WITH CSV HEADER
                DELIMITER as ','
            """
            with open(file_name, "r", encoding="utf-8") as file:
                self.cursor.copy_expert(sql=copy_sql, file=file)
                self.conn.commit()
        except psycopg2.errors.UniqueViolation as error:
            LOGGER.error("Store already exist in the database, %s", error)

    async def load_initial_data(self) -> None:
        """Loads initial data
        1. Adding StoreStatus to the table from CSV file
        2. Adding BuisnessHours to the table from CSV file
        3. Adding TimeZone to the table from CSV file
        """
        await self._load_csv_into_db(
            r"C:\Users\admin\Desktop\LoopAI\static\store status.csv",
            StoreStatus.__tablename__,
            ("store_id", "status", "timestamp_utc"),
        )
        LOGGER.info("Store Status added Successfully")

        await self._load_csv_into_db(
            r"C:\Users\admin\Desktop\LoopAI\static\Menu hours.csv",
            BusinessHours.__tablename__,
            ("store_id", "day_of_week", "start_time_local", "end_time_local"),
        )
        LOGGER.info("Store Buisness Hours added Successfully")

        await self._load_csv_into_db(
            r"C:\Users\admin\Desktop\LoopAI\static\timezome.csv",
            Timezone.__tablename__,
            ("store_id", "timezone_str"),
        )
        LOGGER.info("Store Buisness Hours added Successfully")


if __name__ == "__main__":
    BASE.metadata.drop_all(DB_ENGINE)
    BASE.metadata.create_all(DB_ENGINE)
    inital_setup: InitialSetup = InitialSetup()
    asyncio.run(inital_setup.load_initial_data())
    inital_setup.cursor.close()
    inital_setup.conn.close()
