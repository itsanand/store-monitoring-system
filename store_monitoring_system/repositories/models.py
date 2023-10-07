"""Handles database connection and tables"""
import psycopg2
from psycopg2.extensions import connection, cursor as Cursor
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Time
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import ProgrammingError
import store_monitoring_system.settings as Config
from store_monitoring_system.logger import LOGGER

# Database Configuration
DB_ENGINE: Engine = create_engine(Config.DATABASE_URL)
BASE = declarative_base()


# Define SQLAlchemy Models
class StoreStatus(BASE):
    """Model to store status of each Store"""

    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(String)
    timestamp_utc = Column(DateTime(timezone=True))
    status = Column(String)


class BusinessHours(BASE):
    """Model to store status of each Store"""

    __tablename__ = "business_hours"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(String)
    day_of_week = Column(Integer)
    start_time_local = Column(Time)
    end_time_local = Column(Time)


class Timezone(BASE):
    """Model to store timezone of stores"""

    __tablename__ = "timezones"
    store_id = Column(String, primary_key=True)
    timezone_str = Column(String)


def _load_csv_into_db(
    file_path: str, table_name: str, conn: connection, cursor: Cursor, columns: tuple
) -> None:
    """Method will load CSV file data into respective
    table using psycopg2 export_expert method
    """

    column_names: str = ",".join(columns)
    copy_sql: str = f"""COPY {table_name}({column_names}) FROM stdin WITH CSV HEADER
        DELIMITER as ','
    """
    with open(file_path, "r", encoding="utf-8") as file:
        cursor.copy_expert(sql=copy_sql, file=file)
        conn.commit()


def _create_tables_if_not_exist():
    """Create all tables and add data from ec=ach csv file
    NOTE: Instead of reading from CSV file diretly this part
    can be moved to polling service.
    """
    try:
        BASE.metadata.create_all(bind=DB_ENGINE)
        conn: connection = psycopg2.connect(Config.DATABASE_URL)
        cursor: Cursor = conn.cursor()
        _load_csv_into_db(
            "file_path",
            Timezone.__tablename__,
            conn,
            cursor,
            ("store_id", "timezone_str"),
        )
        _load_csv_into_db(
            "file_path",
            BusinessHours.__tablename__,
            conn,
            cursor,
            ("store_id", "day_of_week", "start_time_local", "end_time_local"),
        )
        _load_csv_into_db(
            "file_path",
            StoreStatus.__tablename__,
            conn,
            cursor,
            ("store_id", "status", "timestamp_utc"),
        )
        LOGGER.info("tables created and data added from CSV")
    except ProgrammingError:
        LOGGER.warning("Tables already exist")
    except psycopg2.IntegrityError as error:
        LOGGER.warning("Error while adding record into DB %s", str(error))
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    _create_tables_if_not_exist()
