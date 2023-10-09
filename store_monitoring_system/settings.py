"""Handles configuration of environment variables"""
from pathlib import Path
from starlette.config import Config

CONFIG = Config(".env")
BASE_DIR = Path(__file__).parent

DATABASE_URL = CONFIG(
    "DATABASE_URL",
    cast=str,
    default="postgresql://postgres:8045@localhost/postgres",
)
DB_HOST = CONFIG("DB_HOST", cast=str, default="localhost")
DB_NAME = CONFIG("DB_NAME", cast=str, default="postgres")
DB_USER = CONFIG("DB_USER", cast=str, default="postgres")
DB_PASSWORD = CONFIG("DB_PASSWORD", cast=str, default="8045")
