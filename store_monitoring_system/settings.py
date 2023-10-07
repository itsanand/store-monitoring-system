"""Handles configuration of environment variables"""
from pathlib import Path
from starlette.config import Config

CONFIG = Config(".env")
BASE_DIR = Path(__file__).parent

DATABASE_URL = CONFIG(
    "DATABASE_URL",
    cast=str,
    default="postgresql://postgres:8045@localhost:5432/postgres",
)
