"""데이터베이스 연결 로직을 작성하는 모듈입니다."""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


load_dotenv()


DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "12399"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")


DATABASE_URL = URL.create(
    drivername="mysql+pymysql",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
)