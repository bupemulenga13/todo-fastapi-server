"""Configure the database engine and session"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.config import MS_SQL_SERVER_DATABASE_URI

engine = create_engine(MS_SQL_SERVER_DATABASE_URI)

SessionLocal = sessionmaker(autocommit=False, autoflush=False,bind=engine)

def get_db() -> SessionLocal:
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()