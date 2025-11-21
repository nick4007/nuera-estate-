# src/neuraestate/db/session.py
import os
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from dotenv import load_dotenv

# Load .env (project root) so DATABASE_URL is available
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set in environment (.env)")

# Create engine. echo=False by default; set echo=True for SQL debug
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Session factory - expire_on_commit=False is often convenient in web apps
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, expire_on_commit=False)


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a SQLAlchemy Session and ensures it is closed.
    Usage:
        def endpoint(db: Session = Depends(get_db)):
            ...
    """
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        yield db
    finally:
        if db:
            db.close()
