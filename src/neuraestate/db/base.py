from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from ..config import settings
from contextlib import contextmanager


# Base class for all ORM models
class Base(DeclarativeBase):
    pass


# Create the SQLAlchemy engine
engine = create_engine(
    settings.DATABASE_URL,
    echo=False,            # set to True for raw SQL debugging
    future=True,           # enforce SQLAlchemy 2.x style
    pool_pre_ping=True     # recycle dead connections automatically
)

# Session factory
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False
)


# Convenience session context manager
@contextmanager
def get_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

