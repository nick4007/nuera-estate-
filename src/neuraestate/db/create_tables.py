# src/neuraestate/db/create_tables.py
"""
Create DB tables from SQLAlchemy models. Safe for development.
Run as module: python -m neuraestate.db.create_tables
"""
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# import your declarative base
from neuraestate.db.base import Base

# try to reuse existing engine if available, otherwise build one from config
engine: Engine | None = None
try:
    # many projects expose engine in db.session
    from neuraestate.db.session import engine as _engine  # type: ignore
    engine = _engine
    print("Using engine imported from neuraestate.db.session")
except Exception:
    try:
        # fall back to config (pydantic settings)
        from neuraestate.config import settings
        db_url = str(settings.database_url)
        engine = create_engine(db_url)
        print("Created engine from settings.database_url:", db_url)
    except Exception as e:
        print("Could not import existing engine or settings:", e)
        raise

if engine is None:
    raise RuntimeError("Could not obtain a SQLAlchemy engine.")

try:
    print("Creating tables (this is idempotent)...")
    Base.metadata.create_all(bind=engine)
    print("✅ Tables created (or already existed).")
except SQLAlchemyError as e:
    print("Error creating tables:", e)
    raise
