from sqlalchemy import inspect
from src.neuraestate.db.base import Base, engine

TABLES = {"listings", "images", "amenities", "listing_amenities"}

def test_tables_exist():
    Base.metadata.create_all(engine)
    insp = inspect(engine)
    names = set(insp.get_table_names())
    assert TABLES.issubset(names)
