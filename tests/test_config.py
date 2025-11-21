from src.neuraestate.config import settings

def test_has_database_url():
    assert settings.DATABASE_URL and settings.DATABASE_URL.startswith("postgresql+psycopg")
