from __future__ import annotations
import logging
from contextlib import contextmanager

from neuraestate.scrapers.mb_scraper import iter_mb_listings, MB_SOURCE
from neuraestate.schemas import ListingIn
from neuraestate.db.repository import ListingRepository
from neuraestate.scrapers.safe_fetch import SafeFetcher
from neuraestate.logging_setup import setup_logging

# 👉 adjust this import if SessionLocal lives elsewhere
from neuraestate.db.base import SessionLocal

logger = logging.getLogger(__name__)

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def _save_with_best_effort(repo: ListingRepository, listing: ListingIn) -> bool:
    # Duplicate safety (if repo exposes it)
    if hasattr(repo, "exists_by_url"):
        try:
            if repo.exists_by_url(listing.url):
                logger.info("Skip duplicate: %s", listing.url)
                return False
        except Exception:
            pass

    if hasattr(repo, "upsert_by_url"):
        try:
            repo.upsert_by_url(listing)
            return True
        except Exception as e:
            logger.debug("upsert_by_url failed: %s", e)

    if hasattr(repo, "create"):
        try:
            repo.create(listing)
            return True
        except Exception as e:
            logger.info("create failed: %s", e)

    logger.error("No usable save method on ListingRepository")
    return False

def ingest_magicbricks(url_limit: int = 50):
    fetcher = SafeFetcher(default_rps=0.5)
    parsed = saved = 0

    with session_scope() as session:
        repo = ListingRepository(session=session)

        for li in iter_mb_listings(fetcher, url_limit_from_sitemaps=url_limit):
            parsed += 1
            if _save_with_best_effort(repo, li):
                saved += 1

    summary = {"source": MB_SOURCE, "parsed": parsed, "saved": saved}
    logger.info("Ingest summary: %s", summary)
    return summary

if __name__ == "__main__":
    setup_logging()
    logging.getLogger(__name__).info("Starting MagicBricks ingest…")
    ingest_magicbricks(20)




