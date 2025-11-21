import asyncio
from neuraestate.db.base import get_session
from neuraestate.db.repository import ListingRepository
from neuraestate.scrapers.demo_site import DemoSiteScraper
from neuraestate.logging_setup import setup_logging
import logging

# initialize logging
setup_logging()
logger = logging.getLogger("neuraestate")


async def main() -> None:
    scraper = DemoSiteScraper()
    items = await scraper.run()

    with get_session() as session:
        repo = ListingRepository(session)
        for dto in items:
            repo.upsert_listing(dto)
        session.commit()

    logger.info(f"✅ Upserted {len(items)} listings.")


if __name__ == "__main__":
    asyncio.run(main())
