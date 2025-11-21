from __future__ import annotations
import hashlib
from bs4 import BeautifulSoup
from neuraestate.scrapers.base import Scraper
from neuraestate.schemas import ListingIn
from neuraestate.logging_setup import setup_logging
import logging

setup_logging()
logger = logging.getLogger("neuraestate")


class DemoSiteScraper(Scraper):
    def seed_urls(self) -> list[str]:
        return ["https://example.com/"]  # demo-only

    def parse_listing(self, html: str, url: str) -> ListingIn | None:
        soup = BeautifulSoup(html, "lxml")
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Example Listing"
        ext_id = hashlib.md5(url.encode()).hexdigest()
        logger.info(f"Parsed demo listing from {url}")
        return ListingIn(
            external_id=ext_id,
            title=title,
            price=None,
            location=None,
            url=url,
            image_urls=[],
            amenities=["demo"],
        )
