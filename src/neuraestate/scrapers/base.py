from abc import ABC, abstractmethod
import httpx
from tenacity import retry, wait_fixed, stop_after_attempt
from neuraestate.schemas import ListingIn
from neuraestate.logging_setup import setup_logging
import logging

setup_logging()
logger = logging.getLogger("neuraestate")


class Scraper(ABC):
    @abstractmethod
    def seed_urls(self) -> list[str]:
        ...

    @abstractmethod
    def parse_listing(self, html: str, url: str) -> ListingIn | None:
        ...

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(3))
    async def fetch(self, client: httpx.AsyncClient, url: str) -> str:
        r = await client.get(url, timeout=20)
        r.raise_for_status()
        return r.text

    async def run(self) -> list[ListingIn]:
        results: list[ListingIn] = []
        async with httpx.AsyncClient() as client:
            for url in self.seed_urls():
                try:
                    html = await self.fetch(client, url)
                    dto = self.parse_listing(html, url)
                    if dto:
                        results.append(dto)
                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")
        return results
