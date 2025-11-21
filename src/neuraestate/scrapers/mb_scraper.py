# src/neuraestate/scrapers/mb_scraper.py
from __future__ import annotations

import hashlib
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from neuraestate.config import settings
import requests
from xml.etree import ElementTree as ET

# HTML parsing
from bs4 import BeautifulSoup

# -----------------------
# Logging
# -----------------------
logger = logging.getLogger("neuraestate")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

# -----------------------
# Config (env-overridable)
# -----------------------
BASE = os.getenv("MB_BASE", "https://www.magicbricks.com")
ROBOTS_URL = os.getenv("MB_ROBOTS_URL", urljoin(BASE, "/robots.txt"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))
RETRY_MAX = int(os.getenv("RETRY_MAX", "3"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "1.5"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "1.0"))

# Fallback listing seeds (used if sitemap URLs don’t give index pages)
SEEDS = [
    "https://www.magicbricks.com/property-for-sale-in-mumbai-pppfs",
    "https://www.magicbricks.com/property-for-rent-in-mumbai-pppfr",
    "https://www.magicbricks.com/property-for-sale-in-pune-pppfs",
    "https://www.magicbricks.com/property-for-rent-in-pune-pppfr",
]
MAX_PAGES_PER_SEED = int(os.getenv("MAX_PAGES_PER_SEED", "40"))

# We will only crawl "index/listing" pages (never /property/ detail pages).
INDEX_URL_PATTERNS = [
    "/property-for-sale-in-",
    "/property-for-rent-in-",
    "/flats-for-sale-in-",
    "/flats-for-rent-in-",
]

# -----------------------
# HTTP helpers
# -----------------------
DEFAULT_HEADERS = {
    "User-Agent": os.getenv(
        "SCRAPER_UA",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.7",
    "Connection": "close",
}

def fetch(url: str, session: requests.Session, allow_404: bool = False) -> requests.Response:
    tries = 0
    while True:
        tries += 1
        try:
            resp = session.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            logger.info("GET %s -> %s", url, resp.status_code)
            if resp.status_code == 200:
                return resp
            if allow_404 and resp.status_code == 404:
                return resp
            if 300 <= resp.status_code < 600:
                raise requests.HTTPError(f"HTTP {resp.status_code}")
        except Exception as e:
            logger.warning("Request error (%s): %s", url, e)
            if tries >= RETRY_MAX:
                raise
            time.sleep(RETRY_BACKOFF ** tries)
        finally:
            time.sleep(RATE_LIMIT_SLEEP)

# -----------------------
# Robots parsing
# -----------------------
@dataclass
class RobotsRules:
    disallow: List[str]
    sitemaps: List[str]
    host: Optional[str] = None

ROBOTS_LINE = re.compile(r"^\s*(?P<key>[A-Za-z\-]+)\s*:\s*(?P<val>.+?)\s*$")

def parse_robots(text: str) -> RobotsRules:
    disallow: List[str] = []
    sitemaps: List[str] = []
    host: Optional[str] = None
    ua_block = None  # current User-agent

    for raw in text.splitlines():
        m = ROBOTS_LINE.match(raw)
        if not m:
            continue
        key = m.group("key").strip().lower()
        val = m.group("val").strip()

        if key == "user-agent":
            ua_block = val
        elif key == "disallow" and (ua_block == "*" or ua_block is None):
            if val.startswith("/"):
                disallow.append(val.rstrip())
        elif key == "sitemap":
            sitemaps.append(val)
        elif key == "host":
            host = val

    return RobotsRules(disallow=disallow, sitemaps=sitemaps, host=host)

def is_allowed(path: str, rules: RobotsRules) -> bool:
    for rule in rules.disallow:
        if rule and path.startswith(rule):
            return False
    return True

# -----------------------
# Sitemap discovery & parse
# -----------------------
def discover_sitemaps(session: requests.Session) -> Tuple[RobotsRules, List[str]]:
    robots = fetch(ROBOTS_URL, session)
    rules = parse_robots(robots.text)
    if rules.sitemaps:
        logger.info("Found %d sitemap(s) in robots.txt", len(rules.sitemaps))
        return rules, rules.sitemaps
    guesses = [urljoin(BASE, "/sitemap_index.xml"), urljoin(BASE, "/sitemap.xml")]
    return rules, guesses

def parse_sitemap_xml(xml_text: str) -> Tuple[List[str], List[str]]:
    """
    Returns (child_sitemaps, urlset_urls)
    """
    smaps: List[str] = []
    urls: List[str] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return smaps, urls

    if "sitemapindex" in root.tag:
        for sm in root.findall("{*}sitemap"):
            loc = sm.findtext("{*}loc")
            if loc:
                smaps.append(loc.strip())
    elif "urlset" in root.tag:
        for u in root.findall("{*}url"):
            loc = u.findtext("{*}loc")
            if loc:
                urls.append(loc.strip())

    return smaps, urls

def collect_index_urls_from_sitemaps(session: requests.Session) -> List[str]:
    """
    Crawl: sitemap_index -> (many) child sitemaps -> urlset URLs
    Filter for *index* pages only (never /property/ detail pages).
    """
    rules, sitemap_candidates = discover_sitemaps(session)
    index_urls: Set[str] = set()

    for sm in sitemap_candidates:
        try:
            r = fetch(sm, session)
        except Exception as e:
            logger.warning("Sitemap fetch failed: %s (%s)", sm, e)
            continue

        smaps, urlset = parse_sitemap_xml(r.text)
        if smaps:
            logger.info("Sitemap %s has %d child sitemaps", sm, len(smaps))
            # Traverse children
            for child in smaps:
                try:
                    rc = fetch(child, session)
                except Exception as e:
                    logger.warning("Child sitemap fetch failed: %s (%s)", child, e)
                    continue
                _, urls = parse_sitemap_xml(rc.text)
                for u in urls:
                    if any(p in u for p in INDEX_URL_PATTERNS):
                        pth = urlparse(u).path
                        if is_allowed(pth, rules):
                            index_urls.add(u)
        else:
            # this sitemap was a urlset
            for u in urlset:
                if any(p in u for p in INDEX_URL_PATTERNS):
                    pth = urlparse(u).path
                    if is_allowed(pth, rules):
                        index_urls.add(u)

    # If we got nothing from sitemaps, fall back to seeds
    if not index_urls:
        logger.warning("No index pages found in sitemaps. Falling back to seeds.")
        index_urls.update(SEEDS)

    return sorted(index_urls)

# -----------------------
# Pagination
# -----------------------
def page_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()

def paginate(seed: str, session: requests.Session, max_pages: int = MAX_PAGES_PER_SEED) -> Iterable[Tuple[int, str]]:
    seen_hashes: Set[str] = set()
    for i in range(1, max_pages + 1):
        url = seed if i == 1 else f"{seed}/page-{i}"
        resp = fetch(url, session, allow_404=True)
        if resp.status_code == 404:
            logger.info("Stop pagination (404) at %s", url)
            break
        h = page_hash(resp.text)
        if h in seen_hashes:
            logger.info("Stop pagination (repeat content) at %s", url)
            break
        seen_hashes.add(h)
        yield i, resp.text

# -----------------------
# Field extraction helpers
# -----------------------
PRICE_RE = re.compile(r"₹\s*([0-9,\.]+)\s*(Cr|Crore|Lac|Lakh|K)?", re.I)
BHK_RE = re.compile(r"(\d+)\s*BHK", re.I)
BATH_RE = re.compile(r"(\d+)\s*Bath", re.I)
AREA_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(sq\.?\s*ft|sqft|sq ft|sqm|sq\.?\s*m|sq\.?\s*yd|sqyd|acre)s?", re.I)

UNIT_TO_SQFT = {
    "sqft": 1.0, "sq ft": 1.0, "sq. ft": 1.0, "sq ft.": 1.0, "sq.ft": 1.0, "sq. ft.": 1.0,
    "sqm": 10.7639, "sq m": 10.7639, "sq. m": 10.7639, "sq.m": 10.7639,
    "sqyd": 9.0, "sq yd": 9.0, "sq. yd": 9.0, "sq.yd": 9.0,
    "acre": 43560.0,
}

def _norm_unit(u: str) -> str:
    u = u.lower().replace(".", "").strip()
    if "sqm" in u or "sq m" in u:
        return "sqm"
    if "sqft" in u or "sq ft" in u or "sqft" in u.replace(" ", ""):
        return "sqft"
    if "sqyd" in u or "sq yd" in u:
        return "sqyd"
    if "acre" in u:
        return "acre"
    return "sqft"

def parse_price_to_inr(text: str) -> Optional[int]:
    m = PRICE_RE.search(text)
    if not m:
        return None
    num = m.group(1).replace(",", "")
    unit = (m.group(2) or "").lower()
    try:
        val = float(num)
    except:
        return None
    if unit in ("cr", "crore"):
        val = val * 1e7
    elif unit in ("lac", "lakh"):
        val = val * 1e5
    elif unit == "k":
        val = val * 1e3
    return int(val)

def parse_area_to_sqft(text: str) -> Optional[float]:
    m = AREA_RE.search(text)
    if not m:
        return None
    val = float(m.group(1))
    unit = _norm_unit(m.group(2))
    mul = UNIT_TO_SQFT.get(unit, 1.0)
    return round(val * mul, 2)

def guess_city_from_title(title: str) -> Optional[str]:
    if not title:
        return None
    m = re.search(r"in\s+([A-Za-z .\-]+)", title, re.I)
    if m:
        return m.group(1).split("|")[0].split("-")[0].strip()
    return None

# -----------------------
# Card parsing (index pages only)
# -----------------------
def extract_listing_cards(html: str, page_url: str) -> List[dict]:
    soup = BeautifulSoup(html, "lxml")

    # page title → helps guess city
    page_title = (soup.title.string or "").strip() if soup.title else ""

    # Try broad selectors for card containers
    # Keep it forgiving (site changes won’t break everything).
    candidates = []
    for cls in ["mb-srp__card", "mb-srp-card", "mb-srp__list", "mb-srp__property-card"]:
        candidates.extend(soup.select(f".{cls}"))
    if not candidates:
        # fallback: take large list items/sections with links that look like property references
        candidates = [x for x in soup.select("div,li,article,section") if x.find("a")]

    cards = []
    for idx, node in enumerate(candidates, start=1):
        text = " ".join(node.get_text(separator=" ", strip=True).split())
        if len(text) < 40:
            continue  # too small to be a card

        price_inr = parse_price_to_inr(text)
        bhk = None
        m = BHK_RE.search(text)
        if m:
            bhk = int(m.group(1))
        baths = None
        m = BATH_RE.search(text)
        if m:
            try:
                baths = int(m.group(1))
            except:
                baths = None
        area_sqft = parse_area_to_sqft(text)

        # Try to get a thumbnail (we DO NOT follow the link)
        img = node.find("img")
        img_url = img["src"] if img and img.has_attr("src") else None

        # Try to get a headline/title-ish text
        title_el = node.find(["h2", "h3"])
        title = title_el.get_text(" ", strip=True) if title_el else None

        city = guess_city_from_title(page_title)

        cards.append({
            "source": "magicbricks",
            "source_page_url": page_url,
            "card_index": idx,
            "title": title,
            "price_inr": price_inr,
            "bhk": bhk,
            "bathrooms": baths,
            "area_sqft": area_sqft,
            "city": city,
            "image_url": img_url,
            "card_text": text[:4000],  # keep raw for backup parsing
        })

    logger.info("Parsed %d cards from %s (title: %s)", len(cards), page_url, page_title or "n/a")
    return cards

# -----------------------
# DB: robust staging sink (works even if project ORM names differ)
# -----------------------
from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Text, DateTime, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

DATABASE_URL = settings.DATABASE_URL  # e.g. postgresql+psycopg2://user:pass@host:5432/db
logger.info("DB URL host: %s", DATABASE_URL.split("@")[1]) 
Base = declarative_base()

class StgMBListing(Base):
    __tablename__ = "stg_mb_listings"
    # deterministic key to prevent duplicates per (page, index, title)
    pk = Column(String(64), primary_key=True)
    source = Column(String(50), nullable=False)
    source_page_url = Column(Text, nullable=False)
    card_index = Column(Integer, nullable=False)
    title = Column(Text)
    price_inr = Column(Integer)
    bhk = Column(Integer)
    bathrooms = Column(Integer)
    area_sqft = Column(Float)
    city = Column(String(120))
    image_url = Column(Text)
    card_text = Column(Text)

    first_seen_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("source_page_url", "card_index", name="uq_stg_mb_page_card"),
    )

def make_pk(rec: dict) -> str:
    base = f"{rec.get('source_page_url','')}|{rec.get('card_index','')}|{rec.get('title') or ''}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()

def get_session():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set. Example: postgresql+psycopg2://postgres:postgres@localhost:5432/neuraestate")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()

def upsert_listings(rows: List[dict]) -> int:
    if not rows:
        return 0
    sess = get_session()
    inserted = 0
    try:
        for r in rows:
            pk = make_pk(r)
            obj = sess.get(StgMBListing, pk)
            now = datetime.utcnow()
            if obj is None:
                obj = StgMBListing(
                    pk=pk,
                    source=r["source"],
                    source_page_url=r["source_page_url"],
                    card_index=r["card_index"],
                    title=r.get("title"),
                    price_inr=r.get("price_inr"),
                    bhk=r.get("bhk"),
                    bathrooms=r.get("bathrooms"),
                    area_sqft=r.get("area_sqft"),
                    city=r.get("city"),
                    image_url=r.get("image_url"),
                    card_text=r.get("card_text"),
                    first_seen_at=now,
                    last_seen_at=now,
                )
                sess.add(obj)
                inserted += 1
            else:
                # update lightweight fields + last_seen
                obj.title = r.get("title") or obj.title
                obj.price_inr = r.get("price_inr") or obj.price_inr
                obj.bhk = r.get("bhk") or obj.bhk
                obj.bathrooms = r.get("bathrooms") or obj.bathrooms
                obj.area_sqft = r.get("area_sqft") or obj.area_sqft
                obj.city = r.get("city") or obj.city
                obj.image_url = r.get("image_url") or obj.image_url
                obj.card_text = r.get("card_text") or obj.card_text
                obj.last_seen_at = now
        sess.commit()
    except Exception as e:
        sess.rollback()
        logger.exception("DB upsert error: %s", e)
        raise
    finally:
        sess.close()
    return inserted

# -----------------------
# Main
# -----------------------
def main() -> int:
    session = requests.Session()

    # 1) From sitemap index, collect index-page URLs (filters out /property/ detail pages)
    index_urls = collect_index_urls_from_sitemaps(session)
    logger.info("Index-URL candidates: %d", len(index_urls))

    # 2) For each index page, paginate and parse cards
    total_cards = 0
    for idx_url in index_urls:
        pages = 0
        batch: List[dict] = []
        for page_no, html in paginate(idx_url, session):
            pages += 1
            cards = extract_listing_cards(html, idx_url if page_no == 1 else f"{idx_url}/page-{page_no}")
            batch.extend(cards)

            # occasional batch flush to DB
            if len(batch) >= 200:
                n = upsert_listings(batch)
                total_cards += n
                logger.info("Inserted %d new rows (batch).", n)
                batch = []

        # flush remaining
        if batch:
            n = upsert_listings(batch)
            total_cards += n
            logger.info("Inserted %d new rows (final for seed).", n)

        if pages == 0:
            logger.warning("No pages crawled for %s", idx_url)

    logger.info("Done. New rows inserted: %d", total_cards)
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)







