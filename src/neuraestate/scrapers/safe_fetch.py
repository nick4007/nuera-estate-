import os, re, json, time, pathlib, urllib.parse, requests, gzip
from typing import Optional, List
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET
from io import BytesIO
from urllib import robotparser
from neuraestate.logging_setup import setup_logging
import logging

setup_logging()
logger = logging.getLogger("neuraestate")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36; "
    "NeuraEstateBot/1.0 (+you@example.com)"
)

def should_pause() -> bool:
    return os.getenv("CRAWLER_PAUSE", "0") == "1"

CONSENT_DIR = pathlib.Path("consent_artifacts")
CONSENT_DIR.mkdir(exist_ok=True)

def log_robots(host: str, robots_url: str, robots_text: str, crawl_cfg: dict):
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (CONSENT_DIR / f"{host}_robots_{ts}.txt").write_text(robots_text or "", encoding="utf-8")
    (CONSENT_DIR / f"{host}_crawlcfg_{ts}.json").write_text(
        json.dumps(crawl_cfg, indent=2, ensure_ascii=False), encoding="utf-8"
    )

class RobotsPolicy:
    def __init__(self, base_url: str, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        parsed = urllib.parse.urlparse(base_url)
        self.scheme, self.host = parsed.scheme, parsed.netloc
        self.robots_url = f"{self.scheme}://{self.host}/robots.txt"
        self.rp = robotparser.RobotFileParser()
        self.text = ""

        try:
            r = self.session.get(self.robots_url, timeout=15)
            if r.status_code == 200:
                self.text = r.text
                self.rp.parse(self.text.splitlines())
            else:
                self.rp.parse([])
        except requests.RequestException:
            self.rp.parse([])

        self.crawl_delay = self._extract_crawl_delay(self.text, USER_AGENT)

        crawl_cfg = {
            "user_agent": USER_AGENT,
            "robots_url": self.robots_url,
            "detected_crawl_delay_hint_seconds": self.crawl_delay,
            "default_min_interval_seconds": 2.0,
        }
        log_robots(self.host, self.robots_url, self.text or "", crawl_cfg)

    def can_fetch(self, url: str) -> bool:
        return self.rp.can_fetch(USER_AGENT, url)

    @staticmethod
    def _extract_crawl_delay(text: str, user_agent: str) -> Optional[float]:
        if not text:
            return None
        groups, current_uas, current_lines = [], [], []
        for line in text.splitlines():
            m = re.match(r"(?i)^\s*User-agent\s*:\s*(.+)$", line)
            if m:
                if current_uas or current_lines:
                    groups.append((current_uas, current_lines))
                current_uas = [ua.strip().lower() for ua in m.group(1).split(",")]
                current_lines = []
            else:
                current_lines.append(line)
        if current_uas or current_lines:
            groups.append((current_uas, current_lines))

        target = user_agent.lower()
        delays = []
        for uas, lines in groups:
            if not any(ua == "*" or ua in target for ua in uas):
                continue
            for line in lines:
                m = re.match(r"(?i)^\s*Crawl-delay\s*:\s*([0-9]+(?:\.[0-9]+)?)", line)
                if m:
                    delays.append(float(m.group(1)))
        return min(delays) if delays else None

class HostRateLimiter:
    def __init__(self, default_rps: float = 0.5):
        self.default_interval = 1.0 / max(default_rps, 0.0001)
        self.last_time = {}

    def wait(self, host: str, crawl_delay_hint: Optional[float] = None):
        min_interval = max(self.default_interval, float(crawl_delay_hint or 0.0))
        now = time.monotonic()
        last = self.last_time.get(host, 0.0)
        wait_for = max(0.0, (last + min_interval) - now)
        if wait_for > 0:
            time.sleep(wait_for)
        self.last_time[host] = time.monotonic()

def _parse_retry_after(h: str) -> Optional[float]:
    try:
        return float(int(h.strip()))
    except Exception:
        try:
            dt = parsedate_to_datetime(h)
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
        except Exception:
            return None

def get_with_backoff(session: requests.Session, url: str,
                     max_retries: int = 2, base: float = 1.0, max_total: float = 12.0):
    start = time.monotonic()
    last_exc, last_resp = None, None

    for attempt in range(max_retries + 1):
        if time.monotonic() - start > max_total:
            logger.warning(f"Gave up (time cap) on: {url}")
            return None
        try:
            r = session.get(url, timeout=15)
            last_resp = r
        except requests.RequestException as e:
            last_exc, r = e, None
        if r and 200 <= r.status_code < 300:
            return r
        if r and r.status_code in (400, 401, 403, 404):
            return r
        delay = None
        if r and r.status_code in (429, 500, 502, 503, 504):
            ra = r.headers.get("Retry-After")
            if ra:
                delay = _parse_retry_after(ra)
        if delay is None:
            delay = base * (2 ** attempt)
        remaining = max_total - (time.monotonic() - start)
        if remaining <= 0:
            logger.warning(f"Gave up (time cap) on: {url}")
            return None
        time.sleep(min(delay, remaining))
    if last_exc:
        logger.error(f"{url} → {last_exc.__class__.__name__}: {last_exc}")
    elif last_resp is not None:
        logger.warning(f"Last HTTP status for {url}: {last_resp.status_code}")
    return last_resp

class SafeFetcher:
    def __init__(self, default_rps: float = 0.5):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        })
        self.robots_cache = {}
        self.limiter = HostRateLimiter(default_rps=default_rps)

    def _policy_for(self, url: str) -> RobotsPolicy:
        parsed = urllib.parse.urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        if parsed.netloc not in self.robots_cache:
            self.robots_cache[parsed.netloc] = RobotsPolicy(base, session=self.session)
        return self.robots_cache[parsed.netloc]

    def fetch(self, url: str):
        if should_pause():
            raise RuntimeError("Crawler paused via CRAWLER_PAUSE=1")
        policy = self._policy_for(url)
        if not url.lower().endswith("/robots.txt"):
            if not policy.can_fetch(url):
                logger.info(f"Disallowed by robots.txt: {url}")
                return None
        self.limiter.wait(policy.host, crawl_delay_hint=policy.crawl_delay)
        resp = get_with_backoff(self.session, url)
        if resp is None:
            logger.warning(f"No response: {url}")
            return None
        if resp.status_code >= 400:
            logger.warning(f"HTTP {resp.status_code}: {url}")
        return resp



