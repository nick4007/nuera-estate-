from __future__ import annotations
import logging, logging.config
from pathlib import Path
import yaml

def setup_logging(config_path: str | None = None) -> None:
    """
    Load logging.yaml from a few sensible locations and fall back to basicConfig.
    Search order:
      1) explicit config_path arg (if provided)
      2) same folder as this file  (neuraestate/logging.yaml)
      3) project root (one level up) (../logging.yaml)
      4) current working directory (logging.yaml)
    """
    candidates: list[Path] = []
    if config_path:
        candidates.append(Path(config_path))

    here = Path(__file__).resolve().parent
    candidates.extend([
        here / "logging.yaml",
        here.parent / "logging.yaml",
        Path.cwd() / "logging.yaml",
    ])

    for p in candidates:
        try:
            if p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                logging.config.dictConfig(config)
                logging.getLogger(__name__).info(f"Loaded logging config from {p}")
                return
        except Exception as e:
            # try next candidate
            continue

    # Fallback if none found/loaded
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger(__name__).warning("logging.yaml not found; using basicConfig")

