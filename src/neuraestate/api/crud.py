# src/neuraestate/api/crud.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Tuple, List, Dict, Any

def get_listings_count(db: Session, filters: dict) -> int:
    where, params = _build_where(filters)
    q = f"SELECT COUNT(*) FROM ods_listings {where}"
    r = db.execute(text(q), params).scalar_one()
    return int(r or 0)

def get_listings(db: Session, filters: dict, skip: int, limit: int) -> List[Dict[str,Any]]:
    where, params = _build_where(filters)
    q = f"""
    SELECT source_id, title, price_inr, area_sqft, bhk, bathrooms, city,
           price_per_sqft, price_per_bhk, source_page_url, image_url
    FROM ods_listings
    {where}
    ORDER BY processed_at DESC NULLS LAST
    OFFSET :skip LIMIT :limit
    """
    params.update({"skip": skip, "limit": limit})
    rows = db.execute(text(q), params).mappings().all()
    return [dict(r) for r in rows]

def get_listing_by_id(db: Session, source_id: str):
    q = """
    SELECT source_id, title, price_inr, area_sqft, bhk, bathrooms, city,
           price_per_sqft, price_per_bhk, source_page_url, image_url, card_text
    FROM ods_listings
    WHERE source_id = :sid
    LIMIT 1
    """
    r = db.execute(text(q), {"sid": source_id}).mappings().first()
    return dict(r) if r else None

def get_price_summary(db: Session, filters: dict):
    where, params = _build_where(filters)
    # use percentile_cont(0.5) for median
    q = f"""
    SELECT
      min(price_inr) AS min_price,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price_inr) AS median_price,
      max(price_inr) AS max_price,
      AVG(price_per_sqft) AS avg_price_per_sqft
    FROM ods_listings
    {where}
    """
    r = db.execute(text(q), params).mappings().first()
    return dict(r) if r else None

def _build_where(filters: dict) -> Tuple[str, dict]:
    clauses = []
    params = {}
    if not filters:
        return ("", params)
    if filters.get("city"):
        clauses.append("city = :city")
        params["city"] = filters["city"]
    if filters.get("min_price") is not None:
        clauses.append("price_inr >= :min_price")
        params["min_price"] = filters["min_price"]
    if filters.get("max_price") is not None:
        clauses.append("price_inr <= :max_price")
        params["max_price"] = filters["max_price"]
    if filters.get("bhk") is not None:
        clauses.append("bhk = :bhk")
        params["bhk"] = filters["bhk"]
    if clauses:
        return ("WHERE " + " AND ".join(clauses), params)
    return ("", params)
