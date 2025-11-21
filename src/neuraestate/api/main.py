# # src/neuraestate/api/main.py
# from fastapi import FastAPI, Depends, HTTPException, Query
# from sqlalchemy.orm import Session
# from sqlalchemy import text
# from typing import Optional
# import logging

# from . import db as db_module
# from . import schemas

# # ------------------------------------------------------------
# # Setup
# # ------------------------------------------------------------
# app = FastAPI(title="NeuraEstate API", version="1.0")
# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)

# # Dependency for DB session
# def get_db():
#     db = db_module.SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()


# # ------------------------------------------------------------
# # Health check
# # ------------------------------------------------------------
# @app.get("/health")
# def health_check():
#     return {"status": "ok"}


# # ------------------------------------------------------------
# # Listings endpoint
# # ------------------------------------------------------------
# @app.get("/listings", response_model=schemas.ListingListResponse)
# def list_listings(
#     page: int = Query(1, ge=1),
#     per_page: int = Query(50, ge=1, le=1000),
#     city: Optional[str] = None,
#     min_bhk: Optional[int] = None,
#     max_area: Optional[float] = None,
#     db: Session = Depends(get_db),
# ):
#     """
#     Paginated and filterable property listings
#     """
#     try:
#         limit = per_page
#         offset = (page - 1) * per_page

#         # Base SQL — includes id, url, and avoids missing columns
#         base_sql = """
#             SELECT
#               id,
#               source_id::text AS external_id,
#               title,
#               NULLIF(price_inr::text, '')::double precision AS price,
#               area_sqft::double precision AS area_sqft,
#               bhk::integer AS bhk,
#               bathrooms::double precision AS bathrooms,
#               city::text AS city,
#               COALESCE(NULLIF(url, ''), ('https://example.com/listing/' || source_id::text))::text AS url,
#               COALESCE(image_url, '')::text AS image_url,
#               processed_at
#             FROM ods_listings
#             WHERE 1=1
#         """

#         params = {"limit": limit, "offset": offset}
#         where_clauses = []

#         if city:
#             where_clauses.append("(LOWER(city) LIKE :city OR LOWER(title) LIKE :city)")
#             params["city"] = f"%{city.lower()}%"
#         if min_bhk is not None:
#             where_clauses.append("COALESCE(bhk,0) >= :min_bhk")
#             params["min_bhk"] = int(min_bhk)
#         if max_area is not None:
#             where_clauses.append("COALESCE(area_sqft,0) <= :max_area")
#             params["max_area"] = float(max_area)

#         if where_clauses:
#             base_sql += " AND " + " AND ".join(where_clauses)

#         # Pagination query
#         query_sql = base_sql + " ORDER BY processed_at DESC NULLS LAST LIMIT :limit OFFSET :offset"

#         # Count query
#         count_sql = "SELECT COUNT(*) FROM ods_listings WHERE 1=1"
#         if where_clauses:
#             count_sql += " AND " + " AND ".join(where_clauses)

#         total = db.execute(text(count_sql), params).scalar()
#         rows = db.execute(text(query_sql), params).mappings().all()

#         items = []
#         for r in rows:
#             items.append({
#                 "id": r.get("id"),
#                 "external_id": r.get("external_id"),
#                 "title": r.get("title"),
#                 "price": r.get("price"),
#                 "location": r.get("city"),
#                 "url": r.get("url"),
#             })

#         return {
#             "total": total,
#             "page": page,
#             "per_page": per_page,
#             "items": items,
#         }

#     except Exception as e:
#         logger.exception("Error fetching listings.")
#         raise HTTPException(status_code=500, detail=f"Failed to fetch listings: {e}")


# # ------------------------------------------------------------
# # Summary endpoint
# # ------------------------------------------------------------
# @app.get("/summary", response_model=schemas.PriceSummary)
# def price_summary(
#     city: Optional[str] = None,
#     db: Session = Depends(get_db)
# ):
#     """
#     Simple price summary stats by city (min, median, max, avg per sqft)
#     """
#     try:
#         sql = """
#             SELECT
#               MIN(price_inr) AS min_price,
#               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_inr) AS median_price,
#               MAX(price_inr) AS max_price,
#               AVG(price_inr / NULLIF(area_sqft, 0)) AS avg_price_per_sqft
#             FROM ods_listings
#             WHERE price_inr IS NOT NULL
#         """

#         params = {}
#         if city:
#             sql += " AND LOWER(city) LIKE :city"
#             params["city"] = f"%{city.lower()}%"

#         row = db.execute(text(sql), params).mappings().first()

#         return {
#             "min_price": row.get("min_price"),
#             "median_price": row.get("median_price"),
#             "max_price": row.get("max_price"),
#             "avg_price_per_sqft": row.get("avg_price_per_sqft"),
#         }

#     except Exception as e:
#         logger.exception("Error fetching price summary.")
#         raise HTTPException(status_code=500, detail=f"Failed to fetch summary: {e}")


# # ------------------------------------------------------------
# # Root
# # ------------------------------------------------------------
# @app.get("/")
# def root():
#     return {"message": "Welcome to NeuraEstate API"}


# src/neuraestate/api/main.py
import os
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session, sessionmaker

# ---------------------------
# Config / DB
# ---------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@db:5432/neuraestate"
)

# create SQLAlchemy engine & session factory
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

app = FastAPI(title="NeuraEstate API (fixed pagination & filters)")

# ---------------------------
# Local response models (keeps compatibility with frontend)
# ---------------------------
class ListingOut(BaseModel):
    id: int
    external_id: Optional[str] = None
    title: Optional[str] = None
    price: Optional[float] = None
    area_sqft: Optional[float] = None
    bhk: Optional[int] = None
    bathrooms: Optional[float] = None
    city: Optional[str] = None
    location: Optional[str] = None
    image_url: Optional[str] = None
    url: Optional[str] = None


class ListingsResponse(BaseModel):
    total: int
    page: int
    per_page: int
    items: List[ListingOut]


class PriceSummary(BaseModel):
    min_price: Optional[float]
    median_price: Optional[float]
    max_price: Optional[float]
    avg_price_per_sqft: Optional[float]


# ---------------------------
# Seller listings (user-submitted)
# ---------------------------
class SellerListingIn(BaseModel):
    title: str
    price: Optional[float] = None
    area_sqft: Optional[float] = None
    bhk: Optional[int] = None
    bathrooms: Optional[float] = None
    city: Optional[str] = None

from datetime import datetime

class SellerListingOut(SellerListingIn):
    id: int
    created_at: datetime


# ---------------------------
# DB dependency
# ---------------------------
def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------
# Health
# ---------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# Ensure user_listings table exists (lightweight bootstrap)
def _ensure_user_listings(db: Session) -> None:
    db.execute(text(
        """
        CREATE TABLE IF NOT EXISTS user_listings (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            price DOUBLE PRECISION NULL,
            area_sqft DOUBLE PRECISION NULL,
            bhk INTEGER NULL,
            bathrooms DOUBLE PRECISION NULL,
            city TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    ))
    db.commit()


@app.post("/seller/listings", response_model=SellerListingOut)
def create_seller_listing(payload: SellerListingIn, db: Session = Depends(get_db)):
    try:
        _ensure_user_listings(db)
        insert_sql = text(
            """
            INSERT INTO user_listings (title, price, area_sqft, bhk, bathrooms, city)
            VALUES (:title, :price, :area_sqft, :bhk, :bathrooms, :city)
            RETURNING id, title, price, area_sqft, bhk, bathrooms, city, created_at;
            """
        )
        row = db.execute(insert_sql, payload.dict()).mappings().one()
        db.commit()
        return dict(row)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create seller listing: {e}")


@app.get("/seller/listings", response_model=List[SellerListingOut])
def list_seller_listings(limit: int = Query(20, ge=1, le=200), db: Session = Depends(get_db)):
    try:
        _ensure_user_listings(db)
        rows = db.execute(text("SELECT id, title, price, area_sqft, bhk, bathrooms, city, created_at FROM user_listings ORDER BY created_at DESC, id DESC LIMIT :limit"), {"limit": int(limit)}).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list seller listings: {e}")


class AdminStats(BaseModel):
    total_properties: int
    new_listings_today: int

class AdminAnalytics(BaseModel):
    prices: List[float]
    areas: List[float]
    areas_for_prices: List[float]
    bhks: List[int]


@app.get("/admin/stats", response_model=AdminStats)
def admin_stats(db: Session = Depends(get_db)):
    try:
        _ensure_user_listings(db)
        total_ods = int(db.execute(text("SELECT COUNT(*) FROM ods_listings")).scalar_one() or 0)
        total_user = int(db.execute(text("SELECT COUNT(*) FROM user_listings")).scalar_one() or 0)
        total = total_ods + total_user

        new_today_ods = int(db.execute(text("SELECT COUNT(*) FROM ods_listings WHERE processed_at::date = CURRENT_DATE")).scalar_one() or 0)
        new_today_user = int(db.execute(text("SELECT COUNT(*) FROM user_listings WHERE created_at::date = CURRENT_DATE")).scalar_one() or 0)
        return {"total_properties": total, "new_listings_today": new_today_ods + new_today_user}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute admin stats: {e}")


@app.get("/admin/analytics", response_model=AdminAnalytics)
def admin_analytics(db: Session = Depends(get_db)):
    """Return sanitized arrays for charts using both tables."""
    try:
        _ensure_user_listings(db)
        # Prices from marketplace
        prices_rows = db.execute(text("""
            SELECT NULLIF(price_inr::text,'')::double precision AS price
            FROM ods_listings
            WHERE NULLIF(price_inr::text,'') IS NOT NULL
            LIMIT 2000
        """)).fetchall()
        prices = [float(r[0]) for r in prices_rows if r[0] is not None]

        # Areas and prices (paired) for scatter
        ap_rows = db.execute(text("""
            SELECT area_sqft::double precision AS area, NULLIF(price_inr::text,'')::double precision AS price
            FROM ods_listings
            WHERE area_sqft IS NOT NULL AND area_sqft > 0 AND NULLIF(price_inr::text,'') IS NOT NULL
            LIMIT 2000
        """)).fetchall()
        areas = [float(r[0]) for r in ap_rows if r[0] is not None and r[1] is not None]
        areas_for_prices = [float(r[1]) for r in ap_rows if r[0] is not None and r[1] is not None]

        # Include user_listings as well
        up_rows = db.execute(text("""
            SELECT area_sqft::double precision AS area, price::double precision AS price
            FROM user_listings
            WHERE area_sqft IS NOT NULL AND area_sqft > 0 AND price IS NOT NULL
            LIMIT 2000
        """)).fetchall()
        for a, p in up_rows:
            if a is not None and p is not None and a > 0 and p > 0:
                areas.append(float(a))
                areas_for_prices.append(float(p))
                prices.append(float(p))

        # BHKs
        bhk_rows = db.execute(text("""
            SELECT bhk FROM ods_listings WHERE bhk IS NOT NULL LIMIT 2000
        """)).fetchall()
        bhks = [int(r[0]) for r in bhk_rows if r[0] is not None]
        ub_rows = db.execute(text("""
            SELECT bhk FROM user_listings WHERE bhk IS NOT NULL LIMIT 2000
        """)).fetchall()
        bhks.extend([int(r[0]) for r in ub_rows if r[0] is not None])

        return {"prices": prices, "areas": areas, "areas_for_prices": areas_for_prices, "bhks": bhks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute analytics: {e}")


# ---------------------------
# /listings: filters + pagination
# ---------------------------
@app.get("/listings", response_model=ListingsResponse)
def list_listings(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=1000),
    city: str = Query("", description="Partial city/title search"),
    min_bhk: int = Query(0, ge=0),
    max_price: float = Query(0.0, ge=0.0),
    min_area: float = Query(0.0, ge=0.0),
    max_area: float = Query(0.0, ge=0.0),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Return paginated listings from ods_listings with the chosen filters.
    Important: this function computes a COUNT(*) with the same WHERE clauses
    (so the frontend can compute correct number of pages).
    """

    # Build WHERE clauses and parameter dict
    where_clauses: List[str] = ["1=1"]
    params: Dict[str, Any] = {}

    if city:
        # case-insensitive partial match against city and title
        where_clauses.append("(lower(coalesce(city,'') ) LIKE :city OR lower(coalesce(title,'')) LIKE :city)")
        params["city"] = f"%{city.lower()}%"

    if min_bhk and min_bhk > 0:
        where_clauses.append("coalesce(bhk,0) >= :min_bhk")
        params["min_bhk"] = int(min_bhk)

    if max_price and max_price > 0:
        # price stored in price_inr - defensive coalesce
        where_clauses.append("coalesce(price_inr,0) <= :max_price")
        params["max_price"] = float(max_price)

    if min_area and min_area > 0:
        where_clauses.append("coalesce(area_sqft,0) >= :min_area")
        params["min_area"] = float(min_area)

    if max_area and max_area > 0:
        where_clauses.append("coalesce(area_sqft,0) <= :max_area")
        params["max_area"] = float(max_area)

    where_sql = " AND ".join(where_clauses)

    # Apply a short statement timeout so slow queries don't freeze the UI
    try:
        db.execute(text("SET LOCAL statement_timeout TO 4000"))  # 4s
    except Exception:
        pass

    # 1) total count (include both marketplace and user-submitted listings)
    where_sql_ods = where_sql.replace("coalesce(price_inr,0)", "coalesce(price_inr,0)")
    where_sql_user = where_sql
    # Adjust column names for user_listings (price_inr -> price)
    where_sql_user = where_sql_user.replace("coalesce(price_inr,0)", "coalesce(price,0)")

    count_sql = (
        "SELECT (" 
        f"SELECT COUNT(*) FROM ods_listings WHERE {where_sql_ods}" 
        ") + (" 
        f"SELECT COUNT(*) FROM user_listings WHERE {where_sql_user}" 
        ") AS total;"
    )
    try:
        total_res: Result = db.execute(text(count_sql), params)
        total = int(total_res.scalar() or 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute total listings: {e}")

    # 2) paginated select (union marketplace + user listings)
    limit = int(per_page)
    offset = int((page - 1) * per_page)

    select_sql = f"""
      SELECT id, external_id, title, price, area_sqft, bhk, bathrooms, city, location, image_url, url
      FROM (
        SELECT
          id,
          source_id::text AS external_id,
          title,
          NULLIF(price_inr::text, '')::double precision AS price,
          area_sqft::double precision AS area_sqft,
          bhk::integer AS bhk,
          bathrooms::double precision AS bathrooms,
          city::text AS city,
          coalesce(city::text, '') AS location,
          image_url::text AS image_url,
          COALESCE(NULLIF(url::text,''), ('https://example.com/listing/' || source_id::text))::text AS url,
          processed_at AS ts
        FROM ods_listings
        WHERE {where_sql_ods}
        UNION ALL
        SELECT
          (1000000000 + id) AS id,   -- avoid id collision
          NULL::text AS external_id,
          title,
          price::double precision AS price,
          area_sqft::double precision AS area_sqft,
          bhk::integer AS bhk,
          bathrooms::double precision AS bathrooms,
          city::text AS city,
          coalesce(city::text, '') AS location,
          NULL::text AS image_url,
          ('/seller/' || id)::text AS url,
          created_at AS ts
        FROM user_listings
        WHERE {where_sql_user}
      ) q
      ORDER BY ts DESC NULLS LAST, id DESC
      LIMIT :limit OFFSET :offset;
    """

    params_paged = params.copy()
    params_paged["limit"] = limit
    params_paged["offset"] = offset

    try:
        rows_result: Result = db.execute(text(select_sql), params_paged)
        rows = rows_result.mappings().all()  # RowMapping objects (dict-like)
    except Exception as e:
        # If referencing 'url' caused problems (older schema), fall back to a simpler select
        fallback_sql = f"""
          SELECT
            id,
            source_id::text AS external_id,
            title,
            NULLIF(price_inr::text, '')::double precision AS price,
            area_sqft::double precision AS area_sqft,
            bhk::integer AS bhk,
            bathrooms::double precision AS bathrooms,
            city::text AS city,
            coalesce(city::text, '') AS location,
            image_url::text AS image_url,
            ('https://example.com/listing/' || source_id::text)::text AS url
          FROM ods_listings
          WHERE {where_sql}
          ORDER BY processed_at DESC NULLS LAST, id DESC
          LIMIT :limit OFFSET :offset;
        """
        try:
            rows_result = db.execute(text(fallback_sql), params_paged)
            rows = rows_result.mappings().all()
        except Exception as e2:
            raise HTTPException(status_code=500, detail=f"Failed to query listings (primary & fallback): {e2}")

    # convert RowMappings to plain dicts and ensure required 'id' exists
    items: List[Dict[str, Any]] = []
    for r in rows:
        rowdict = dict(r)
        # ensure id present and int
        if "id" not in rowdict or rowdict["id"] is None:
            # If somehow id is missing, skip that row (shouldn't happen)
            continue
        # guarantee types for JSON
        items.append(
            {
                "id": int(rowdict.get("id")),
                "external_id": rowdict.get("external_id"),
                "title": rowdict.get("title"),
                "price": float(rowdict["price"]) if rowdict.get("price") is not None else None,
                "area_sqft": float(rowdict["area_sqft"]) if rowdict.get("area_sqft") is not None else None,
                "bhk": int(rowdict["bhk"]) if rowdict.get("bhk") is not None else None,
                "bathrooms": float(rowdict["bathrooms"]) if rowdict.get("bathrooms") is not None else None,
                "city": rowdict.get("city"),
                "location": rowdict.get("location"),
                "image_url": rowdict.get("image_url"),
                "url": rowdict.get("url"),
            }
        )

    return {"total": total, "page": page, "per_page": per_page, "items": items}


# ---------------------------
# /summary: small stats used on admin/sidebar
# ---------------------------
@app.get("/summary", response_model=PriceSummary)
def summary(db: Session = Depends(get_db)):
    """
    Return a small price summary computed from ods_listings.
    Uses database aggregation for min and max; median computed in Python by fetching price_inr values (safe for ~10k rows).
    """
    try:
        # fetch numeric prices and area for computing median and avg per sqft
        sql = text("SELECT NULLIF(price_inr::text,'')::double precision AS price, area_sqft::double precision AS area_sqft FROM ods_listings WHERE NULLIF(price_inr::text,'') IS NOT NULL;")
        res: Result = db.execute(sql)
        rows = res.fetchall()
        prices = [r[0] for r in rows if r[0] is not None]
        areas = [r[1] for r in rows if r[1] is not None and r[1] > 0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Summary query failed: {e}")

    if not prices:
        return PriceSummary(min_price=None, median_price=None, max_price=None, avg_price_per_sqft=None)

    prices_sorted = sorted(prices)
    n = len(prices_sorted)
    if n % 2 == 1:
        median_price = prices_sorted[n // 2]
    else:
        median_price = 0.5 * (prices_sorted[n // 2 - 1] + prices_sorted[n // 2])

    min_price = float(min(prices_sorted))
    max_price = float(max(prices_sorted))

    # average price per sqft: compute only for rows where area exists
    price_per_sqft_values = []
    try:
        sql_pps = text("SELECT NULLIF(price_inr::text,'')::double precision AS price, area_sqft::double precision as area FROM ods_listings WHERE NULLIF(price_inr::text,'') IS NOT NULL AND area_sqft IS NOT NULL AND area_sqft > 0;")
        res2 = db.execute(sql_pps).fetchall()
        for price, area in res2:
            if price is not None and area:
                price_per_sqft_values.append(price / area)
        avg_price_per_sqft = float(sum(price_per_sqft_values) / len(price_per_sqft_values)) if price_per_sqft_values else None
    except Exception:
        avg_price_per_sqft = None

    return PriceSummary(
        min_price=min_price,
        median_price=float(median_price),
        max_price=max_price,
        avg_price_per_sqft=avg_price_per_sqft,
    )


# --- paste into src/neuraestate/api/main.py ---
# (Add imports near top of file)
from pydantic import BaseModel, Field
from typing import Optional
from fastapi import FastAPI, HTTPException

# If you already import Pydantic models from your schemas (preferred), use those.
# We'll define local models only if needed to avoid import errors.

class PredictInput(BaseModel):
    area_sqft: float = Field(..., gt=0)
    bhk: int = Field(..., gt=0)
    bathrooms: Optional[float] = None
    city: Optional[str] = None

class PredictOutput(BaseModel):
    predicted_price_inr: float
    predicted_price_per_sqft: Optional[float] = None
    model_version: Optional[str] = "v0-local"

# Assuming `app` is your FastAPI() instance defined earlier in main.py:
# @app.post("/predict", response_model=PredictOutput)
# def predict_price(inp: PredictInput):
#     """
#     Minimal local fallback prediction endpoint.
#     Replace this with your real ML model loading/inference later.
#     This returns a simple deterministic estimate:
#       predicted_price = area_sqft * base_pps * bhk_factor
#     """
#     try:
#         # Basic, deterministic rule-of-thumb estimator (replace with model)
#         base_pps = 5000.0  # base price per sqft (INR) — tweak as you like
#         bhk_factor = 1.0 + max(0, (inp.bhk - 1)) * 0.25  # +25% per extra bhk after 1
#         predicted_price = float(inp.area_sqft) * base_pps * bhk_factor
#         predicted_pps = predicted_price / float(inp.area_sqft)
#         return {
#             "predicted_price_inr": round(predicted_price, 2),
#             "predicted_price_per_sqft": round(predicted_pps, 2),
#             "model_version": "v0-local"
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")
# # --- end paste ---


# src/neuraestate/api/main.py (inside FastAPI app)
from fastapi import HTTPException
from .schemas import PredictInput, PredictOutput

@app.post("/predict", response_model=PredictOutput)
def predict_price(inp: PredictInput):
    """
    Simple rule-based prediction (placeholder).
    Returns predicted price and optional valuation when actual_price is provided.
    """
    try:
        # Basic rule-based prediction (existing logic you used)
        base_pps = 5000.0  # base INR per sqft (adjust later for city)
        bhk_factor = 1.0 + max(0, (inp.bhk - 1)) * 0.25  # +25% per extra BHK
        predicted_price = float(inp.area_sqft) * base_pps * bhk_factor
        predicted_pps = predicted_price / float(inp.area_sqft)

        # Optional valuation classification when actual_price provided
        valuation = None
        if inp.actual_price is not None:
            actual = float(inp.actual_price)
            if actual > predicted_price * 1.1:
                valuation = "Overpriced"
            elif actual < predicted_price * 0.9:
                valuation = "Underpriced"
            else:
                valuation = "Fairly Priced"

        return {
            "predicted_price_inr": round(predicted_price, 2),
            "predicted_price_per_sqft": round(predicted_pps, 2),
            "model_version": "v0-local",
            "valuation": valuation,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")


