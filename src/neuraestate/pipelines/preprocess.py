# src/neuraestate/pipelines/preprocess.py
"""
NeuraEstate - preprocessing pipeline with ETL run logging and staging marking.

Usage:
    python src/neuraestate/pipelines/preprocess.py

Dependencies:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv tqdm
"""

import os
import json
from datetime import datetime, timezone
from typing import Optional, List

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from tqdm import tqdm

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

import psycopg2
from psycopg2.extras import execute_values

# -------------------------
# CONFIG
# -------------------------
load_dotenv()  # loads .env from repo root
DATABASE_URL = os.getenv("DATABASE_URL")
BATCH_SIZE = int(os.getenv("PREPROCESS_BATCH_SIZE", "1000"))

# sanity thresholds (tweak if needed)
MIN_AREA_SQFT = float(os.getenv("MIN_AREA_SQFT", "50"))
MAX_AREA_SQFT = float(os.getenv("MAX_AREA_SQFT", "20000"))
MIN_PRICE_INR = float(os.getenv("MIN_PRICE_INR", "100"))
MAX_PRICE_INR = float(os.getenv("MAX_PRICE_INR", "200000000"))

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set in .env")

# -------------------------
# HELPERS
# -------------------------
def safe_str(x: Optional[object]) -> Optional[str]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    return s if s != "" else None


def normalize_city(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return safe_str(s).title()


def parse_timestamp(x) -> Optional[datetime]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo is not None else x.replace(tzinfo=timezone.utc)
    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return None
        # make timezone-aware UTC if naive
        if dt.tzinfo is None:
            dt = dt.tz_localize(timezone.utc)
        return dt.to_pydatetime()
    except Exception:
        return None


def compute_price_per_sqft(price_inr, area_sqft) -> Optional[float]:
    try:
        if price_inr is None or area_sqft is None:
            return None
        if float(area_sqft) <= 0:
            return None
        return round(float(price_inr) / float(area_sqft), 2)
    except Exception:
        return None


def compute_price_per_bhk(price_inr, bhk) -> Optional[float]:
    try:
        if price_inr is None or bhk is None:
            return None
        if float(bhk) <= 0:
            return None
        return round(float(price_inr) / float(bhk), 2)
    except Exception:
        return None


# -------------------------
# DB DDL / Upsert
# -------------------------
def create_ods_table_if_not_exists(engine):
    """
    Create ods_listings using engine.begin() so it is committed and visible
    to other DB connections (psycopg2) immediately.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS ods_listings (
        id SERIAL PRIMARY KEY,
        source_id TEXT UNIQUE,
        source TEXT,
        source_page_url TEXT,
        card_index INTEGER,
        title TEXT,
        raw_price TEXT,
        price_inr BIGINT,
        bhk INTEGER,
        bathrooms REAL,
        area_sqft REAL,
        city TEXT,
        image_url TEXT,
        card_text TEXT,
        first_seen_at TIMESTAMP,
        last_seen_at TIMESTAMP,
        price_per_sqft REAL,
        price_per_bhk REAL,
        processed_at TIMESTAMP,
        raw_json JSONB
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def create_etl_runs_table_if_not_exists(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS etl_runs (
        id SERIAL PRIMARY KEY,
        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        rows_in INT,
        rows_out INT,
        notes TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def upsert_df_to_postgres(df: pd.DataFrame, database_url: str, table_name: str = "ods_listings", batch_size: int = 500):
    """
    Upsert DataFrame into Postgres using a real psycopg2 connection and execute_values.
    Converts numpy/pandas types to native Python types before insert.
    """
    if df.empty:
        print("No rows to upsert.")
        return

    # Replace NaNs with None
    df = df.where(pd.notnull(df), None)

    # Parse DB URL
    url = make_url(database_url)
    conn_kwargs = {
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
        "host": url.host or "localhost",
        "port": url.port or 5432,
    }

    cols = list(df.columns)
    col_names = ", ".join(cols)
    update_cols = [c for c in cols if c != "source_id"]
    update_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    insert_sql = f"""
    INSERT INTO {table_name} ({col_names})
    VALUES %s
    ON CONFLICT (source_id) DO UPDATE
    SET {update_sql};
    """

    # Convert each value to a native Python type acceptable to psycopg2
    def _convert_value(v):
        if v is None:
            return None
        # numpy scalar
        if isinstance(v, np.generic):
            if np.issubdtype(type(v), np.integer):
                return int(v)
            if np.issubdtype(type(v), np.floating):
                return float(v)
            if np.issubdtype(type(v), np.bool_):
                return bool(v)
            return v.item()
        # pandas Timestamp
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        # lists/dicts -> JSON string
        if isinstance(v, (list, dict)):
            try:
                return json.dumps(v)
            except Exception:
                return str(v)
        # bytes ok
        if isinstance(v, (bytes, bytearray)):
            return v
        return v

    # Build values list of tuples
    records = df.to_records(index=False)
    values = []
    for rec in records:
        row = tuple(_convert_value(v) for v in rec)
        values.append(row)

    # Upsert via psycopg2
    pg_conn = None
    try:
        pg_conn = psycopg2.connect(**conn_kwargs)
        with pg_conn.cursor() as cur:
            for i in range(0, len(values), batch_size):
                batch = values[i : i + batch_size]
                execute_values(cur, insert_sql, batch, page_size=batch_size)
        pg_conn.commit()
    except Exception as e:
        if pg_conn:
            pg_conn.rollback()
        print("ERROR during upsert:", e)
        raise
    finally:
        if pg_conn:
            pg_conn.close()


# -------------------------
# Mark staging rows & ETL logging
# -------------------------
def mark_staging_processed_for_source_ids(database_url: str, source_ids: List[str]):
    """
    Mark rows in stg_mb_listings as processed by setting processed_at to the value in ods_listings.
    Uses psycopg2 to run a parameterized query with array of source_ids.
    """
    if not source_ids:
        return

    url = make_url(database_url)
    conn_kwargs = {
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
        "host": url.host or "localhost",
        "port": url.port or 5432,
    }

    sql = """
    UPDATE stg_mb_listings s
    SET processed_at = o.processed_at
    FROM ods_listings o
    WHERE s.pk::text = o.source_id
      AND s.pk::text = ANY(%s)
    """

    pg_conn = None
    try:
        pg_conn = psycopg2.connect(**conn_kwargs)
        with pg_conn.cursor() as cur:
            cur.execute(sql, (source_ids,))
        pg_conn.commit()
    except Exception as e:
        if pg_conn:
            pg_conn.rollback()
        print("ERROR marking staging processed:", e)
        raise
    finally:
        if pg_conn:
            pg_conn.close()


def insert_etl_run(database_url: str, start_ts: datetime, end_ts: datetime, rows_in: int, rows_out: int, notes: Optional[str] = None):
    """
    Insert a row into etl_runs table (uses psycopg2).
    """
    url = make_url(database_url)
    conn_kwargs = {
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
        "host": url.host or "localhost",
        "port": url.port or 5432,
    }

    sql = """
    INSERT INTO etl_runs (start_ts, end_ts, rows_in, rows_out, notes)
    VALUES (%s, %s, %s, %s, %s)
    """

    pg_conn = None
    try:
        pg_conn = psycopg2.connect(**conn_kwargs)
        with pg_conn.cursor() as cur:
            cur.execute(sql, (start_ts, end_ts, rows_in, rows_out, notes))
        pg_conn.commit()
    except Exception as e:
        if pg_conn:
            pg_conn.rollback()
        print("ERROR inserting etl_runs:", e)
        raise
    finally:
        if pg_conn:
            pg_conn.close()


# -------------------------
# CLEAN / FEATURE ENGINEERING
# -------------------------
def df_clean_steps(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    expected = [
        "pk",
        "source",
        "source_page_url",
        "card_index",
        "title",
        "price_inr",
        "bhk",
        "bathrooms",
        "area_sqft",
        "city",
        "image_url",
        "card_text",
        "first_seen_at",
        "last_seen_at",
        "raw_json",
        "raw_price",
    ]
    for c in expected:
        if c not in df.columns:
            df[c] = None

    df["source_id"] = df["pk"].astype(str)

    # numeric conversions
    df["price_inr"] = pd.to_numeric(df["price_inr"], errors="coerce")
    df["area_sqft"] = pd.to_numeric(df["area_sqft"], errors="coerce")
    df["bhk"] = pd.to_numeric(df["bhk"], errors="coerce")
    df["bathrooms"] = pd.to_numeric(df["bathrooms"], errors="coerce")

    # timestamps
    df["first_seen_at"] = df["first_seen_at"].apply(parse_timestamp)
    df["last_seen_at"] = df["last_seen_at"].apply(parse_timestamp)

    before = len(df)
    df = df.dropna(subset=["price_inr", "area_sqft", "bhk"])
    after_drop = len(df)
    print(f"Dropped {before - after_drop} rows that were missing price/area/bhk")

    if df.empty:
        return df

    # filter outliers
    df = df[(df["area_sqft"] >= MIN_AREA_SQFT) & (df["area_sqft"] <= MAX_AREA_SQFT)]
    df = df[(df["price_inr"] >= MIN_PRICE_INR) & (df["price_inr"] <= MAX_PRICE_INR)]

    # fill bathrooms by city median, fallback to global median
    global_median_bath = df["bathrooms"].median()
    df["bathrooms"] = df.groupby("city")["bathrooms"].transform(lambda x: x.fillna(x.median()))
    df["bathrooms"] = df["bathrooms"].fillna(global_median_bath)

    # normalize text
    df["city"] = df["city"].apply(normalize_city)
    df["title"] = df["title"].apply(safe_str)
    df["source"] = df["source"].apply(safe_str)
    df["source_page_url"] = df["source_page_url"].apply(safe_str)
    df["image_url"] = df["image_url"].apply(safe_str)
    df["card_text"] = df["card_text"].apply(safe_str)
    df["raw_price"] = df["raw_price"].apply(safe_str)

    # features
    df["price_per_sqft"] = df.apply(lambda r: compute_price_per_sqft(r["price_inr"], r["area_sqft"]), axis=1)
    df["price_per_bhk"] = df.apply(lambda r: compute_price_per_bhk(r["price_inr"], r["bhk"]), axis=1)

    # timezone-aware processed_at
    df["processed_at"] = datetime.now(timezone.utc)

    # raw_json serialization
    def serialize_raw_json(v):
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            try:
                return json.dumps(v)
            except Exception:
                return str(v)
        return safe_str(v)

    df["raw_json"] = df["raw_json"].apply(serialize_raw_json)

    ods_cols = [
        "source_id",
        "source",
        "source_page_url",
        "card_index",
        "title",
        "raw_price",
        "price_inr",
        "bhk",
        "bathrooms",
        "area_sqft",
        "city",
        "image_url",
        "card_text",
        "first_seen_at",
        "last_seen_at",
        "price_per_sqft",
        "price_per_bhk",
        "processed_at",
        "raw_json",
    ]

    df_out = df.reindex(columns=ods_cols)
    return df_out


# -------------------------
# MAIN
# -------------------------
def main():
    start_ts = datetime.now(timezone.utc)
    print("Starting NeuraEstate preprocess pipeline...", start_ts.isoformat())

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    # Create ods_listings and etl_runs tables (committed so psycopg2 sees them)
    create_ods_table_if_not_exists(engine)
    create_etl_runs_table_if_not_exists(engine)
    print("Ensured ods_listings and etl_runs tables exist.")

    # Load raw staging table
    query = "SELECT * FROM stg_mb_listings;"
    print("Loading stg_mb_listings from DB...")
    df_raw = pd.read_sql(query, engine)
    total = len(df_raw)
    print(f"Loaded {total} raw rows from stg_mb_listings")

    if total == 0:
        print("No rows in staging; exiting.")
        end_ts = datetime.now(timezone.utc)
        insert_etl_run(DATABASE_URL, start_ts, end_ts, 0, 0, "no rows")
        return

    # We'll track source_ids from the staging snapshot (so we mark only these rows processed)
    source_ids_all = df_raw["pk"].astype(str).tolist()

    # process in batches
    start = 0
    pbar = tqdm(total=total, desc="Rows processed")
    while start < total:
        end = min(start + BATCH_SIZE, total)
        batch = df_raw.iloc[start:end]
        cleaned = df_clean_steps(batch)
        if not cleaned.empty:
            upsert_df_to_postgres(cleaned, DATABASE_URL, table_name="ods_listings", batch_size=500)
        pbar.update(len(batch))
        start = end
    pbar.close()

    # finished processing
    end_ts = datetime.now(timezone.utc)

    # compute rows_out: how many of the staging source_ids have a matching ods record
    url = make_url(DATABASE_URL)
    conn_kwargs = {
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
        "host": url.host or "localhost",
        "port": url.port or 5432,
    }
    rows_out = 0
    try:
        pg_conn = psycopg2.connect(**conn_kwargs)
        with pg_conn.cursor() as cur:
            # count how many staging pk values now exist in ods_listings
            cur.execute(
                """
                SELECT COUNT(*) FROM stg_mb_listings s
                JOIN ods_listings o ON s.pk::text = o.source_id
                WHERE s.pk::text = ANY(%s)
                """,
                (source_ids_all,),
            )
            rows_out = cur.fetchone()[0]
    finally:
        if pg_conn:
            pg_conn.close()

    # mark corresponding staging rows processed
    try:
        mark_staging_processed_for_source_ids(DATABASE_URL, source_ids_all)
    except Exception as e:
        print("Warning: failed to mark staging rows processed:", e)

    # insert ETL run record
    notes = f"Processed snapshot of {len(source_ids_all)} staging rows"
    try:
        insert_etl_run(DATABASE_URL, start_ts, end_ts, len(source_ids_all), rows_out, notes)
        print(f"Inserted etl_runs entry: rows_in={len(source_ids_all)} rows_out={rows_out}")
    except Exception as e:
        print("Warning: failed to insert etl_runs record:", e)

    print("Preprocess finished. Cleaned data upserted to ods_listings.")


if __name__ == "__main__":
    main()

