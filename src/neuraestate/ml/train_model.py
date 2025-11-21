# src/neuraestate/ml/train_model.py
"""
Train a baseline price prediction model on ods_listings and save as joblib.

Usage:
    python src/neuraestate/ml/train_model.py

Requirements (in requirements.txt):
    scikit-learn, joblib, pandas, sqlalchemy, python-dotenv

Outputs:
    - src/neuraestate/ml/price_model.joblib
    - src/neuraestate/ml/model_metadata.json
"""

import os
import json
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib

# -----------------------
# Config
# -----------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set in .env")

# Where to save
OUT_DIR = Path(__file__).resolve().parents[0]
MODEL_PATH = OUT_DIR / "price_model.joblib"
META_PATH = OUT_DIR / "model_metadata.json"

RANDOM_STATE = 42
TEST_SIZE = 0.2

# Features to use (adjust if you have more)
NUM_FEATURES = ["area_sqft", "bhk", "bathrooms"]
CAT_FEATURES = ["city"]
TARGET = "price_inr"

# Minimal sanity thresholds (you can tweak)
MIN_ROWS = 100

# -----------------------
# Load data
# -----------------------
def load_data():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    sql = """
    SELECT
      price_inr,
      area_sqft,
      bhk,
      bathrooms,
      city
    FROM ods_listings
    WHERE price_inr IS NOT NULL
      AND area_sqft IS NOT NULL
      AND bhk IS NOT NULL
    """
    df = pd.read_sql(sql, engine)
    return df

# -----------------------
# Build pipeline
# -----------------------
def build_pipeline():
    # numeric transformer: median imputer + scaler
    numeric_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    # categorical transformer: fill missing and one-hot encode
    # NOTE: use sparse_output=False for modern sklearn versions
    categorical_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value="__missing__")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    preprocessor = ColumnTransformer(transformers=[
        ("num", numeric_transformer, NUM_FEATURES),
        ("cat", categorical_transformer, CAT_FEATURES),
    ], remainder="drop")

    model = Pipeline([
        ("pre", preprocessor),
        ("rf", RandomForestRegressor(n_estimators=200, n_jobs=-1, random_state=RANDOM_STATE))
    ])

    return model

# -----------------------
# Train / evaluate
# -----------------------
def train_and_save(df):
    if len(df) < MIN_ROWS:
        raise RuntimeError(f"Not enough rows to train (found {len(df)}). Need >= {MIN_ROWS}")

    X = df[NUM_FEATURES + CAT_FEATURES].copy()
    y = df[TARGET].astype(float).copy()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    pipeline = build_pipeline()
    print("Training model on", len(X_train), "rows...")
    pipeline.fit(X_train, y_train)

    print("Predicting on test set...")
    y_pred = pipeline.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    # compute RMSE without using squared= kwarg (compatibility)
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    r2 = r2_score(y_test, y_pred)

    print(f"MAE: {mae:.2f}")
    print(f"RMSE: {rmse:.2f}")
    print(f"R2: {r2:.4f}")

    # Save model
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)
    print("Saved model to", MODEL_PATH)

    # Save metadata
    meta = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "rows_total": len(df),
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)),
        "mae": float(mae),
        "rmse": float(rmse),
        "r2": float(r2),
        "target": TARGET,
        "features": NUM_FEATURES + CAT_FEATURES,
        "sklearn_version": None,
    }
    try:
        import sklearn
        meta["sklearn_version"] = sklearn.__version__
    except Exception:
        pass

    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print("Saved metadata to", META_PATH)

    return pipeline, meta

# -----------------------
# Main
# -----------------------
def main():
    print("Loading data from DB...")
    df = load_data()
    print("Loaded rows:", len(df))

    # quick data checks
    print("Sample:")
    print(df.head(3))

    pipeline, meta = train_and_save(df)
    print("Training finished. Summary:")
    print(json.dumps(meta, indent=2))

if __name__ == "__main__":
    main()
