# pip install pandas scikit-learn sqlalchemy psycopg2-binary
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
import joblib

ENGINE = create_engine("postgresql+psycopg2://appuser:appsecret@localhost:5433/neuraestate", future=True)

# 1) Read features
core = pd.read_sql("SELECT * FROM re.vw_listing_core", ENGINE)
amen = pd.read_sql("SELECT listing_id, name AS amenity FROM re.mv_listing_amenities", ENGINE)

# 2) Pivot amenities to one-hot
amen_wide = (amen.assign(val=1)
                  .pivot_table(index="listing_id", columns="amenity", values="val", fill_value=0)
                  .add_prefix("amen_"))
df = core.merge(amen_wide, left_on="id", right_index=True, how="left").fillna(0)

# 3) Build X/y
target = "price"
drop_cols = ["id","posted_at","scraped_at","currency"]
X = df.drop(columns=[c for c in drop_cols if c in df.columns] + [target])
y = df[target]

num_cols = X.select_dtypes(include=["number"]).columns.tolist()
cat_cols = X.select_dtypes(exclude=["number"]).columns.tolist()

pre = ColumnTransformer([
    ("num", SimpleImputer(strategy="median"), num_cols),
    ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                      ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat_cols)
])

model = Pipeline([("prep", pre),
                  ("rf", RandomForestRegressor(n_estimators=300, random_state=42))])

Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
model.fit(Xtr, ytr)
print("R^2 on holdout:", model.score(Xte, yte))

joblib.dump(model, "price_model.joblib")

# 4) (Optional) write predictions back for dashboards
pred = model.predict(X)
pred_df = df[["id"]].copy()
pred_df["pred_price"] = pred

with ENGINE.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS re.predictions(
          listing_id BIGINT PRIMARY KEY REFERENCES re.listings(id) ON DELETE CASCADE,
          pred_price NUMERIC(15,2) NOT NULL,
          scored_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """))
    # Upsert predictions
    rows = [{"listing_id": int(r.id), "pred_price": float(r.pred_price)} for r in pred_df.itertuples()]
    conn.execute(text("""
        INSERT INTO re.predictions(listing_id, pred_price)
        VALUES (:listing_id, :pred_price)
        ON CONFLICT (listing_id) DO UPDATE SET
          pred_price = EXCLUDED.pred_price,
          scored_at = now();
    """), rows)

print("Wrote predictions to re.predictions")
