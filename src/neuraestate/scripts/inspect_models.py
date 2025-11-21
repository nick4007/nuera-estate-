# run from project root: python scripts/inspect_models.py
import os
import sys

# ensure app src is on path (adjust if your package path differs)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from neuraestate import models
from neuraestate.db import SessionLocal  # adjust import if your session factory is elsewhere

# show table column names
try:
    cols = [c.name for c in models.Listing.__table__.columns]
    print("Listing table columns:", cols)
except Exception as e:
    print("Error listing columns:", e)

# attempt a quick sample query
try:
    with SessionLocal() as db:
        sample = db.query(models.Listing).limit(1).all()
        print("Sample rows (repr):", sample)
except Exception as e:
    print("Error querying sample rows:", e)
