# -*- coding: utf-8 -*-
"""
inspect_models.py -- robust inspector for neuraestate Listing model
Run from project root (where src/ lives): python .\scripts\inspect_models.py
"""
import os, sys, importlib, traceback

# ensure src is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

print("Running inspect_models.py")
print("cwd:", os.getcwd())
print("sys.path[0]:", sys.path[0])
print("sys.path (first 5):", sys.path[:5])

# helper to try importing module a few ways
candidates = [
    "neuraestate.models",
    "models",
]

models_mod = None
for cand in candidates:
    try:
        models_mod = importlib.import_module(cand)
        print(f"Imported module: {cand} ->", getattr(models_mod, "__file__", "<no __file__>"))
        break
    except Exception as e:
        print(f"Could not import {cand!r}: {e}")

if models_mod is None:
    # try listing files under src/neuraestate to help debug
    pkg_dir = os.path.join(os.path.dirname(__file__), "..", "src", "neuraestate")
    print("Package dir expected at:", os.path.abspath(pkg_dir))
    try:
        print("Files in package dir:", os.listdir(os.path.abspath(pkg_dir)))
    except Exception as e:
        print("Could not list package dir:", e)
    raise SystemExit("Failed to import models module; check module path or filenames.")

# now import session / engine - try common locations
SessionLocal = None
for cand in ["neuraestate.db", "neuraestate.database", "db", "database"]:
    try:
        mod = importlib.import_module(cand)
        print(f"Imported DB helper module: {cand} ->", getattr(mod, "__file__", "<no __file__>"))
        if hasattr(mod, "SessionLocal"):
            SessionLocal = getattr(mod, "SessionLocal")
            print("Found SessionLocal in", cand)
            break
    except Exception as e:
        print(f"Could not import DB module {cand!r}: {e}")

# fallback: try to import SessionLocal from neuraestate.db directly if not found
if SessionLocal is None:
    try:
        from neuraestate.db import SessionLocal  # noqa
        print("Imported SessionLocal from neuraestate.db")
    except Exception as e:
        print("Could not import SessionLocal from neuraestate.db:", e)

# locate Listing class
if not hasattr(models_mod, "Listing"):
    print("models module does NOT have attribute 'Listing'. Available names:", [n for n in dir(models_mod) if not n.startswith("_")][:50])
    raise SystemExit("models.Listing not found; check model filename/classname.")

Listing = models_mod.Listing

# print info about Listing
try:
    print("Listing repr:", Listing)
    # try to print mapped table columns if SQLAlchemy model
    if hasattr(Listing, "__table__"):
        cols = [c.name for c in Listing.__table__.columns]
        print("Listing.__table__ columns:", cols)
    else:
        print("Listing has no __table__ attribute. Attributes on Listing class:", [a for a in dir(Listing) if not a.startswith("_")][:200])
except Exception:
    print("Error introspecting Listing:")
    traceback.print_exc()

# try a quick DB query if we have SessionLocal
if SessionLocal:
    try:
        with SessionLocal() as db:
            print("Session opened OK. Attempting sample query...")
            sample = db.query(Listing).limit(2).all()
            print("Sample rows (repr):", sample)
    except Exception:
        print("Error running DB query:")
        traceback.print_exc()
else:
    print("SessionLocal not available; skipping DB query.")
