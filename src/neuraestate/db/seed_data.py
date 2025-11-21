# src/neuraestate/db/seed_data.py
from sqlalchemy.exc import IntegrityError
from .session import SessionLocal
from .models import Listing

def seed():
    db = SessionLocal()
    try:
        sample = Listing(
            external_id="seed-001",
            title="Seeded Demo Apartment",
            price=8500000.0,
            location="Mumbai",
            url="https://example.com/seed-001"
        )
        db.add(sample)
        db.commit()
        print("Seeded listing id:", sample.id)
    except IntegrityError:
        db.rollback()
        print("Seed already exists or constraint error.")
    finally:
        db.close()

if __name__ == "__main__":
    seed()
