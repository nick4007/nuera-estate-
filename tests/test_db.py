# pip install sqlalchemy psycopg2-binary pandas
from sqlalchemy import create_engine, text
from datetime import datetime

engine = create_engine(
    "postgresql+psycopg2://appuser:appsecret@localhost:5433/neuraestate",
    future=True
)


def upsert_listing(conn, d):
    sql = text("""
    INSERT INTO re.listings (
      source, external_id, title, price, city, scraped_at,
      listing_type, property_type, bedrooms, bathrooms, carpet_area_sqft
    )
    VALUES (
      :source, :external_id, :title, :price, :city, :scraped_at,
      :listing_type, :property_type, :bedrooms, :bathrooms, :carpet_area_sqft
    )
    ON CONFLICT (source, external_id) DO UPDATE SET
      title=EXCLUDED.title,
      price=EXCLUDED.price,
      city=EXCLUDED.city,
      listing_type=EXCLUDED.listing_type,
      property_type=EXCLUDED.property_type,
      bedrooms=EXCLUDED.bedrooms,
      bathrooms=EXCLUDED.bathrooms,
      carpet_area_sqft=EXCLUDED.carpet_area_sqft,
      scraped_at=EXCLUDED.scraped_at
    RETURNING id;
    """)
    return conn.execute(sql, d).scalar_one()

def ensure_amenity(conn, name):
    conn.execute(text("INSERT INTO re.amenities(name) VALUES (:n) ON CONFLICT (name) DO NOTHING;"), {"n": name})
    return conn.execute(text("SELECT id FROM re.amenities WHERE name=:n;"), {"n": name}).scalar_one()

def link_amenity(conn, listing_id, amenity_id):
    conn.execute(text("""
        INSERT INTO re.listing_amenities(listing_id, amenity_id)
        VALUES (:l, :a) ON CONFLICT DO NOTHING;
    """), {"l": listing_id, "a": amenity_id})

def add_image(conn, listing_id, url, pos=1):
    conn.execute(text("""
        INSERT INTO re.images(listing_id, url, position, scraped_at)
        VALUES (:l, :u, :p, now());
    """), {"l": listing_id, "u": url, "p": pos})

if __name__ == "__main__":
    with engine.begin() as conn:
        lid = upsert_listing(conn, {
            "source": "99acres",
            "external_id": "demo-2",
            "title": "Sea View 2BHK",
            "price": 12500000,
            "city": "Navi Mumbai",
            "scraped_at": datetime.utcnow(),
            "listing_type": "sale",
            "property_type": "apartment",
            "bedrooms": 2,
            "bathrooms": 2,
            "carpet_area_sqft": 700,
        })
        a_id = ensure_amenity(conn, "Gym")
        link_amenity(conn, lid, a_id)
        add_image(conn, lid, "https://example.com/demo2.jpg", 1)

    # optional: refresh MV after a batch
    with engine.begin() as conn:
        conn.execute(text("REFRESH MATERIALIZED VIEW re.mv_listing_amenities;"))
    print("Inserted/updated listing id:", lid)
