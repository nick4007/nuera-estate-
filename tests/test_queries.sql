-- =========================
-- NeuraEstate - test_queries.sql
-- Idempotent demo inserts + checks
-- =========================

SET search_path TO re, public;

-- 0) Quick sanity: what objects exist?
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 're'
ORDER BY table_name;

-- ---------------------------------
-- 1) Demo listings (UPSERTS)
-- ---------------------------------
INSERT INTO re.listings (
  source, external_id, title, description, price, currency,
  listing_type, property_type, bedrooms, bathrooms, carpet_area_sqft,
  furnishing, floor, total_floors, facing,
  address, locality, city, state, pincode,
  latitude, longitude, builder_name, project_name,
  listing_url, posted_at, scraped_at, status
)
VALUES
('99acres','demo-1','Test Flat','sample 2BHK', 9500000,'INR',
 'sale','apartment',2,2,700,
 'semi-furnished',3,12,'East',
 'Sector 5, Ghansoli','Ghansoli','Navi Mumbai','Maharashtra','400701',
 19.1300,73.0005,'ABC Builders','Blue Skies',
 'https://example.com/demo-1', now(), now(),'active'),

('99acres','demo-2','Sea View 2BHK','corner unit', 12500000,'INR',
 'sale','apartment',2,2,780,
 'unfurnished',12,22,'West',
 'Palm Beach Rd','Seawoods','Navi Mumbai','Maharashtra','400706',
 19.0300,73.0200,'XYZ Constructions','Sea Breeze',
 'https://example.com/demo-2', now(), now(),'active'),

('99acres','demo-3','Modern 3BHK with Balcony','near station', 18500000,'INR',
 'sale','apartment',3,3,980,
 'semi-furnished',12,22,'East',
 'Sector 5, Ghansoli','Ghansoli','Navi Mumbai','Maharashtra','400701',
 19.1304,73.0009,'ABC Builders','Blue Skies',
 'https://example.com/demo-3', now(), now(),'active')
ON CONFLICT (source, external_id) DO UPDATE SET
  title            = EXCLUDED.title,
  description      = EXCLUDED.description,
  price            = EXCLUDED.price,
  currency         = EXCLUDED.currency,
  listing_type     = EXCLUDED.listing_type,
  property_type    = EXCLUDED.property_type,
  bedrooms         = EXCLUDED.bedrooms,
  bathrooms        = EXCLUDED.bathrooms,
  carpet_area_sqft = EXCLUDED.carpet_area_sqft,
  furnishing       = EXCLUDED.furnishing,
  floor            = EXCLUDED.floor,
  total_floors     = EXCLUDED.total_floors,
  facing           = EXCLUDED.facing,
  address          = EXCLUDED.address,
  locality         = EXCLUDED.locality,
  city             = EXCLUDED.city,
  state            = EXCLUDED.state,
  pincode          = EXCLUDED.pincode,
  latitude         = EXCLUDED.latitude,
  longitude        = EXCLUDED.longitude,
  builder_name     = EXCLUDED.builder_name,
  project_name     = EXCLUDED.project_name,
  listing_url      = EXCLUDED.listing_url,
  posted_at        = EXCLUDED.posted_at,
  scraped_at       = EXCLUDED.scraped_at,
  status           = EXCLUDED.status;

-- ---------------------------------
-- 2) Amenities dictionary (dedupe)
-- ---------------------------------
INSERT INTO re.amenities(name) VALUES
 ('Swimming Pool'), ('Gym'), ('Power Backup'), ('Lift'),
 ('Parking'), ('Club House'), ('Children Play Area')
ON CONFLICT (name) DO NOTHING;

-- ---------------------------------
-- 3) Link amenities to listings (idempotent)
-- ---------------------------------
-- demo-1: Pool, Lift, Parking
INSERT INTO re.listing_amenities(listing_id, amenity_id)
SELECT l.id, a.id
FROM re.listings l
JOIN re.amenities a ON a.name IN ('Swimming Pool','Lift','Parking')
WHERE l.external_id = 'demo-1'
ON CONFLICT DO NOTHING;

-- demo-2: Gym, Power Backup, Club House
INSERT INTO re.listing_amenities(listing_id, amenity_id)
SELECT l.id, a.id
FROM re.listings l
JOIN re.amenities a ON a.name IN ('Gym','Power Backup','Club House')
WHERE l.external_id = 'demo-2'
ON CONFLICT DO NOTHING;

-- demo-3: Pool, Gym, Lift, Children Play Area
INSERT INTO re.listing_amenities(listing_id, amenity_id)
SELECT l.id, a.id
FROM re.listings l
JOIN re.amenities a ON a.name IN ('Swimming Pool','Gym','Lift','Children Play Area')
WHERE l.external_id = 'demo-3'
ON CONFLICT DO NOTHING;

-- ---------------------------------
-- 4) Images (replace-or-append demo)
-- ---------------------------------
-- Add one image per demo listing
INSERT INTO re.images(listing_id, url, position, scraped_at)
VALUES
((SELECT id FROM re.listings WHERE external_id='demo-1'), 'https://example.com/img/demo1_1.jpg', 1, now()),
((SELECT id FROM re.listings WHERE external_id='demo-2'), 'https://example.com/img/demo2_1.jpg', 1, now()),
((SELECT id FROM re.listings WHERE external_id='demo-3'), 'https://example.com/img/demo3_1.jpg', 1, now())
ON CONFLICT DO NOTHING;

-- ---------------------------------
-- 5) Refresh MV and quick checks
-- ---------------------------------
REFRESH MATERIALIZED VIEW re.mv_listing_amenities;

-- Latest 5 listings
SELECT * FROM re.vw_listing_core ORDER BY id DESC LIMIT 5;

-- Amenities for each demo
SELECT l.external_id, a.name AS amenity
FROM re.listings l
JOIN re.mv_listing_amenities a ON a.listing_id = l.id
WHERE l.external_id IN ('demo-1','demo-2','demo-3')
ORDER BY l.external_id, a.name;

-- Images for demo-3
SELECT i.*
FROM re.images i
WHERE i.listing_id = (SELECT id FROM re.listings WHERE external_id='demo-3')
ORDER BY i.position;

-- ---------------------------------
-- 6) Optional clean-up helpers (comment-in to use)
-- ---------------------------------
-- -- Remove all rows for the demo listings (keeps structure)
-- -- DELETE FROM re.images WHERE listing_id IN (SELECT id FROM re.listings WHERE external_id LIKE 'demo-%');
-- -- DELETE FROM re.listing_amenities WHERE listing_id IN (SELECT id FROM re.listings WHERE external_id LIKE 'demo-%');
-- -- DELETE FROM re.listings WHERE external_id LIKE 'demo-%';
-- -- REFRESH MATERIALIZED VIEW re.mv_listing_amenities;