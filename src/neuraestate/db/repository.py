from __future__ import annotations
from typing import List
from sqlalchemy import select
from sqlalchemy.orm import Session

from neuraestate.db.models import Listing, Image, Amenity
from neuraestate.schemas import ListingIn


class ListingRepository:
    def __init__(self, session: Session):
        self.session = session

    def upsert_listing(self, dto: ListingIn) -> Listing:
        """
        Upsert a listing by external_id and replace its images & amenity links.
        Note: commit is the caller's responsibility.
        """
        # --- Get or create listing
        stmt = select(Listing).where(Listing.external_id == dto.external_id)
        obj: Listing | None = self.session.scalar(stmt)

        if obj is None:
            obj = Listing(
                external_id=dto.external_id,
                title=dto.title,
                price=dto.price,
                location=dto.location,
                url=str(dto.url),
            )
            self.session.add(obj)
        else:
            obj.title = dto.title
            obj.price = dto.price
            obj.location = dto.location
            obj.url = str(dto.url)

        # --- Replace images
        obj.images.clear()
        for u in dto.image_urls:
            obj.images.append(Image(url=str(u)))

        # --- Sync amenities
        names: List[str] = [n.strip() for n in dto.amenities if n and n.strip()]
        if names:
            existing = {
                a.name: a
                for a in self.session.scalars(
                    select(Amenity).where(Amenity.name.in_(names))
                ).all()
            }
            to_create = [Amenity(name=n) for n in names if n not in existing]
            if to_create:
                self.session.add_all(to_create)
                self.session.flush()  # ensure IDs are assigned
            amenities = [existing.get(n) or next(a for a in to_create if a.name == n) for n in names]
            obj.amenities = amenities
        else:
            obj.amenities.clear()

        # Ensure obj.id is populated
        self.session.flush()
        return obj

