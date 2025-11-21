# from __future__ import annotations
# from typing import List
# from sqlalchemy import String, Integer, Float, ForeignKey, Table, Text, Column
# from sqlalchemy.orm import Mapped, mapped_column, relationship

# from src.neuraestate.db.base import Base


# # --- Association table for many-to-many (Listings ↔ Amenities) ---
# listing_amenities = Table(
#     "listing_amenities",
#     Base.metadata,
#     Column("listing_id", ForeignKey("listings.id"), primary_key=True),
#     Column("amenity_id", ForeignKey("amenities.id"), primary_key=True),
# )


# class Listing(Base):
#     __tablename__ = "listings"

#     id: Mapped[int] = mapped_column(Integer, primary_key=True)
#     external_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
#     title: Mapped[str] = mapped_column(String(512), nullable=False)
#     price: Mapped[float | None] = mapped_column(Float, nullable=True)   # could switch to Numeric later
#     location: Mapped[str | None] = mapped_column(String(512), nullable=True)
#     url: Mapped[str] = mapped_column(Text, nullable=False)

#     images: Mapped[List["Image"]] = relationship(
#         back_populates="listing",
#         cascade="all, delete-orphan",
#         lazy="selectin",
#     )
#     amenities: Mapped[List["Amenity"]] = relationship(
#         secondary=listing_amenities,
#         back_populates="listings",
#         lazy="selectin",
#     )


# class Image(Base):
#     __tablename__ = "images"

#     id: Mapped[int] = mapped_column(Integer, primary_key=True)
#     listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id", ondelete="CASCADE"), nullable=False)
#     url: Mapped[str] = mapped_column(Text, nullable=False)

#     listing: Mapped[Listing] = relationship(back_populates="images", lazy="selectin")


# class Amenity(Base):
#     __tablename__ = "amenities"

#     id: Mapped[int] = mapped_column(Integer, primary_key=True)
#     name: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)

#     listings: Mapped[List[Listing]] = relationship(
#         secondary=listing_amenities,
#         back_populates="amenities",
#         lazy="selectin",
#     )


# src/neuraestate/db/models.py
from __future__ import annotations
from typing import List, Optional

from sqlalchemy import String, Integer, Float, ForeignKey, Table, Text, Column
from sqlalchemy.orm import Mapped, mapped_column, relationship

# use a relative import so the package works when run with --app-dir src
from .base import Base


# --- Association table for many-to-many (Listings ↔ Amenities) ---
listing_amenities = Table(
    "listing_amenities",
    Base.metadata,
    Column("listing_id", ForeignKey("listings.id"), primary_key=True),
    Column("amenity_id", ForeignKey("amenities.id"), primary_key=True),
)


class Listing(Base):
    __tablename__ = "listings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    external_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)   # could switch to Numeric later
    location: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)

    images: Mapped[List["Image"]] = relationship(
        "Image",
        back_populates="listing",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    amenities: Mapped[List["Amenity"]] = relationship(
        "Amenity",
        secondary=listing_amenities,
        back_populates="listings",
        lazy="selectin",
    )


class Image(Base):
    __tablename__ = "images"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id", ondelete="CASCADE"), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)

    listing: Mapped["Listing"] = relationship("Listing", back_populates="images", lazy="selectin")


class Amenity(Base):
    __tablename__ = "amenities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)

    listings: Mapped[List["Listing"]] = relationship(
        "Listing",
        secondary=listing_amenities,
        back_populates="amenities",
        lazy="selectin",
    )
