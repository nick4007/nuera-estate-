# # src/neuraestate/api/schemas.py
# from typing import Optional, List
# from pydantic import BaseModel, Field


# # ------------------------------------------------------------
# # Listing schemas (aligned with db.models.Listing)
# # ------------------------------------------------------------
# class ListingBase(BaseModel):
#     id: int
#     external_id: str
#     title: str
#     price: Optional[float] = None
#     location: Optional[str] = None
#     url: Optional[str] = None

#     # Pydantic v2 equivalent of orm_mode=True
#     model_config = {"from_attributes": True}


# class ListingListResponse(BaseModel):
#     total: int
#     page: int
#     per_page: int
#     items: List[ListingBase]


# # ------------------------------------------------------------
# # Price summary schema (used in /summary endpoint)
# # ------------------------------------------------------------
# class PriceSummary(BaseModel):
#     min_price: Optional[float] = None
#     median_price: Optional[float] = None
#     max_price: Optional[float] = None
#     avg_price_per_sqft: Optional[float] = None


# ------------------------------------------------------------
# ML Prediction schemas (used in /predict endpoint)
# ------------------------------------------------------------
# class PredictInput(BaseModel):
#     """Input features for price prediction"""
#     area_sqft: float = Field(..., gt=0, description="Area in sqft")
#     bhk: int = Field(..., gt=0, description="Number of BHK")
#     bathrooms: Optional[float] = Field(None, description="Number of bathrooms")
#     city: Optional[str] = Field(None, description="City name (optional)")


# class PredictOutput(BaseModel):
#     """Output prediction from ML model"""
#     predicted_price_inr: float
#     predicted_price_per_sqft: Optional[float] = None
#     model_version: Optional[str] = None




# src/neuraestate/api/schemas.py
from typing import Optional, List
from pydantic import BaseModel, Field

# ------------------------------------------------------------
# Listing schemas (aligned with db.models.Listing)
# ------------------------------------------------------------
class ListingBase(BaseModel):
    # make id optional so rows that don't include id won't break validation
    id: Optional[int] = None
    external_id: Optional[str] = None
    title: Optional[str] = None
    price: Optional[float] = None
    location: Optional[str] = None
    url: Optional[str] = None

    # Pydantic v2 equivalent of orm_mode=True
    model_config = {"from_attributes": True}


class ListingListResponse(BaseModel):
    total: int
    page: int
    per_page: int
    items: List[ListingBase]


# ------------------------------------------------------------
# Price summary schema (used in /summary endpoint)
# ------------------------------------------------------------
class PriceSummary(BaseModel):
    min_price: Optional[float] = None
    median_price: Optional[float] = None
    max_price: Optional[float] = None
    avg_price_per_sqft: Optional[float] = None


# ------------------------------------------------------------
# ML Prediction schemas (used in /predict endpoint)
# ------------------------------------------------------------
# src/neuraestate/api/schemas.py
from typing import Optional, List
from pydantic import BaseModel, Field

# Listing and other schemas (unchanged) ...
# ------------------------------------------------------------
class PredictInput(BaseModel):
    """Input features for price prediction"""
    area_sqft: float = Field(..., gt=0, description="Area in sqft")
    bhk: int = Field(..., gt=0, description="Number of BHK")
    bathrooms: Optional[float] = Field(None, description="Number of bathrooms")
    city: Optional[str] = Field(None, description="City name (optional)")
    actual_price: Optional[float] = Field(None, description="(Optional) actual listing price to classify valuation")

class PredictOutput(BaseModel):
    """Output prediction from ML model"""
    predicted_price_inr: float
    predicted_price_per_sqft: Optional[float] = None
    model_version: Optional[str] = None
    valuation: Optional[str] = None  # "Fairly Priced" / "Overpriced" / "Underpriced"
