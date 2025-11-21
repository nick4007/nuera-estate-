from typing import List, Optional
from pydantic import BaseModel, HttpUrl, Field

class ListingIn(BaseModel):
    external_id: str
    title: str
    price: Optional[float] = None
    location: Optional[str] = None
    url: HttpUrl
    image_urls: List[HttpUrl] = Field(default_factory=list)
    amenities: List[str] = Field(default_factory=list)

    model_config = {
        "extra": "ignore"  # ignore unexpected fields from scrapers
    }
