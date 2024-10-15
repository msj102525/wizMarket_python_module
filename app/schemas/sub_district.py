from pydantic import BaseModel
from typing import Optional

class SubDistrict(BaseModel):
    sub_district_id: Optional[int]= None
    district_id: int
    city_id: int
    sub_district_name: str

    class Config:
        from_attributes = True

