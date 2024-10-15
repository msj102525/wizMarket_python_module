from pydantic import BaseModel
from typing import Optional

class District(BaseModel):
    district_id: Optional[int]= None
    city_id: int
    district_name: str

    class Config:
        from_attributes = True

class DistrictRequest(BaseModel):
    district_name: str  # 클라이언트가 보내는 시/군/구명

