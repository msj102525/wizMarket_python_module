from pydantic import BaseModel
from typing import Optional

class SubDistrict(BaseModel):
    sub_district_id: Optional[int]= None
    district_id: int
    city_id: int
    sub_district_name: str

    class Config:
        from_attributes = True

class AllRegionIdOutPut(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int

    class Config:
        from_attributes = True

class AllCitySubDistrictIdOutPut(BaseModel):
    city_id: int
    sub_district_id: int

    class Config:
        from_attributes = True


class AllDistrictSubDistrictIdOutPut(BaseModel):
    district_id: int
    sub_district_id: int

    class Config:
        from_attributes = True