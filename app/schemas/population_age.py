from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime, timedelta

class PopulationAge(BaseModel):
    pop_age_id: int
    city_id: int
    district_id: int
    sub_district_id: int
    gender_id: int
    ref_date: date
    age_under_10s: int
    age_10s: int
    age_20s: int
    age_30s: int
    age_40s: int
    age_50s: int
    age_plus_60s: int
    total_population_by_gender:int
    total_population:int
    created_at: datetime

    class Config:
        from_attributes = True


class PopAgeByRegionOutPut(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    gender_id: int
    reference_id: int
    ref_date: date
    age_under_10s: int
    age_10s: int
    age_20s: int
    age_30s: int
    age_40s: int
    age_50s: int
    age_plus_60s: int
    total_population_by_gender:int
    total_population:int