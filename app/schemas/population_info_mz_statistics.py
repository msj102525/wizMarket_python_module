from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime, timedelta

class PopulationMZ(BaseModel):
    pop_info_mz_stat_id: int
    city_id: int
    district_id: int
    sub_district_id: int
    avg_val: float
    med_val: float
    std_val: float
    max_val: float
    min_val: float
    j_score: float
    ref_date: date
    stat_level: str
    created_at: datetime

    class Config:
        from_attributes = True


class NationMZPopOutPut(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    ref_date: date
    mz_pop:int

    class Config:
        from_attributes = True


class CityMZPopOutPut(BaseModel):
    city_id: int
    sub_district_id: int
    ref_date: date
    mz_pop:int

    class Config:
        from_attributes = True


class DistrictMZPopOutPut(BaseModel):
    district_id: int
    sub_district_id: int
    ref_date: date
    mz_pop:int

    class Config:
        from_attributes = True

class InsertMzPopStat(BaseModel):
    city_id: Optional[int]
    district_id: Optional[int]
    sub_district_id: int
    ref_date: date
    avg_val: Optional[float]
    med_val: Optional[float]
    std_val: Optional[float]
    max_val: Optional[int]
    min_val: Optional[int]
    j_score_rank: Optional[float]
    j_score_per: Optional[float]
    j_score: Optional[float]
    j_score_rank_non_outliers: Optional[float]
    j_score_per_non_outliers: Optional[float]
    j_score_non_outliers: Optional[float]
    stat_level: str

    class Config:
        from_attributes = True