from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime, timedelta

class LocInfoStat(BaseModel):
    loc_info_stat_id: int
    city_id: int
    district_id: int
    sub_district_id: int
    reference_id: int
    target_item:str
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


class NationLocInfoOutPut(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    ref_date: date
    reference_id: int
    target_item:  Optional[int]


    class Config:
        from_attributes = True


class CityLocInfoOutPut(BaseModel):
    city_id: int
    sub_district_id: int
    ref_date: date
    reference_id: int
    target_item:  Optional[int]



    class Config:
        from_attributes = True


class DistrictLocInfoOutPut(BaseModel):
    district_id: int
    sub_district_id: int
    ref_date: date
    reference_id: int
    target_item:  Optional[int]


    class Config:
        from_attributes = True


class InsertLocInfoStat(BaseModel):
    city_id: Optional[int]
    district_id: Optional[int]
    sub_district_id: int
    reference_id: int
    target_item: str
    avg_val: Optional[float]
    med_val: Optional[float]
    std_val: Optional[float]
    max_val: Optional[int]
    min_val: Optional[int]
    j_score_rank: Optional[float]
    j_score_per: Optional[float]
    j_score: Optional[float]
    j_score_per_non_outliers: Optional[float]
    j_score_non_outliers: Optional[float]
    ref_date: date
    stat_level: str

    class Config:
        from_attributes = True


class LocInfoJScoreRank(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    target_item:str
    j_score_rank:Optional[float]

    class Config:
        from_attributes = True


class MzPopJScoreRank(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    j_score_rank:Optional[float]

    class Config:
        from_attributes = True


class LocInfoJScorePer(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    target_item:str
    j_score_per:Optional[float]

    class Config:
        from_attributes = True


class MzPopJScorePer(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    j_score_per:Optional[float]
    
    class Config:
        from_attributes = True


######### 이상치 제거


class LocInfoJScorePerNonOutLiers(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    target_item:str
    j_score_per_non_outliers:Optional[float]

    class Config:
        from_attributes = True


class MzPopJScorePerNonOutLiers(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    j_score_per_non_outliers :Optional[float]
    
    class Config:
        from_attributes = True

############################################

class InsertLocInfoAvgRankJscore(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    average_j_score: float

    class Config:
        from_attributes = True




