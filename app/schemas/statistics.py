from typing import Optional
from pydantic import BaseModel
from datetime import date, datetime


class Statistics(BaseModel):
    statistics_id: int
    stat_item_id: int
    city_id: int | None = None
    district_id: int | None = None
    sub_district_id: int | None = None
    avg_val: float | None = None
    med_val: float | None = None
    std_val: float | None = None
    max_value: float | None = None
    min_value: float | None = None
    j_score: float | None = None
    created_at: datetime
    referenc_id: int | None = None

    class Config:
        from_attributes = True


class StatisticsJscoreOutput(BaseModel):
    ref_date: date | None = None
    j_score: float | None = None

    class Config:
        from_attributes = True


class LocStatisticsOutput(BaseModel):
    resident_jscore: float
    work_pop_jscore: float
    house_jscore: float
    shop_jscore: float
    income_jscore: float

    class Config:
        from_attributes = True


class LocInfoStatisticsOutput(BaseModel):
    mz_population_jscore: float  # mz 인구
    shop_jscore: float  # 업소수
    move_pop_jscore: float  # 유동인구
    resident_jscore: float  # 주거인구
    house_jscore: float  # 세대수
    income_jscore: float  # 평균 소득
    spend_jscore: float  # 평균 소비
    sales_jscore: float  # 매장평균매출

    class Config:
        from_attributes = True


class DataRefDateSubDistrictName(BaseModel):
    sub_district_name: str
    reference_date: date

    class Config:
        from_attributes = True


class LocInfoStatisticsDataRefOutput(BaseModel):
    loc_info: LocInfoStatisticsOutput
    data_ref: DataRefDateSubDistrictName

    class Config:
        from_attributes = True


class LocInfoAvgJscoreOutput(BaseModel):
    city_name: str
    district_name: str
    sub_district_name: str
    sub_district_id: int
    ref_date: date
    weighted_avg_val: float

    class Config:
        from_attributes = True


class PopulationCompareResidentWorkPop(BaseModel):
    city_name: str
    district_name: str
    sub_district_name: str
    work_pop: int
    resident: int
    resident_percentage: int
    work_pop_percentage: int

    class Config:
        from_attributes = True


class CommercialStatistics(BaseModel):
    avg_val: Optional[float] = 0.0
    med_val: Optional[float] = 0.0
    std_val: Optional[float] = 0.0
    max_val: Optional[float] = 0.0
    min_val: Optional[float] = 0.0
    j_score: Optional[float] = 0.0

    class Config:
        from_attributes = True


class GPTReport(BaseModel):
    content: str  

    class Config:
        from_attributes = True