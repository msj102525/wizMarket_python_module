from typing import Optional
from pydantic import BaseModel
from datetime import date, datetime


class Report(BaseModel):
    store_business_number: str  # VARCHAR(100)
    city_name: Optional[str] = None  # VARCHAR(50)
    district_name: Optional[str] = None  # VARCHAR(50)
    sub_district_name: Optional[str] = None  # VARCHAR(50)

    detail_category_name: Optional[str]  # VARCHAR(100)
    store_name: Optional[str]  # VARCHAR(255)
    road_name: Optional[str] = None  # VARCHAR(255)
    building_name: Optional[str] = None  # VARCHAR(255)
    floor_info: Optional[str] = None  # VARCHAR(10)
    latitude: Optional[float] = None  # DOUBLE
    longitude: Optional[float] = None  # DOUBLE
    business_area_category_id: Optional[int] = None
    biz_detail_category_rep_name: Optional[str] = None
    biz_main_categort_id: Optional[int] = None
    biz_sub_categort_id: Optional[int] = None

    detail_category_top1_ordered_menu: Optional[str] = None  # VARCHAR(50)
    detail_category_top2_ordered_menu: Optional[str] = None  # VARCHAR(50)
    detail_category_top3_ordered_menu: Optional[str] = None  # VARCHAR(50)
    detail_category_top4_ordered_menu: Optional[str] = None  # VARCHAR(50)
    detail_category_top5_ordered_menu: Optional[str] = None  # VARCHAR(50)

    loc_info_j_score_average: Optional[float] = None  # FLOAT

    population_total: Optional[int] = None  # INT
    population_male_percent: Optional[float] = None  # FLOAT
    population_female_percent: Optional[float] = None  # FLOAT
    population_age_10_under: Optional[int] = None  # INT
    population_age_10s: Optional[int] = None  # INT
    population_age_20s: Optional[int] = None  # INT
    population_age_30s: Optional[int] = None  # INT
    population_age_40s: Optional[int] = None  # INT
    population_age_50s: Optional[int] = None  # INT
    population_age_60_over: Optional[int] = None  # INT

    loc_info_resident_k: Optional[float] = None  # INT
    loc_info_work_pop_k: Optional[float] = None  # INT
    loc_info_move_pop_k: Optional[float] = None  # INT
    loc_info_shop_k: Optional[float] = None  # INT
    loc_info_income_won: Optional[int] = None  # INT
    loc_info_average_sales_k: Optional[float] = None  # INT
    loc_info_average_spend_k: Optional[float] = None  # INT
    loc_info_house_k: Optional[float] = None  # INT

    loc_info_resident_j_score: Optional[float] = None  # FLOAT
    loc_info_work_pop_j_score: Optional[float] = None  # FLOAT
    loc_info_move_pop_j_score: Optional[float] = None  # FLOAT
    loc_info_shop_j_score: Optional[float] = None  # FLOAT
    loc_info_income_j_score: Optional[float] = None  # FLOAT
    loc_info_mz_population_j_score: Optional[float] = None  # FLOAT
    loc_info_average_spend_j_score: Optional[float] = None  # FLOAT
    loc_info_average_sales_j_score: Optional[float] = None  # FLOAT
    loc_info_house_j_score: Optional[float] = None  # FLOAT

    loc_info_resident: Optional[int] = None  # INT
    loc_info_work_pop: Optional[int] = None  # INT
    loc_info_resident_percent: Optional[float] = None  # FLOAT
    loc_info_work_pop_percent: Optional[float] = None  # FLOAT

    loc_info_move_pop: Optional[int] = None  # INT
    loc_info_city_move_pop: Optional[int] = None  # INT

    commercial_district_j_score_average: Optional[float] = None  # FLOAT

    commercial_district_food_business_count: Optional[int] = None  # INT
    commercial_district_healthcare_business_count: Optional[int] = None  # INT
    commercial_district_education_business_count: Optional[int] = None  # INT
    commercial_district_entertainment_business_count: Optional[int] = None  # INT
    commercial_district_lifestyle_business_count: Optional[int] = None  # INT
    commercial_district_retail_business_count: Optional[int] = None  # INT

    commercial_district_national_market_size: Optional[int] = None  # Int
    commercial_district_sub_district_market_size: Optional[int] = None  # Int
    commercial_district_national_density_average: Optional[float] = None  # Int
    commercial_district_sub_district_density_average: Optional[float] = None  # Int
    commercial_district_national_average_sales: Optional[int] = None  # Int
    commercial_district_sub_district_average_sales: Optional[int] = None  # Int
    commercial_district_national_average_payment: Optional[int] = None  # Int
    commercial_district_sub_district_average_payment: Optional[int] = None  # Int
    commercial_district_national_usage_count: Optional[int] = None  # Int
    commercial_district_sub_district_usage_count: Optional[int] = None  # Int

    commercial_district_market_size_j_score: Optional[float] = None  # FLOAT
    commercial_district_average_sales_j_score: Optional[float] = None  # FLOAT
    commercial_district_usage_count_j_score: Optional[float] = None  # FLOAT
    commercial_district_sub_district_density_j_score: Optional[float] = None  # FLOAT
    commercial_district_average_payment_j_score: Optional[float] = None  # FLOAT

    commercial_district_average_sales_percent_mon: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_tue: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_wed: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_thu: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_fri: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_sat: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_sun: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_06_09: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_09_12: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_12_15: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_15_18: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_18_21: Optional[float] = None  # FLOAT
    commercial_district_average_sales_percent_21_24: Optional[float] = None  # FLOAT
    # VARCHAR(100)
    commercial_district_avg_client_per_m_20s: Optional[float] = None
    commercial_district_avg_client_per_m_30s: Optional[float] = None
    commercial_district_avg_client_per_m_40s: Optional[float] = None
    commercial_district_avg_client_per_m_50s: Optional[float] = None
    commercial_district_avg_client_per_m_60_over: Optional[float] = None
    commercial_district_avg_client_per_f_20s: Optional[float] = None
    commercial_district_avg_client_per_f_30s: Optional[float] = None
    commercial_district_avg_client_per_f_40s: Optional[float] = None
    commercial_district_avg_client_per_f_50s: Optional[float] = None
    commercial_district_avg_client_per_f_60_over: Optional[float] = None

    commercial_district_detail_category_average_sales_top1_info: Optional[str] = None
    commercial_district_detail_category_average_sales_top2_info: Optional[str] = None
    commercial_district_detail_category_average_sales_top3_info: Optional[str] = None
    commercial_district_detail_category_average_sales_top4_info: Optional[str] = None
    commercial_district_detail_category_average_sales_top5_info: Optional[str] = None

    rising_business_national_rising_sales_top1_info: Optional[str] = None
    rising_business_national_rising_sales_top2_info: Optional[str] = None
    rising_business_national_rising_sales_top3_info: Optional[str] = None
    rising_business_national_rising_sales_top4_info: Optional[str] = None
    rising_business_national_rising_sales_top5_info: Optional[str] = None
    rising_business_sub_district_rising_sales_top1_info: Optional[str] = None
    rising_business_sub_district_rising_sales_top2_info: Optional[str] = None
    rising_business_sub_district_rising_sales_top3_info: Optional[str] = None
    # VARCHAR(100)
    loc_info_district_hot_place_top1_info: Optional[str] = None
    loc_info_district_hot_place_top2_info: Optional[str] = None
    loc_info_district_hot_place_top3_info: Optional[str] = None
    loc_info_district_hot_place_top4_info: Optional[str] = None
    loc_info_district_hot_place_top5_info: Optional[str] = None
    # VARCHAR(100)
    loc_info_data_ref_date: Optional[date] = None  # DATE
    nice_biz_map_data_ref_date: Optional[date] = None  # DATE
    population_data_ref_date: Optional[date] = None  # DATE
    created_at: datetime  # DATETIME
    updated_at: datetime  # DATETIME

    class Config:
        from_attributes = True


class LocalStoreImage(BaseModel):
    local_store_image_id: int  # INT AUTO_INCREMENT
    store_business_number: str  # VARCHAR(100)
    local_store_image_url: str  # VARCHAR(255)
    created_at: datetime  # DATETIME
    updated_at: datetime  # DATETIME

    class Config:
        from_attributes = True


# 매장 기본 정보
class LocalStoreBasicInfo(BaseModel):
    store_business_number: str
    city_name: str
    district_name: str
    sub_district_name: str
    detail_category_name: Optional[str] = None  # VARCHAR(100)
    store_name: Optional[str] = None
    road_name: Optional[str] = None  # VARCHAR(255)
    building_name: Optional[str] = None
    floor_info: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    business_area_category_id: Optional[int] = None
    biz_detail_category_rep_name: Optional[str] = None
    biz_main_categort_id: Optional[int] = None
    biz_sub_categort_id: Optional[int] = None

    class Config:
        from_attributes = True


# 매장 소분류 비즈맵 매핑 대표 id
class LocalStoreMappingRepId(BaseModel):
    store_business_number: str
    sub_district_id: int
    rep_id: Optional[int] = 3  # 3: 비즈맵 소분류 없음 default 값

    class Config:
        from_attributes = True


# 매장별 top5 메뉴
class LocalStoreTop5Menu(BaseModel):
    store_business_number: str
    detail_category_top1_ordered_menu: Optional[str] = None
    detail_category_top2_ordered_menu: Optional[str] = None
    detail_category_top3_ordered_menu: Optional[str] = None
    detail_category_top4_ordered_menu: Optional[str] = None
    detail_category_top5_ordered_menu: Optional[str] = None
    nice_biz_map_data_ref_date: date

    class Config:
        from_attributes = True


# 매장별 읍/면/동 id 조회
class LocalStoreSubdistrictId(BaseModel):
    store_business_number: str
    sub_district_id: int

    class Config:
        from_attributes = True


# 매장별 시/군/구 id 조회
class LocalStoreDistrictId(BaseModel):
    store_business_number: str
    district_id: int

    class Config:
        from_attributes = True


# 매장별 읍/면/동 id 조회
class LocalStorePopulationData(BaseModel):
    store_business_number: str
    population_total: int
    population_male_percent: float
    population_female_percent: float
    population_age_10_under: int
    population_age_10s: int
    population_age_20s: int
    population_age_30s: int
    population_age_40s: int
    population_age_50s: int
    population_age_60_over: int
    population_date_ref_date: date

    class Config:
        from_attributes = True


# 매장별 입지정보
class LocalStoreLocInfoData(BaseModel):
    store_business_number: str
    loc_info_resident_k: Optional[float] = 0.0
    loc_info_work_pop_k: Optional[float] = 0.0
    loc_info_move_pop_k: Optional[float] = 0.0
    loc_info_shop_k: Optional[float] = 0.0
    loc_info_income_won: Optional[int] = 0
    loc_info_average_sales_k: Optional[float] = 0.0
    loc_info_average_spend_k: Optional[float] = 0.0
    loc_info_average_house_k: Optional[float] = 0.0
    loc_info_data_ref_date: date

    class Config:
        from_attributes = True


# 매장별 입지정보 J_Score
class LocalStoreLocInfoJscoreData(BaseModel):
    store_business_number: str
    loc_info_resident_j_score: float
    loc_info_work_pop_j_score: float
    loc_info_move_pop_j_score: float
    loc_info_shop_j_score: float
    loc_info_income_j_score: float
    loc_info_average_spend_j_score: float
    loc_info_average_sales_j_score: float
    loc_info_house_j_score: float
    population_mz_population_j_score: float

    class Config:
        from_attributes = True


# 읍/면/동 입지정보 주거인구/직장인구
class LocalStoreResidentWorkPopData(BaseModel):
    store_business_number: str
    loc_info_resident: int
    loc_info_work_pop: int
    loc_info_resident_percent: float
    loc_info_work_pop_percent: float

    class Config:
        from_attributes = True


# 읍/면/동 입지정보 유동인구, 시/도 평균 유동인구
class LocalStoreMovePopData(BaseModel):
    store_business_number: str
    loc_info_move_pop: int
    loc_info_city_move_pop: int

    class Config:
        from_attributes = True


# 입지분석 읍/면/동 소분류 J_Score 가중치 평균 합
class LocalStoreLIJSWeightedAverage(BaseModel):
    store_business_number: str
    loc_info_j_score_average: float

    class Config:
        from_attributes = True


# 상권분석 읍/면/동 소분류 J_Score 가중치 평균 합
class LocalStoreCDJSWeightedAverage(BaseModel):
    store_business_number: str
    commercial_district_j_score_average: float

    class Config:
        from_attributes = True


# 상권분석 읍/면/동 대분류 갯수
class LocalStoreMainCategoryCount(BaseModel):
    store_business_number: str
    commercial_district_food_business_count: int
    commercial_district_healthcare_business_count: int
    commercial_district_education_business_count: int
    commercial_district_entertainment_business_count: int
    commercial_district_lifestyle_business_count: int
    commercial_district_retail_business_count: int

    class Config:
        from_attributes = True


# 매장 소분류 비즈맵 매핑 대표 id
class LocalStoreMappingSubDistrictDetailCategoryId(BaseModel):
    store_business_number: str
    sub_district_id: int
    detail_category_id: Optional[int] = 3  # 3: 비즈맵 소분류 없음 default 값

    class Config:
        from_attributes = True


# 상권분석 읍/면/동 소분류 J_Score 평균
class LocalStoreCommercialDistrictJscoreAverage(BaseModel):
    store_business_number: str
    commercial_district_market_size_j_socre: float
    commercial_district_average_sales_j_socre: float
    commercial_district_usage_count_j_socre: float
    commercial_district_sub_district_density_j_socre: float
    commercial_district_sub_average_payment_j_socre: float

    class Config:
        from_attributes = True


# 매장 상권분석 동별 소분류별 요일,시간대 매출 비중
class LocalStoreCDWeekdayTiemAveragePercent(BaseModel):
    store_business_number: str
    commercial_district_average_sales_percent_mon: Optional[float]
    commercial_district_average_sales_percent_tue: Optional[float]
    commercial_district_average_sales_percent_wed: Optional[float]
    commercial_district_average_sales_percent_thu: Optional[float]
    commercial_district_average_sales_percent_fri: Optional[float]
    commercial_district_average_sales_percent_sat: Optional[float]
    commercial_district_average_sales_percent_sun: Optional[float]
    commercial_district_average_sales_percent_06_09: Optional[float]
    commercial_district_average_sales_percent_09_12: Optional[float]
    commercial_district_average_sales_percent_12_15: Optional[float]
    commercial_district_average_sales_percent_15_18: Optional[float]
    commercial_district_average_sales_percent_18_21: Optional[float]
    commercial_district_average_sales_percent_21_24: Optional[float]

    commercial_district_avg_clent_per_m_20s: Optional[float]
    commercial_district_avg_clent_per_m_30s: Optional[float]
    commercial_district_avg_clent_per_m_40s: Optional[float]
    commercial_district_avg_clent_per_m_50s: Optional[float]
    commercial_district_avg_clent_per_m_60_over: Optional[float]

    commercial_district_avg_clent_per_f_20s: Optional[float]
    commercial_district_avg_clent_per_f_30s: Optional[float]
    commercial_district_avg_clent_per_f_40s: Optional[float]
    commercial_district_avg_clent_per_f_50s: Optional[float]
    commercial_district_avg_clent_per_f_60_over: Optional[float]

    def __init__(self, **data):
        super().__init__(**data)
        if self.commercial_district_average_sales_percent_mon is None:
            self.commercial_district_average_sales_percent_mon = 0.0

        if self.commercial_district_average_sales_percent_tue is None:
            self.commercial_district_average_sales_percent_tue = 0.0

        if self.commercial_district_average_sales_percent_wed is None:
            self.commercial_district_average_sales_percent_wed = 0.0

        if self.commercial_district_average_sales_percent_thu is None:
            self.commercial_district_average_sales_percent_thu = 0.0

        if self.commercial_district_average_sales_percent_fri is None:
            self.commercial_district_average_sales_percent_fri = 0.0

        if self.commercial_district_average_sales_percent_sat is None:
            self.commercial_district_average_sales_percent_sat = 0.0

        if self.commercial_district_average_sales_percent_sun is None:
            self.commercial_district_average_sales_percent_sun = 0.0

        if self.commercial_district_avg_clent_per_m_20s is None:
            self.commercial_district_avg_clent_per_m_20s = 0.0

        if self.commercial_district_avg_clent_per_m_30s is None:
            self.commercial_district_avg_clent_per_m_30s = 0.0

        if self.commercial_district_avg_clent_per_m_40s is None:
            self.commercial_district_avg_clent_per_m_40s = 0.0

        if self.commercial_district_avg_clent_per_m_50s is None:
            self.commercial_district_avg_clent_per_m_50s = 0.0

        if self.commercial_district_avg_clent_per_m_60_over is None:
            self.commercial_district_avg_clent_per_m_60_over = 0.0

        if self.commercial_district_avg_clent_per_f_20s is None:
            self.commercial_district_avg_clent_per_f_20s = 0.0

        if self.commercial_district_avg_clent_per_f_30s is None:
            self.commercial_district_avg_clent_per_f_30s = 0.0

        if self.commercial_district_avg_clent_per_f_40s is None:
            self.commercial_district_avg_clent_per_f_40s = 0.0

        if self.commercial_district_avg_clent_per_f_50s is None:
            self.commercial_district_avg_clent_per_f_50s = 0.0

        if self.commercial_district_avg_clent_per_f_60_over is None:
            self.commercial_district_avg_clent_per_f_60_over = 0.0

    class Config:
        from_attributes = True


# 상권 분석 시/군/구에서 매핑된 소분류들 매출합 TOP5
class LocalStoreCDDistrictAverageSalesTop5(BaseModel):
    store_business_number: str
    commercial_districdt_detail_category_average_sales_top1_info: Optional[str] = ","
    commercial_districdt_detail_category_average_sales_top2_info: Optional[str] = ","
    commercial_districdt_detail_category_average_sales_top3_info: Optional[str] = ","
    commercial_districdt_detail_category_average_sales_top4_info: Optional[str] = ","
    commercial_districdt_detail_category_average_sales_top5_info: Optional[str] = ","

    class Config:
        from_attributes = True


# 상권 분석 시/군/구에서 매핑된 소분류들 매출합 TOP5
class LocalStoreRisingBusinessNTop5SDTop3(BaseModel):
    store_business_number: str
    rising_business_national_rising_sales_top1_info: Optional[str] = ","
    rising_business_national_rising_sales_top2_info: Optional[str] = ","
    rising_business_national_rising_sales_top3_info: Optional[str] = ","
    rising_business_national_rising_sales_top4_info: Optional[str] = ","
    rising_business_national_rising_sales_top5_info: Optional[str] = ","

    rising_business_sub_district_rising_sales_top1_info: Optional[str] = ","
    rising_business_sub_district_rising_sales_top2_info: Optional[str] = ","
    rising_business_sub_district_rising_sales_top3_info: Optional[str] = ","

    class Config:
        from_attributes = True


# 상권분석 읍/면/동 소분류 상권분석
class LocalStoreCDCommercialDistrict(BaseModel):
    store_business_number: str
    commercial_district_national_market_size_average: Optional[int]
    commercial_district_sub_district_market_size_average: Optional[int]
    commercial_district_national_density_average: Optional[float]
    commercial_district_sub_district_density_average: Optional[float]
    commercial_district_national_average_sales: Optional[int]
    commercial_district_sub_district_average_sales: Optional[int]
    commercial_district_national_average_payment: Optional[int]
    commercial_district_sub_district_average_payment: Optional[int]
    commercial_district_national_usage_count: Optional[int]
    commercial_district_sub_district_usage_count: Optional[int]

    def __init__(self, **data):
        super().__init__(**data)
        if self.commercial_district_national_market_size_average is None:
            self.commercial_district_national_market_size_average = 0.0

        if self.commercial_district_sub_district_market_size_average is None:
            self.commercial_district_sub_district_market_size_average = 0.0

        if self.commercial_district_national_density_average is None:
            self.commercial_district_national_density_average = 0.0

        if self.commercial_district_sub_district_density_average is None:
            self.commercial_district_sub_district_density_average = 0.0

        if self.commercial_district_national_average_sales is None:
            self.commercial_district_national_average_sales = 0.0

        if self.commercial_district_sub_district_average_sales is None:
            self.commercial_district_sub_district_average_sales = 0.0

        if self.commercial_district_national_average_payment is None:
            self.commercial_district_national_average_payment = 0.0

        if self.commercial_district_sub_district_average_payment is None:
            self.commercial_district_sub_district_average_payment = 0.0

        if self.commercial_district_national_usage_count is None:
            self.commercial_district_national_usage_count = 0.0

        if self.commercial_district_sub_district_usage_count is None:
            self.commercial_district_sub_district_usage_count = 0.0


# 입지분석 시/군/구 핫플레이스 TOP5 (읍/면/동, 평균유동인구, 매장평균매출, JSCORE점수)
class LocalStoreLocInfoDistrictHotPlaceTop5(BaseModel):
    store_business_number: str
    loc_info_district_hot_place_top1_info: Optional[str]
    loc_info_district_hot_place_top2_info: Optional[str]
    loc_info_district_hot_place_top3_info: Optional[str]
    loc_info_district_hot_place_top4_info: Optional[str]
    loc_info_district_hot_place_top5_info: Optional[str]

    def __init__(self, **data):
        super().__init__(**data)
        if self.loc_info_district_hot_place_top1_info is None:
            self.loc_info_district_hot_place_top1_info = ""

        if self.loc_info_district_hot_place_top2_info is None:
            self.loc_info_district_hot_place_top2_info = ""

        if self.loc_info_district_hot_place_top3_info is None:
            self.loc_info_district_hot_place_top3_info = ""

        if self.loc_info_district_hot_place_top4_info is None:
            self.loc_info_district_hot_place_top4_info = ""

        if self.loc_info_district_hot_place_top5_info is None:
            self.loc_info_district_hot_place_top5_info = ""

    class Config:
        from_attributes = True


####################
# store_business_number='MA010120220807561997'
# commercial_district_national_density_average=2.678424835205078
# commercial_district_sub_district_density_average=3.5
# commercial_district_national_average_sales=34123132.0
# commercial_district_sub_district_average_sales=20710000.0
# commercial_district_national_average_payment=68006.9609375
# commercial_district_sub_district_average_payment=101914.0
# commercial_district_national_usage_count=11912.2744140625
# commercial_district_sub_district_usage_count=3658.0
