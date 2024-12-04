from pydantic import BaseModel
from typing import Optional




class LocalStore(BaseModel):
    city_id: int
    district_id: int
    sub_district_id: int
    store_business_number: str
    store_name: Optional[str]
    branch_name: Optional[str]
    large_category_code: Optional[str]
    large_category_name: Optional[str]
    medium_category_code: Optional[str]
    medium_category_name: Optional[str]
    small_category_code: Optional[str]
    small_category_name: Optional[str]
    industry_code: Optional[str]
    industry_name: Optional[str]
    province_code: Optional[str]
    province_name: Optional[str]
    district_code: Optional[str]
    district_name: Optional[str]
    administrative_dong_code: Optional[str]
    administrative_dong_name: Optional[str]
    legal_dong_code: Optional[str]
    legal_dong_name: Optional[str]
    lot_number_code: Optional[str]
    land_category_code: Optional[str]
    land_category_name: Optional[str]
    lot_main_number: Optional[str]
    lot_sub_number: Optional[str]
    lot_address: Optional[str]
    road_name_code: Optional[str]
    road_name: Optional[str]
    building_main_number: Optional[str]
    building_sub_number: Optional[str]
    building_management_number: Optional[str]
    building_name: Optional[str]
    road_name_address: Optional[str]
    old_postal_code: Optional[str]
    new_postal_code: Optional[str]
    dong_info: Optional[str]
    floor_info: Optional[str]
    unit_info: Optional[str]
    longitude: Optional[str]
    latitude: Optional[str]
    local_year: int
    local_quarter: int
    is_exist : bool

    class Config:
        from_attributes = True


class LocalOldStore(BaseModel):
    store_business_number: str
    is_exist : bool

    class Config:
        from_attributes = True


class FilterRequest(BaseModel):
    city: Optional[int] = None  # 기본값 None을 설정
    district: Optional[int] = None
    subDistrict: Optional[int] = None
    storeName: Optional[str] = None
    matchType: Optional[str] = "="  # = 또는 LIKE 검색
    mainCategory: Optional[str] = None
    subCategory: Optional[str] = None
    detailCategory: Optional[str] = None
    page: Optional[int] = 1  # 페이지 번호 (기본값 1)
    page_size: Optional[int] = 20  # 페이지 크기 (기본값 20)


class LocalStoreSubdistrict(BaseModel):
    local_store_id: int
    store_business_number: str
    sub_district_id: int
    sub_district_name: str

    class Config:
        from_attributes = True


class LocalStoreCityDistrictSubDistrict(BaseModel):
    local_store_id: int
    store_business_number: str
    store_name: str
    city_id: int
    city_name: str
    district_id: int
    district_name: str
    sub_district_id: int
    sub_district_name: str
    large_category_name: str
    medium_category_name: str
    small_category_name: str
    reference_id: int

    class Config:
        from_attributes = True
    
class BusinessAreaCategoryReportOutput(BaseModel):
    business_area_category_id: int

    class Config:
        from_attributes = True   

class BizDetailCategoryIdOutPut(BaseModel):
    rep_id: int
    biz_detail_category_name: str

    class Config:
        from_attributes = True  

class BizCategoriesNameOutPut(BaseModel):
    biz_main_category_name: str
    biz_sub_category_name: str
    biz_detail_category_name: str


class RisingMenuOutPut(BaseModel):
    market_size: Optional[int] = 0
    average_sales: Optional[int] = 0
    average_payment: Optional[int] = 0
    usage_count: Optional[int] = 0
    avg_profit_per_mon: Optional[float] = 0.0
    avg_profit_per_tue: Optional[float] = 0.0
    avg_profit_per_wed: Optional[float] = 0.0
    avg_profit_per_thu: Optional[float] = 0.0
    avg_profit_per_fri: Optional[float] = 0.0
    avg_profit_per_sat: Optional[float] = 0.0
    avg_profit_per_sun: Optional[float] = 0.0
    avg_profit_per_06_09: Optional[float] = 0.0
    avg_profit_per_09_12: Optional[float] = 0.0
    avg_profit_per_12_15: Optional[float] = 0.0
    avg_profit_per_15_18: Optional[float] = 0.0
    avg_profit_per_18_21: Optional[float] = 0.0
    avg_profit_per_21_24: Optional[float] = 0.0
    avg_profit_per_24_06: Optional[float] = 0.0
    avg_client_per_m_20: Optional[float] = 0.0
    avg_client_per_m_30: Optional[float] = 0.0
    avg_client_per_m_40: Optional[float] = 0.0
    avg_client_per_m_50: Optional[float] = 0.0
    avg_client_per_m_60: Optional[float] = 0.0
    avg_client_per_f_20: Optional[float] = 0.0
    avg_client_per_f_30: Optional[float] = 0.0
    avg_client_per_f_40: Optional[float] = 0.0
    avg_client_per_f_50: Optional[float] = 0.0
    avg_client_per_f_60: Optional[float] = 0.0
    top_menu_1: Optional[str] = 0.0
    top_menu_2: Optional[str] = 0.0
    top_menu_3: Optional[str] = 0.0
    top_menu_4: Optional[str] = 0.0
    top_menu_5: Optional[str] = 0.0

    class Config:
        from_attributes = True



class LocalStoreInfo(BaseModel):
    road_name_address: Optional[str] = ""
    store_name: Optional[str] = ""
    building_name: Optional[str] = ""
    floor_info: Optional[str] = ""
    small_category_name: Optional[str] = ""
    store_img_url: Optional[str] = "/static/images/report/basic_store_img.png"

    class Config:
        from_attributes = True


class LocalStoreLatLng(BaseModel):
    longitude: Optional[str] = ""
    latitude: Optional[str] = ""

    class Config:
        from_attributes = True


class WeatherInfo(BaseModel):
    icon: str
    temp: float

    class Config:
        from_attributes = True

class WeatherToday(BaseModel):
    weather: str
    temp: float
    sunset: int

    class Config:
        from_attributes = True


class LocalStoreInfoWeaterInfo(BaseModel):
    localStoreInfo: LocalStoreInfo
    weatherInfo: WeatherInfo

    class Config:
        from_attributes = True

class LocalStoreCityDistrictSubDistrict(BaseModel):
    local_store_id: int
    store_business_number: str
    store_name: str
    city_id: int
    city_name: str
    district_id: int
    district_name: str
    sub_district_id: int
    sub_district_name: str
    large_category_name: str
    medium_category_name: str
    small_category_name: str

    class Config:
        from_attributes = True


class LocalStoreBusinessNumber(BaseModel):
    store_business_number: str

    class Config:
        from_attributes = True



class UpdateLocalStoreReview(BaseModel):
    kakao_review_score: Optional[float] = None
    kakao_review_count: Optional[int] = None
    menu_1: Optional[str] = None
    menu_1_price: Optional[int] = None
    menu_2: Optional[str] = None
    menu_2_price: Optional[int] = None
    menu_3: Optional[str] = None
    menu_3_price: Optional[int] = None
    store_business_number: str


    class Config:
        from_attributes = True