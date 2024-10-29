from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class Population(BaseModel):
    pop_id: Optional[int] = None  # 자동 증가 필드
    city_id: int
    district_id: int
    sub_district_id: int
    gender_id: int
    admin_code: int
    reference_date: datetime
    province_name: str
    district_name: str
    sub_district_name: str
    total_population: int
    male_population: int
    female_population: int
    age_0: int
    age_1: int
    age_2: int
    age_3: int
    age_4: int
    age_5: int
    age_6: int
    age_7: int
    age_8: int
    age_9: int
    age_10: int
    age_11: int
    age_12: int
    age_13: int
    age_14: int
    age_15: int
    age_16: int
    age_17: int
    age_18: int
    age_19: int
    age_20: int
    age_21: int
    age_22: int
    age_23: int
    age_24: int
    age_25: int
    age_26: int
    age_27: int
    age_28: int
    age_29: int
    age_30: int
    age_31: int
    age_32: int
    age_33: int
    age_34: int
    age_35: int
    age_36: int
    age_37: int
    age_38: int
    age_39: int
    age_40: int
    age_41: int
    age_42: int
    age_43: int
    age_44: int
    age_45: int
    age_46: int
    age_47: int
    age_48: int
    age_49: int
    age_50: int
    age_51: int
    age_52: int
    age_53: int
    age_54: int
    age_55: int
    age_56: int
    age_57: int
    age_58: int
    age_59: int
    age_60: int
    age_61: int
    age_62: int
    age_63: int
    age_64: int
    age_65: int
    age_66: int
    age_67: int
    age_68: int
    age_69: int
    age_70: int
    age_71: int
    age_72: int
    age_73: int
    age_74: int
    age_75: int
    age_76: int
    age_77: int
    age_78: int
    age_79: int
    age_80: int
    age_81: int
    age_82: int
    age_83: int
    age_84: int
    age_85: int
    age_86: int
    age_87: int
    age_88: int
    age_89: int
    age_90: int
    age_91: int
    age_92: int
    age_93: int
    age_94: int
    age_95: int
    age_96: int
    age_97: int
    age_98: int
    age_99: int
    age_100: int
    age_101: int
    age_102: int
    age_103: int
    age_104: int
    age_105: int
    age_106: int
    age_107: int
    age_108: int
    age_109: int
    age_110_over: int
    created_at: Optional[datetime] = None  # 기본값을 None으로 설정
    updated_at: Optional[datetime] = None  # 기본값을 None으로 설정

    class Config:
        from_attributes = True


class PopulationRequest(BaseModel):
    city_name: str
    district_name: str
    sub_district_name: str
    start_year_month: str  # YYYY-MM-DD 형식의 문자열


class PopulationSearch(BaseModel):
    city: Optional[int] = None  # 기본값 None을 설정
    district: Optional[int] = None
    subDistrict: Optional[int] = None

    gender: Optional[int] = None
    ageGroupMin: Optional[str] = None
    ageGroupMax: Optional[str] = None

    startDate: Optional[str] = None
    endDate: Optional[str] = None


class PopulationOutput(BaseModel):
    pop_id: int
    city_name: str
    district_name: str
    sub_district_name: str
    male_population: int
    female_population: int
    total_population: int
    reference_date: datetime
    age_under_10: int
    age_10s: int
    age_20s: int
    age_30s: int
    age_40s: int
    age_50s: int
    age_60_plus: int
    male_population_percent: float  # 남자 인구 비율 추가
    female_population_percent: float  # 여자 인구 비율 추가

    class Config:
        from_attributes = True





class Population_by_ages(BaseModel):
    age_under_10: int
    age_10s: int
    age_20s: int
    age_30s: int
    age_40s: int
    age_50s: int
    age_60_plus: int

    class Config:
        from_attributes = True


class Population_by_gender(BaseModel):
    male_population: int
    female_population: int

    class Config:
        from_attributes = True