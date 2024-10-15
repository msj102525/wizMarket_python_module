from pydantic import BaseModel
from typing import Optional

class Crime(BaseModel):
    loc_info_id: Optional[int] = None
    CITY_ID: int
    QUARTER: str
    CRIME_MAJOR_CATEGORY: str
    CRIME_MINOR_CATEGORY: str
    INCIDENT_COUNT: int
    ARREST_COUNT: int
    INCIDENT_TO_ARREST_RATIO: Optional[float]
    ARREST_PERSONNEL: Optional[int]
    LEGAL_ENTITY: Optional[int]

    class Config:
        from_attributes = True

class CrimeRequest(BaseModel):
    city_name: str
    start_year_month: str