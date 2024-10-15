from pydantic import BaseModel
from typing import Optional

class City(BaseModel):
    city_id: Optional[int] = None
    city_name: str

    class Config:
        from_attributes = True

class SidoRequest(BaseModel):
    city_name: str  # 클라이언트가 보내는 시/도명
