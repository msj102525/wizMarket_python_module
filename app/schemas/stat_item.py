from pydantic import BaseModel


class StatItemInfo(BaseModel):
    table_name: str
    column_name: str
    biz_detail_category_id: int

    class Config:
        from_attributes = True
