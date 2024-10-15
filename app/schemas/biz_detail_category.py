from typing import Union
from pydantic import BaseModel


class BizDetailCategory(BaseModel):
    biz_detail_category_id: int
    biz_sub_category_id: int
    biz_detail_category_name: str

    class Config:
        from_attributes = True


class BizDetailCategoryOutput(BaseModel):
    biz_detail_category_id: Union[int, str]
    biz_detail_category_name: str

    class Config:
        from_attributes = True

class BizDetailCategoryId(BaseModel):
    biz_detail_category_id: int

    class Config:
        from_attributes = True