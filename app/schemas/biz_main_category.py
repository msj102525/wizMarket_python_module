from typing import Union
from pydantic import BaseModel


class BizMainCategory(BaseModel):
    biz_main_category_id: int
    biz_main_category_name: str
    reference_id : int

    class Config:
        from_attributes = True


class BizMainCategoryOutput(BaseModel):
    biz_main_category_id: Union[int, str]
    biz_main_category_name: str
    biz_sub_category_count: int

    class Config:
        from_attributes = True
