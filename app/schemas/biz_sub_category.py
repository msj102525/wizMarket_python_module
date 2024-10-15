from typing import Union
from pydantic import BaseModel


class BizSubCategory(BaseModel):
    biz_sub_category_id: int
    biz_main_category_id: int
    biz_sub_category_name: str

    class Config:
        from_attributes = True


class BizSubCategoryOutput(BaseModel):
    biz_sub_category_id: Union[int, str]
    biz_sub_category_name: str
    biz_detail_cateogry_count: int

    class Config:
        from_attributes = True
