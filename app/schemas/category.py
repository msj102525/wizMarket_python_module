from pydantic import BaseModel


class CategoryListOutput(BaseModel):
    category_id: int
    main_category_name: str
    sub_category_name: str
    detail_category_name: str

    class Config:
        from_attributes = True