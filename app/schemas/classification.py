from pydantic import BaseModel


class Classification(BaseModel):
    classification_id: int
    main_category_code: str
    main_category_name: str
    sub_category_code: str
    sub_category_name: str
    detail_category_code: str
    detail_category_name: str
    sub_detail_category_code: str
    sub_detail_category_name: str
    sub_sub_detail_category_code: str
    sub_sub_detail_category_name: str
    reference_id: int

    class Config:
        from_attributes = True

    class Config:
        from_attributes = True
