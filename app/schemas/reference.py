from pydantic import BaseModel


class Reference(BaseModel):
    reference_id: int
    reference_name: str
    reference_url: str

    class Config:
        from_attributes = True


class ReferenceCategoryCountOutput(BaseModel):
    reference_id: int
    reference_name: str
    category_count: int

    class Config:
        from_attributes = True
