from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class CommonInformation(BaseModel):
    common_information_id: Optional[int] = None
    title: Optional[str] = None
    content: Optional[str] = None
    file_group_id: Optional[int] = None
    is_deleted: str = "N"
    etc: Optional[str] = None
    reg_id: Optional[int] = None
    reg_date: Optional[datetime] = None
    mod_id: Optional[int] = None
    mod_date: Optional[datetime] = None

    class Config:
        from_attributes = True


class FileOutput(BaseModel):
    file_id: int
    file_group_id: int
    original_name: Optional[str]
    save_path: Optional[str]
    save_name: Optional[str]
    url: Optional[str]
    is_deleted: str
    etc: Optional[str]
    reg_id: int
    reg_date: datetime
    mod_id: Optional[int]
    mod_date: Optional[datetime]


class FileGroupOutput(BaseModel):
    file_group_id: int
    reg_id: int
    reg_date: datetime


class CommonInformationOutput(BaseModel):
    common_information_id: int
    title: str
    content: Optional[str]
    file_group_id: int
    is_deleted: str
    etc: Optional[str]
    reg_id: Optional[int]
    reg_date: Optional[datetime]
    mod_id: Optional[int]
    mod_date: Optional[datetime]
    file_groups: List[FileGroupOutput]
    files: List[FileOutput]
