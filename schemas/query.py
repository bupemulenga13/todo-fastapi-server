from datetime import date
from pydantic import BaseModel
from typing import Optional, Union, List

# class OurBaseModel(BaseModel):
#     class Config:
#         orm_mode = True

class QueryBaseSchema(BaseModel):
    start_date: Union[str, date] = None
    end_date: Union[str, date] = None
    status_code: Optional[int] = None
    message: Optional[str] = None
    selected_date: Union[str, date] = None

class GetCount(QueryBaseSchema):
    count: int

class GetList(QueryBaseSchema):
    query_list: Optional[List[dict]] = None
    