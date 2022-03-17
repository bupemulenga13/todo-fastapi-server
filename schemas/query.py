from datetime import date
from pydantic import BaseModel
from typing import Optional, Union, List

class QueryBaseSchema(BaseModel):
    start_date: Union[str, date] = None
    end_date: Union[str, date] = None
    status_code: Optional[int] = None
    message: Optional[str] = None
    selected_date: Union[str, date] = None

class GetCount(QueryBaseSchema):
    count: int

class GetList(QueryBaseSchema):
    list_count: int
    query_list: Optional[List[dict]] = None
    