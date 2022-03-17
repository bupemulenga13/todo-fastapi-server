from cgitb import text
from pydantic import BaseModel
from pydantic import EmailStr

class TodoBase(BaseModel):
    text: str
    description: str
    completed: bool

class TodoSchema(TodoBase):
    owner_id: int

    class Config:
        orm_mode = True
        
class TodoResponseSchema(TodoSchema):
    id: int

class TodoUpdate(BaseModel):
    id: int