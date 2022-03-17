from pydantic import BaseModel

class UserBase(BaseModel):
   email: str
class UserCreate(UserBase):
   lname: str
   fname: str
   password: str