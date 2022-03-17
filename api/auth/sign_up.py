from typing import Optional
from fastapi import HTTPException

from fastapi import Depends, FastAPI
from db.set_up import SessionLocal, get_db

from schemas.user_py import UserCreate
from models.user import User

app = FastAPI()


@app.post("/api/users/sign_up", response_model=User)
def sign_up(user_data: UserCreate, db: SessionLocal = Depends(get_db)):
    """Add new user"""
    user = crud.get_user_by_email(db, user_data.email)
    if user:
        raise HTTPException(status_code=409, detail="Email already registered")
    signedup_user = crud.create_user(db, user_data)
    return signedup_user
    

