from datetime import timedelta
from schemas.token_py import Token
from fastapi import Depends, FastAPI
from db.set_up import SessionLocal, get_db
from fastapi import HTTPException
from middleware.security import authenticate_user
from fastapi.security import OAuth2PasswordRequestForm

app = FastAPI()

@app.post("/api/token", response_model=Token)
def login_for_access_token(db: SessionLocal = Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()):
    """Generate access token for valid credentials"""
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect email or password", headers={"WWW-Authenticate": "Bearer"})
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user.email},
                                  	expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}