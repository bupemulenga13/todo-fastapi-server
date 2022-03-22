from schemas import query
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Request
from app.utils import api_utility_sd_ed
from db.set_up import get_db
from middleware.dqa_views.interactions.interactions import getinteractions
router = APIRouter()

@router.post('/interactions',response_model=query.GetList)
async def read_interactions(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(getinteractions, req_body, db)
    return result
