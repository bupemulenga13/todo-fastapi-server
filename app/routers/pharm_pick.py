from schemas import query
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Request
from app.utils import api_utility_sd_ed
from db.set_up import get_db
from middleware.dqa_views.pharm_pick.pharm_pick import getpharm_pick

router = APIRouter()

@router.post('/pharm_pick',response_model=query.GetList)
async def read_pharm_pick(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(getpharm_pick, req_body, db)
    return result
