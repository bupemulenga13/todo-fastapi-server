from fastapi import APIRouter, Request, Depends
from db.set_up import get_db
from middleware.dqa_views.hts.hts import get_hts_index, get_hts_positive
from schemas import query
from sqlalchemy.orm import Session
from ..utils import api_utility_sd_ed


router = APIRouter()


@router.post("/hts_index", response_model=query.GetList)
async def read_hts_index(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(get_hts_index, req_body, db)
    return result


@router.post("/hts_positive", response_model=query.GetList)
async def read_hts_positive(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(get_hts_positive, req_body, db)
    return result
