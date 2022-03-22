from schemas import query
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Request
from app.utils import *
from db.set_up import get_db
from middleware.dqa_views.tx.tx import *

router = APIRouter()

@router.post('/tx_curr',response_model=query.GetList)
async def read_tx_curr(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_ed(gettx_curr, req_body, db)
    return result

@router.post('/tx_curr_denominator',response_model=query.GetList)
async def read_tx_curr_denominator(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_ed(gettx_curr_denominator, req_body, db)
    return result

@router.post('/tx_curr_numerator',response_model=query.GetList)
async def read_tx_curr_numerator(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_ed(gettx_curr_numerator, req_body, db)
    return result

@router.post('/tx_new',response_model=query.GetList)
async def read_tx_new(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(gettx_new, req_body, db)
    return result

@router.post('/extendtx_curr',response_model=query.GetList)
async def read_tx_new(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(getextendTx_curr, req_body, db)
    return result
