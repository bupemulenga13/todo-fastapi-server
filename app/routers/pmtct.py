from schemas import query
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Request
from app.utils import api_utility_sd_ed
from db.set_up import get_db
from middleware.dqa_views.pmtct.pmtct import *

router=APIRouter()

@router.post('/pmtct_eid_0_2',response_model=query.GetList)
async def read_pmtct_eid_0_2(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(getpmtct_eid_0_2, req_body, db)
    return result


@router.post('/pmtct_eid_2_12',response_model=query.GetList)
async def read_pmtct_eid_2_12(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(getpmtct_eid_2_12, req_body, db)
    return result


@router.post('/pmtct_eid_pos_0_2',response_model=query.GetList)
async def read_pmtct_eid__pos_0_2(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(getpmtct_eid_pos_0_2, req_body, db)
    return result


@router.post('/pmtct_eid_pos_2_12',response_model=query.GetList)
async def read_pmtct_eid_pos_2_12(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(getpmtct_eid_pos_2_12, req_body, db)
    return result


@router.post('/pmtct_stat_pos',response_model=query.GetList)
async def read_pmtct_stat_pos(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_sd_ed(getpmtct_stat_pos, req_body, db)
    return result
