from schemas import query
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Request
from app.utils import api_utility_db, api_utility_sd_ed
from db.set_up import get_db
from middleware.dqa_views.facility_details.facility_details import get_facility_details, getfacility

router = APIRouter()


@router.post('/facility_details_list', response_model=query.GetList)
async def read_facility_details(db: Session = Depends(get_db)):
    result = api_utility_db(get_facility_details, db)
    return "hello"


@router.post('/facility', response_model=query.GetList)
async def read_facility(req: Request, db: Session = Depends(get_db)):
    req_body = await req.json()
    result = api_utility_db(getfacility, req_body, db)
    return result
