from db.set_up import get_db
from dsa.referals import *
from fastapi import APIRouter, Request, Depends, HTTPException
from sqlalchemy.orm import Session
from schemas import query

router = APIRouter(
    prefix="/api",
    tags=["referals"],
)

@router.post('/referals_list', response_model=query.GetList)
async def read_referals(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of referals.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_referals(db,req_start_date, req_end_date)
    
    query_list = items['items_list']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']
    list_count = len(query_list)

    if items:
     return {
            'query_list': query_list,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            "end_date": end_date,
            "list_count": list_count
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")