from db.set_up import get_db
from dsa.testing import *
from typing import Any

from fastapi import APIRouter, Request, Depends, HTTPException
from sqlalchemy.orm import Session
from schemas import query

router = APIRouter(
    prefix="/api",
    tags=["testing"],
)

@router.post('/testing_list', response_model=query.GetList)
async def read_testing(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of testing data.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_testing(db,req_start_date, req_end_date)
    
    query_list = items['items_list']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']
    list_count = items['list_count']

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


@router.post('/search_testing', response_model=query.GetList)
async def search_testing_list(req: Request, db: Session = Depends(get_db)):
    """
    Search list of testing data.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')
    req_art_number = req_body.get('artNumber')

    items = search_testing(db,req_start_date, req_end_date,req_art_number)
    
    query_list = items['items_list']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']
    list_count = items['list_count']


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