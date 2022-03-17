from db.set_up import get_db
from schemas import query
from dsa.indicators import *
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Request, HTTPException

router = APIRouter(
    prefix="/api",
    tags=["indicators"],
)

@router.post('/tx_curr_list', response_model=query.GetList)
async def read_tx_curr(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of TX Curr.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_tx_curr(db,req_start_date, req_end_date)
    
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


@router.post('/tx_curr_active_list', response_model=query.GetList)
async def read_tx_curr_active(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of TX Curr active.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_tx_curr_active(db,req_start_date, req_end_date)

    query_list = items['items_list']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']
    list_count = items['list_count']
    

    if items:
     return {
            "query_list": query_list,
            "status_code": status_code,
            "message": message,
            "start_date": start_date,
            "end_date": end_date,
            "list_count": list_count
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/tx_new_list', response_model=query.GetList)
async def read_tx_new(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of TX New.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_tx_new(db,req_start_date, req_end_date)
    
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


@router.post('/search_tx_new', response_model=query.GetList)
async def search_tx_new_list(req: Request, db: Session = Depends(get_db)):
    """
    Search list of TX New.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')
    req_art_number = req_body.get('artNumber')

    items = search_tx_new(db,req_start_date, req_end_date, req_art_number)
    
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


@router.post('/search_tx_curr', response_model=query.GetList)
async def search_tx_curr_list(req: Request, db: Session = Depends(get_db)):
    """
    Search list of TX Curr.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')
    req_art_number = req_body.get('artNumber')

    items = search_tx_curr(db,req_start_date, req_end_date, req_art_number)
    
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



