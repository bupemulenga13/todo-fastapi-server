
from scripts.dsa.dashboard import *
from fastapi import APIRouter, Request, Depends, HTTPException
from sqlalchemy.orm import Session
from db.set_up import get_db
from schemas import query
from dsa.dashboard import *

router = APIRouter(
    prefix="/api",
    tags=["dashboard"],
)

@router.post('/tx_curr_active_count', response_model=query.GetCount)
async def tx_curr_active_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of TX Curr active.
    """
    req_body = await req.json()
    req_end_date = req_body.get('endDate')

    items = count_tx_curr_active(db,req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    end_date = items['end_date']

    if items:
      return  {
            'count': count,
            'status_code': status_code,
            'message': message,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/appointments_count', response_model=query.GetCount)
async def appointments_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of appointments.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_appointments(db,req_start_date, req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
       return {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/labs_count', response_model=query.GetCount)
async def labs_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of labs.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_labs(db,req_start_date, req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
      return   {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/vitals_count', response_model=query.GetCount)
async def vitals_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of vitals.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')
    items = count_vitals(db,req_start_date,req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
        return {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/testing_count', response_model=query.GetCount)
async def testing_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of testing data.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_testing_data(db,req_start_date,req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
      return  {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")
    

@router.post('/morbidity_count', response_model=query.GetCount)
async def morbidity_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of morbidity data.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_morbidity_data(db,req_start_date,req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
       return {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/referals_count', response_model=query.GetCount)
async def referals_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of referals.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_referals(db,req_start_date,req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
       return {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/pharm_pick_count', response_model=query.GetCount)
async def pharm_pick_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of pharmacy drug pickups.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_pharm_pick_data(db,req_start_date,req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
       return {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/diagnostics_count', response_model=query.GetCount)
async def diagnostics_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of diganostics data.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_diagnostics(db,req_start_date, req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
       return {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/clinical_visits_count', response_model=query.GetCount)
async def clinical_visits_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of clinical visits.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_clinical_visits(db,req_start_date, req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
       return {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")


@router.post('/labour_and_delivery_count', response_model=query.GetCount)
async def labour_delivery_count(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve count of labour and delivery data.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = count_labour_delivery(db,req_start_date, req_end_date)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if items:
      return  {
            'count': count,
            'status_code': status_code,
            'message': message,
            "start_date": start_date,
            'end_date': end_date
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")
