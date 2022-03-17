from http.client import HTTPException
from schemas import query
from db.set_up import get_db
from sqlalchemy.orm import Session
from middleware.dashboard import *

from fastapi import FastAPI, Depends, Request, Response

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello Genius"}

@app.post('/tx_curr_active', response_model=query.GetCount)
async def tx_curr_active_count(req: Request, db: Session = Depends(get_db)):
    """Retrieve count of TX Curr active"""
    req_body = await req.json()
    req_end_date = req_body.get('endDate')
    items = count_tx_curr_active(db,req_end_date)

    if items:
        return {

            "count": items['items_count'], 
            "end_date": items['end_date']
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong with the server")
    
@app.get('/appointments', response_model=query.GetCount)
async def appointments_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_appointments(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/labs', response_model=query.GetCount)
async def labs_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_labs(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/vitals', response_model=query.GetCount)
async def vitals_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_vitals(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/testing', response_model=query.GetCount)
async def testing_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_testing_data(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}
    

@app.post('/morbidity', response_model=query.GetCount)
async def morbidity_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_morbidity_data(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/referals', response_model=query.GetCount)
async def referals_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_referals(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/pharm_pick', response_model=query.GetCount)
async def pharm_pick_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_pharm_pick_data(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/diagnostics', response_model=query.GetCount)
async def diagnostics_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_diagnostics(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/clinical_visits', response_model=query.GetCount)
async def clinical_visits_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_clinical_visits(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/labour_and_delivery', response_model=query.GetCount)
async def labour_delivery_count(req: Request, res: Response, db: Session = Depends(get_db)):
    req_body = await req.json()
    items = count_labour_delivery(db,req_body, res)
    
    count = items['items_count']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']

    if status_code == 200:
        item = query.GetCount(start_date=start_date,end_date=end_date, status_code=status_code, message=message, count=count)
        return item
    else:
        return {"message": message, "status_code": status_code}

@app.post('/appointments_list', response_model=query.GetList)
async def read_appointments(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of appointments.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_appointments(db,req_start_date, req_end_date)
    
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
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post('/appointments_abnormal_vitals_list', response_model=query.GetList)
async def read_appointments_abnormal_vitals(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of appointments for abnormal vitals.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_appointments_abnormal(db,req_start_date, req_end_date)

    query_list = items['items_list']
    status_code = items['status_code']
    message = items['message']
    start_date = items['start_date']
    end_date = items['end_date']
    list_count = len(query_list)
    

    if items:
     return {
            'query_list': query_list,
            "status_code": status_code,
            "list_count": list_count
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post('/appointments_tpt', response_model=query.GetList)
async def read_appointments_tpt(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of appointments tpt.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_appointments_tpt(db,req_start_date, req_end_date)
    
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
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post('/attendance_list', response_model=query.GetList)
async def read_attendances(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of attendances.
    """
    req_body = await req.json()
    req_selected_date = req_body.get('selectedDate')

    items = get_attendances(db,req_selected_date)
    
    query_list = items['items_list']
    status_code = items['status_code']
    message = items['message']
    selected_date = items['selected_date']
    list_count = len(query_list)

    if items:
     return {
            'query_list': query_list,
            'status_code': status_code,
            'message': message,
            "selected_date": selected_date,
            "list_count": list_count
        }
    if not items:
        raise HTTPException(status_code=404, detail="Not found")
    else:
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post('/clinical_appointments', response_model=query.GetList)
async def read_clinical_appointments(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of clinical appointments.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_clinical_appointments(db,req_start_date, req_end_date)
    
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
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post('/pharmacy_appointments', response_model=query.GetList)
async def read_pharmacy_appointments(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of pharmacy appointments.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_pharmacy_appointments(db,req_start_date, req_end_date)
    
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
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post('/upcoming_appointments', response_model=query.GetList)
async def read_upcoming_appointments(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of upcoming appointments.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_upcoming_appoitnments(db,req_start_date, req_end_date)
    
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
        raise HTTPException(status_code=500, detail="Something went wrong")

@app.get("/diagnostics_list")
async def read_diagnostics(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of diagnostics.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_diagnostics(db,req_start_date, req_end_date)
    
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

@app.post('/vitals_list', response_model=query.GetList)
async def read_vitals(req: Request, db: Session = Depends(get_db)):
    """
    Retrieve list of vitals.
    """
    req_body = await req.json()
    req_start_date = req_body.get('startDate')
    req_end_date = req_body.get('endDate')

    items = get_vitals(db,req_start_date, req_end_date)
    
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
