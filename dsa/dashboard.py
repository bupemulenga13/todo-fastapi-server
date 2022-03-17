from typing import Any, Optional, List
from fastapi import Depends, Request, Response
from sqlalchemy.orm import Session

from config.env import start_date, end_date

from scripts.dsa.dashboard import *

def count_tx_curr_active(db: Session, req_end_date: str) -> Any:
    try:
        sql_query = get_tx_current_active_count(db, req_end_date or end_date )
        items_count = len(sql_query)
        print ("Items count for tx curr active", items_count)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "end_date": req_end_date or end_date}
    except Exception as e:
        return{"message": f"Error: {e}"}

def count_appointments(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_appointments_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_labs(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_labs_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_vitals(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_vitals_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}
    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_testing_data(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_testing_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_morbidity_data(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_morbidity_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_referals(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_referals_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_pharm_pick_data(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_pharm_picks_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_diagnostics(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_diagnostics_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_clinical_visits(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_clinical_visits_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_labour_delivery(db: Session, req_start_date: str, req_end_date: str) -> dict:
    try:
        sql_query = get_labour_and_delivery_count(db, req_start_date or start_date, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": req_start_date or start_date, "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}