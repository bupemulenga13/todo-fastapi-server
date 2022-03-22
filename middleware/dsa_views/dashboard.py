from fastapi import Request, Response
from sqlalchemy.orm import Session
from typing import List, Optional
from config.env import start_date, end_date

from scripts.dsa.dashboard import *
from scripts.dsa.appointments import *

def count_tx_curr_active(db: Session, req_end_date: str) -> dict:
    try:
        sql_query = get_tx_current_active_count(db, req_end_date or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "end_date": req_end_date or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_appointments(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_appointments_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_labs(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_labs_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_vitals(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_vitals_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_testing_data(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_testing_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_morbidity_data(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_morbidity_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_referals(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_referals_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_pharm_pick_data(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_pharm_picks_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {
            "items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_diagnostics(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_diagnostics_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_dispensations(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_dispensations_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_diagnostics(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_diagnostics_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_clinical_visits(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_clinical_visits_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def count_labour_delivery(db: Session, req: Request, res: Response) -> int:
    try:
        start_date_from_req_body = req.get('startDate')
        end_date_from_req_body = req.get('endDate')
        sql_query = get_labour_and_delivery_count(db, start_date_from_req_body or start_date, end_date_from_req_body or end_date)
        items_count = len(sql_query)
        return {"items_count": items_count, "status_code": 200, "message": "Success", "start_date": start_date_from_req_body or start_date, "end_date": end_date_from_req_body or end_date}

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_appointments(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_appointments_list(db, req_start_date or start_date, req_end_date or end_date)
        list_count = len(sql_query)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date,
            "list_count": list_count
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_appointments_abnormal(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_appointments_abnormal_vitals_list(db, req_start_date or start_date, req_end_date or end_date)
        list_count = len(sql_query)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date,
            "list_count": list_count
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_appointments_tpt(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_appointments_tpt_list(db, req_start_date or start_date, req_end_date or end_date)
        list_count = len(sql_query)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date,
            "list_count": list_count
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_attendances(db: Session, req_selected_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_attendance_list(db, req_selected_date or start_date)
        list_count = len(sql_query)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "selected_date": req_selected_date or start_date, 
            "list_count": list_count
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_clinical_appointments(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_clinical_appointments_list(db, req_start_date or start_date, req_end_date or end_date)
        list_count = len(sql_query)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date,
            "list_count": list_count
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_pharmacy_appointments(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_pharmacy_appointments_list(db, req_start_date or start_date, req_end_date or end_date)
        list_count = len(sql_query)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date,
            "list_count": list_count
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_upcoming_appoitnments(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_upcoming_appointments_list(db, req_start_date or start_date, req_end_date or end_date)
        list_count = len(sql_query)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date,
            "list_count": list_count
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

def search_appointments(db: Session, req_start_date: str, req_end_date: str, art_number: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_appointments_search_list(db, req_start_date or start_date, req_end_date or end_date, art_number)
        
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date 
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}

