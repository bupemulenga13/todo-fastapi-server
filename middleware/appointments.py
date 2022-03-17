from typing import List, Dict, Any, Optional
from fastapi import Depends, Request, Response
from sqlalchemy.orm import Session

from config.env import start_date, end_date

from scripts.dsa.appointments import *


def get_appointments(db: Session, req_start_date, req_end_date ) -> Optional[List[dict]]:
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


def get_appointments_abnormal(db: Session, req_start_date, req_end_date ) -> Optional[List[dict]]:
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


def get_appointments_tpt(db: Session, req_start_date, req_end_date ) -> Optional[List[dict]]:
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


def get_attendances(db: Session, req_selected_date ) -> Optional[List[dict]]:
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


def get_clinical_appointments(db: Session, req_start_date, req_end_date ) -> Optional[List[dict]]:
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


def get_pharmacy_appointments(db: Session, req_start_date, req_end_date ) -> Optional[List[dict]]:
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


def get_upcoming_appoitnments(db: Session, req_start_date, req_end_date ) -> Optional[List[dict]]:
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