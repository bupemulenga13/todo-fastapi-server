from typing import List, Optional
from sqlalchemy.orm import Session

from config.env import start_date, end_date

from scripts.dsa.indicators import *
from scripts.dsa.appointments import *


def get_tx_curr_active(db: Session, req_start_date:str, req_end_date:str ) -> Optional[List[dict]]:
    try:
        sql_query = get_tx_current_active_list(db, req_start_date or start_date, req_end_date or end_date)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_tx_new(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_tx_new_list(db, req_start_date or start_date, req_end_date or end_date)      
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def get_tx_curr(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_tx_current_list(db, req_start_date or start_date, req_end_date or end_date)
        
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def search_tx_new(db: Session, req_start_date: str, req_end_date: str, art_number: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_tx_new_search_list(db, req_start_date or start_date, req_end_date or end_date, art_number)
        
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date 
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}


def search_tx_curr(db: Session, req_start_date: str, req_end_date: str, art_number: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_tx_current_search_list(db, req_start_date or start_date, req_end_date or end_date, art_number)
        
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date
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