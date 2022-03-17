from typing import List, Optional
from sqlalchemy.orm import Session

from config.env import start_date, end_date

from scripts.dsa.medications import *


def get_pharm_pick(db: Session, req_start_date:str, req_end_date:str ) -> Optional[List[dict]]:
    try:
        sql_query = get_pharm_pick_list(db, req_start_date or start_date, req_end_date or end_date)
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


def get_dispensations(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_dispensations_list(db, req_start_date or start_date, req_end_date or end_date)      
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


def search_pharm_pick(db: Session, req_start_date: str, req_end_date: str, art_number: str) -> Optional[List[dict]]:
    try:
        sql_query = get_pharm_pick_search_list(db, req_start_date or start_date, req_end_date or end_date, art_number)
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


def search_dispensations(db: Session, req_start_date: str, req_end_date: str, art_number: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_dispensations_search_list(db, req_start_date or start_date, req_end_date or end_date, art_number)
        list_count = len(sql_query)
        
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date ,
            "list_count": list_count
        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}



