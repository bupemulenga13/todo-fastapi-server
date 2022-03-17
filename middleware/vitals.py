from typing import List, Optional
from sqlalchemy.orm import Session

from config.env import start_date, end_date

from scripts.dsa.vitals import *


def get_vitals(db: Session, req_start_date: str, req_end_date: str ) -> Optional[List[dict]]:
    try:
        sql_query = get_vitals_list(db, req_start_date or start_date, req_end_date or end_date)
        list_count = len(sql_query)
        return {
            "items_list": sql_query, 
            "status_code": 200, 
            "message": "Success", 
            "start_date": req_start_date or start_date, 
            "end_date": req_end_date or end_date,
            "llist_count": list_count

        }

    except Exception as e:
        return{"message": f"Error: {e}", "status_code": 500}