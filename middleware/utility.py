# views utils.py
from typing import List, Optional
from sqlalchemy.orm import Session
from config.env import start_date, end_date
#utility functions for differnt view cases 

def case__db_sd_ed(db:Session, sql_query, get_startdate, get_enddate)->Optional[List[dict]]:
	try:
		get_list =sql_query(db, get_startdate or start_date, get_enddate or end_date)
		return {
				"items_list": get_list, 
				"status_code": 200, 
				"message": "Success", 
				"start_date": get_startdate or start_date, 
				"end_date": get_enddate or end_date,
			}
	except Exception as e:
		return{'message': f"Error: {e}", "status_code":500}


def case__db(db:Session, sql_query)->Optional[List[dict]]:
	try:
		get_list = sql_query(db)
		return {
			"items_list": get_list, 
			"status_code": 200, 
			"message": "Success",
			}
	except Exception as e:
		return{'message': f"Error: {e}", "status_code":500}


def case__db_ed(db:Session, sql_query, get_enddate)->Optional[List[dict]]:
	try:
		get_list =sql_query(db,get_enddate or end_date)
		return {
				"items_list": get_list, 
				"status_code": 200, 
				"message": "Success", 
				"end_date": get_enddate or end_date,
			}
	except Exception as e:
		return{'message': f"Error: {e}", "status_code":500}


def case__db_selected_date(db:Session, sql_query, get_selected_date)->Optional[List[dict]]:
	try:
		get_list =sql_query(db, get_selected_date)
		return {
				"items_list": get_list, 
				"status_code": 200, 
				"message": "Success", 
				"selected_date": get_selected_date, 
			}
	except Exception as e:
		return{'message': f"Error: {e}", "status_code":500}


def case__db_sd_ed_art_no(db:Session, sql_query, get_startdate, get_enddate, get_art_no )->Optional[List[dict]]:
	try:
		get_list = sql_query(db,get_startdate, get_enddate, get_art_no)
		return {
				"items_list": get_list, 
				"status_code": 200, 
				"message": "Success", 
				"start_date": get_startdate or start_date, 
				"end_date": get_enddate or end_date,
				"art_no": get_art_no,
			}
	except Exception as e:
		return{'message': f"Error: {e}", "status_code":500}


def case__db_sd_ed_patient_id(db:Session, sql_query, get_startdate, get_enddate, get_patient_id )->Optional[List[dict]]:
	try:
		get_list = sql_query(db,get_startdate, get_enddate, get_patient_id)
		return {
				"items_list": get_list, 
				"status_code": 200, 
				"message": "Success", 
				"start_date": get_startdate or start_date, 
				"end_date": get_enddate or end_date,
				"patient_id": get_patient_id,
			}
	except Exception as e:
		return{'message': f"Error: {e}", "status_code":500}
