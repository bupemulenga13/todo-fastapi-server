import re
from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session
from db.set_up import get_db


def api_utility_sd_ed(view_, req: Request, db: Session = Depends(get_db)):
	start_date = req.get("start_date")
	end_date = req.get("end_date")
	result = view_(db, start_date, end_date)

	query_list = result["items_list"]
	status_code = result["status_code"]
	message = result["message"]
	start__date = result["start_date"]
	end__date = result["end_date"]
	if result:
		return {
			"query_list": query_list,
			"status_code": status_code,
			"message": message,
			"start_date": start__date,
			"end_date": end__date
		}

	if not result:
		raise HTTPException(status_code=400, detail="Not found")

	else:
		raise HTTPException(status_code=500, detail="we good")


def api_utility_ed(view_, req: Request, db: Session = Depends(get_db)):
	end_date = req.get("end_date")
	result = view_(db, end_date)

	query_list = result["items_list"]
	status_code = result["status_code"]
	message = result["message"]
	end__date = result["end_date"]
	if result:
		return {
			"query_list": query_list,
			"status_code": status_code,
			"message": message,
			"end_date": end__date
		}

	if not result:
		raise HTTPException(status_code=400, detail="Not found")

	else:
		raise HTTPException(status_code=500, detail="we good")


def api_utility_db(view_,db: Session = Depends(get_db)):
	result = view_(db)
	return print(result)

	# if not result:
	#     raise HTTPException(status_code=400, detail="Not found")

	# else:
	#     raise HTTPException(status_code=500, detail="we good")
