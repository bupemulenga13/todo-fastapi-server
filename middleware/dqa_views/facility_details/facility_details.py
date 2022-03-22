from middleware.utility import case__db
from scripts.dqa.facility_details import facility_details,get_facility
from sqlalchemy.orm import Session

def get_facility_details(db: Session):
	result = case__db(db, facility_details.get_facility_details)
	return result

def getfacility(db:Session):
	result = case__db(db, get_facility.get_facility)
	return result
