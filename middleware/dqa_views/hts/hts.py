from middleware.utility import case__db_sd_ed
from scripts.dqa.hts import hts_index, hts_positive
from sqlalchemy.orm import Session

def get_hts_index(db: Session, start_date, end_date ):
	result = case__db_sd_ed(db, hts_index.get_hts_index, start_date, end_date)
	return result

def get_hts_positive(db:Session, start_date, end_date):
	result = case__db_sd_ed(db, hts_positive.get_hts_positive, start_date, end_date)
	return result
