from middleware.utility import case__db_sd_ed
from scripts.dqa.interactions.interactions import get_interactions
from sqlalchemy.orm import Session



def getinteractions(db: Session, start_date, end_date ):
	result = case__db_sd_ed(db, get_interactions, start_date, end_date)
	return result
