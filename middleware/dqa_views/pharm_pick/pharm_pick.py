from middleware.utility import case__db_sd_ed
from scripts.dqa.pharm_pick.pharm_pick import get_pharm_pick
from sqlalchemy.orm import Session


def getpharm_pick(db: Session, start_date, end_date ):
	result = case__db_sd_ed(db, get_pharm_pick, start_date, end_date)
	return result
