from middleware.utility import case__db_sd_ed
from scripts.dqa.pmtct import pmtct_eid_0_2, pmtct_eid_2_12, pmtct_eid_pos_0_2,pmtct_eid_pos_2_12, pmtct_stat_pos
from typing import List, Optional
from sqlalchemy.orm import Session




def getpmtct_eid_0_2(db: Session, start_date, end_date ):
	result = case__db_sd_ed(db, pmtct_eid_0_2.get_pmtc_eid_0_2, start_date, end_date)
	return result

def getpmtct_eid_2_12(db: Session, start_date, end_date ):
	result = case__db_sd_ed(db, pmtct_eid_2_12.get_pmtc_eid_2_12, start_date, end_date)
	return result

def getpmtct_eid_pos_0_2(db: Session, start_date, end_date ):
	result = case__db_sd_ed(db, pmtct_eid_pos_0_2.get_pmtc_eid_pos_0_2, start_date, end_date)
	return result

def getpmtct_eid_pos_2_12(db: Session, start_date, end_date ):
	result = case__db_sd_ed(db, pmtct_eid_pos_2_12.get_pmtc_eid_pos_2_12, start_date, end_date)
	return result

def getpmtct_stat_pos(db: Session, start_date, end_date ):
	result = case__db_sd_ed(db, pmtct_stat_pos.get_pmtc_stat_pos, start_date, end_date)
	return result
