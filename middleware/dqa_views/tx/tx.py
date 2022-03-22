from middleware.utility import case__db_ed, case__db_sd_ed
from scripts.dqa.tx import tx_curr, tx_curr_denominator, tx_curr_numerator, tx_new, extendTx_curr
from sqlalchemy.orm import Session


def gettx_curr(db: Session,end_date):
	result=case__db_ed(db,tx_curr.get_tx_curr,end_date)
	return result
	

def gettx_curr_denominator(db: Session,end_date):
	result=case__db_ed(db,tx_curr_denominator.get_tx_curr_denominator,end_date)
	return result

def gettx_curr_numerator(db: Session, end_date):
	result = case__db_ed(db, tx_curr_numerator.get_tx_curr_numerator, end_date)
	return result

def gettx_new(db: Session,start_date,_end_date ):
	result = case__db_sd_ed(db, tx_new.get_tx_new, start_date, _end_date)
	return result

def getextendTx_curr(db: Session,start_date,end_date ):
	result = case__db_sd_ed(db, extendTx_curr.get_extendTx_curr, start_date, end_date)
	return result