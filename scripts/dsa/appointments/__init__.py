"""Query package for all DSA appointments"""

from db.set_up import get_db
from .attendance_list import get_attendance_list
from .appointments_list import get_appointments_list
from .appointment_tpt import get_appointments_tpt_list
from .appointment_search import get_appointments_search_list
from .clinical_appointments_list import get_clinical_appointments_list
from .pharmacy_appointments_list import get_pharmacy_appointments_list
from .upcoming_appointments_list import get_upcoming_appointments_list
from .appointment_abnormal_vt_list import get_appointments_abnormal_vitals_list

def execute():
	get_appointments_list(get_db,start_date="2020-01-01", end_date="2020-01-07")