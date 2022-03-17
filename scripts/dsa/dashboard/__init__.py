"""Query package for DSA dashboard cards"""

from .labs_count import get_labs_count
from .vitals_count import get_vitals_count
from .testing_count import get_testing_count
from .referals_count import get_referals_count
from .morbidity_count import get_morbidity_count
from .pharm_pick_count import get_pharm_picks_count
from .diagnostics_count import get_diagnostics_count
from .appointments_count import get_appointments_count
from .dispensations_count import get_dispensations_count
from .clinical_visits_count import get_clinical_visits_count
from .tx_current_active_count import get_tx_current_active_count
from .labour_delivery_count import  get_labour_and_delivery_count