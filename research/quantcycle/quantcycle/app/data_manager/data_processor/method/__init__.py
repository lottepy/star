from .append_window_data import MethodAPPEND
from .backup import MethodBACKUP
from .calculate_interest_rate import MethodINT
from .ccyfx import MethodCCYFX
from .min2hour import MethodMIN2HOUR
from .stack import MethodSTACK
from .symbols_info import MethodINFO
from .metrics import MethodMETRICS
from .strat_id_to_sym_map import MethodSTRATIDMAP
from .pass_3d_np_to_DD import MethodPASS3DNP
from .tradable_table import MethodTRADABLE
from .settlement_data import MethodSETTLEMENT
from .calculate_split_ratio import MethodSPLIT

METHOD_MAP = {'INT': MethodINT,
              'SPLIT': MethodSPLIT,
              'STACK': MethodSTACK,
              'CCYFX': MethodCCYFX,
              'BACKUP': MethodBACKUP,
              'APPEND': MethodAPPEND,
              'MIN2HOUR': MethodMIN2HOUR,
              'INFO': MethodINFO,
              'METRICS':MethodMETRICS,
              'STRATIDMAP': MethodSTRATIDMAP,
              'PASS3DNP': MethodPASS3DNP,
              'TRADABLE': MethodTRADABLE,
              'SETTLEMENT': MethodSETTLEMENT}
