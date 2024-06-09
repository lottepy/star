from enum import Enum

RISK_TYPES_NUM = 7

class Risk_types(Enum):

     order_volume = 0
     traded = 1
     cancelled = 2
     flow_count = 3
     security_weight=4
     industry_limit=5
     black_list=6
