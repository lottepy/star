import  numpy as np
from fx_daily.data.config.config import ConfigManager

# cp = ConfigManager('data/config/config.ini')
# COMMISSION = cp.configs['commission']

def cross_order_fx(ccp_order, ccy_matrix, pv, last_order, COMMISSION):
    #可以增加bid_ask
    ccp_order_diag = np.diag(ccp_order)
    ccy_position = np.matmul(ccy_matrix, ccp_order_diag)
    commission_fee = np.abs(ccp_order - last_order) * COMMISSION
    ccy_position[-1] += pv - commission_fee
    return ccy_position, ccp_order