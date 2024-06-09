import numpy as np
import numba as nb
from numba.experimental import jitclass
import math

##===========================================================================##

Moving_series_spec = [
    ('length', nb.int64),
    ('n_security', nb.int64),
    ('value_matrix',  nb.float64[:,:]),
    ('removed_value',  nb.float64[:]),
    ('init',  nb.boolean),
    ('index',  nb.int64),
    ]
@jitclass(Moving_series_spec)
class Moving_series(object):
    def __init__(self, length,n_security):
        self.length = length 
        self.n_security = n_security
        self.value_matrix = np.empty((length,n_security))
        self.init = False
        self.index = 0
        self.removed_value = np.zeros(n_security)

    def on_data(self, value):
        if not self.init:
            self.value_matrix[self.index] = np.copy(value)
            self.index += 1
            if self.index >= self.length:
                self.init = True
        else:
            self.removed_value = np.copy(self.value_matrix[0])
            self.value_matrix[0:-1] = np.copy(self.value_matrix[1:])
            self.value_matrix[-1] = np.copy(value)

try:
    moving_series_type = Moving_series.class_type.instance_type
except:
    moving_series_type = "moving_series_type"
##===========================================================================##

base_spec = [
            ('moving_series', moving_series_type),
            ('ready', nb.boolean),
            ('value',  nb.float64[:]),
        ]


##===========================================================================##

MA_spec = [ ]
@jitclass(base_spec+MA_spec)
class MA(object):
    def __init__(self,length,n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

    def on_data(self, value):
        self.moving_series.on_data(value)
        if self.moving_series.init:
            if not self.ready:
                self.value = np.sum(self.moving_series.value_matrix,axis = 0) / self.moving_series.length
                self.ready = True
            else:
                self.value += (self.moving_series.value_matrix[-1]-self.moving_series.removed_value) / self.moving_series.length

try:
    ma_type = MA.class_type.instance_type
except:
    ma_type = "MA.class_type.instance_type"


##===========================================================================##

HIGH_spec = [  ]
@jitclass(base_spec + HIGH_spec)
class HIGH(object):
    def __init__(self,length,n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)


    def on_data(self, value):
        self.moving_series.on_data(value)
        if self.moving_series.init:
            self.value = np.zeros(self.moving_series.n_security)
            for i in range(self.moving_series.n_security):
                self.value[i] = np.max(self.moving_series.value_matrix[:,i])
            self.ready = True

try:
    high_type=HIGH.class_type.instance_type
except:
    high_type = "high_type"
##===========================================================================##

LOW_spec = [  ]
@jitclass(base_spec + LOW_spec)
class LOW(object):
    def __init__(self,length,n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)


    def on_data(self, value):
        self.moving_series.on_data(value)
        if self.moving_series.init:
            self.value = np.zeros(self.moving_series.n_security)
            for i in range(self.moving_series.n_security):
                self.value[i] = np.min(self.moving_series.value_matrix[:,i])
            self.ready = True

try:
    low_type = LOW.class_type.instance_type
except:
    low_type = "low_type"

##===========================================================================##

EMA_spec = [  ]
@jitclass(base_spec + EMA_spec)
class EMA(object):
    def __init__(self,length,n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

    def on_data(self, value):
        self.moving_series.on_data(value)
        if self.moving_series.init:
            if not self.ready:
                self.value = np.sum(self.moving_series.value_matrix,axis = 0) / self.moving_series.length
                self.ready = True
            else:
                self.value += 2*(value - self.value)/(self.moving_series.length+1)

try:
    ema_type = EMA.class_type.instance_type
except:
    ema_type = "ema_type"



##===========================================================================##

SMMA_spec = [  ]
@jitclass(base_spec + SMMA_spec)
class SMMA(object):
    def __init__(self,length,n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

    def on_data(self, value):
        self.moving_series.on_data(value)
        if self.moving_series.init:
            if not self.ready:
                self.value = np.sum(self.moving_series.value_matrix,axis = 0) / self.moving_series.length
                self.ready = True
            else:
                self.value += (value - self.value)/(self.moving_series.length+1)

try:
    smma_type = SMMA.class_type.instance_type
except:
    smma_type = "smma_type"


##===========================================================================##

DIFF_spec = [  ]
@jitclass(base_spec + DIFF_spec)
class DIFF(object):
    def __init__(self,length,n_security):
        self.moving_series = Moving_series(length + 1,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

    def on_data(self, value):
        self.moving_series.on_data(value)
        if self.moving_series.init:
            self.value = self.moving_series.value_matrix[-1] - self.moving_series.value_matrix[0]
            self.ready = True
try:
    diff_type = DIFF.class_type.instance_type
except:
    diff_type = "diff_type"
##===========================================================================##

RSI_spec = [
                ('up_diff', diff_type),
                ('down_diff', diff_type),
                ('up_smma', smma_type),
                ('down_smma', smma_type),
                ('n_security',nb.int64)
            ]
@jitclass(base_spec + RSI_spec)
class RSI(object):
    def __init__(self,length,n_security):
        self.n_security = n_security
        self.ready = False
        self.value = np.zeros(n_security)

        self.up_diff   = DIFF(1,n_security)
        self.down_diff = DIFF(1,n_security)
        self.up_smma   = SMMA(length,n_security)
        self.down_smma = SMMA(length,n_security)


    def on_data(self, value):
        self.up_diff.on_data(value)
        self.down_diff.on_data(value)
        if not self.up_diff.ready or not self.down_diff.ready :
            return
        up_value = np.zeros(self.n_security)
        down_value = np.zeros(self.n_security)
        for i in range(self.n_security):
            up_value[i] =  np.maximum(self.up_diff.value[i],0)
            down_value[i] = -1*np.minimum(self.down_diff.value[i],0)
        self.up_smma.on_data(up_value)
        self.down_smma.on_data(down_value)
        if not self.up_smma.ready or not self.down_smma.ready :
            return
        rs = self.up_smma.value / self.down_smma.value
        self.ready = True
        self.value = 100*(1-1/(1+rs))

##===========================================================================##

#rsi = RSI(5,2)
#for i in range(30):
#    rsi.on_data(np.ones(2)*i)
#    print(rsi.value)

##===========================================================================##

Momentum_spec = [ ('diff', diff_type),    ]
@jitclass(base_spec + Momentum_spec)
class Momentum(object):

    def __init__(self,length,n_security):
        self.diff = DIFF(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

    def on_data(self,value):
        self.diff.on_data(value)
        if self.diff.ready:
            self.ready = True
            self.value = self.diff.value

try:
    momentum_type = Momentum.class_type.instance_type
except:
    momentum_type = "momentum_type"

##===========================================================================##

Roc_spec = [  ]
@jitclass(base_spec + Roc_spec)
class ROC(object):

    def __init__(self,length,n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

    def on_data(self,value):
        self.moving_series.on_data(value)
        if self.moving_series.init:
            self.ready = True
            self.value = 100*(self.moving_series.value_matrix[-1]/self.moving_series.value_matrix[0])

try:
    roc_type = ROC.class_type.instance_type
except:
    roc_type = "roc_type"


##===========================================================================##

LMA_spec = [  ]
@jitclass(base_spec + LMA_spec)
class LMA(object):
    
    def __init__(self,length,n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)


    def on_data(self,value):
        self.moving_series.on_data(value)
        if self.moving_series.init:
            self.ready = True
            mask = np.arange(1, self.moving_series.length+1)
            self.value = np.zeros(self.moving_series.n_security)
            for i in range(self.moving_series.n_security):
                temp_array = self.moving_series.value_matrix[:,i] * mask
                self.value[i] = np.sum(temp_array) / np.sum(mask)

try:
    lma_type = LMA.class_type.instance_type
except:
    lma_type = "lma_type"

##===========================================================================##

#rsi = LMA(5,2)
#for i in range(30):
#    rsi.on_data(np.ones(2)*i)
#    print(rsi.value)

cci_spec = [
    ('ma', ma_type),
    ('md', ma_type)
]

@jitclass(base_spec + cci_spec)
class CCI(object):

    def __init__(self, length, n_security):
        self.moving_series = Moving_series(length, n_security)
        self.ma = MA(length, n_security)
        self.md = MA(length, n_security)
        self.ready = False
        self.value = np.zeros(n_security)


    def on_data(self, high, low, close):

        TP = high + low + close/3
        self.ma.on_data(TP)

        if not self.ma.ready:
            return

        curr_ma = self.ma.value
        self.md.on_data(np.absolute(TP - curr_ma))

        if not self.md.ready:
            return

        md = self.md.value

        self.ready = True
        self.value = (TP - curr_ma) / (0.015 * md)
        self.value[md==0] = 0

try:
    cci_type = CCI.class_type.instance_type
except:
    cci_type = "cci_type"
##===========================================================================##

R_spec = [ ('hh', high_type),
            ('ll', low_type),
            ('n_security', nb.int64),
            ]
@jitclass(base_spec + R_spec)
class R(object):

    def __init__(self, length,n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)
        self.n_security = n_security
        self.hh = HIGH(length,n_security)
        self.ll = LOW(length,n_security)

    def on_data(self, high, low, close):

        self.hh.on_data(high)
        self.ll.on_data(low)

        if not self.hh.ready or not self.ll.ready:
            return
        self.ready = True
        self.value = (close-self.ll.value)/(self.hh.value-self.ll.value)*100

        #need to be vectorized,but i am tired today
        for i in range(self.n_security):
            if self.hh.value[i]-self.ll.value[i] != 0:
                self.value[i] = (close[i]-self.ll.value[i])/(self.hh.value[i]-self.ll.value[i])*100
            else:
                self.value[i] = 50

try:
    r_type = R.class_type.instance_type
except:
    r_type = "r_type"


##===========================================================================##
KD_spec = [ ('r', r_type),
            ('fast_d', ma_type),
            ('slow_d', ma_type),
            ]
@jitclass(base_spec + KD_spec)
class KD(object):

    def __init__(self,length_R,length_fastd,length_slowd,n_security):
        self.moving_series = Moving_series(length_R,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

        self.r = R(length_R,n_security)
        self.fast_d = MA(length_fastd,n_security)
        self.slow_d = MA(length_slowd,n_security)

    def on_data(self, high, low, close):

        self.r.on_data(high, low, close)

        if not self.r.ready:
            return 

        r_value = self.r.value
        self.fast_d.on_data(r_value)

        if not self.fast_d.ready:
            return

        fast_d_value = self.fast_d.value
        self.slow_d.on_data(fast_d_value)

        if not self.slow_d.ready:
            return
        
        self.ready = True
        self.value = self.slow_d.value

try:
    kd_type = KD.class_type.instance_type
except:
    kd_type = "kd_type"
#kd_type.define(KD.class_type.instance_type)

##===========================================================================##
#rsi = KD(5,3,6,2)
#for i in range(30):
#    rsi.on_data(np.ones(2)*i,np.ones(2)*i,np.ones(2)*i)
#    print(rsi.ready)
#    print(rsi.value)
##===========================================================================##

MACD_spec = [
                ('fast_l', ema_type),
                ('slow_l', ema_type),
                ('dea', ema_type),
            ]
@jitclass(base_spec + MACD_spec)

class MACD(object):

    def __init__(self,length_fast_l,length_slow_l,length_dea,n_security):
        self.moving_series = Moving_series(length_dea,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

        self.fast_l = EMA(length_fast_l,n_security)
        self.slow_l = EMA(length_slow_l,n_security)
        self.dea = EMA(length_dea,n_security)

    def on_data(self, value):

        self.fast_l.on_data(value)
        self.slow_l.on_data(value)

        if not self.fast_l.ready or not self.slow_l.ready:
            return 

        dif = self.fast_l.value - self.slow_l.value

        self.dea.on_data(dif)

        if not self.dea.ready:
            return
        dea = self.dea.value

        self.ready = True
        self.value = 2*(dif-dea)

try:
    macd_type = MACD.class_type.instance_type
except:
    macd_type = "macd_type"

##===========================================================================##

# MACD% 与MACD略有不同
MACD_spec = [
                ('fast_l', ema_type),
                ('slow_l', ema_type),
                ('dif', nb.float64[:]),
                ('dea', ema_type),
            ]
@jitclass(base_spec + MACD_spec)
class MACD_P(object):

    def __init__(self,length_fast_l,length_slow_l,length_dea,n_security):
        self.moving_series = Moving_series(length_dea,n_security)
        self.ready = False
        self.value = np.zeros(n_security)
        self.dif = np.zeros(n_security)

        self.fast_l = EMA(length_fast_l,n_security)
        self.slow_l = EMA(length_slow_l,n_security)
        self.dea = EMA(length_dea,n_security)

    def on_data(self, value):

        self.fast_l.on_data(value)
        self.slow_l.on_data(value)

        if not self.fast_l.ready or not self.slow_l.ready:
            return

        dif = (self.fast_l.value - self.slow_l.value)/(self.slow_l.value)
        self.dif = dif

        self.dea.on_data(dif)

        if not self.dea.ready:
            return
        dea = self.dea.value

        self.ready = True
        self.value = 2*(dif-dea)

try:
    macd_p_type = MACD_P.class_type.instance_type
except:
    macd_p_type = "macd_p_type"



##===========================================================================##
KDJ_spec = [
                ('kd', kd_type),
                ('K', nb.float64[:]),
                ('D', nb.float64[:]),
                ('J', nb.float64[:]),
            ]
@jitclass(base_spec + KDJ_spec)
class KDJ(object):

    def __init__(self, length_R,length_fastd,length_slowd,n_security):
        self.moving_series = Moving_series(length_R,n_security)
        self.ready = False
        self.value = np.zeros(n_security)

        self.kd = KD(length_R,length_fastd,length_slowd,n_security)
        self.K = np.zeros(n_security)
        self.D = np.zeros(n_security)
        self.J = np.zeros(n_security)

    def on_data(self, high, low, close):

        self.kd.on_data(high, low, close)

        if not self.kd.ready:
            return

        self.ready = True
        self.K = self.kd.fast_d.value
        self.D = self.kd.slow_d.value
        self.J = 3 * self.K - 2 * self.D

try:
    kdj_type = KDJ.class_type.instance_type
except:
    kdj_type = "kdj_type"
##===========================================================================##
MA_envelope_spec = [
                        ('ma',ma_type),
                        ('envelope',nb.float64),
                        ('upper',nb.float64[:]),
                        ('lower',nb.float64[:]),
                    ]
@jitclass(base_spec + MA_envelope_spec)
class MA_envelope(object):
    
    def __init__(self,length,n_security, envelope):
        self.moving_series = Moving_series(length,n_security)
        self.ma       = MA(length,n_security)
        self.envelope = envelope
        self.upper = np.empty(n_security)
        self.lower = np.empty(n_security)
        self.ready = False

    def on_data(self,value):
        self.ma.on_data(value)
        if self.ma.ready:
            self.upper    = self.ma.value * (1 + self.envelope)
            self.lower    = self.ma.value * (1 - self.envelope)
            self.ready = True

try:
    ma_envelope_type = MA_envelope.class_type.instance_type
except:
    ma_envelope_type = "ma_envelope_type"


##===========================================================================##

Bollinger_Bands_spec = [
                        ('ma',ma_type),
                        ('ma_square',ma_type),
                        ('band_width',nb.float64),
                        ('n_security',nb.int64),
                        ('upper',nb.float64[:]),
                        ('lower',nb.float64[:]),
                        ('middle',nb.float64[:]),
                        ('std',nb.float64[:]),
                        ('variance',nb.float64[:]),
                    ]
@jitclass(base_spec + Bollinger_Bands_spec)
class Bollinger_Bands(object):

    def __init__(self, length,n_security,band_width):
        self.moving_series = Moving_series(length,n_security)
        self.band_width = band_width
        self.n_security = n_security
        self.ma = MA(length,n_security)
        self.ma_square = MA(length,n_security)
        self.upper = np.empty(n_security)
        self.lower = np.empty(n_security)
        self.middle = np.empty(n_security)
        self.std = np.empty(n_security)
        self.variance = np.empty(n_security)
        self.ready = False

    def on_data(self, value):
        self.ma.on_data(value)
        self.ma_square.on_data(value*value)
        if not self.ma.ready or not self.ma_square.ready:
            return
        self.variance = self.ma_square.value - self.ma.value*self.ma.value
        self.std = np.array([math.pow(var, 1 / 2) for var in self.variance])
        self.middle = self.ma.value
        self.upper = self.middle + (self.band_width * self.std)
        self.lower = self.middle - (self.band_width * self.std)
        self.ready = True

try:
    bollinger_bands_type = Bollinger_Bands.class_type.instance_type
except:
    bollinger_bands_type = "bollinger_bands_type"
##===========================================================================##

## 加try except
#
# ##===========================================================================##
#
Donchian_spec = [
                        ('low',low_type),
                        ('high',high_type),
                        ('upper',nb.float64[:]),
                        ('lower',nb.float64[:]),
                        ('middle',nb.float64[:]),
                    ]
@jitclass(base_spec + Donchian_spec)
class Donchian(object):

    def __init__(self, length, n_security):
        self.moving_series = Moving_series(length,n_security)
        self.low = LOW(length,n_security)
        self.high = HIGH(length,n_security)
        self.upper = np.empty(n_security)
        self.lower = np.empty(n_security)
        self.middle = np.empty(n_security)
        self.ready = False

    def on_data(self, high, low):
        self.low.on_data(high)
        self.high.on_data(low)
        if not self.high.ready or not self.low.ready:
            return
        self.upper = self.high.value
        self.lower = self.low.value
        self.middle = (self.lower + self.upper)*1/2
        self.ready = True

try:
    donchian_type = Donchian.class_type.instance_type
except:
    donchian_type = "donchian_type"

##===========================================================================##
# TR_spec = [
#         ]
# @jitclass( base_spec + TR_spec)
# class TR(object):
#
#     def __init__(self, n_security):
#         self.moving_series = Moving_series(2, n_security)
#         self.ready = False
#         self.value = np.empty(n_security)
#
#     def on_data(self, high, low, close):
#         self.moving_series.on_data(close)
#
#         if not self.moving_series.init:
#             return
#
#         close_p = self.moving_series.value_matrix[-2]
#         self.ready = True
#         self.value = np.maximum(high-low,np.absolute(high-close_p),np.absolute(low-close_p))
#
# tr_type = deferred_type()
# tr_type.define(TR.class_type.instance_type)
#
#
#
# ##===========================================================================##
TR_spec = [
        ]
@jitclass( base_spec + TR_spec)
class TR(object):

    def __init__(self, n_security):
        self.moving_series = Moving_series(2, n_security)
        self.ready = False
        self.value = np.empty(n_security)

    def on_data(self, high, low, close):
        self.moving_series.on_data(close)

        if not self.moving_series.init:
            return

        close_p = self.moving_series.value_matrix[-2]
        self.ready = True
        self.value = np.maximum(high-low,np.absolute(high-close_p),np.absolute(low-close_p))
try:
    tr_type = TR.class_type.instance_type
except:
    tr_type = "tr_type"

# ##===========================================================================##
ATR_spec = [
         ('ma',ma_type),
         ('tr',tr_type),
        ]
@jitclass(base_spec + ATR_spec)

class ATR(object):

    def __init__(self, length, n_security):
        self.moving_series = Moving_series(length,n_security)
        self.ready = False
        self.value = np.zeros(n_security)
        self.ma = MA(length,n_security)
        self.tr = TR(n_security)

    def on_data(self, high, low, close):
        self.tr.on_data(high, low, close)
        if self.tr.ready:
            self.ma.on_data(self.tr.value)
        if self.ma.ready:
            self.ready = True
            self.value = self.ma.value
try:
    atr_type = ATR.class_type.instance_type
except:
    atr_type = "atr_type"

# ##===========================================================================##
# STARC_Bands_spec = [
#          ('atr',atr_type),
#          ('ma',ma_type),
#          ('upper',float64[:]),
#          ('lower',float64[:]),
#          ('middle',float64[:]),
#         ]
# @jitclass(base_spec + STARC_Bands_spec)
# class STARC_Bands(object):
#
#
#     def __init__(self,N_ATR, N_MA,n_security):
#         self.moving_series = Moving_series(N_ATR,n_security)
#         self.atr    = ATR(N_ATR,n_security)
#         self.ma     = MA(N_MA,n_security)
#         self.upper  = np.empty(n_security)
#         self.lower  = np.empty(n_security)
#         self.middle = np.empty(n_security)
#         self.ready = False
#
#     def on_data(self, high, low, close):
#         self.atr.on_data(high, low, close)
#         self.ma.on_data(close)
#         if self.ma.ready and self.atr.ready:
#             self.middle = self.ma.value
#             self.upper  = self.middle + 2*self.atr.value
#             self.lower  = self.middle - 2*self.atr.value
#             self.ready  = True
#
# starc_bands_type = deferred_type()
# starc_bands_type.define(STARC_Bands.class_type.instance_type)
#
# ##===========================================================================##
# Keltner_Channels_spec = [
#          ('atr',atr_type),
#          ('ema',ema_type),
#          ('upper',float64[:]),
#          ('lower',float64[:]),
#          ('middle',float64[:]),
#         ]
# @jitclass(base_spec + Keltner_Channels_spec)
# class Keltner_Channels(object):
#
#     def __init__(self,N_ATR, N_EMA,n_security):
#         self.moving_series = Moving_series(N_ATR,n_security)
#         self.atr    = ATR(N_ATR,n_security)
#         self.ema    = EMA(N_EMA,n_security)
#         self.upper  = np.empty(n_security)
#         self.lower  = np.empty(n_security)
#         self.middle = np.empty(n_security)
#         self.ready = False
#
#     def on_data(self, high, low, close):
#         self.atr.on_data(high, low, close)
#         self.ema.on_data(close)
#         if self.ema.ready and self.atr.ready: #写的好啊
#             self.middle = self.ema.value
#             self.upper  = self.middle + 2*self.atr.value
#             self.lower  = self.middle - 2*self.atr.value
#             self.ready  = True
#
# keltner_channels_type = deferred_type()
# keltner_channels_type.define(Keltner_Channels.class_type.instance_type)
#
# ##===========================================================================##
# ''' SAR_spec = [
#          ('unit_AF',float64),
#          ('AF',float64),
#          ('max_AF',float64),
#          ('EP',float64[:]),
#          ('SAR_value',float64[:]),
#          ('trend',boolean[:]),
#          ('prev_low',float64[:]),
#          ('prev_high',float64[:]),
#          ('n_security',int64),
#         ]
# @jitclass(base_spec + SAR_spec)
# class SAR(object):
#
#     def __init__(self, unit_AF, max_AF, high, low,n_security):
#         self.unit_AF   = unit_AF
#         self.max_AF    = max_AF
#         self.AF        = unit_AF
#         self.EP        = high
#         self.trend     = np.array([True for i in range(n_security)])
#         self.SAR_value = low
#
#         self.ready = False
#         self.value = np.empty(n_security)
#         self.n_security = n_security
#         self.prev_low = np.zeros(n_security)
#         self.prev_high = np.zeros(n_security)
#
#     def on_data(self, high, low):
#         for i in range(self.n_security):
#             if self.trend[i]:
#                 self.SAR_value[i]  =  self.SAR_value[i] + (self.AF * (self.EP[i] - self.SAR_value[i]))
#                 if low[i]    <  self.SAR_value[i]:
#                     self.trend[i] = False
#                     self.SAR_value[i]   = self.EP[i]
#                     self.EP[i]    = self.prev_low[i]
#                     self.AF    = self.unit_AF
#                 else:
#                     if high[i] > self.EP[i]:
#                         self.EP[i] = high[i]
#                         if self.AF <= (self.max_AF - self.unit_AF):
#                             self.AF += self.unit_AF
#             else:
#                 self.SAR_value[i] = self.SAR_value[i] - (self.AF * (self.SAR_value[i] - self.EP[i]))
#                 if high[i]  > self.SAR_value[i]:
#                     self.trend[i] = True
#                     self.SAR_value[i]   = self.EP[i]
#                     self.EP[i]    = self.prev_high[i]
#                     self.AF    = self.unit_AF
#                 else:
#                     if low[i] < self.EP[i]:
#                         # self.EP = self.prev_low
#                         self.EP[i] = low[i]
#                         if self.AF <= (self.max_AF - self.unit_AF):
#                             self.AF += self.unit_AF
#
# sar_type = deferred_type()
# sar_type.define(SAR.class_type.instance_type)
# ##===========================================================================##
# sar = SAR(0.02,0.2,np.ones(2),np.ones(2)*10,2)
# for i in range(1,30):
#     sar.on_data(np.ones(2)*i,np.ones(2)*(i-1)) '''
# ##===========================================================================##
# ##===========================================================================##
# ##===========================================================================##
ADX_spec = [
                ('ll', moving_series_type),
                ('hh', moving_series_type),
                ('P_DI', ma_type),
                ('N_DI', ma_type),
                ('atr', atr_type),
                ('adx', ema_type),
            ]
@jitclass(base_spec + ADX_spec)
class ADX(object):

    def __init__(self,length1, length2, n_security):
        self.ready = False
        self.value = np.zeros(n_security)

        self.hh = Moving_series(2, n_security)
        self.ll = Moving_series(2, n_security)
        self.P_DI = MA(length1, n_security)
        self.N_DI = MA(length1, n_security)
        self.atr = ATR(length1, n_security)
        self.adx = EMA(length2, n_security)

    def on_data(self, high, low, close):

        self.hh.on_data(high)
        self.ll.on_data(low)

        if not self.hh.init or not self.ll.init:
            return

        up_move = self.hh.value_matrix[-1] - self.hh.value_matrix[-2]
        down_move = self.ll.value_matrix[-2] - self.ll.value_matrix[-1]

        up_move[(up_move<=down_move)|(up_move<=0)] = 0
        down_move[(down_move<=up_move)|(down_move<=0)] = 0

        self.P_DI.on_data(up_move)
        self.N_DI.on_data(down_move)
        self.atr.on_data(high, low, close)

        if not self.atr.ready or not self.P_DI.ready or not self.N_DI.ready:
            return

        P_DI = 100 * self.P_DI.value/self.atr.value
        N_DI = 100 * self.N_DI.value/self.atr.value

        self.adx.on_data(np.absolute((P_DI-N_DI)/(P_DI+N_DI)))

        if not self.adx.ready:
            return
        self.ready = True
        self.value = 100*self.adx.value

try:
    adx_type = ADX.class_type.instance_type
except:
    adx_type = "adx_type"

# ################################################
# CMI_spec = [
#                 ('close_diff', diff_type),
#                 ('hh', high_type),
#                 ('ll', low_type),
#             ]
# @jitclass(base_spec + CMI_spec)
# class CMI(object):
#
#     def __init__(self, length, n_security):
#         self.ready = False
#         self.value = np.zeros(n_security)
#
#         self.closs_diff = DIFF(length, n_security)
#         self.hh = HIGH(length, n_security)
#         self.ll = LOW(length, n_security)
#
#     def on_data(self, high, low, close):
#
#         self.hh.on_data(high)
#         self.ll.on_data(low)
#         self.closs_diff.on_data(close)
#
#         if not self.hh.ready or not self.ll.ready or not self.closs_diff.ready:
#             return
#
#         close_diff = self.closs_diff.value
#         hh = self.hh.value
#         ll = self.ll.value
#         self.value = 100 * np.absolute(close_diff)/(hh - ll)
#
# cmi_type = deferred_type()
# cmi_type.define(CMI.class_type.instance_type)
#
# # class KD_strategy:
# # # if (Kt-1 < Dt-1 & Kt > Dt) & Dt < 50 , open long position
# # # if (Kt-1 > Dt-1 & Kt < Dt) & Dt > 50 , open short position
# # # if Dt > up_threshold, close long position
# # # if Dt < down_threshold, close short position
# # # if close long and open short at the same time, open short
# # # though such condition won't happen here
# #
# # # further improvement include divergence between D and price
# # # however, it maybe not be easy to write this into code
# #     def __init__(self, order_manager: OrderManager, strategy_utility: FX_utility):
# #         self.order_manager = order_manager
# #         self.strategy_utility = strategy_utility
# #         n_security = len(self.strategy_utility.symbol_batch)
# #         length1 = np.int64(self.strategy_utility.strategy_param[0])
# #         length2 = np.int64(self.strategy_utility.strategy_param[1])
# #         length3 = np.int64(self.strategy_utility.strategy_param[2])
# #         self.kd = KD(length1, length2, length3, n_security)
# #         self.trade_time = np.zeros(n_security)
# #         self.trade_price = np.zeros(n_security)
# #
# #     def on_data(self, window_data, time_data):
# #         break_threshold = np.float64(self.strategy_utility.strategy_param[3])
# #         stop_loss = np.float64(self.strategy_utility.strategy_param[4])
# #         stop_profit = np.float64(self.strategy_utility.strategy_param[5])
# #
# #         high_price = window_data[-1, :, 1]
# #         low_price = window_data[-1, :, 2]
# #         close_price = window_data[-1, :, 3]
# #         n_security = len(self.strategy_utility.symbol_batch)
# #
# #         prev_K_value = np.copy(self.kd.fast_d.value)
# #         prev_D_value = np.copy(self.kd.slow_d.value)
# #
# #         self.kd.on_data(high_price, low_price, close_price)
# #
# #         curr_K_value = self.kd.fast_d.value
# #         curr_D_value = self.kd.slow_d.value
# #         #### signals ####
# #         up_cross_condition = (prev_K_value < prev_D_value) * \
# #                              (curr_K_value >= curr_D_value)
# #         down_cross_condition = (prev_K_value > prev_D_value) * \
# #                                (curr_K_value <= curr_D_value)
# #         overbought_condition = curr_D_value >= 50 + break_threshold
# #         oversold_condition = curr_D_value <= 50 - break_threshold
# #
# #         #### position ####
# #         ccp_current = self.order_manager.ccp_position.copy()
# #
# #         open_long_condition = up_cross_condition * (curr_D_value <= 50)
# #         open_short_condition = down_cross_condition * (curr_D_value >= 50)
# #         close_long_condition = (ccp_current > 0) * overbought_condition
# #         close_short_condition = (ccp_current < 0) * oversold_condition
# #
# #         # stop loss
# #         current_price_level = np.ones_like(close_price)
# #         current_price_level[self.trade_price != 0] = np.divide(close_price, self.trade_price)[self.trade_price != 0]
# #         current_pnl = current_price_level - 1
# #         stop_loss_condition = ((ccp_current > 0.) * (current_pnl < -stop_loss)) | \
# #                               ((ccp_current < 0.) * (current_pnl > stop_loss))
# #         stop_profit_condition = ((ccp_current > 0.) * (current_pnl > stop_profit)) | \
# #                                 ((ccp_current < 0.) * (current_pnl < -stop_profit))
# #         close_condition = stop_loss_condition|stop_profit_condition|close_long_condition|close_short_condition
# #         close_condition = stop_loss_condition|close_long_condition|close_short_condition
# #
# #         #### update target position ####
# #         ccp_target = self.order_manager.ccp_position.copy()
# #         ccp_target[ccp_target > 0] = 1
# #         ccp_target[ccp_target < 0] = -1
# #
# #         if self.kd.ready:
# #             ccp_target[close_condition] = 0
# #             ccp_target[open_long_condition] = 1
# #             ccp_target[open_short_condition] = -1
# #
# #         weight = ccp_target / n_security
# #         ref_aum = self.order_manager.ref_aum
# #         self.strategy_utility.place_order(weight * ref_aum)
# #         self.strategy_utility.place_signal_remark(Dict.empty(string,string))
# #
# #         #### record trade has happened ####
# #         has_trade = (ccp_target != ccp_current)
# #         self.trade_time[has_trade] = time_data[-1][0]
# #         self.trade_price[has_trade] = close_price[has_trade]