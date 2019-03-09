import talib
import pandas as pd
import numpy as np
import json
import utils
import sys

#def CDL2CROWS(raw):
#    fc_name=utils.get_func_name()
#    df = raw['quotes']
#    df[fc_name] = talib.CDL2CROWS(df.open, df.high, df.low, df.close)
#    raw['quotes'] = df
#    return raw


def __TAPattern(name):
    def f(raw):
        df = raw['quotes']
        try:
            df.loc[:,name] = ta.EMA(getattr(talib,name)(df.open, df.high, df.low, df.close)/100,5).astype('float16')
            #df.loc[:,name] = df[name].astype('int16')
        except:
            #print(df)
            #df.loc[:,name]=np.nan
            pass
        #raw['quotes'] = df
        return raw
    return f


#def CDL3BLACKCROWS(raw):
#    fc_name=utils.get_func_name()
#    return __TAPattern(fc_name)(raw)

TAPatterns = {
    "CDL2CROWS":"Two Crows",
    "CDL3BLACKCROWS":"Three Black Crows",
    "CDL3INSIDE":"Three Inside Up/Down", 
    "CDL3LINESTRIKE":"Three-Line Strike",
    "CDL3OUTSIDE":"Three Outside Up/Down",
    "CDL3STARSINSOUTH":"Three Stars In The South",
    "CDL3WHITESOLDIERS":"Three Advancing White Soldiers",
    "CDLABANDONEDBABY":"Abandoned Baby",
    "CDLADVANCEBLOCK":"Advance Block",
    "CDLBELTHOLD":"Belt-hold",
    "CDLBREAKAWAY":"Breakaway",
    "CDLCLOSINGMARUBOZU":"Closing Marubozu",
    "CDLCONCEALBABYSWALL":"Concealing Baby Swallow",
    "CDLCOUNTERATTACK":"Counterattack",
    "CDLDARKCLOUDCOVER":"Dark Cloud Cover",
    "CDLDOJI":"Doji",
    "CDLDOJISTAR":"Doji Star",
    "CDLDRAGONFLYDOJI":"Dragonfly Doji",
    "CDLENGULFING":"Engulfing Pattern",
    "CDLEVENINGDOJISTAR":"Evening Doji Star",
    "CDLEVENINGSTAR":"Evening Star",
    "CDLGAPSIDESIDEWHITE":"Up/Down-gap side-by-side white lines",
    "CDLGRAVESTONEDOJI":"Gravestone Doji",
    "CDLHAMMER":"Hammer",
    "CDLHANGINGMAN":"Hanging Man",
    "CDLHARAMI":"Harami Pattern",
    "CDLHARAMICROSS":"Harami Cross Pattern",
    "CDLHIGHWAVE":"High-Wave Candle",
    "CDLHIKKAKE":"Hikkake Pattern",
    "CDLHIKKAKEMOD":"Modified Hikkake Pattern",
    "CDLHOMINGPIGEON":"Homing Pigeon",
    "CDLIDENTICAL3CROWS":"Identical Three Crows",
    "CDLINNECK":"In-Neck Pattern",
    "CDLINVERTEDHAMMER":"Inverted Hammer",
    "CDLKICKING":"Kicking",
    "CDLKICKINGBYLENGTH":"Kicking - bull/bear determined by the longer marubozu",
    "CDLLADDERBOTTOM":"Ladder Bottom",
    "CDLLONGLEGGEDDOJI":"Long Legged Doji",
    "CDLLONGLINE":"Long Line Candle",
    "CDLMARUBOZU":"Marubozu",
    "CDLMATCHINGLOW":"Matching Low",
    "CDLMATHOLD":"Mat Hold",
    "CDLMORNINGDOJISTAR":"Morning Doji Star",
    "CDLMORNINGSTAR":"Morning Star",
    "CDLONNECK":"On-Neck Pattern",
    "CDLPIERCING":"Piercing Pattern",
    "CDLRICKSHAWMAN":"Rickshaw Man",
    "CDLRISEFALL3METHODS":"Rising/Falling Three Methods",
    "CDLSEPARATINGLINES":"Separating Lines",
    "CDLSHOOTINGSTAR":"Shooting Star",
    "CDLSHORTLINE":"Short Line Candle",
    "CDLSPINNINGTOP":"Spinning Top",
    "CDLSTALLEDPATTERN":"Stalled Pattern",
    "CDLSTICKSANDWICH":"Stick Sandwich",
    "CDLTAKURI":"Takuri (Dragonfly Doji with very long lower shadow)",
    "CDLTASUKIGAP":"Tasuki Gap",
    "CDLTHRUSTING":"Thrusting Pattern",
    "CDLTRISTAR":"Tristar Pattern",
    "CDLUNIQUE3RIVER":"Unique 3 River",
    "CDLUPSIDEGAP2CROWS":"Upside Gap Two Crows",
    "CDLXSIDEGAP3METHODS":"Upside/Downside Gap Three Methods"
}

interface={
    'dataframe':[] + list(TAPatterns.keys())
}

def init(current_module):
    global interface
    print(current_module)
    for func_name in TAPatterns.keys():
        setattr(current_module,func_name,__TAPattern(func_name))
