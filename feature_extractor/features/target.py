import json
import pandas as pd
import numpy as np
import utils

interface={
    'dataframe':[
        'mdw5',
        'mdw10',
        'mdw20',
        'pma5',
        'pma10',
        'pma20'
    ]
}
    


def __max_drawdownN(N,name):
    def f(raw):
        df = raw['quotes']
        #df[name] = df['low'].rolling_min(-N)
        #open_next = df.open.shfit(-1)
        df.loc[:,name] = (df.open_next - df['low'].rolling(N).min())/df.open_next*100
        df.loc[:,name] = np.where(df[name]<0,0,df[name])
        #df[name] = (pd.rolling_min(df['low'],N)-df.close)/df.close*100
        #raw['quotes'] = df 
        return raw
    return f


def mdw5(raw):
    fc_name=utils.get_func_name()
    return __max_drawdownN(5,'Y_'+fc_name)(raw)

def mdw10(raw):
    fc_name=utils.get_func_name()
    return __max_drawdownN(10,'Y_'+fc_name)(raw)

def mdw20(raw):
    fc_name=utils.get_func_name()
    return __max_drawdownN(20,'Y_'+fc_name)(raw)

def __profit_maN(N,name):
    def f(raw):
        df = raw['quotes']
        #open_next = df.open.shfit(-1)
        #df[name] = df['low'].rolling_min(-N)
        df.loc[:,name] = (df['close'].rolling(N).mean()-df.open_next)/df.open_next*100
        #df[name] = (pd.rolling_mean(df['close'],N)-df.close)/df.close*100
        #raw['quotes'] = df 
        return raw
    return f

def pma5(raw):
    fc_name=utils.get_func_name()
    return __profit_maN(5,'Y_'+fc_name)(raw)

def pma10(raw):
    fc_name=utils.get_func_name()
    return __profit_maN(10,'Y_'+fc_name)(raw)

def pma20(raw):
    fc_name=utils.get_func_name()
    return __profit_maN(20,'Y_'+fc_name)(raw)
