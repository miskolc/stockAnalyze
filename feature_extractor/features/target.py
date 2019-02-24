import json
import pandas as pd
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
        df = raw['stock']
        #df[name] = df['low'].rolling_min(-N)
        df[name] = (df['low'].rolling(N).mean()-df.close)/df.close*100
        #df[name] = (pd.rolling_min(df['low'],N)-df.close)/df.close*100
        return df
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
        df = raw['stock']
        #df[name] = df['low'].rolling_min(-N)
        df[name] = (df['close'].rolling(N).mean()-df.close)/df.close*100
        #df[name] = (pd.rolling_mean(df['close'],N)-df.close)/df.close*100
        return df
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
