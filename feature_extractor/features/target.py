import json
import pandas as pd
import numpy as np
import utils

interface={
    'dataframe':[
    ]
}
    


def __max_drawdownN(N,name):
    def f(raw):
        df = raw['quotes']
        #df[name] = df['low'].rolling_min(-N)
        #open_next = df.open.shfit(-1)
        df.loc[:,name] = ((df.open_next - df['low'].rolling(N).min())/df.open_next*100).astype('float16')
        df.loc[:,name] = np.where(df[name]<0,0,df[name])
        #df[name] = (pd.rolling_min(df['low'],N)-df.close)/df.close*100
        raw['quotes'] = df 
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
        df.loc[:,name] = ((df['close'].rolling(N).mean()-df.open_next)/df.open_next*100).astype('float16')
        #df[name] = (pd.rolling_mean(df['close'],N)-df.close)/df.close*100
        raw['quotes'] = df 
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

def __MAXDRAWDOWN(name,field,N):
    def f(raw):
        if field in raw['quotes'].columns:
            raw['quotes'].loc[:,name] = ((raw['quotes'][field] - raw['quotes']['low'].rolling(N).min())/raw['quotes'][field]*100).astype('float16')
            raw['quotes'].loc[:,name] = np.where(raw['quotes'][name]<0,0,raw['quotes'][name])
        return raw
    return f

def __PMA(name,field,N):
    def f(raw):
        if field in raw['quotes'].columns:
            raw['quotes'].loc[:,name] = ((raw['quotes']['close'].rolling(N).mean()-raw['quotes'][field])/raw['quotes'][field]*100).astype('float16')
        return raw
    return f

def __PDELTA(name,field,N):
    def f(raw):
        if field in raw['quotes'].columns:
            raw['quotes'].loc[:,name] = ((raw['quotes']['close'].shift(N)-raw['quotes'][field])/raw['quotes'][field]*100).astype('float16')
        return raw
    return f

FUNC_TABLE ={
    "MAXDRAWDOWN":__MAXDRAWDOWN,
    "PMA":__PMA,
    "PDELTA":__PDELTA
}

def init(current_module):
    global interface
    print(current_module)
    
    future_benchmark = [
        ('MAXDRAWDOWN','close',(5,10,20,50,100)),
        ('MAXDRAWDOWN','open_next',(5,10,20,50,100)),
        ('PMA','close',(5,10,20,50,100)),
        ('PMA','open_next',(5,10,20,50,100)),
        ('PDELTA','close',(5,10,20,50,100)),
        ('PDELTA','open_next',(5,10,20,50,100))
    ]


    for benchmark in future_benchmark:
        func,field,period = benchmark
        for i in period:
            feature = 'Y_{}_{}_{}'.format(func,field,i)
            interface['dataframe'].append(feature)
            setattr(current_module,feature,FUNC_TABLE[func](feature,field,i))
