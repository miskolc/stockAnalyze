import json
import pandas
import utils

interface={
    'dataframe':[
        'last_delta',
        'MA5',
        'MA10',
        'MA20'
    ]
}
    

def last_delta(raw):
    fc_name=utils.get_func_name()
    df = raw['stock']
    df[fc_name] = (df.close/df.close.shift(1) - 1)[1:]*100
    return df

def __MAN(raw,N,name):
    def f(raw):
        df = raw['stock']
        df[name] = df['close'].rolling(N).mean()
        return df
    return f


def MA5(raw):
    fc_name=utils.get_func_name()
    return __MAN(raw,5,fc_name)(raw)

def MA10(raw):
    fc_name=utils.get_func_name()
    return __MAN(raw,10,fc_name)(raw)

def MA20(raw):
    fc_name=utils.get_func_name()
    return __MAN(raw,20,fc_name)(raw)
