import json
import pandas
import utils

interface={
    'dataframe':[
    ]
}

def __XDELTAN(name,field,N):
    def f(raw):
        if field in raw['quotes'].columns:
            raw['quotes'].loc[:,name] = ((raw['quotes'][field]/raw['quotes'][field].shift(N) - 1)*100).astype('float16')
        return raw
    return f

def __XMAN(name,field,N):
    def f(raw):
        if field in raw['quotes'].columns:
            raw['quotes'].loc[:,name] = (raw['quotes'][field].rolling(N).mean()/raw['quotes'][field]*100).astype('float16')
        return raw
    return f

def __XMINN(name,field,N):
    def f(raw):
        if field in raw['quotes'].columns:
            raw['quotes'].loc[:,name] = (raw['quotes'][field].rolling(N).min()/raw['quotes'][field]*100).astype('float16')
        return raw
    return f

def __XMAXN(name,field,N):
    def f(raw):
        if field in raw['quotes'].columns:
            raw['quotes'].loc[:,name] = (raw['quotes'][field].rolling(N).max()/raw['quotes'][field]*100).astype('float16')
        return raw
    return f


FUNC_TABLE ={
    "DELTA":__XDELTAN,
    "MA":__XMAN,
    "MIN":__XMINN,
    "MAX":__XMAXN,
}

def init(current_module):
    global interface
    print(current_module)

    targets = [
        ('close','DELTA',(1,5,10,20,50,100,200)),
        ('close','MA',(1,5,10,20,50,100,200)),
        ('close','MIN',(1,5,10,20,50,100,200)),
        ('close','MAX',(1,5,10,20,50,100,200)),
        ('vol','MA',(1,5,10,20,50,100,200)),
        ('vol','MIN',(1,5,10,20,50,100,200)),
        ('vol','MAX',(1,5,10,20,50,100,200)),
        ('volume','MA',(1,5,10,20,50,100,200)),
        ('volume','MIN',(1,5,10,20,50,100,200)),
        ('volume','MAX',(1,5,10,20,50,100,200)),
        ('range','MA',(1,2,3,4,5,10,20)),
        ('range','MIN',(1,2,3,4,5,10,20)),
        ('range','MAX',(1,2,3,4,5,10,20)),
        ('turnover_rate','MA',(1,2,3,4,5,10,20)),
        ('turnover_rate_f','MA',(1,2,3,4,5,10,20)),
        ('volume_ratio','MA',(1,2,3,4,5,10,20)),
    ]
    
    for target in targets:
        field,func,period = target
        for i in period:
            feature = '{}_{}_{}'.format(field,func,i)
            interface['dataframe'].append(feature)
            setattr(current_module,feature,FUNC_TABLE[func](feature,field,i))
