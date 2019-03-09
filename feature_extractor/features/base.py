import json
import pandas
import utils

interface={
    'dataframe':[
        #'last_delta',
        "range",
        "market",
    ]
}

def range(raw):
    fc_name=utils.get_func_name()
    df = raw['quotes']
    df.loc[:,fc_name] = ((df.high -df.low)/df.open*100).astype('float16')
    raw['quotes'] = df
    return raw

def market(raw):
    fc_name=utils.get_func_name()
    code = raw['code']
    # sh:1 sz:2 cy:3
    if code.startswith('60'):
        market = 1
    if code.startswith('00'):
        market = 2
    if code.startswith('30'):
        market = 3
    try:
        raw['quotes'][fc_name] = market 
    except:
        print(code)
    return raw
    
    

