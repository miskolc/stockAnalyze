import json
import pandas as pd
import utils

interface={
    'dataframe':[
        'range_percent',
    ]
}


def range_percent(raw):
    df = raw['quotes']
    market = raw['market']
    bins = pd.cut(df['pct_chg'],[-10,-9.5,-7,-5,-3,0,3,5,7,9.5,10])
    d = dict(pd.value_counts(bins)/df.shape[0]*100)
    for k,v in d.items():
        market.loc[:str(k)]=v
    raw['market']=market
    return raw
        
    

    
    

