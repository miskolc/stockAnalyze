import json
import pandas as pd
import utils

interface={
    'dataframe':[
        'range_percent',
        'market_pe',
        'market_pb'
    ]
}


def range_percent(raw):
    df = raw['quotes']
    market = raw['market']
    bins = pd.cut(df['pct_chg'],[-100,-9.5,-7,-5,-3,0,3,5,7,9.5,200])
    d = dict(pd.value_counts(bins)/df.shape[0]*100)
    for k,v in d.items():
        market.loc[:,'market'+str(k).replace('(','|').replace(',','_').replace(']','|')]=v
    bins = pd.cut(df['pct_chg'],[-200,0,200])
    d = dict(pd.value_counts(bins)/df.shape[0]*100)
    for k,v in d.items():
        market.loc[:,'market'+str(k).replace('(','|').replace(',','_').replace(']','|')]=v
    raw['market']=market
    return raw
        
    

    
def market_pe(raw):
    df = raw['quotes']
    market = raw['market']
    market.loc[:,'pe_median'] = df.pe.median()
    market.loc[:,'pe_min'] = df.pe.min()
    market.loc[:,'pe_max'] = df.pe.max()
    raw['market']=market
    return raw
    

def market_pb(raw):
    df = raw['quotes']
    market = raw['market']
    market.loc[:,'pb_median'] = df.pb.median()
    market.loc[:,'pb_min'] = df.pb.min()
    market.loc[:,'pb_max'] = df.pb.max()
    raw['market']=market
    return raw
