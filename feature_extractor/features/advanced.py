import json
import pandas
import utils

interface={
    'dataframe':[
        "open_next",
        "next_open_p"
    ]
}

def open_next(raw):
    fc_name=utils.get_func_name()
    df = raw['quotes']
    df.loc[:,fc_name] = df.open.shift(-1)
    raw['quotes'] = df
    return raw

    
def next_open_p(raw):
    fc_name=utils.get_func_name()
    df = raw['quotes']
    df.loc[:,fc_name] = ((df.open_next/df.close - 1)*100).astype('float16')
    raw['quotes'] = df
    return raw
