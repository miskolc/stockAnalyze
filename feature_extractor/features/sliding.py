import json
import pandas
import utils

interface={
    'dataframe':[
    ]
}


def field_slidingN(field,N):
    global interface
    def f(raw):
        df = raw['quotes']
        if field in raw['quotes'].columns:
            for i in range(1,N):
                feature = 'window_{}_{}'.format(field,i)
                df.loc[:,feature]=df[field].shift(i)
            raw['quotes']=df
        return raw

    return f

def init(current_module):
    global interface
    print(current_module)

    targets = [
        ('close',100),
        ('open',100),
        ('vol',100),
        ('volume',100),
        ('pct_chg',20),
        ('next_open_p',20)
    ]
    
    for target in targets:
        field,window = target
        func_name='{}_sliding_{}'.format(field,window)
        interface['dataframe'].append(func_name)
        setattr(current_module,func_name,field_slidingN(field,window))
