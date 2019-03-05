import json
import pandas
import utils

interface={
    'dataframe':[
        #'last_delta',
        "market",
        'PDelta1',
        'PDelta5',
        'PDelta10',
        'PDelta20',
        'MA5',
        'MA10',
        'MA20',
        'VMA1',
        'VMA5',
        'VMA10',
        'VMA20',
        'VRI1',
        'VRI5',
        'VRI10',
        'VRI20',
    ]
}

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
    

def __PDeltaN(raw,N,name):
    def f(raw):
        df = raw['quotes']
        if df is None:
            print(raw)
        assert df is not None
        df[name] = (df.close/df.close.shift(N) - 1)[N:]*100
        raw['quotes'] = df
        return raw
    return f

def PDelta1(raw):
    fc_name=utils.get_func_name()
    return __PDeltaN(raw,1,fc_name)(raw)

def PDelta5(raw):
    fc_name=utils.get_func_name()
    return __PDeltaN(raw,5,fc_name)(raw)

def PDelta10(raw):
    fc_name=utils.get_func_name()
    return __PDeltaN(raw,10,fc_name)(raw)

def PDelta20(raw):
    fc_name=utils.get_func_name()
    return __PDeltaN(raw,20,fc_name)(raw)
    

def last_delta(raw):
    fc_name=utils.get_func_name()
    df = raw['quotes']
    df[fc_name] = (df.close/df.close.shift(1) - 1)[1:]*100
    raw['quotes'] = df
    return raw

def __MAN(raw,N,name):
    def f(raw):
        df = raw['quotes']
        df[name] = df['close'].rolling(N).mean()
        raw['quotes'] = df
        return raw
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

def __VMAN(raw,N,name):
    def f(raw):
        df = raw['quotes']
        if 'volume' in df.columns:
            df[name] = df['volume'].rolling(N).mean()
        if 'vol' in df.columns:
            df[name] = df['vol'].rolling(N).mean()
        raw['quotes'] = df
        return raw
    return f

def VMA1(raw):
    fc_name=utils.get_func_name()
    return __VMAN(raw,1,fc_name)(raw)


def VMA5(raw):
    fc_name=utils.get_func_name()
    return __VMAN(raw,5,fc_name)(raw)

def VMA10(raw):
    fc_name=utils.get_func_name()
    return __VMAN(raw,10,fc_name)(raw)

def VMA20(raw):
    fc_name=utils.get_func_name()
    return __VMAN(raw,20,fc_name)(raw)

def __VRIN(raw,N,name):
    def f(raw):
        df = raw['quotes']
        if 'volume' in df.columns:
            df[name] = df['volume']/df['VMA{}'.format(N)]
        if 'vol' in df.columns:
            df[name] = df['vol']/df['VMA{}'.format(N)]
        raw['quotes'] = df
        return raw
    return f

def VRI1(raw):
    fc_name=utils.get_func_name()
    return __VRIN(raw,1,fc_name)(raw)

def VRI5(raw):
    fc_name=utils.get_func_name()
    return __VRIN(raw,5,fc_name)(raw)

def VRI10(raw):
    fc_name=utils.get_func_name()
    return __VRIN(raw,10,fc_name)(raw)

def VRI20(raw):
    fc_name=utils.get_func_name()
    return __VRIN(raw,20,fc_name)(raw)
