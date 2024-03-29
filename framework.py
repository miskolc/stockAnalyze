import os
import shutil
import time
import datetime
import pandas as pd
from multiprocessing import Process,Queue,cpu_count 
from data_engine import DataEngine,load_data_config
from feature_extractor.core import get_extract_cache_path,load_extractor_config,extract_feature,flat_feature,extract_index_feature,extract_market_feature
import datetime
from functools import reduce
import tushare as ts 
import importlib
from analyzer.core import load_analyzer_config,prepare_data,split_cv,get_X,get_Y

import data_engine
import json
import utils

import gc

def do_extract_stock_feature(queue,config_file,cache_dir,index,start,end):
    config = load_extractor_config(config_file)
    print('start process {}...'.format(index))
    count = 0
    dfs = []
    engine = DataEngine()
    while not queue.empty():
        code = queue.get()
        print('process {} process stock {} total:{}'.format(index,code,count))
        #try:
        stock_df=engine.get_k_data(code,start=start,end=end)
        if stock_df is None or stock_df.shape[0]==0:
            print('stock {} no data between {} and {}'.format(code,start,end))
            continue
        stock_info = engine.get_stock_basics(code,start=start,end=end)
        if stock_info is not None:
            stock_df = stock_df.merge(stock_info,on=['date'],how='left')
        feature=extract_feature({'quotes':stock_df,'code':code},config)
        #except:
        #    continue
        flated = flat_feature(feature,config)
        dfs.append(flated)
        count = count + 1
    if len(dfs)>0:
        df = pd.concat(dfs,axis=0)
        df.to_pickle(os.path.join(cache_dir,'cache{}.pkl'.format(index)))
    print('end process {}... process stock:{}'.format(index,count))

def extract_stock_feature(data_config_file,feature_config_file,np=None,force=False):
    print('mp_extract_stock_feature start-{}'.format(datetime.datetime.now()))
    stock_num,index_pool,start_date,end_date = load_data_config(data_config_file)
    queue = Queue()
    dir_name = get_extract_cache_path('stock',stock_num,feature_config_file,start_date,end_date)
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
    print(os.listdir(dir_name))
    if os.listdir(dir_name):
        print('dir is not empty')
        if not force:
            print('extract file path:{} is not empty, wait 3s to check if the data is ok. You can remove the files in the directory,or pass argument force=True'.format(dir_name))
            time.sleep(3)
            return
    engine = DataEngine()
    stock_pool = engine.get_all_stocks() 
    if stock_num is not None:
        stock_pool = stock_pool[:stock_num]
    for code in stock_pool:
        queue.put(code)

    tasks = []
    n = np if np is not None else cpu_count()
    for i in range(n):
        pro = Process(target=do_extract_stock_feature, args=(queue,feature_config_file,dir_name,i,start_date,end_date))
        pro.start()
        tasks.append(pro)

    print('waiting for task complete')
    for task in tasks:
        task.join()
    print('tasks end')
    print('mp_extract_stock_feature end-{}'.format(datetime.datetime.now()))

def do_extract_market_feature(queue,config_file,cache_dir,index):
    config = load_extractor_config(config_file)
    print('start process {}...'.format(index))
    count = 0
    dfs = []
    engine = DataEngine()
    while not queue.empty():
        date = queue.get()
        print('process {} process stock {} total:{}'.format(index,date,count))
        #try:
        market_df=engine.get_market_data(date)
        if market_df is None or market_df.shape[0]==0:
            print('No market data at {}'.format(date))
            continue
        feature=extract_market_feature({'quotes':market_df,'date':date,'market':pd.DataFrame({'date':[date]})},config)
        #except:
        #    continue
        flated = feature['market']
        dfs.append(flated)
        count = count + 1
    if len(dfs)>0:
        df = pd.concat(dfs,axis=0)
        df.to_pickle(os.path.join(cache_dir,'cache{}.pkl'.format(index)))
    print('end process {}... process stock:{}'.format(index,count))

def market_extract_feature(data_config_file,feature_config_file,np=None,force=False):
    print('mp_extract_market_feature start-{}'.format(datetime.datetime.now()))
    print(data_config_file)
    stock_num,index_pool,start_date,end_date = load_data_config(data_config_file)
    dir_name = get_extract_cache_path('market',stock_num,feature_config_file,start_date,end_date)
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
    print(os.listdir(dir_name))
    if os.listdir(dir_name):
        print('dir is not empty')
        if not force:
            print('extract file path:{} is not empty, wait 3s to check if the data is ok. You can remove the files in the directory,or pass argument force=True'.format(dir_name))
            time.sleep(3)
            return
    queue = Queue()
    engine = DataEngine()
    dates = engine.get_trade_dates(utils.date_delta(start_date,1),end_date)
    for date in dates:
        queue.put(date)

    tasks = []
    n = np if np is not None else cpu_count()
    for i in range(n):
        pro = Process(target=do_extract_market_feature, args=(queue,feature_config_file,dir_name,i))
        pro.start()
        tasks.append(pro)

    print('waiting for task complete')
    for task in tasks:
        task.join()
    print('tasks end')
    print('mp_extract_market_feature end-{}'.format(datetime.datetime.now()))


def index_pool_extract_feature(data_config_file,feature_config_file):
    stock_num,index_pool,start,end= load_data_config(data_config_file)
    feature_config = load_extractor_config(feature_config_file)
    
    feature_dfs = []
    engine = DataEngine()
    for code in index_pool:
        index_df=engine.get_k_data(code,index=True,start=start,end=end)
        feature=extract_index_feature({'quotes':index_df,'code':code},feature_config)
        feature_dfs.append(feature)
    return feature_dfs

def load_market_feature(data_config_file,feature_config_file):
    stock_num,index_pool,start_date,end_date = load_data_config(data_config_file)
    dir_name = get_extract_cache_path('market',stock_num,feature_config_file,start_date,end_date)
    if not os.path.isdir(dir_name):
        print('extracted market feature not exist')
        return None
    cached_features = list(map(lambda x:os.path.join(dir_name,x),os.listdir(dir_name)))   
    feature_config = load_extractor_config(feature_config_file)
    def merge_cached(x,y):
        dfx = x if not isinstance(x,str) else pd.read_pickle(x) 
        dfy = y if not isinstance(y,str) else pd.read_pickle(y) 
        return pd.concat([dfx,dfy],axis=0)
       
    df = reduce(merge_cached,cached_features) 
    return df
        

def load_extracted_feature(data_config_file,feature_config_file):
    stock_num,index_pool,start_date,end_date = load_data_config(data_config_file)
    dir_name = get_extract_cache_path('stock',stock_num,feature_config_file,start_date,end_date)
    if not os.path.isdir(dir_name):
        print('extracted feature not exist')
        return None

    cached_features = list(map(lambda x:os.path.join(dir_name,x),os.listdir(dir_name)))   
    
    
    def merge_cached(x,y):
        dfx = x if not isinstance(x,str) else pd.read_pickle(x) 
        dfy = y if not isinstance(y,str) else pd.read_pickle(y) 
        return pd.concat([dfx,dfy],axis=0)
       
    df = reduce(merge_cached,cached_features) 


    utils.show_sys_mem('MERGE CACHED')
   
    print('df cached') 
    print(df.info(memory_usage='deep'))


    print('create index feature')
    index_pool_features = index_pool_extract_feature(data_config_file,feature_config_file) 
    for raw in index_pool_features:
        df = df.merge(raw['quotes'],on=['date'],how='inner')
    utils.show_sys_mem('MERGE INDEX')

    print('create market feature')
    market_extract_feature(data_config_file,feature_config_file)
    market_df = load_market_feature(data_config_file,feature_config_file)    
    df = df.merge(market_df,on=['date'],how='inner')
    del market_df
    utils.show_sys_mem('MERGE MARKET')

    print('drop na')
    MA_20 = list(filter(lambda x:x.find('MA_20'),df.columns))
    
    df.dropna(subset=[MA_20[0]],inplace=True)


    print('df merge index') 
    print(df.info(memory_usage='deep'))
    return df
        

def prepare_stock_data(code,data_config_file,feature_config_file,start,end):
    feature_config = load_extractor_config(feature_config_file)
    print('get k data {} {}'.format(start,end))
    engine = DataEngine()
    stock_df=engine.get_k_data(code,start=start,end=end)
    print(stock_df.date.min())
    print(stock_df.date.max())
    stock_info = engine.get_stock_basics(code,start=start,end=end)
    if stock_info is not None:
        stock_df = stock_df.merge(stock_info,on=['date'],how='left')
    feature=extract_feature({'quotes':stock_df,'code':code},feature_config)
    df = flat_feature(feature,feature_config)
    print(df.date.min())
    print(df.date.max())
    index_pool_features = index_pool_extract_feature({'start':start,'end':end},feature_config_file) 
    for raw in index_pool_features:
        df = df.merge(raw['quotes'],on=['date'],how='inner')
    print(df.date.min())
    print(df.date.max())
    return df


def create_analyzer(data_config_file,feature_config_file,analyzer_config_file,mod_name=None,base_path=None):   
    assert data_config_file is not None
    assert feature_config_file is not None
    assert analyzer_config_file is not None
    if base_path is None:
        base_path='./records'
    if not os.path.isdir(base_path):
        os.mkdir(base_path)
    if mod_name is None:
        mod_name=datetime.datetime.now().strftime('%y-%m-%d_%H:%M:%S')
    mod_path = os.path.join(base_path,mod_name)
    if not os.path.isdir(mod_path):
        os.mkdir(mod_path)
    if os.listdir(mod_path):
        print('the mod is exist')
        exit(0)
    
    analyzer_config = load_analyzer_config(analyzer_config_file)


    print(analyzer_config)
    assert analyzer_config.get('Y')
    assert analyzer_config.get('Y').get('lib')
    assert analyzer_config.get('Y').get('target')
    target_lib = importlib.import_module("analyzer.{}".format(analyzer_config.get('Y').get('lib')))
    target_func = getattr(target_lib,analyzer_config.get('Y').get('target'))

    assert analyzer_config.get('Analyzer')
    assert analyzer_config.get('Analyzer').get('lib')
    assert analyzer_config.get('Analyzer').get('name')
    assert analyzer_config.get('Analyzer').get('args')
    analyzer_lib=importlib.import_module("analyzer.model.{}".format(analyzer_config.get('Analyzer').get('lib')))
    XModel = getattr(analyzer_lib,analyzer_config.get('Analyzer').get('name'))
    mod_config = analyzer_config.get('Analyzer').get('args')
    
    #load data to traino
    utils.show_sys_mem('BEGIN')
    extract_stock_feature(data_config_file,feature_config_file,force=False)
    utils.show_sys_mem('EXTRACT FEATURE')
    
    df = load_extracted_feature(data_config_file,feature_config_file)
    utils.show_sys_mem('LOAD FEATURE')
    
    df = target_func(df,analyzer_config.get('Y').get('args'))
    utils.show_sys_mem('SET ANALYZER TARGET')

    #k = int(df.shape[0]/3)

    #k=int(k/2)

    #def sampling_k_elements(group):
        #if len(group) < k:
        #    return group
     #   return group.sample(k,replace=True)

    #df = df.groupby('Y_'+analyzer_config.get('Y').get('target')).apply(sampling_k_elements).reset_index(drop=True)
    #utils.show_sys_mem('BALLANCE DATA')
    #print(df.describe())
    #print(df.columns)
    print('df after count target') 
    print(df.info(memory_usage='deep'))
    df = prepare_data(df,analyzer_config)
    print('df after prepare data') 
    print(df.info(memory_usage='deep'))
    utils.show_sys_mem('PREPARE DATA')
    _train_df,_test_df = split_cv(df,analyzer_config)
    print('train:{},test:{}'.format(_train_df.shape,_test_df.shape))
    del df
    utils.show_sys_mem('SPLIT CV')
    k = int(_train_df.shape[0]/3)

    k=int(k/2)

    def sampling_k_elements(group):
        #if len(group) < k:
        #    return group
        return group.sample(k,replace=True)

    _train_df = _train_df.groupby('Y_'+analyzer_config.get('Y').get('target')).apply(sampling_k_elements).reset_index(drop=True)
    utils.show_sys_mem('BALLANCE DATA')
    train_df = _train_df
    #train_df = _train_df.head(int(_train_df.shape[0]))
    #train_df = _train_df
    test_df = _test_df
    print('train df') 
    print(train_df.info(memory_usage='deep'))
    print('test df') 
    print(test_df.info(memory_usage='deep'))
    #del _train_df
    train_X = get_X(train_df,analyzer_config)
    train_Y = get_Y(train_df,analyzer_config)
    test_X = get_X(test_df,analyzer_config)
    test_Y = get_Y(test_df,analyzer_config)
    del train_df
    del test_df 
    utils.show_sys_mem('GET XY')
    print('train X') 
    print(train_X.info(memory_usage='deep'))
    print('test X') 
    print(test_X.info(memory_usage='deep'))
    model = XModel(mod_path)
    model.train(mod_config,train_X,train_Y,test_X,test_Y)
    utils.show_sys_mem('AFTER TRAIN')

    shutil.copyfile(data_config_file,os.path.join(mod_path,'data.json'))
    shutil.copyfile(feature_config_file,os.path.join(mod_path,'feature.json'))
    shutil.copyfile(analyzer_config_file,os.path.join(mod_path,'analyzer.json'))
    
    return model


def load_analyzer_record(path):
    data_config_file = os.path.join(path,'data.json')
    feature_config_file = os.path.join(path,'feature.json')
    analyzer_config_file = os.path.join(path,'analyzer.json')
    mod_path = os.path.join(path,'mod.bin')
    assert os.path.exists(data_config_file)
    assert os.path.exists(feature_config_file)
    assert os.path.exists(analyzer_config_file)
    assert os.path.exists(mod_path)

    
    analyzer_config = load_analyzer_config(analyzer_config_file)
    analyzer_lib=importlib.import_module("analyzer.model.{}".format(analyzer_config.get('Analyzer').get('lib')))
    XModel = getattr(analyzer_lib,analyzer_config.get('Analyzer').get('name'))
    
    model = XModel(path)
    model.load()
    return {
        "data_config_file":data_config_file,
        "feature_config_file":feature_config_file,
        "analyzer_config_file":analyzer_config_file,
        "model":model
    }

def use_analyzer_on_stock_of_the_date(code,analyzer,date):
    print(date)
    start = utils.date_delta(date,-200)
    df = prepare_stock_data(code,analyzer.get('data_config_file'),analyzer.get('feature_config_file'),start,date)
    print(df)
    df = df[(df.date==date)|(df.date==utils.format_date_ts_pro(date))]
    print(len(list(df.columns)))
    
    analyzer_config = load_analyzer_config(analyzer.get('analyzer_config_file'))
    X = get_X(df,analyzer_config)
    mod_features = analyzer.get('model').feature_names
    print('parse columns:{},model columns:{}'.format(len(list(X.columns)),len(mod_features)))
    print(X.shape)
    y = analyzer.get('model').predict(X)
    return (df,y)

def use_analyzer_on_stock(code,analyzer,start,end):
    print(start)
    print(end)
    df = prepare_stock_data(code,analyzer.get('data_config_file'),analyzer.get('feature_config_file'),start,end)
    analyzer_config = load_analyzer_config(analyzer.get('analyzer_config_file'))
    X = get_X(df,analyzer_config)
    y = analyzer.get('model').predict(X)
    return (df,y)


def use_analyzer_on_the_date(analyzer,date):
    feature_config_file = analyzer.get('feature_config_file')
    analyzer_config = load_analyzer_config(analyzer.get('analyzer_config_file'))
    start_date = analyzer_config.get('data').get('validate_start') if analyzer_config.get('data').get('validate_start') else analyzer_config.get('data').get('cv_date')
    data_config = {
        "start":start_date,
        "end":date
    }
    print('use_analyzer_on_stock start_date:{},end_date:{}'.format(start_date,date))
    df = load_extracted_feature(data_config,feature_config_file)
    if df is None:
        extract_stock_feature(data_config,feature_config_file)
        df = load_extracted_feature(data_config,feature_config_file)
    #print(df)
    df=df[df.date==date]
    X = get_X(df,analyzer_config)
    print(X.shape)
    y = analyzer.get('model').predict(X)
    nclass = set(list(y))
    summary = {}
    for k in nclass:
        summary[str(int(k))] = list(df[y==k]['code'])
    return (summary,df,y)

if __name__=="__main__":
    pd.set_option('display.max_columns', None)
    data_config_file =  "configs/data_config/300.json"
    feature_config_file = "configs/feature_config/test_config.json"
    analyzer_config_file = "configs/analyzer_config/simple.json"
    start='2016-01-01'
    end='2018-01-01'
    code='000651.SZ'
    market_extract_feature(data_config_file,feature_config_file)
    df = load_market_feature(data_config_file,feature_config_file)
    print(df)
    exit(0)
    engine = DataEngine()
    stock_df=engine.get_k_data(code,start=start,end=end)
    feature_config = load_extractor_config(feature_config_file)
    stock_info = engine.get_stock_basics(code,start=start,end=end)
    if stock_info is not None:
        stock_df = stock_df.merge(stock_info,on=['date'],how='left')
    feature=extract_feature({'quotes':stock_df,'code':code},feature_config)
    print(list(feature['quotes'].columns))
    print(feature['quotes'])

