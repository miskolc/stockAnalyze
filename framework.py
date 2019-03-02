import os
import shutil
import time
import datetime
import pandas as pd
from multiprocessing import Process,Queue,cpu_count 
from data_engine import DataEngine,load_data_config
from feature_extractor.core import get_extract_cache_path,load_extractor_config,extract_feature,flat_feature,extract_index_feature
import datetime
from functools import reduce
import tushare as ts 
import importlib
from analyzer.core import load_analyzer_config,prepare_data,split_cv,get_X,get_Y

import data_engine
import json


def do_extract_feature(queue,config_file,cache_dir,index,start,end):
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
        feature=extract_feature({'quotes':stock_df,'code':code},config)
        #except:
        #    continue
        flated = flat_feature(feature,config)
        dfs.append(flated)
        count = count + 1
    df = pd.concat(dfs,axis=0)
    df.to_pickle(os.path.join(cache_dir,'cache{}.pkl'.format(index)))
    print('end process {}... process stock:{}'.format(index,count))

def extract_stock_feature(data_config_file,feature_config_file,np=None,force=False):
    print('mp_extract_stock_feature start-{}'.format(datetime.datetime.now()))
    stock_num,index_pool,start_date,end_date = load_data_config(data_config_file)
    queue = Queue()
    dir_name = get_extract_cache_path(stock_num,feature_config_file,start_date,end_date)
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
    print(os.listdir(dir_name))
    if os.listdir(dir_name):
        print('dir is not empty')
        if not force:
            print('extract file path:{} is not empty, wait 3s to check if the data is ok. You can remove the files in the directory,or pass argument force=True'.format(dir_name))
            time.sleep(3)
            return
    stocks = ts.get_stock_basics() 
    stock_pool = list(stocks.index)
    if stock_num is not None:
        stock_pool = stock_pool[:stock_num]
    for code in stock_pool:
        queue.put(code)

    tasks = []
    n = np if np is not None else cpu_count()
    for i in range(n):
        pro = Process(target=do_extract_feature, args=(queue,feature_config_file,dir_name,i,start_date,end_date))
        pro.start()
        tasks.append(pro)

    print('waiting for task complete')
    for task in tasks:
        task.join()
    print('tasks end')
    print('mp_extract_stock_feature end-{}'.format(datetime.datetime.now()))


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
        

def load_extracted_feature(data_config_file,feature_config_file):
    stock_num,index_pool,start_date,end_date = load_data_config(data_config_file)
    dir_name = get_extract_cache_path(stock_num,feature_config_file,start_date,end_date)
    if not os.path.isdir(dir_name):
        print('extracted feature not exist')
        return None

    cached_features = list(map(lambda x:os.path.join(dir_name,x),os.listdir(dir_name)))   
    
    
    def merge_cached(x,y):
        dfx = x if not isinstance(x,str) else pd.read_pickle(x) 
        dfy = y if not isinstance(y,str) else pd.read_pickle(y) 
        return pd.concat([dfx,dfy],axis=0)
       
    df = reduce(merge_cached,cached_features) 

    print('create index feature')
    index_pool_features = index_pool_extract_feature(data_config_file,feature_config_file) 
    for raw in index_pool_features:
        df = df.merge(raw['quotes'],on=['date'],how='inner')
    return df
        

def prepare_stock_data(code,data_config_file,feature_config_file,start,end):
    feature_config = load_extractor_config(feature_config_file)
    print('get k data {} {}'.format(start,end))
    engine = DataEngine()
    stock_df=engine.get_k_data(code,start=start,end=end)
    print(stock_df.date.min())
    print(stock_df.date.max())
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
    
    #load data to train
    extract_stock_feature(data_config_file,feature_config_file,force=False)
    df = load_extracted_feature(data_config_file,feature_config_file)
    
    df = target_func(df,analyzer_config.get('Y').get('args'))
    print(df.describe())
    print(df.columns)
    df = prepare_data(df,analyzer_config)
    train_df,test_df = split_cv(df,analyzer_config)
    print('train:{},test:{}'.format(train_df.shape,test_df.shape))
    train_X = get_X(train_df,analyzer_config)
    train_Y = get_Y(train_df,analyzer_config)
    test_X = get_X(test_df,analyzer_config)
    test_Y = get_Y(test_df,analyzer_config)
    model = XModel(mod_path)
    model.train(mod_config,train_X,train_Y,test_X,test_Y)

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
