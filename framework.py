import os
import time
import pandas as pd
from multiprocessing import Process,Queue,cpu_count 
from data_engine import get_k_data,load_data_config
from feature_extractor.core import get_extract_cache_path,load_extractor_config,extract_feature,flat_feature,extract_index_feature
import datetime
from functools import reduce
import tushare as ts 


def do_extract_feature(queue,config_file,cache_dir,index,start,end):
    config = load_extractor_config(config_file)
    print('start process {}...'.format(index))
    count = 0
    dfs = []
    while not queue.empty():
        code = queue.get()
        print('process {} process stock {} total:{}'.format(index,code,count))
        stock_df=get_k_data(code,start=start,end=end)
        feature=extract_feature({'quotes':stock_df,'code':code},config)
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
    for code in index_pool:
        index_df=get_k_data(code,index=True,start=start,end=end)
        feature=extract_index_feature({'quotes':index_df,'code':code},feature_config)
        feature_dfs.append(feature)
    return feature_dfs
        

def load_extracted_feature(data_config_file,feature_config_file,np=None):
    stock_num,index_pool,start_date,end_date = load_data_config(data_config_file)
    dir_name = get_extract_cache_path(stock_num,feature_config_file,start_date,end_date)
    if not os.path.isdir(dir_name):
        print('extracted feature not exist')
        exit(0)

    cached_features = list(map(lambda x:os.path.join(dir_name,x),os.listdir(dir_name)))   
    
    
    def merge_cached(x,y):
        dfx = x if not isinstance(x,str) else pd.read_pickle(x) 
        dfy = y if not isinstance(y,str) else pd.read_pickle(y) 
        return pd.concat([dfx,dfy],axis=0)
       
    df = reduce(merge_cached,cached_features) 

    print('create index feature')
    index_pool_features = index_pool_extract_feature(data_config_file,feature_config_file) 
    for raw in index_pool_features:
        df = df.merge(raw['quotes'],on=['date'],how='outer')
    return df
        

   
