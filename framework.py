import os
import pandas as pd
from multiprocessing import Process,Queue 
from data_engine import get_k_data,load_data_config
from feature_extractor.core import get_extract_cache_path,load_extractor_config,extract_feature,flat_feature
import datetime

def process_extract_feature(queue,config_file,cache_dir,index,start,end):
    config = load_extractor_config(config_file)
    print('start process {}...'.format(index))
    count = 0
    dfs = []
    while not queue.empty():
        code = queue.get()
        print('process {} process stock {} total:{}'.format(index,code,count))
        stock_df=get_k_data(code,start=start,end=end)
        feature=extract_feature({'stock':stock_df},config)
        flated = flat_feature(feature,config)
        dfs.append(flated)
        count = count + 1
    df = pd.concat(dfs,axis=0)
    df.to_pickle(os.path.join(cache_dir,'cache{}.pkl'.format(index)))
    print('end process {}... process stock:{}'.format(index,count))

def mp_extract_stock_feature(data_config_file,feature_config_file,n=24,cache_path='/tmp/features',force=False):
    print('mp_extract_stock_feature start-{}'.format(datetime.datetime.now()))
    stock_pool,index_pool,start_date,end_date = load_data_config(data_config_file)
    queue = Queue()
    for code in stock_pool:
        queue.put(code)
    dir_name = get_extract_cache_path(cache_path,stock_pool,feature_config_file,start_date,end_date)
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
    print(os.listdir(dir_name))
    if os.listdir(dir_name):
        print('dir is not empty')
        if not force:
            print('extract file path:{} is not empty,please remove the files in the directory,or pass argument force=True'.format(dir_name))
            exit(0)

    tasks = []
    for i in range(n):
        pro = Process(target=process_extract_feature, args=(queue,feature_config_file,dir_name,i,start_date,end_date))
        pro.start()
        tasks.append(pro)

    print('waiting for task complete')
    for task in tasks:
        task.join()
    print('tasks end')
    print('mp_extract_stock_feature end-{}'.format(datetime.datetime.now()))

def load_extracted_feature(config_file):
    return
        
    
