import os
import json
import utils
import importlib
from multiprocessing import Process
#import feature_extractor.features.base as base

cache_path='/tmp/features'

def load_extractor_config(filename):
    config_json = json.load(open(filename))
    #print('load config file:{}'.format(filename))
    #print(config_json)
    assert config_json.get("stock")
    assert config_json.get("index")
    assert config_json.get("stock").get("X")
    assert config_json.get("index").get("Y")
    config={
        "stock":{
            "X":{},"Y":{}
        },
        "index":{
            "X":{},"Y":{}
        }
    }
    for section in ['stock','index']:
        for label in ['X','Y']:
            for k in config_json[section][label].keys():
                option = config_json[section][label][k]
                if option=="*":
                    config[section][label][k]={}
                    mod=importlib.import_module("feature_extractor.features.{}".format(k))
                    config[section][label][k]["lib"]=mod
                    config[section][label][k]["interface"]=getattr(mod,"interface")
    return config
            

def get_extract_cache_path(stock_num,config_file,start_date,end_date):
    stock_num = '*' if stock_num is None else stock_num 
    _,config_file_name = os.path.split(config_file) 
    config_name,_ = os.path.splitext(config_file_name) 
    dir_name = os.path.join( cache_path,'pool{}_{}_{}-{}'.format(stock_num,config_name,start_date,end_date))
    return dir_name

def extract_feature(raw,config):
    for k in config["stock"]["X"].keys():
        mod=config["stock"]["X"][k]["lib"]
        interface = config["stock"]["X"][k]["interface"]
        for fc_name in interface['dataframe']:
            func = getattr(mod,fc_name)
            #print(func)
            raw = func(raw)
    #print(raw['stock'])
    raw['quotes']=raw['quotes'][::-1]
    #print('after reverse')
    #print(raw['stock'])
    for k in config["stock"]["Y"].keys():
        mod=config["stock"]["Y"][k]["lib"]
        interface = config["stock"]["Y"][k]["interface"]
        for fc_name in interface['dataframe']:
            func = getattr(mod,fc_name)
            raw = func(raw)
    raw['quotes']=raw['quotes'][::-1]
    return raw

def flat_feature(raw,config):
    return raw['quotes']
    

def rename_columns(df,suffix):
    def __add_suffixes(x):
        if x in ['date','code']:
            return x
        else:
            return '_'.join([x,suffix])
    df.rename(columns=__add_suffixes, inplace=True)
    return df
    
def extract_index_feature(raw,config):
    for k in config["index"]["X"].keys():
        mod=config["index"]["X"][k]["lib"]
        interface = config["index"]["X"][k]["interface"]
        for fc_name in interface['dataframe']:
            func = getattr(mod,fc_name)
            #print(func)
            raw = func(raw)
    #print(raw['stock'])
    raw['quotes']=raw['quotes'][::-1]
    #print('after reverse')
    #print(raw['stock'])
    for k in config["index"]["Y"].keys():
        mod=config["index"]["Y"][k]["lib"]
        interface = config["index"]["Y"][k]["interface"]
        for fc_name in interface['dataframe']:
            func = getattr(mod,fc_name)
            raw = func(raw)
    raw['quotes']=raw['quotes'][::-1]
    rename_columns(raw['quotes'],raw['code'])
    raw['quotes'].drop(['code'],axis=1,inplace=True)
    return raw
