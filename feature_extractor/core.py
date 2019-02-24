import os
import json
import utils
import importlib
from multiprocessing import Process
#import feature_extractor.features.base as base

def load_extractor_config(filename):
    config_json = json.load(open(filename))
    #print('load config file:{}'.format(filename))
    #print(config_json)
    assert config_json.get("X")
    assert config_json.get("Y")
    config={
        "X":{},"Y":{}
    }
    for k in config_json["X"].keys():
        option = config_json["X"][k]
        if option=="*":
            config["X"][k]={}
            mod=importlib.import_module("feature_extractor.features.{}".format(k))
            config["X"][k]["lib"]=mod
            config["X"][k]["interface"]=getattr(mod,"interface")
    
    for k in config_json["Y"].keys():
        option = config_json["Y"][k]
        if option=="*":
            config["Y"][k]={}
            mod=importlib.import_module("feature_extractor.features.{}".format(k))
            config["Y"][k]["lib"]=mod
            config["Y"][k]["interface"]=getattr(mod,"interface")
    #print('load config module')
    #print(config)
    return config

def get_extract_cache_path(base_path,stock_pool,config_file,start_date,end_date):
    stock_num = len(stock_pool)
    _,config_file_name = os.path.split(config_file) 
    config_name,_ = os.path.splitext(config_file_name) 
    dir_name = os.path.join( base_path,'pool{}_{}_{}-{}'.format(stock_num,config_name,start_date,end_date))
    return dir_name

def extract_feature(raw,config):
    assert config.get("X")
    #assert config.get("Y")

    for k in config["X"].keys():
        mod=config["X"][k]["lib"]
        interface = config["X"][k]["interface"]
        for fc_name in interface['dataframe']:
            func = getattr(mod,fc_name)
            #print(func)
            raw['stock'] = func(raw)
    #print(raw['stock'])
    raw['stock']=raw['stock'][::-1]
    #print('after reverse')
    #print(raw['stock'])
    for k in config["Y"].keys():
        mod=config["Y"][k]["lib"]
        interface = config["Y"][k]["interface"]
        for fc_name in interface['dataframe']:
            func = getattr(mod,fc_name)
            #print(func)
            raw['stock'] = func(raw)
    raw['stock']=raw['stock'][::-1]
    return raw

def flat_feature(raw,config):
    return raw['stock']
    
    
    
