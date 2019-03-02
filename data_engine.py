import tushare as ts
import sys
import os
import random
import pandas as pd
import progressbar
import numpy as np
import tensorflow as tf
import sys
import pickle
import multiprocessing

import utils
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

stock_path = './cached'
train_path='/dev/shm'
db_engine=None

#def change(x,y):
#    return round(float((y-x)/x*100),2)
def init_cache(config):
    global conn
    api = config['api']
    cache = config['cache']
    if cache.get('db')=='mysql':
        user=cache.get('user')
        password=cache.get('password')
        host =cache.get('host')
        port =cache.get('port')
        schema =cache.get('schema')
        db_engine = create_engine('mysql+mysqldb://{}:{}@{}:{}/{}?charset=utf8'.format(user,password,host,port,schema))
        

def load_data_config(config_file):
    if isinstance(config_file,str):
        config_json = json.load(open(config_file))
    else:
        config_json = config_file
    assert config_json.get("start") is not None
    assert config_json.get("end") is not None
    #stocks = ts.get_stock_basics()
    #stock_pool = list(stocks.index)
    num_str = config_json.get("num")
    num = None if num_str is None else int(num_str)
    #if num_str is not None:
    #    stock_pool = stock_pool[:int(num_str)]    

    index_pool=["000001","399001","399005","399006"]
    return (num,index_pool,config_json.get("start"),config_json.get("end"))


def get_k_data(code,index=False,ktype='D',start='2004-01-01',end='2018-09-28'):
    #pro = ts.pro_api()
    file_name = os.path.join(stock_path,'k',str(code)+'-'+start+'-'+end+'.bin')
    #file_name = os.path.join(stock_path,'k',str(code)+'-'+min(start,'2004-01-01')+'-'+end+'.bin')
    print(file_name)
    try:
        df = pd.read_pickle(file_name)
    except:
        df = ts.get_k_data(code,index=index,ktype=ktype,start=start,end=end)
        df.to_pickle(file_name)
    #return df.drop(['code'],axis=1)
    return df

def __get_k_data(code,index=False,ktype='D',start='2004-01-01',end='2018-09-28'):
    #pro = ts.pro_api()
    file_name = os.path.join(stock_path,'k',str(code)+'-'+start+'-'+end+'.bin')
    #file_name = os.path.join(stock_path,'k',str(code)+'-'+min(start,'2004-01-01')+'-'+end+'.bin')
    print(file_name)
    try:
        df = pd.read_pickle(file_name)
    except:
        df = ts.get_k_data(code,index=index,ktype=ktype,start=start,end=end)
        df.to_pickle(file_name)
    #return df.drop(['code'],axis=1)
    return df

class DataEngine():
    def __init__(self,config_file='./config.json'):
        self.cache = None
        self.api = None
        config_json = json.load(open(config_file)) 
        assert config_json.get('data_engine')
        config = config_json['data_engine']
        api = config['api']
        cache = config['cache']
        print(cache)
        if cache.get('db')=='mysql':
            self.cache='mysql'
            user=cache.get('user')
            password=cache.get('password')
            host =cache.get('host')
            port =cache.get('port')
            schema =cache.get('schema')
            self.conn = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(user,password,host,port,schema))

        
    def get_k_data(self,code,index=False,ktype='D',start=None,end=None):
        if ktype=='D':
            return self.get_k_data_daily(code,index,start,end)
       
    def get_k_data_daily(self,code,index,start,end): 
        df,cached_start,cached_end = self.get_k_data_daily_cached(code,index,start,end)
        if df is None:
            begin_date = '2004-01-01' if cached_end is None else cached_end
            df = ts.get_k_data(code,index=index,ktype='D',start=begin_date)
            if df is not None:
                self.cache_data_daily(df,cached_end)
            if df.shape[0]==0:
                return df
            else:
                return df[(df.date>=start)&(df.date<=end)]
        else:
            return df

    def get_k_data_daily_cached(self,code,index,start,end):
        if index==True:
            if code.startswith('0'):
                code ='sh'+code
            if code.startswith('3'):
                code='sz'+code
         
        query_min_max = "select min(date),max(date) from trade_daily where code='{}';".format(code)
        DBSession = sessionmaker(self.conn)
        session = DBSession()
        res = list(session.execute(query_min_max))
        session.close()
        #print(list(res))
        if res[0]==(None,None):
            return None,None,None
        cached_start,cached_end = res[0]
        if start>=cached_start and end<=cached_end:
            return pd.read_sql_query("select * from trade_daily where date>='{}' and date<='{}' and code='{}';".format(start,end,code),self.conn),cached_start,cached_end 
        else:
            return None,cached_start,cached_end

    def cache_data_daily(self,df,cached_end):
        if cached_end is None:
            print('######## cache new trade daily ############')
            new = df
        else:
            new = df[df.date>cached_end]
        new.to_sql('trade_daily',con=self.conn,if_exists='append',index=False)

if __name__=="__main__":
    engine = DataEngine()
    df = engine.get_k_data('000319',start='2010-07-01',end='2017-07-01')
    df = engine.get_k_data('000333',start='2010-07-01',end='2017-07-01')
    df = engine.get_k_data('000651',start='2010-07-01',end='2017-07-01')
    print(df)

    
    
