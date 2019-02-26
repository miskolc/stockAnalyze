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

stock_path = './cached'
train_path='/dev/shm'

#def change(x,y):
#    return round(float((y-x)/x*100),2)

def load_data_config(config_file):
    config_json = json.load(open(config_file))
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
    file_name = os.path.join(stock_path,'k',str(code)+'-'+min(start,'2004-01-01')+'-'+end+'.bin')
    try:
        df = pd.read_pickle(file_name)
    except:
        df = ts.get_k_data(code,index=index,ktype=ktype,start=start,end=end)
        df.to_pickle(file_name)
    #return df.drop(['code'],axis=1)
    return df


class Stock_engine():
    def __init__(self):
        self.train_start= '2010-07-01'
        self.train_end= '2017-09-18'
        self.validate_start= '2017-10-18'
        self.validate_end= '2018-09-01'
        self.k_data={}
        self.batch_list = None
        self.validate_list = None
        #self.dataset = None
        #self.validate_dataset = None
        self.columns = None
        self.param = {
            "validate_num":1000,
            "stock_num":110,
            "per_stock":3,
            "window":30
        }
        
        #try:
        #    file_name = os.path.join(tmp_path,'stock_engine_meta.bin')
        #    with open(file_name, 'rb') as f:
        #        self.meta= pickle.load(f)
        #        self.columns = self.meta['columns']
        #except:
        #    print('stock_engine_meta not found!')
        #    pass 
        return
    def load_index_data(self):
        self.sh_index = get_k_data('000001',index=True,ktype='D',start='2004-01-01')
        self.sz_index = get_k_data('399001',index=True,ktype='D',start='2004-01-01')
        self.zx_index = get_k_data('399005',index=True,ktype='D',start='2004-01-01')
        self.cy_index = get_k_data('399006',index=True,ktype='D',start='2004-01-01')

        self.available_dates=list(self.sh_index.date[(self.sh_index.date>self.train_start)&(self.sh_index.date<self.train_end)])
        random.shuffle(self.available_dates)
        self.train_dates = self.available_dates[:1000]
        self.index = self.cy_index.merge(self.zx_index,on='date' ,how='right',suffixes=['_cy','_zx'],sort=True).\
        merge(self.sz_index,on='date' ,how='right',sort=True).\
        merge(self.sh_index,on='date' ,how='right',suffixes=['_sz','_sh'],sort=True)
    def load_all_stock_data(self,code=None,n=-1):
        if sys.platform=='linux':
            if code is None:
                self.codes = self.stocks_pool
            else:
                self.codes=[code]
        else:
            self.codes=['000333','000651','603019','000488']
        codes = self.codes
        if n==-1<0:
            N = len(codes)
        else:
            N = n
        print('stock pool:{}'.format(N))
        p = progressbar.ProgressBar()
        p.start()
        i = 0
        for code in codes[:N]:
            self.k_data[code] = get_k_data(code,ktype='D',start='2004-01-01')
            i = i + 1
            p.update(min(100,int((i / (N )) * 100)))
        p.finish()
        #print(self.k_data)

    def load_stock_info(self):
        stocks = ts.get_stock_basics()
        stocks = stocks[stocks.name.str.find('ST')<0]
        stocks = stocks[(stocks.timeToMarket<20160101)&(stocks.timeToMarket>0)]
        stocks = stocks[stocks.pe>0]
        self.stocks = stocks;
        self.stocks_pool = list(stocks.index)
        print(stocks.describe())


    def get_stock_data(self,code):
        df = self.k_data.get(code)
        if df is None:
            return None;
        min_date = df.date.min()
        max_date = df.date.max()
        df = df.merge(self.index,on='date',how='right',sort=True)
        return (df,min_date,max_date);

    def construct_ob_window(self,data,ob_time,delta=100):
        #for suffix in ['','_sh','_sz','_cy']:
        #    data['volume'+suffix] = data['volume'+suffix]/1000000.0
        df = data[data.date<=ob_time]
        df = df.tail(delta)
        if df.shape[0]<delta:
            return None
        if df.tail(1).close.isnull().iloc[0]:
            return None
        if df.head(1).close.isnull().iloc[0]:
            return None

        #nan_list = list(df[df.isnull().any(axis=1)].index)
        nan_list = list(df[df.close.isnull()].index)
        for index in nan_list:
            #print(nan_list)
            #print(index)
            #print(df.head(1).index)
            #print(df.head(1).close.isnull().iloc[0])
            #print(df.head(1).close.isnull())
            #print(df.head(1))
            df.loc[index,'open'] = df.loc[index-1,'close']
            df.loc[index,'close'] = df.loc[index-1,'close']
            df.loc[index,'high'] = df.loc[index-1,'close']
            df.loc[index,'low'] = df.loc[index-1,'close']
            df.loc[index,'volume'] = 0
        #df = df.fillna(999)
        close = float(df.tail(1).close)
        pred = data[data.date>=ob_time].head(11)
        nan_list = list(pred[pred.close.isnull()].index)
        for index in nan_list:
            pred.loc[index,'open'] = pred.loc[index-1,'close']
            pred.loc[index,'close'] = pred.loc[index-1,'close']
            pred.loc[index,'high'] = pred.loc[index-1,'close']
            pred.loc[index,'low'] = pred.loc[index-1,'close']
            pred.loc[index,'volume'] = 0
        #pred = pred.fillna(999)
        pred_num = pred.shape[0]
        pred = pred.tail(pred_num-1)
        pred_1 = pred.head(1)
        if pred_1.shape[0]==0:
            pred_1_close = 1.0
            pred_1_high = 1.0
            pred_1_low = 1.0
        else:
            pred_1_close = float(pred_1.tail(1).close) 
            pred_1_high = float(pred_1.tail(1).high) 
            pred_1_low = float(pred_1.tail(1).low) 
        pred_5 = pred.head(5)
        if pred_5.shape[0]<5:
            pred_5_close = 1.0
            pred_5_high = 1.0
            pred_5_low = 1.0
        else:
            pred_5_close = float(pred_5.tail(1).close) 
            pred_5_high = float(pred_5.tail(1).high) 
            pred_5_low = float(pred_5.tail(1).low) 
        pred_10 = pred.head(10)
        if pred_10.shape[0]<10:
            pred_10_close = 1.0
            pred_10_high = 1.0
            pred_10_low = 1.0
        else:
            pred_10_close = float(pred_10.tail(1).close) 
            pred_10_high = float(pred_10.tail(1).high) 
            pred_10_low = float(pred_10.tail(1).low) 
        #return (df,pred,close,pred_1_close,pred_5_close,pred_10_close,change(close,pred_1_close),change(close,pred_5_close),change(close,pred_10_close))
        return { "df_d":df,
                 "pred_10":pred,
                "close":close,
                "close_1":pred_1_close,
                "close_5":pred_5_close,
                "close_10":pred_10_close,
                "delta_1":utils.change(close,pred_1_close),
                "delta_5":utils.change(close,pred_5_close),
                "delta_10":utils.change(close,pred_10_close),
        }

    def gen_dataset(self):
        self.gen_train_dataset();
        self.gen_validate_dataset();

    def dump_train_dataset(self):
        cores = multiprocessing.cpu_count()
        n_chunk = len(self.batch_list) // cores 
        for i in range(cores):
            start_index = i*n_chunk;
            end_index = start_index + n_chunk 
            data = self.batch_list[start_index:end_index]
            file_name = os.path.join(train_path,'train_%d.bin'%i)
            with open(file_name, 'wb') as f:
                pickle.dump(data,f)

    #gen original stock train data, from train_start to train_end
    def gen_train_dataset(self):
        num = self.param.get('stock_num')
        per_stock = self.param.get('per_stock')
        window = self.param.get('window')
        self.batch_list=[]
        p = progressbar.ProgressBar()
        p.start()
        for i in range(num):
            code = random.sample(self.codes, 1)[0]
            #print(code)
            df,date_min,date_max = self.get_stock_data(code)
            time_zone = df[(df.date>date_min)&(df.date<date_max)&(df.date<self.train_end)&(df.date>self.train_start)]
            date_list = list(time_zone.date)
            random.shuffle(date_list)
            for date in date_list[:per_stock]:
                #if df[df.date==date].open.isnull().iloc[0]:
                #    continue
                vector = self.construct_ob_window(df,date,delta=window)
                if vector is None:
                    continue
                self.batch_list.append(vector)
            p.update(int((i / (num- 1)) * 100))
        p.finish()
        print('dump train dataset')
        self.dump_train_dataset();
        return;

    #gen original stock validate data, from validate_start to validate_end
    def gen_validate_dataset(self):
        num = self.param.get('validate_num')
        per_stock = self.param.get('per_stock')
        window = self.param.get('window')

        self.validate_list = []
        p = progressbar.ProgressBar()
        p.start()
        for i in range(num):
            code = random.sample(self.codes, 1)[0]
            # print(code)
            df, date_min, date_max = self.get_stock_data(code)
            time_zone = df[
                (df.date > date_min) & (df.date < date_max) & (df.date < self.validate_end) & (df.date > self.validate_start)]
            date_list = list(time_zone.date)
            random.shuffle(date_list)
            for date in date_list[:per_stock]:
                # if df[df.date==date].open.isnull().iloc[0]:
                #    continue
                vector = self.construct_ob_window(df, date, delta=window)
                if vector is None:
                    continue
                self.validate_list.append(vector)
            p.update(int((i / (num - 1)) * 100))
        p.finish()

    def gen_test_dataset(self,code):
        window = self.param.get('window')
        df, date_min, date_max = self.get_stock_data(code)
        time_zone = df[
                (df.date > date_min) & (df.date < date_max) & (df.date > self.validate_start)]
        df = time_zone
        #df = time_zone[:-10]
        date_list = list(df.date)
        test_list = []
        for date in date_list:
            vector = self.construct_ob_window(df, date, delta=window)
            if vector is None:
                continue
            print(date)
            test_list.append(vector)
        return test_list
        
            
if __name__ == '__main__':
    stock_engine = Stock_engine()
    stock_engine.load_stock_info()
    stock_engine.load_index_data()
    stock_engine.load_all_stock_data()
    #df = stock_engine.get_stock_data('000651')
    #print(df)
    #print(len(stock_engine.available_dates))
    #print(stock_engine.construct_ob_window(df,'2018-08-10'))
    #print(df.describe())

