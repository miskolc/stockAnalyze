import talib
import numpy  as np
import pandas as pd
import tushare as ts
from data_engine import Stock_engine 
import data_engine
import argparse
from feature_extractor.core import load_extractor_config,extract_feature 
from framework import extract_stock_feature,load_extracted_feature 
import analyzer
from analyzer.core import load_analyzer_config,prepare_data,split_cv,get_X,get_Y
from analyzer.target import tri_trend
from analyzer.metrics import showMetrics
import xgboost as xgb


'''
 check if is the signal of the date
'''
def check_signal(code,check_date,stock_history=None,forecast=30,window=30):
    df = stock_history
    check_window = df[df.date<=check_date] 
    forecast_window = df[df.date>check_date] 


if __name__=='__main__':
    #stock_engine = Stock_engine()
    #stock_engine.load_stock_info()
    analyzer_config = load_analyzer_config("configs/analyzer_config/simple.cfg")
    extract_stock_feature("configs/data_config/300.json","configs/feature_config/test_config.json",force=True)
    df = load_extracted_feature("configs/data_config/300.json","configs/feature_config/test_config.json")
    #print(df)
    #config = load_extractor_config("configs/test_config.json") 
    #print(config)
    #df = data_engine.get_k_data('000333')
    #print(df)
    #feature = extract_feature({'stock':df},config)
    #print(feature)
    df = tri_trend(df,{})
    print(df.describe())
    print(df.columns)
    df = prepare_data(df,analyzer_config)
    train_df,test_df = split_cv(df,analyzer_config)
    print('train:{},test:{}'.format(train_df.shape,test_df.shape))
    train_X = get_X(train_df,analyzer_config)
    train_Y = get_Y(train_df,analyzer_config)

    params = {'max_depth':4, 'eta':1, 'silent':1, 'objective':'multi:softmax','num_class':3 }
    num_round = 100 
    
    dtrain = xgb.DMatrix( train_X,train_Y)
    model = xgb.train(params.items(), dtrain, num_round)
    print('train loss:')
    ytrain = model.predict(dtrain)
    showMetrics(train_Y,ytrain)
    print('test loss:')
    test_X = get_X(test_df,analyzer_config)
    test_Y = get_Y(test_df,analyzer_config)
    dtest= xgb.DMatrix( test_X)
    ytest = model.predict(dtest) 
    print(ytest)
    print(min(ytest))
    print(np.array(list(test_Y)))
    print(min(test_Y))
    showMetrics(test_Y,ytest)
    
    exit(0)
    stock_engine = Stock_engine()
    stock_engine.load_stock_info()
    stock_engine.load_index_data()
    stock_engine.load_all_stock_data(n=10)
    #print(stock_engine.k_data)
    codes = stock_engine.k_data.keys()
    for code in codes:
        stock_history = stock_engine.k_data[code]
        print('process {}'.format(code))
        print(stock_history)
        signals = search_signal(stock_history)
        print(signals)
    

