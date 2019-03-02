import talib
import numpy  as np
import pandas as pd
import argparse
from feature_extractor.core import load_extractor_config,extract_feature 
from framework import load_analyzer_record,use_analyzer_on_the_date,extract_stock_feature,load_extracted_feature,prepare_stock_data,create_analyzer
from analyzer.core import load_analyzer_config,prepare_data,split_cv,get_X,get_Y
import xgboost as xgb
import datetime





if __name__=='__main__':
    #stock_engine.load_stock_info()
    parser = argparse.ArgumentParser()
    #parser.add_argument('-d','--data',type=int,default=0,help='create stock num to train model')
    parser.add_argument('-d','--data',help='load data file')
    parser.add_argument('-f','--feature',help='load feature file')
    parser.add_argument('-a','--analyze',help='load analyze file')
    parser.add_argument('-n','--name',help='set mod name')
    parser.add_argument('-p','--path',help='set mod path')
    args = parser.parse_args()
    data_config_file = args.data if args.data is not None else "configs/data_config/300.json"
    feature_config_file = args.feature if args.feature is not None else "configs/feature_config/test_config.json"
    analyzer_config_file = args.analyze if args.analyze is not None else "configs/analyzer_config/simple.json"
    mod_name = args.name if args.name is not None else datetime.datetime.now().strftime('%y-%m-%d_%H:%M:%S')
    base_path = args.path


    #model = create_analyzer(data_config_file,feature_config_file,analyzer_config_file,mod_name=mod_name,base_path=base_path)
    record_path = "records/talib"
    analyzer = load_analyzer_record(record_path)
    result = {}
    days = ['2019-02-28','2019-03-01','2019-02-27']
    #days = ['2019-02-26','2019-02-27','2019-02-28']
    sell = set()
    buy = set()
    for day in days:
        summary,df,y=use_analyzer_on_the_date(analyzer,day)
        result[day] = summary
        #print(summary)
        sell = sell.intersection(summary['0'])
        buy= buy.intersection(summary['2'])
    print('sell 3days')
    print(sell)
    print('buy 3days')
    print(buy)


      


