import os
import configparser
import json
from sklearn.utils import shuffle

def trim_quo(string):
    if string is None:
        return None
    return string.strip().strip("\"\'").strip()

def __load_analyzer_config(config_file): 
    config = configparser.ConfigParser()
    config.read(config_file)
    print(config)
    print(config.keys())
    for section in config.keys():
        print("[{s}]".format(s=section))
        for key in config[section]:
            print("{k} = {v}".format(k=key, v=config[section][key]))
    return config

def load_analyzer_config(config_file): 
    config_json = json.load(open(config_file))
    return config_json 
   
def clean_X(df): 
    columns = df.columns
    labels = set(['date','code','ts_code','trade_date'])
    columns = list(filter(lambda x: not x in labels,columns))
    return df[columns]

def get_X(df,config):
    exclude = config.get('X').get('exclude')
    columns = df.columns
    columns = list(filter(lambda x: not x.startswith("Y_"),columns))
    #labels = set(['date','code','ts_code','trade_date'])
    #columns = list(filter(lambda x: not x in labels,columns))
    
    return df[columns]

def get_Y(df,config):
    target = 'Y_'+trim_quo(config.get('Y').get('target'))
    return df[target]
    

def prepare_data(df,config):
    target = 'Y_'+trim_quo(config.get('Y').get('target'))
    df.dropna(subset=[target],inplace=True)
    return df
    

def split_cv(df,config):
    cv_date = trim_quo(config.get('data').get('cv_date')) 
    train_start =  trim_quo(config.get('data').get('train_start'))
    train_end =  trim_quo(config.get('data').get('train_end'))
    validate_start =  trim_quo(config.get('data').get('validate_start'))
    validate_end =  trim_quo(config.get('data').get('validate_end'))
       
    train_start = train_start if train_start is not None else df.date.min() 
    train_end = train_end if train_end is not None else cv_date 
    validate_start = validate_start if validate_start is not None else cv_date 
    validate_end = validate_end if validate_end is not None else df.date.max() 

    assert (train_start is not None and train_end is not None and validate_start is not None and validate_end is not None)
    assert train_start < train_end
    assert validate_start < validate_end
    assert train_end <= validate_start
    
    print(df.columns)
    #print(df.date)
    #print(df.describe())
    print(cv_date)
    print(df.date.min())
    print(df.date.max())
    #return (df[(df.date>=train_start)&(df.date<=train_end)],df[(df.date>validate_start)&(df.date<=validate_end)])
    return (shuffle(df[(df.date>=train_start)&(df.date<=train_end)]),df[(df.date>validate_start)&(df.date<=validate_end)])


class Model():
    def __init__(self,path='./'):
        self.model = None
        self.analyzer_path = path
        self.log_path=os.path.join(path,'analyzer.log')
        self.mod_path=os.path.join(path,'mod.bin')
        self.feature_path = os.path.join(path,'features.txt') 
        self.feature_names = None
    def log(self,line):
        string = "{}".format(line)
        print(string)
        with open(self.log_path, 'a+') as f:
            f.write(string+'\n') 
    def save_feature_names(self):
        with open(self.feature_path, 'w') as f:
            f.write('\n'.join(self.feature_names))
    def load_feature_names(self):
        with open(self.feature_path, "r") as f:
            self.feature_names = f.read().split("\n")


