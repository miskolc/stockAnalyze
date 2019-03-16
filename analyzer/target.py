import utils
import pandas as pd
import numpy as np

def tri_trend(df,args):
    fc_name=utils.get_func_name()
    target_name = 'Y_'+fc_name
    df[target_name]=1
    N = args.get('N',20)
    pos_mdw=args.get('pos_mdw',5)
    neg_mdw=args.get('neg_mdw',10)
    pos_ma=args.get('pos_ma',5)
    neg_ma=args.get('neg_ma',-5)
    df.loc[:,target_name] = np.where(((df['Y_PMA_open_next_{}'.format(N)]>pos_ma) & (df['Y_MAXDRAWDOWN_open_next_{}'.format(N)]<pos_mdw)),2,df[target_name])
    df.loc[:,target_name] = np.where(((df['Y_PMA_open_next_{}'.format(N)]<neg_ma) | (df['Y_MAXDRAWDOWN_open_next_{}'.format(N)]>neg_mdw)),0,df[target_name])
    print('describe target distribution===========')
    print(df[target_name].value_counts())
    return df
    
