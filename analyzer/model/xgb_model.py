import os
import xgboost as xgb
import datetime
import matplotlib.pyplot as plt
from analyzer.core import Model,clean_X
from analyzer.metrics import nclass_metrics
from sklearn.metrics import mean_squared_error
import utils
import math
class XGBModel(Model):
    def train(self,config,train_X,train_Y,test_X=None,test_Y=None,iter_round=100):
        assert self.model is None
        self.config = config
        self.hparams = config.get('hparams')
        self.iter_round = config.get('iter') if config.get('iter') else iter_round
        dtrain = xgb.DMatrix( clean_X(train_X),train_Y,nthread=-1)
        start_time = datetime.datetime.now()
        watchlist = [(dtrain, 'train')]
        #if test_X is not None and test_Y is not None:
        #    dvalidte= xgb.DMatrix(test_X,test_Y,nthread=-1)
        #    watchlist.append((dvalidte,'validate'))
        def my_rmse(preds, dtrain):       # written by myself
            #print(preds)
            labels = dtrain.get_label()
            # return a pair metric_name, result
            # since preds are margin(before logistic transformation, cutoff at 0)
            return 'rmse', math.sqrt(mean_squared_error(preds,labels))
        self.model = xgb.train(self.hparams.items(), dtrain, self.iter_round,evals = watchlist,early_stopping_rounds=10,verbose_eval=10,feval=my_rmse)
        self.feature_names = self.model.feature_names;
        self.save()
        #print(self.model.get_fscore())
        self.save_feature_names()
        self.save_f1_score()
        end_time = datetime.datetime.now()
        self.log('Train from:{} to {}'.format(start_time,end_time))
        self.log('######### train loss: ##########')
        ytrain = self.model.predict(dtrain)
        self.log(nclass_metrics(train_Y,ytrain))
        del dtrain
        if test_X is not None and test_Y is not None:
            self.log('########## test loss: ##########')
            first = test_X.date.min()
            test_X.loc[:,'Y___']=test_Y
            t = first
            while True:
                t_d = utils.date_delta(t,30)
                df = test_X[(test_X.date>=t)&(test_X.date<=t_d)] 
                if df.shape[0]==0:
                   break 
                self.log('---validate zone:{}-{}---'.format(t,t_d))
                dtest=xgb.DMatrix(clean_X(df.drop('Y___',axis=1)))
                ytest = self.model.predict(dtest) 
                self.log(nclass_metrics(df['Y___'],ytest))
                t = t_d
                del df

            self.log('===validate zone:{}-{}==='.format(test_X.date.min(),test_X.date.max()))
            test_X = test_X.drop('Y___',axis=1)
            dtest= xgb.DMatrix(clean_X(test_X))
            ytest = self.model.predict(dtest) 
            self.log(nclass_metrics(test_Y,ytest))
            del dtest
        return

    def save_f1_score(self):
        score_path = os.path.join(self.analyzer_path,'fscore.txt')
        scores = self.model.get_fscore()
        scores = list(map(lambda x:'{}\t{}'.format(x[0],x[1]),sorted(scores.items(),key=lambda x:x[1],reverse=True)))
        with open(score_path, 'w') as f:
            f.write('\n'.join(scores))
        return
        
    def predict(self,X):
        assert self.model is not None
        X = X[self.feature_names]
        data = xgb.DMatrix(utils.clean_X(X))
        y = self.model.predict(data) 
        return y
    def save(self,path=None):
        if path is None:
            path = self.mod_path
        self.model.save_model(path)
        return
    def load(self,path=None):
        if path is None:
            path = self.mod_path
        self.model = xgb.Booster()
        self.model.load_model(path)
        self.load_feature_names()
        self.model.feature_names = self.feature_names
        print(len(self.feature_names))
        self.model.feature_types = None
        return

    def display(self):
        assert self.model is not None
        fig,ax = plt.subplots(figsize=(15,15))
        xgb.plot_importance(self.model,height=0.5,ax=ax)
        plt.show()
