from sklearn.metrics import mean_squared_error,confusion_matrix,accuracy_score,f1_score,roc_auc_score,recall_score,precision_score
import math
import logging 
import numpy as np

def nclass_metrics(target,pred):
    logs=[]
    def log(line):
        string='{}'.format(line)
        logs.append(string)
    accuracy = accuracy_score(target, pred)
    precision = precision_score(target, pred,average='macro')
    f1 = f1_score(target, pred,average='macro')
    recall = recall_score(target, pred,average='macro')
    log('accuracy:%f precision:%f recall:%f f1:%f'%(accuracy,precision,recall,f1))
    log('class precision')
    log(precision_score(target, pred,average=None))
    log('class recall')
    log(recall_score(target, pred,average=None))
    log('class f1')
    log(f1_score(target, pred,average=None))

    rmse= math.sqrt(mean_squared_error(target,pred))
    log('rmse:{}'.format(rmse))
    
    log('buy {} opposite {} ({}%)'.format(np.sum(pred==2),np.sum((pred-target)==2),np.sum((pred-target)==2)/np.sum(pred==2)*100))
    log('sell {} opposite {} ({}%)'.format(np.sum(pred==0),np.sum((pred-target)==-2),np.sum((pred-target)==-2)/np.sum(pred==0)*100))
        
    return '\n'.join(logs)


def show_nclass_metrics(target,pred):
    print(nclass_metrics(target,pred))
