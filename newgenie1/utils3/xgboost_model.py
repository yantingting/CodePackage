# encoding: utf-8
"""
@author: di.wang@quantgroup.cn
"""


from sklearn import linear_model
import sys
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import OneHotEncoder
import json
import pickle
from itertools import product
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.linear_model import LogisticRegressionCV

from sklearn.model_selection import StratifiedKFold
import os
import logging
import xgboost as xgb
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials


'''
params = {
    'num_boost_round':1000, # 最大迭代次数
    'nfold';3, # CV-K折
    'early_stopping_rounds':3,
    'silent':1, # 是否打印训练过程中间的一些信息到屏幕
    'objective':reg:logistic, # 目标函数
    'eval_metric':auc, # 评价指标
    'seed':1123,
    'nthread':-1, # 线程数量，-1是选当前机器所有核
    'booster':gbtree, # 用于boosting的子模型类别
    'max_depth':hp.choice("max_depth", [2,3,4,5])),
    'min_child_weight':hp.quniform("min_child_weight", 1, 7, 1),
    'gamma':hp.uniform("gamma", 0, 0.4),
    'subsample':hp.uniform("subsample", 0.6, 1),
    'colsample_bytree':hp.uniform("colsample_bytree", 0.6, 1),
    'eta':hp.uniform("eta", 0.05, 0.2)
}
'''

def xgb_hyperopt(X_train, y_train, param=None):

    print('imported xgb:',xgb)
    print('imported tpe:',tpe)

    if param == None:
        param = {
            'num_boost_round':1000, # 最大迭代次数
            'nfold':3, # CV-K折
            'early_stopping_rounds':3,
            'silent':1, # 是否打印训练过程中间的一些信息到屏幕
            'objective': 'reg:logistic', # 目标函数
            'eval_metric':'auc', # 评价指标
            'seed':1123,
            'nthread':-1, # 线程数量，-1是选当前机器所有核
            'booster':'gbtree', # 用于boosting的子模型类别
            'max_depth':hp.choice("max_depth", [2,3,4,5]),
            'min_child_weight':hp.quniform("min_child_weight", 1, 7, 1),
            'gamma':hp.uniform("gamma", 0, 0.4),
            'subsample':hp.uniform("subsample", 0.6, 1),
            'colsample_bytree':hp.uniform("colsample_bytree", 0.6, 1),
            'eta':hp.uniform("eta", 0.05, 0.2)
        }

    def f(param):
        cv_result = xgb.cv(param,
            dtrain,
            num_boost_round = param['num_boost_round'],
            nfold = param['nfold'],
            early_stopping_rounds = param['early_stopping_rounds'],
            verbose_eval=10)
        cv_auc = cv_result["test-auc-mean"].max()
        print("cv_auc", cv_auc)
        print("param", param)
        return {'loss': -cv_auc, 'status': STATUS_OK}

    train_x = X_train.astype(float)
    train_y = y_train.astype(int)
    #test_x = X_test.astype(float)
    #test_y  = y_test.astype(int)
    dtrain = xgb.DMatrix(train_x,train_y)
    #dtest = xgb.DMatrix(test_x,test_y)
    trials = Trials()
    best = fmin(f, param, algo=tpe.suggest, max_evals=20, trials=trials)
    print('best:',best)
    cv_result = xgb.cv(best,
                    dtrain,
                    num_boost_round = param['num_boost_round'],
                    nfold = param['nfold'],
                    early_stopping_rounds = param['early_stopping_rounds'],
                    verbose_eval=10)
    best_round = cv_result.shape[0]

    model = xgb.train(best, dtrain, num_boost_round=best_round)
    #with open(pickle_file, 'wb') as f:
    #    pickle.dump(model, f)

    #train_proba = model.predict(dtrain)
    #test_proba = model.predict(dtest)
    #test["predict_score"] = test_proba
    #test.to_csv(predict_test_file, index=False)
    #print('xgb_hyperopt AUC: ', roc_auc_score(test_y, test_proba))

    # best param
    df_params = pd.DataFrame(list(best.items()),columns=['param', 'value'])

    # importance
    imp_dict = model.get_fscore()
    feature_importance = pd.DataFrame(list(imp_dict.items()),columns=['varName', 'importance'])
    # 没有被使用来划分节点的feature不在get_fscore结果中，要加入
    for i in list(X_train.columns):
        if i not in list(feature_importance['varName'].values):
            add_data = pd.Series({'varName': i, 'importance': 0})
            feature_importance = feature_importance.append(add_data, ignore_index=True)
    # 调整feature_importance的顺序与变量入模顺序一致
    a = feature_importance.set_index(['varName'])
    sort_list = X_train.columns.tolist()
    new_importance = a.reindex(sort_list).reset_index(drop=False)

    return model, df_params, new_importance


'''
def xgboost_randomgridsearch(X_train, y_train, X_test, y_test, NFOLD=5, param=None):
    """
    随机搜索xgb的最佳参数；如果有test数据，则在gridsearch每一组参数训练时，
    XGBClassifier fit 时使用early_stopping，以在test上的效果不再提速时结束训练
    """
    if param == None:
        param = {
            'learning_rate': [0.05,0.1,0.2],
            'max_depth': [2, 3],
            'gamma': [0, 0.1],
            'min_child_weight':[1,3,10,20],
            'subsample': [0.6,0.7,0.8],
            'colsample_bytree': [0.6,0.7,0.8],
            'n_estimators': [50,100,150,200,300]
        }

    #fit_params = {
    #    "early_stopping_rounds": 30,
    #    "eval_metric": "auc",
    #    "eval_set": [(X_test, y_test)],
    #    "verbose": False
    #}

    xgboost_params_default = {
            #'objective': 'binary:logistic',
            'silent': 0,
            'nthread': 4
            #'eval_metric': 'auc',
            #'subsample': 0.8,
            #'colsample_bytree': 0.8,
    }

    clf_obj = xgb.XGBClassifier(objective= 'binary:logistic', random_state=1,\
                                n_estimators=10, subsample=0.8,
                                colsample_bytree=0.8)
    cv_obj = RandomizedSearchCV(clf_obj, param, scoring='roc_auc', cv=NFOLD)
    cv_obj.fit(X_train, y_train)
    optimal_params = cv_obj.cv_results_['params'][cv_obj.cv_results_['mean_test_score'].argmax()]

    logging.info("RandomSearch搜索的组合:{}".format(len(cv_obj.cv_results_["params"])))
    logging.info("RandomSearch最佳mean_test_score:{}".format(cv_obj.cv_results_["mean_test_score"].max()))
    logging.info("RandomSearch最佳参数:{}".format(optimal_params))

    optimal_params = dict(optimal_params, **xgboost_params_default)
    if 'learning_rate' in optimal_params:
        optimal_params['eta'] = optimal_params.pop('learning_rate')

    """
    调参完成后
    """
    model = xgb.XGBClassifier(random_state=1,\
                              early_stopping_rounds = 10, **optimal_params)
    model.fit(X_train,y_train)
    # best params
    df_params = pd.DataFrame(list(optimal_params.items()),columns=['param', 'value'])
    #importance
    best_importance = model.feature_importances_
    model_importance = pd.DataFrame(columns=["varName", 'importance'])
    model_importance['varName'] = list(X_train.columns)
    model_importance['importance'] = best_importance

    return model, df_params, model_importance
'''



def xgboost_randomgridsearch(X_train, y_train, X_test, y_test, NFOLD=5, param=None):
    """
    随机搜索xgb的最佳参数；如果有test数据，则在gridsearch每一组参数训练时，
    XGBClassifier fit 时使用early_stopping，以在test上的效果不再提速时结束训练
    """
    if param == None:
        param = {
            'learning_rate': [0.05,0.1,0.2],
            'max_depth': [2, 3],
            'gamma': [0, 0.1],
            'min_child_weight':[1,3,10,20],
            'subsample': [0.6,0.7,0.8],
            'colsample_bytree': [0.6,0.7,0.8],
            'n_estimators': [50,100,150,200,300]
        }

    clf_obj = xgb.XGBClassifier(objective= 'binary:logistic', random_state=1,\
                                n_estimators=10, subsample=0.8,\
                                colsample_bytree=0.8, silent = 0, nthread = 4, eval_metric = 'auc')
    cv_obj = RandomizedSearchCV(clf_obj, param, scoring='roc_auc', cv=NFOLD)
    cv_obj.fit(X_train, y_train)
    optimal_params = cv_obj.cv_results_['params'][cv_obj.cv_results_['mean_test_score'].argmax()]

    if 'learning_rate' in optimal_params:
        optimal_params['eta'] = optimal_params.pop('learning_rate')

    """
    调参完成后
    """
    model = xgb.XGBClassifier(random_state=1,\
                              early_stopping_rounds = 10, **optimal_params)
    model.fit(X_train,y_train)
    # best params
    df_params = pd.DataFrame(list(optimal_params.items()),columns=['param', 'value'])
    #importance
    best_importance = model.feature_importances_
    model_importance = pd.DataFrame(columns=["varName", 'importance'])
    model_importance['varName'] = list(X_train.columns)
    model_importance['importance'] = best_importance

    return model, df_params, model_importance
