import pandas as pd
import numpy as np
import os
import pickle
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score

import sys
sys.path.append('D:/Model/201911_uku_ios_model/03_code/newgenie/')

import utils3.misc_utils as mu
import utils3.summary_statistics as ss

import utils3.feature_selection as fs  # from genie
fs_obj = fs.FeatureSelection()

import utils3.metrics as mt
pf = mt.Performance()
bw = mt.BinWoe()

reload(mt)
from imp import reload
from utils3.data_io_utils import *


data_path = 'D:/Model/201911_uku_ios_model/01_data/'
result_path = 'D:/Model/201911_uku_ios_model/02_result/'


""" 读入数据 """

x_with_y = load_data_from_pickle(data_path,'x_with_y_20191203_v3.pkl')
x_with_y = x_with_y.loc[~ (x_with_y.effective_date == '2019-08-29')]

x_with_y = x_with_y.loc[(x_with_y.effective_date < '2019-08-13')|(x_with_y.effective_date > '2019-08-22')]  #去掉izi phoneage缺数据的时间段
x_with_y.shape #25935

x_with_y.loc[x_with_y.effective_date >= '2019-08-01','sample_set'] = 'test'
x_with_y.loc[x_with_y.effective_date < '2019-08-01','sample_set'] = 'train'

x_with_y.sample_set.value_counts()

x_with_y.index = x_with_y.loan_id

""" 处理occupation_type, bankcode, provincecode变量 """
x_with_y.occupation_type.value_counts()
#0.0    21192 OFFICE
#3.0     4554 ENTREPRENEUR
#2.0      127 UNEMPLOYED
#-1.0       55
#1.0        7 PELAJAR

#occupation
x_with_y['occupation_office'] = x_with_y.occupation_type.apply(lambda x:1 if x == 0  else (-1 if x == -1 else 0))
x_with_y['occupation_entre'] = x_with_y.occupation_type.apply(lambda x:1 if x == 3 else (-1 if x == -1 else 0))
x_with_y['occupation_unemp'] = x_with_y.occupation_type.apply(lambda x:1 if x == 2 else (-1 if x == -1 else 0))
x_with_y['occupation_student'] = x_with_y.occupation_type.apply(lambda x:1 if x == 1 else (-1 if x == -1 else 0))

x_with_y.occupation_student.value_counts()

#bank_code
x_with_y.bank_code.value_counts()
#"BNI": 0,
#"MAYBANK": 1,
#"CIMB": 2,
#"BCA": 3,
#"PANIN": 4,
#"PERMATA": 5,
#"BRI": 6,
#"DANAMON": 7,
#"BTN": 8,
#"MANDIRI": 9
x_with_y['bank_code_BNI'] = x_with_y.bank_code.apply(lambda x:1 if x == 0  else (-1 if x == -1 else 0))
x_with_y['bank_code_MAYBANK'] = x_with_y.bank_code.apply(lambda x:1 if x == 1  else (-1 if x == -1 else 0))
x_with_y['bank_code_CIMB'] = x_with_y.bank_code.apply(lambda x:1 if x == 2  else (-1 if x == -1 else 0))
x_with_y['bank_code_BCA'] = x_with_y.bank_code.apply(lambda x:1 if x == 3  else (-1 if x == -1 else 0))
x_with_y['bank_code_PANIN'] = x_with_y.bank_code.apply(lambda x:1 if x == 4  else (-1 if x == -1 else 0))
x_with_y['bank_code_PERMATA'] = x_with_y.bank_code.apply(lambda x:1 if x == 5  else (-1 if x == -1 else 0))
x_with_y['bank_code_BRI'] = x_with_y.bank_code.apply(lambda x:1 if x == 6  else (-1 if x == -1 else 0))
x_with_y['bank_code_DANAMON'] = x_with_y.bank_code.apply(lambda x:1 if x == 7  else (-1 if x == -1 else 0))
x_with_y['bank_code_BTN'] = x_with_y.bank_code.apply(lambda x:1 if x == 8  else (-1 if x == -1 else 0))
x_with_y['bank_code_MANDIRI'] = x_with_y.bank_code.apply(lambda x:1 if x == 9  else (-1 if x == -1 else 0))

x_with_y.provincecode = x_with_y.provincecode.map(province_map)

province_map = {}
for (k,v) in var_map['provincecode'].items():
    print(k, v)
    province_map[v] = k


x_with_y.provincecode.value_counts()
x_with_y.provincecode = x_with_y.provincecode.astype(int)

"""数据准备"""
x_with_y.rename(columns = {'effective_date':'applied_at', 'flag_7':'label'}, inplace=True)
x_with_y['applied_from'] = 'UKU'
x_with_y['passdue_day'] = 0
x_with_y['applied_type'] = 0

#train_df = train_df.drop(['bins','bin_no'],1)
#test_df = test_df.drop(['bins','bin_no'],1)


#字典
var_dict = pd.read_excel('D:/Model/201911_uku_ios_model/建模代码可用变量字典.xlsx', sheet_name= '字典')
y_cols = list(set(x_with_y.columns) - set(var_dict.指标英文)) #19

x_col = list(set(x_with_y.columns).intersection(var_dict.指标英文))

x_with_y = x_with_y.replace(-1, -8887)
x_with_y = x_with_y.replace(-8887, -1)

"""""""""EDA"""""""""
X = x_with_y[x_col]

ss.eda(X = X, var_dict=var_dict, data_path = result_path, useless_vars = [],exempt_vars = [], save_label ='all_1205', uniqvalue_cutoff=0.97)
summary = pd.read_excel(os.path.join(result_path, 'all_1205_variables_summary.xlsx'), encoding='utf-8')

exclude_after_eda = summary.loc[summary.exclusion_reason == '缺失NA比例大于0.97', '指标英文'].tolist()
len(exclude_after_eda)


#划分训练和验证
train_df = x_with_y[x_with_y.sample_set == 'train']
test_df = x_with_y[x_with_y.sample_set == 'test']

train_df.shape
train_df.label.value_counts(dropna = False)

test_df.shape
test_df.label.value_counts(dropna = False)

#去掉EDA变量
y_cols2 = [i for i in y_cols if i != 'label']

train = train_df.drop(y_cols2,1).drop(['occupation_type','industry_involved','bank_code','districtcode','citycode','monthly_salary'
                                          ,'whatsapp'],1).drop(exclude_after_eda, 1)
test = test_df.drop(y_cols2,1).drop(['occupation_type','industry_involved','bank_code','districtcode','citycode','monthly_salary'
                                        ,'whatsapp'],1).drop(exclude_after_eda, 1)

# X_train = train.drop(y_cols + ['bins','bin_no'], 1)
# X_test = train.drop(y_cols + ['bins','bin_no'], 1)

X_train = train.drop('label', 1)
X_test = test.drop('label', 1)

y_train = train.label
y_test = test.label

#检查数据量是否一致
X_train.shape[1] == X_test.shape[1]
X_train.shape[0] == y_train.shape[0]
X_test.shape[0] == y_test.shape[0]


"""""""Univariate Chart"""""""

import utils3.plot_tools as pt
import matplotlib.pyplot as plt

FIG_PATH = os.path.join(result_path, 'figure', 'UniVarChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

for i in x_col:
    pt.univariate_chart(x_with_y, i,  'label', n_bins=10,
                     default_value=-1,
                     dftrain=train_df, dftest=test_df,
                     draw_all=True, draw_train_test=True)
    path = os.path.join(FIG_PATH,"uniVarChart_"+i+".png")
    plt.savefig(path,format='png', dpi=100)
    plt.close()
    print(i+': done')


"""计算XGB importance"""
## fit model
model = XGBClassifier()
model.fit(X_train, y_train)
print(model)

## prediction
y_pred = model.predict(X_test)
predictions = [round(value) for value in y_pred]

## evaluation
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0))

features_in_model = list(X_train.columns)
feature_importance = model.feature_importances_

var_importance = pd.DataFrame(columns=["指标英文", 'importance'])
var_importance['指标英文'] = features_in_model
var_importance['importance'] = feature_importance
var_importance.loc[:, 'importance_rank'] = var_importance.importance.rank(ascending=False)
var_importance.head()

""" overall_ranking """

os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

args_dict = {
    'random_forest': {
        'grid_search': False,
        'param': None
    },
    'xgboost': {
        'grid_search': False,
        'param': None
    }
}
methods = [
    'random_forest',
    #'lasso',
    #'xgboost'
]

## Train 分箱
X_cat_train, X_transformed_train, woe_iv_df, rebin_spec, ranking_result = fs_obj.overall_ranking(X_train, y_train,
                                                                                           var_dict, args_dict,
                                                                                           methods, num_max_bins=5)

X_cat_train.head()

rebin_spec = mu.convert_rebin_spec2XGB_rebin_spec(rebin_spec)
rebin_spec_bin_adjusted = {k: v for k, v in rebin_spec.items()}

# woe_iv_df.to_excel(os.path.join(result_path,'woe_iv_df_1204.xlsx'))
# ranking_result.to_excel(os.path.join(result_path,'ranking_result_1204.xlsx'))
# save_data_to_pickle(rebin_spec,result_path,'rebin_spec_1204.pkl')
# save_data_to_pickle(rebin_spec_bin_adjusted,result_path,'rebin_spec_bin_adjusted_1204.pkl')

#save_data_to_pickle(X_cat_train,result_path,'X_cat_train.pkl')

bin_obj = mt.BinWoe()
X_cat_test = bin_obj.convert_to_category(X_test, var_dict, rebin_spec_bin_adjusted)
X_cat_test.shape
X_cat_train = bin_obj.convert_to_category(X_train, var_dict, rebin_spec_bin_adjusted)

"""按月/按sample_set检查变量的稳定性和有效性"""

## train
#train_df['appmon'] = train_df.effective_month
train_df['appmon'] = '0_train'

# X_cat_train_with_y_appmon = pd.merge(X_cat_train,train_df[['flag_7','appmon']] ,left_index=True,right_index=True)
# var_dist_badRate_by_time_train = ss.get_badRate_and_dist_by_time(X_cat_train_with_y_appmon,x_col,'appmon','flag_7')
# var_dist_badRate_by_time_train.to_excel(os.path.join(result_path, 'var_dist_badRate_by_time_train.xlsx'))

## test
#test_df['appmon'] = test_df.effective_month
test_df['appmon'] = '1_test'
# X_cat_test_with_y_appmon = pd.merge(X_cat_test,test_df[['flag_7','appmon']],left_index=True,right_index=True)
# var_dist_badRate_by_time_test = ss.get_badRate_and_dist_by_time(X_cat_test_with_y_appmon,x_col,'appmon','flag_7')
# var_dist_badRate_by_time_test.to_excel(os.path.join(result_path, 'var_dist_badRate_by_time_test.xlsx'))


## all
all_cat = pd.concat([X_cat_train,X_cat_test])
app_data = pd.concat([train_df[['label','appmon']],test_df[['label','appmon']]])
X_cat_with_y_appmon_all = pd.merge(all_cat,app_data[['label','appmon']] ,left_index=True,right_index=True)
var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,x_col,'appmon','label')
var_dist_badRate_by_time_all.to_excel(os.path.join(result_path, 'var_dist_badRate_by_sample_all_1204.xlsx'))

x_with_y.phone_age.value_counts()
x_with_y.loc[ x_with_y.customer_id == 344285282438258688].T

# PSI
var_psi = pf.variable_psi(X_cat_train, X_cat_test, var_dict)
var_psi.loc[:, 'psi_rank'] = var_psi.PSI.rank(ascending=False)
var_psi.head()

# 汇总各项指标
ranking_result = ranking_result.merge(var_psi, on='指标英文', how='left').merge(var_importance, on='指标英文', how='left')
#ranking_result.columns
ranking_result.sort_values('importance', ascending= False)

#train.columns
#train.shape
# train_df = train_df.drop(['appmon'],axis=1)
# test_df = test_df.drop(['appmon'],axis=1)
#
# train_df = train_df.rename(columns = {'flag_7':'label'})
# test_df = test_df.rename(columns = {'flag_7':'label'})
#
# y_cols.remove('flag_7')
# train = train_df.drop(y_cols,1)
# train = train.drop(['bins','bin_no'],1)
# test = test_df.drop(y_cols,1).drop(['bins','bin_no'],1)

""" 变量筛选 """
f_rmv = fs.feature_remove(train,test, ranking_result, result_path, psi = -1, iv = 0, imp = 0.002, corr = 1, slope = 'FALSE')

try:
    train = train.drop(['slope'], axis=1)
    test = test.drop(['slope'], axis=1)
except:
    pass

train_2 = train.drop(f_rmv, axis=1)
test_2 = test.drop(f_rmv, axis=1)


X_train = train_2.drop(['label'], axis=1)
y_train = train_2.label
X_test = test_2.drop(['label'], axis=1)
y_test= test_2.label

features_used = X_train.columns.tolist()
len(features_used)


"""调参1:XGB GridSearch"""
import utils3.xgboost_model as xm

param = {
        'eta': [0.01,0.05,0.1],
        'max_depth': [1,2,3,4],
        'gamma': [0, 0.1, 0.2],
        'min_child_weight':[5,10],
        'subsample': [0.6,0.7,0.8],
        'colsample_bytree': [0.6,0.7,0.8],
        'n_estimators': [50,100,150,200,300]
        }

model, df_params, model_importance = xm.xgboost_randomgridsearch(X_train, y_train, X_test, y_test, NFOLD=5, param=None)
model

# prediction
y_train_pred = model.predict_proba(X_train)[:,1]
y_test_pred = model.predict_proba(X_test)[:,1]


"""调参2：XGB hyperopt """
import utils3.xgboost_model as xm
from hyperopt import hp

params = {
    'num_boost_round':1000, # 最大迭代次数
    'nfold':3, # CV-K折
    'early_stopping_rounds':3,
    'silent':1, # 是否打印训练过程中间的一些信息到屏幕
    'seed':1123,
    'max_depth':hp.choice("max_depth", [2,3,4,5]),
    'min_child_weight':hp.quniform("min_child_weight", 1, 7, 1),
    'gamma':hp.uniform("gamma", 0, 0.4),
    'subsample':hp.uniform("subsample", 0.6, 1),
    'colsample_bytree':hp.uniform("colsample_bytree", 0.6, 1),
    'eta':hp.uniform("eta", 0.05, 0.2)
}

model, df_params, model_importance = xm.xgb_hyperopt(X_train, y_train, param = None)

model
df_params.head()
model_importance.head()


import xgboost as xgb

dtrain = xgb.DMatrix(X_train)
y_train_pred = model.predict(dtrain)

dtest = xgb.DMatrix(X_test)
y_test_pred = model.predict(dtest)
y_test_pred.min()

""" 调参3:XGB Grid Search(local) """
params = {
    'num_boost_round':1000, # 最大迭代次数
    'nfold':3, # CV-K折
    'early_stopping_rounds':3,
    'silent':1, # 是否打印训练过程中间的一些信息到屏幕
    'seed':1123,
    'max_depth':hp.choice("max_depth", [2,3,4,5]),
    'min_child_weight':hp.quniform("min_child_weight", 1, 7, 1),
    'gamma':hp.uniform("gamma", 0, 0.4),
    'subsample':hp.uniform("subsample", 0.6, 1),
    'colsample_bytree':hp.uniform("colsample_bytree", 0.6, 1),
    'eta':hp.uniform("eta", 0.05, 0.2)
}

param_test1 = {'max_depth':range(1,7,2)}
gsearch1 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, gamma=0, subsample=0.8,
                                                  colsample_bytree=0.8, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test1, scoring='roc_auc',n_jobs=4,iid=False,cv=5)
                        #param_grid = param_test1, scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch1.fit(X_train,y_train)
best_max_depth = gsearch1.best_params_['max_depth']
best_max_depth =4

param_test10 = {'min_child_weight':range(1,6,2)}
#param_test10 = {'min_child_weight':range(1,3)}
gsearch10 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, gamma=0, subsample=0.8,
                                                 max_depth =4, colsample_bytree=0.8, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test10, scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch10.fit(X_train,y_train)
best_min_child_weight = gsearch10.best_params_['min_child_weight']
best_min_child_weight
#best_min_child_weight = 5

param_test2 = {'gamma':[i/10.0 for i in range(0,5)]}
#param_test2 = {'gamma':[0,0.05,0.1]}

gsearch2 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, subsample=0.8, colsample_bytree=0.8, max_depth= best_max_depth,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test2,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch2.fit(X_train,y_train)
best_gamma = gsearch2.best_params_['gamma']
best_gamma

param_test3 = {'subsample':[i/10.0 for i in range(5,8)]}
gsearch3 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test3,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch3.fit(X_train,y_train)
best_subsample = gsearch3.best_params_['subsample']
best_subsample

param_test30 = {'colsample_bytree':[i/10.0 for i in range(6,10)]}
gsearch30 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test30,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch30.fit(X_train,y_train)
best_colsample_bytree = gsearch30.best_params_['colsample_bytree']
best_colsample_bytree

param_test4 = {'reg_alpha':[0.1,1,10,50, 100]}
gsearch4 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test4,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch4.fit(X_train,y_train)
best_reg_alpha = gsearch4.best_params_['reg_alpha']
best_reg_alpha

param_test5 = {'n_estimators':range(50,500,50)}
gsearch5 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1,  max_depth= best_max_depth, gamma=best_gamma,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,reg_alpha=best_reg_alpha,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test5,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch5.fit(X_train,y_train)
best_n_estimators = gsearch5.best_params_['n_estimators']       #300
best_n_estimators

param_test6 = {'learning_rate':[0.01,0.1,0.2,0.3]}
gsearch6 = GridSearchCV(estimator = XGBClassifier(max_depth= best_max_depth, gamma=best_gamma, n_estimators = best_n_estimators,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,reg_alpha=best_reg_alpha,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test6,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch6.fit(X_train,y_train)
best_learning_rate = gsearch6.best_params_['learning_rate']
best_learning_rate

#用获取得到的最优参数再次训练模型
best_xgb = XGBClassifier(learning_rate =best_learning_rate, n_estimators=best_n_estimators, max_depth= best_max_depth, gamma=best_gamma,
                         colsample_bytree = best_colsample_bytree, subsample = best_subsample, reg_alpha=best_reg_alpha,
                         min_child_weight=best_min_child_weight,
                         objective= 'binary:logistic',nthread=4,scale_pos_weight=1
                         ,eval_metric='auc', seed=27)
best_xgb.fit(X_train,y_train)

## df_params
param_dict = {"param": ['learning_rate', 'n_estimators','max_depth','gamma','colsample_bytree','subsample','reg_alpha','min_child_weight'],
"value": [best_learning_rate, best_n_estimators, best_max_depth, best_gamma, best_colsample_bytree, best_subsample, best_reg_alpha, best_min_child_weight ]}
df_params = pd.DataFrame(param_dict)
df_params

## model变量重要性
best_importance = best_xgb.feature_importances_
model_importance = pd.DataFrame(columns=["varName", 'importance'])
model_importance['varName'] = features_used
model_importance['importance'] = best_importance

# prediction
y_train_pred = best_xgb.predict_proba(X_train)[:,1]
y_test_pred = best_xgb.predict_proba(X_test)[:,1]

# ### 打分&KS
train_pred = train_df.copy()
train_pred['y_pred'] = y_train_pred

test_pred = test_df.copy()
test_pred['y_pred'] = y_test_pred
train_pred.head()

test_pred.loan_id = test_pred.loan_id.astype(str)

reload(pf)
reload(mt)

import utils3.metrics as mt
pf = mt.Performance()
bw = mt.BinWoe()

data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, data_scored_test, test_proba_ks, test_proba_ks_20, test_score_ks, test_score_ks_20 = pf.data_score_KS(train_pred, test_pred, 'y_pred')

#data_scored_test.order_no = data_scored_test.order_no.astype(int)
test_proba_ks
train_proba_ks

# AUC ACCURACY
def auc_acc_table(df):
    from sklearn.metrics import roc_auc_score
    y = df.label.values
    y_pred = df.y_pred

    ## AUC
    auc = roc_auc_score(y, y_pred)
    print("auc: %.2f" % auc)

    ## Accuracy
    predictions = [round(value) for value in y_pred]
    accuracy = accuracy_score(y, predictions)
    print("Accuracy: %.4f%%" % (accuracy * 100.0))

    return auc, accuracy


auc_train, acc_train = auc_acc_table(train_pred)
auc_test, acc_test = auc_acc_table(test_pred)

""" PDP(all) """

import utils3.plot_tools as pt
import matplotlib.pyplot as plt
import utils3.generate_report as gr

FIG_PATH = os.path.join(result_path, 'figure', 'PDP_26_1206_v2')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

f_imp_list = get_feature_importance(model_importance)
plt.close()

# f_imp_list
select_features = model_importance['varName'].values.tolist()
all_pred = pd.concat([X_train, X_test])

n = 0
while n < len(f_imp_list):
    print (n)
    m = n + 9
    features_draw = [i for i in f_imp_list[n:m]]
    print(features_draw)
    pt.pdpCharts9(best_xgb, all_pred, features_draw, select_features, n_bins=10, dfltValue=-1)
    path = os.path.join(FIG_PATH, "pdp_" + str(n) + "_" + str(m) + ".png")
    plt.savefig(path, format='png', dpi=100)
    plt.close()
    n += 9
    print(n)

"""输出Exccel合集结果"""
## 变量分布
var_dist_badRate_by_time_all['used_in_model'] = var_dist_badRate_by_time_all.varName.apply(lambda x: x in features_used)
## ranking result
ranking_result['used_in_model'] = ranking_result.指标英文.apply(lambda x: x in features_used)

## AUC + ACCURACY
# 若需要首申复申复贷的auc，可调用gerate_report.auc_tabel(df)
auc_list = [auc_train, auc_test]
acc_list = [acc_train, acc_test]
split_list = ['train','test']
df_auc_acc = pd.DataFrame({"sample_set":split_list,"auc":auc_list,"accuracy":acc_list})

## df_prediction
all_pred = pd.concat([train_pred, test_pred])
all_pred['loan_id'] = all_pred['loan_id'].astype(str)

## data_scored
data_scored_train['sample_set'] = "train"
data_scored_test['sample_set'] = "test"
data_scored_all = pd.concat([data_scored_train, data_scored_test])
data_scored_all['order_no'] = data_scored_all['order_no'].astype(str)

# python3.6的dict是ordered，按照顺序这样定义，之后生成excel的时候会按照顺序创建sheet
xgb_dict = {}

# 如果sheet对应的内容是dict，则dict的key会出现在sheet第一列。value会从第二列开始插入
# 如果sheet对应的内容是df，则从sheet的A1位置开始插入整张表格，不包含pd.DataFrame的index
xgb_dict['01_AUC&ACC'] = df_auc_acc
xgb_dict['02_EDA'] = summary
xgb_dict['03_KS'] = {'train_proba_ks':train_proba_ks,
                     'test_proba_ks':test_proba_ks,
                     'train_score_ks':train_score_ks,
                     'test_score_ks':test_score_ks,
                     'train_proba_ks_20':train_proba_ks_20,
                     'test_proba_ks_20':test_proba_ks_20,
                     'train_score_ks_20':train_score_ks_20,
                     'test_score_ks_20':test_score_ks_20,
                      }
xgb_dict['04_model_importance'] = model_importance.reset_index()
xgb_dict['05_model_params'] = df_params
xgb_dict['06_ranking_result'] = ranking_result
xgb_dict['07_data_scored'] = data_scored_all
#xgb_dict['08_data_prediction'] = all_pred
xgb_dict['09_woe_iv_train'] = woe_iv_df
#xgb_dict['09_woe_ivtest'] = woe_iv_df_test
#xgb_dict['09_woe_iv_testnew'] = woe_iv_df_testnew
#xgb_dict['10_var_dist_badRate'] = var_dist_badRate_by_time_all

from datetime import datetime
import utils3.filing as fl

## SAVE MODEL\n
best_xgb.save_model(result_path + 'result2_grid_26_1206_v2.model')

#pickle.dump(best_xgb, open(os.path.join(result_path, 'result2_grid_28_1205' +".pkl"), "wb"))

data_scored_all.dtypes
woe_iv_df.dtypes
var_dist_badRate_by_time_all.dtypes

fl.ModelSummary2Excel(result_path = result_path, fig_path= result_path, file_name = 'result2_grid_26_1206_v2'+".xlsx", data_dic = xgb_dict).run()

data_scored_all.order_no = data_scored_all.order_no.astype(str)
x_with_y.loan_id = x_with_y.loan_id.astype(str)

data_scored_all2 = data_scored_all.merge(x_with_y[['loan_id','extend_times','applied_at']], left_on = 'order_no', right_on = 'loan_id', how = 'left')

data_scored_all2.loc[data_scored_all2.sample_set == 'test'].applied_at.value_counts().sort_index()
data_scored_all2.tail()
data_scored_all2.to_excel(os.path.join(result_path, 'result2_grid27_1206_score_v2.xlsx'))

""" Lift Charts """

import utils3.plot_tools as pt
import matplotlib.pyplot as plt


#分train, test, oot 看模型的效果
FIG_PATH = os.path.join(result_path, 'figure', 'lifechart_26_1206')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

# lift_chart - train test oot
train_lc = pt.show_result_new(data_scored_all2.loc[data_scored_all2.sample_set == 'train'], 'y_pred','Y', n_bins = 10, feature_label='train')
test_lc = pt.show_result_new(data_scored_all2.loc[data_scored_all2.sample_set == 'test'], 'y_pred','Y', n_bins = 10, feature_label='test')
#oot_lc = pt.show_result_new(oot_with_prob3, 'y_pred','flag_7', n_bins = 10, feature_label='oot')
path = os.path.join(FIG_PATH, "LiftChart_train_test" + '26_1206' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

pt.lift_chart_by_time(data_scored_all2.loc[data_scored_all2.sample_set == 'test'],'y_pred','Y', n_bins = 10, by="week")
path = os.path.join(FIG_PATH, "LiftChart_" + 'by_week' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

#test_pred.loc[test_pred.week == 31].applied_at.max() #2019-08-01 -2019-08-04
#test_pred.loc[test_pred.week == 32].applied_at.min() #2019-08-05 -2019-08-11
#test_pred.loc[test_pred.week == 35].applied_at.min() #2019-08-23 -2019-08-25

#test上分extend_time看模型的效果
extend_lc = pt.show_result_new(data_scored_all2.loc[(data_scored_all2.sample_set == 'test') & (data_scored_all2.extend_times > 0)], 'y_pred','Y', n_bins = 10, feature_label='has_extend')
#extend_more3_lc = pt.show_result_new(test_pred_2.loc[(test_pred_2.sample_set == 'test') & (test_pred_2.extend_times > 3) & (test_pred_2.flag_extend != -1)], 'y_pred','flag_extend', n_bins = 10, feature_label='extend_morethan_3')
non_extend_lc = pt.show_result_new(data_scored_all2.loc[(data_scored_all2.sample_set == 'test') & (data_scored_all2.extend_times == 0)], 'y_pred','Y', n_bins = 10, feature_label='non_extend')

path = os.path.join(FIG_PATH, "LiftChart_" + 'extend_test' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

save_data_to_pickle(x_with_y,data_path,'x_with_y_1206_v4.pkl')

#展期区分展期1-3次和3次以上的人群，看模型的效果

#更新展期3次的flag
data_scored_all2

data_scored_all3  = data_scored_all2.merge(x_with_y[['loan_id','latedays','loan_status']], on = 'loan_id')
data_scored_all3.isnull().sum()

data_scored_all3.loc[(data_scored_all3.extend_times > 3) & (data_scored_all3.latedays > 7), 'flag_extend'] = 1
data_scored_all3.loc[(data_scored_all3.extend_times > 3) & (data_scored_all3.loan_status == 'COLLECTION'), 'flag_extend'] = 1
data_scored_all3.loc[(data_scored_all3.extend_times > 3) & (data_scored_all3.loan_status == 'FUNDED'), 'flag_extend'] = -1

data_scored_all3.to_excel(os.path.join(result_path,'result2_grid26_1206_score_v2_add_extendflag.xlsx'))
data_scored_all3 = pd.read_excel(os.path.join(result_path,'result2_grid26_1206_score_v2_add_extendflag.xlsx'))
#test上分extend_time看模型的效果

extend_less3_lc = pt.show_result_new(data_scored_all3.loc[(data_scored_all3.sample_set == 'test') & (data_scored_all3.extend_times > 0) & (data_scored_all3.extend_times <= 3)], 'y_pred','Y', n_bins = 10, feature_label='extend_lessthan_3')
#extend_more3_lc = pt.show_result_new(data_scored_all3.loc[(data_scored_all3.sample_set == 'test') & (data_scored_all3.extend_times > 3) & (data_scored_all3.flag_extend  != -1)], 'y_pred','flag_extend', n_bins = 10, feature_label='extend_morethan_3')
non_extend_lc = pt.show_result_new(data_scored_all2.loc[(data_scored_all2.sample_set == 'test') & (data_scored_all2.extend_times == 0)], 'y_pred','Y', n_bins = 10, feature_label='non_extend')
extend_lc = pt.show_result_new(data_scored_all2.loc[(data_scored_all2.sample_set == 'test') & (data_scored_all2.extend_times > 0)], 'y_pred','Y', n_bins = 10, feature_label='has_extend')

path = os.path.join(FIG_PATH, "LiftChart_" + 'extend_test_v3' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

