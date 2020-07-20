import numpy as np
import pandas as pd
from imp import reload

sys.path.append('/Users/Mint/Desktop/repos/newgenie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
#reload(mt)

fs_obj = fs.FeatureSelection()
pf = mt.Performance()
bw = mt.BinWoe()

from utils3.data_io_utils import *

data_path = 'D:/Model/indn/202004_uku_old_model/01_data/'
result_path = 'D:/Model/indn/202004_uku_old_model/02_result/'
from utils3.misc_utils import convert_right_data_type

from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score


""" 读入数据 """
x_with_y = load_data_from_pickle(data_path,'x_with_y_0513.pkl')

x_with_y.sample_set2.value_counts()

"""数据准备"""
x_with_y.rename(columns = {'effective_date':'applied_at', 'Y':'label'}, inplace=True)
x_with_y['applied_from'] = 'UKU'
x_with_y['passdue_day'] = 0
x_with_y['applied_type'] = 0

x_with_y = x_with_y.loc[x_with_y.label.isin([0,1])]

#字典
var_dict = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/V3模型建模代码可用变量字典.xlsx', sheet_name= 'Sheet1')

y_cols = list(set(x_with_y.columns) - set(var_dict.指标英文))
x_col = list(set(x_with_y.columns).intersection(var_dict.指标英文))

useless_var = list(var_dict.loc[var_dict['是否可以直接使用'] == '否','指标英文'])
useless_var2 = list(set(useless_var).intersection(x_col))

modify_cols = [i for i in x_with_y.columns if 'modify' in i]

#x_with_y = x_with_y.drop(useless_var2,1)

x_with_y, _ = convert_right_data_type(x_with_y, var_dict)
x_with_y = x_with_y.fillna(-1)

pd.crosstab(x_with_y.sample_set, x_with_y.sample_set2)


x_with_y.sample_set.value_counts()
x_with_y.sample_set2.value_counts(dropna=False)

"""""""""EDA"""""""""
X = x_with_y[x_col]

eda_result = ss.eda(X = X, var_dict=var_dict, useless_vars =useless_var2,exempt_vars = [], uniqvalue_cutoff=0.97)
eda_result.to_excel(os.path.join(result_path, 'all_variable_summary.xlsx'))
summary = pd.read_excel(os.path.join(result_path, 'all_variable_summary.xlsx'), encoding='utf-8')
exclude_after_eda = summary.loc[summary.exclusion_reason.notnull(), '指标英文'].tolist()

#划分训练和验证
x_with_y.index = x_with_y.loan_id
x_with_y.head()

train_df = x_with_y[x_with_y.sample_set2.isin(['train','train2'])]
test_df = x_with_y[x_with_y.sample_set2.isin(['test','test2'])]
oot_df1 = x_with_y[x_with_y.sample_set2 == 'oot1']
oot_df2 = x_with_y[x_with_y.sample_set2 == 'oot2']

train_df.label.value_counts(dropna = False)
test_df.label.value_counts(dropna = False)

#去掉EDA变量
y_cols2 = [i for i in y_cols if i != 'label']

drop_var = list(set(useless_var2).union(set(exclude_after_eda)))
drop_var = list(set(useless_var2).union(set(exclude_after_eda)).union(set(modify_cols)))
len(drop_var)

train = train_df.drop(y_cols2,1).drop(drop_var, 1)
test = test_df.drop(y_cols2,1).drop(drop_var,1)
oot1 = oot_df1.drop(y_cols2,1).drop(drop_var,1)
oot2 = oot_df2.drop(y_cols2,1).drop(drop_var,1)

X_train = train.drop(['label'], 1)
X_test = test.drop(['label'], 1)
X_oot1 = oot1.drop(['label'], 1)
X_oot2 = oot2.drop(['label'], 1)


X_train = train.drop(['label','bins','bin_no'], 1)
X_test = test.drop(['label','bins','bin_no'], 1)
X_oot1 = oot1.drop(['label','bins','bin_no'], 1)
X_oot2 = oot2.drop(['label','bins','bin_no'], 1)



set(X_train.columns) - set(var_dict['指标英文'])
set(X_test.columns) - set(var_dict['指标英文'])
set(X_oot1.columns) - set(var_dict['指标英文'])

y_train = train.label
y_test = test.label
y_oot1 = oot1.label
y_oot2 = oot2.label


#检查数据量是否一致

X_train.shape[0] == y_train.shape[0]
X_test.shape[0] == y_test.shape[0]
X_oot1.shape[0] == y_oot1.shape[0]
X_oot2.shape[0] == y_oot2.shape[0]

X_oot2.head()

"""""""Univariate Chart"""""""
import utils3.plot_tools as pt
import matplotlib.pyplot as plt

FIG_PATH = os.path.join(result_path, 'figure', 'UniVarChart_0512')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)


for i in X_train.columns:
    #try:
        x_with_y[i] = x_with_y[i].astype(float)
        train_df[i] = train_df[i].astype(float)
        test_df[i] = test_df[i].astype(float)
        oot_df1[i] = oot_df1[i].astype(float)
        oot_df2[i] = oot_df2[i].astype(float)
        univariate_chart_oot(x_with_y, i,  'label', n_bins=10,default_value=-1,dftrain=train_df, dftest=test_df, dfoot1=oot_df1,dfoot2=oot_df2,
                         draw_all=True, draw_train_test=True)
        path = os.path.join(FIG_PATH,"uniVarChart_"+i+".png")
        plt.savefig(path,format='png', dpi=100)
        plt.close()
        print(i+': done')
    except:
        print(i+': fail')
        pass

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

import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs

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
    'lasso',
    #'xgboost'
]

## Train 分箱
X_cat_train, X_transformed_train, woe_iv_df, rebin_spec, ranking_result = fs_obj.overall_ranking(X_train, y_train,
                                                                                           var_dict, args_dict,
                                                                                           methods, num_max_bins=5)

X_cat_train.head()

rebin_spec = mu.convert_rebin_spec2XGB_rebin_spec(rebin_spec)
rebin_spec_bin_adjusted = {k: v for k, v in rebin_spec.items()}

woe_iv_df.to_excel(os.path.join(result_path,'woe_iv_df.xlsx'))
ranking_result.to_excel(os.path.join(result_path,'ranking_result.xlsx'))
#save_data_to_pickle(rebin_spec,result_path,'rebin_spec.pkl')
save_data_to_pickle(rebin_spec_bin_adjusted,result_path,'rebin_spec_bin_adjusted.pkl')

#save_data_to_pickle(X_cat_train,result_path,'X_cat_train.pkl')

bin_obj = mt.BinWoe()
X_cat_train = bin_obj.convert_to_category(X_train, var_dict, rebin_spec_bin_adjusted)
X_cat_test = bin_obj.convert_to_category(X_test, var_dict, rebin_spec_bin_adjusted)
X_cat_oot1 = bin_obj.convert_to_category(X_oot1, var_dict, rebin_spec_bin_adjusted)
X_cat_oot2 = bin_obj.convert_to_category(X_oot2, var_dict, rebin_spec_bin_adjusted)


"""按月/按sample_set检查变量的稳定性和有效性"""

## train
train_df['appmon'] = '0_train'

## test
test_df['appmon'] = '1_test'

#oot
oot_df1['appmon'] = '2_oot1'
oot_df2['appmon'] = '2_oot2'

## all
all_cat = pd.concat([X_cat_train,X_cat_test, X_cat_oot1, X_cat_oot2])
#all_cat = all_cat.reset_index()
all_cat.head()

app_data = pd.concat([train_df[['label','appmon']],test_df[['label','appmon']], oot_df1[['label','appmon']], oot_df2[['label','appmon']]])
X_cat_with_y_appmon_all = pd.merge(all_cat,app_data[['label','appmon']] ,left_index=True,right_index=True)

X_cat_with_y_appmon_all.head()

x_cols2 = list(set(X_cat_with_y_appmon_all.columns).intersection(x_col))
len(x_cols2)
X_cat_with_y_appmon_all.shape

var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,x_cols2,'appmon','label')

var_dist_badRate_by_time_all.to_excel(os.path.join(result_path, 'var_dist_badRate_by_sample_v2.xlsx'))


x_with_y.loc[ x_with_y.loan_id == '427266794313908224', ['loan_id', 'cnt_loan30', 'cnt_loan30_modify', 'pre_period']].T


x_with_y[['sum_extend_times_180d_modify_modify']]

x_with_y.phone_age.value_counts()
x_with_y.loc[ x_with_y.customer_id == 344285282438258688].T

# PSI
var_psi = pf.variable_psi(X_cat_train, X_cat_test, var_dict)
var_psi.loc[:, 'psi_rank'] = var_psi.PSI.rank(ascending=False)
var_psi.head()

# 汇总各项指标
ranking_result = ranking_result.merge(var_psi, on='指标英文', how='left').merge(var_importance, on='指标英文', how='left')


""" 变量筛选 """
train = train.drop(['bins','bin_no'],axis=1)
test = test.drop(['bins','bin_no'],axis=1)
oot1 = oot1.drop(['bins','bin_no'],axis=1)
oot2 = oot2.drop(['bins','bin_no'],axis=1)

f_rmv = fs.feature_remove(train,test, ranking_result, result_path, psi = -1, iv = 0, imp = 0.001, corr = 0.75, slope = 'TRUE')

try:
    train = train.drop(['slope'], axis=1)
    test = test.drop(['slope'], axis=1)
except:
    pass

train_2 = train.drop(f_rmv, axis=1)
test_2 = test.drop(f_rmv, axis=1)
oot_1 = oot1.drop(f_rmv, axis=1)
oot_2 = oot2.drop(f_rmv, axis=1)

X_train = train_2.drop(['label'], axis=1)
y_train = train_2.label
X_test = test_2.drop(['label'], axis=1)
y_test= test_2.label
X_oot1 = oot_1.drop(['label'], axis=1)
y_oot1 = oot_1.label
X_oot2 = oot_2.drop(['label'], axis=1)
y_oot2 = oot_2.label



# X_train = train_2.drop(['label'], axis=1)
# y_train = train_2.label
# X_test = test_2.drop(['label'], axis=1)
# y_test= test_2.label
# X_oot = oot_2.drop(['label'], axis=1)
# y_oot = oot_2.label

features_used = X_train.columns.tolist()
len(features_used)

#
# features_used.append('app_pred')
#
X_train = train[list(features_used)]
y_train = train.label
X_test = test[list(features_used)]
y_test= test.label
X_oot1 = oot1[list(features_used)]
y_oot1 = oot1.label
X_oot2 = oot2[list(features_used)]
y_oot2 = oot2.label

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
best_max_depth = 2

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



y_train_pred = best_xgb.predict_proba(X_train)[:,1]
y_test_pred = best_xgb.predict_proba(X_test)[:,1]
y_oot_pred1 = best_xgb.predict_proba(X_oot1)[:,1]
y_oot_pred2 = best_xgb.predict_proba(X_oot2)[:,1]
# ### 打分&KS

train_pred = train_df.copy()
train_pred['y_pred'] = y_train_pred

test_pred = test_df.copy()
test_pred['y_pred'] = y_test_pred
train_pred.head()

oot_pred1 = oot_df1.copy()
oot_pred1['y_pred'] = y_oot_pred1

oot_pred2 = oot_df2.copy()
oot_pred2['y_pred'] = y_oot_pred2

reload(pf)
reload(mt)

import utils3.metrics as mt
pf = mt.Performance()
bw = mt.BinWoe()

data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, data_scored_test, test_proba_ks, test_proba_ks_20, test_score_ks, test_score_ks_20 = pf.data_score_KS(train_pred, test_pred, 'y_pred')
data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, data_scored_oot1, oot1_proba_ks, oot1_proba_ks_20, oot1_score_ks, oot1_score_ks_20 = pf.data_score_KS(train_pred, oot_pred1, 'y_pred')
data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, data_scored_oot2, oot2_proba_ks, oot2_proba_ks_20, oot2_score_ks, oot2_score_ks_20 = pf.data_score_KS(train_pred, oot_pred2, 'y_pred')


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
auc_oot1, acc_oot1 = auc_acc_table(oot_pred1)
auc_oot2, acc_oot2 = auc_acc_table(oot_pred2)

""" PDP(all) """

import utils3.plot_tools as pt
import matplotlib.pyplot as plt

FIG_PATH = os.path.join(result_path, 'figure', 'PDP_59')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

f_imp_list = get_feature_importance(model_importance)

select_features = model_importance['varName'].values.tolist()
all_pred = pd.concat([X_train, X_test])
len(select_features)


n = 0
while n < len(f_imp_list):
    m = n + 9
    features_draw = [i for i in f_imp_list[n:m]]
    print(features_draw)
    pt.pdpCharts9(best_xgb, all_pred, features_draw, select_features, n_bins=10, dfltValue=-1)
    path = os.path.join(FIG_PATH, "pdp_" + str(n) + "_" + str(m) + ".png")
    plt.savefig(path, format='png', dpi=100)
    plt.close()
    n += 9
print('end')



"""输出Exccel合集结果"""
## 变量分布
var_dist_badRate_by_time_all['used_in_model'] = var_dist_badRate_by_time_all.varName.apply(lambda x: x in features_used)
## ranking result
ranking_result['used_in_model'] = ranking_result.指标英文.apply(lambda x: x in features_used)

## AUC + ACCURACY
# 若需要首申复申复贷的auc，可调用gerate_report.auc_tabel(df)
auc_list = [auc_train, auc_test, auc_oot1, auc_oot2]
acc_list = [acc_train, acc_test, acc_oot1, acc_oot2]
split_list = ['train','test', 'oot1', 'oot2']

df_auc_acc = pd.DataFrame({"sample_set":split_list,"auc":auc_list,"accuracy":acc_list})

## df_prediction
all_pred = pd.concat([train_pred, test_pred, oot_pred1, oot_pred2])
all_pred['loan_id'] = all_pred['loan_id'].astype(str)

## data_scored
data_scored_train['sample_set'] = "train"
data_scored_test['sample_set'] = "test"
data_scored_oot1['sample_set'] = "oot1"
data_scored_oot2['sample_set'] = "oot2"
data_scored_all = pd.concat([data_scored_train, data_scored_test, data_scored_oot1, data_scored_oot2])
data_scored_all['order_no'] = data_scored_all['order_no'].astype(str)
data_scored_all.head()

# python3.6的dict是ordered，按照顺序这样定义，之后生成excel的时候会按照顺序创建sheet
xgb_dict = {}

# 如果sheet对应的内容是dict，则dict的key会出现在sheet第一列。value会从第二列开始插入
# 如果sheet对应的内容是df，则从sheet的A1位置开始插入整张表格，不包含pd.DataFrame的index
xgb_dict['01_AUC&ACC'] = df_auc_acc
xgb_dict['02_EDA'] = summary
xgb_dict['04_KS'] = {'train_proba_ks':train_proba_ks,
                     'test_proba_ks':test_proba_ks,
                     'oot1_proba_ks':oot1_proba_ks,
                     'oot2_proba_ks': oot2_proba_ks,
                     'train_score_ks':train_score_ks,
                     'test_score_ks':test_score_ks,
                     'oot1_score_ks':oot1_score_ks,
                     'oot2_score_ks': oot2_score_ks,
                     'train_proba_ks_20':train_proba_ks_20,
                     'test_proba_ks_20':test_proba_ks_20,
                     'oot1_proba_ks_20':oot1_proba_ks_20,
                     'oot2_proba_ks_20': oot2_proba_ks_20,
                     'train_score_ks_20':train_score_ks_20,
                     'test_score_ks_20':test_score_ks_20,
                     'oot1_score_ks_20':oot1_score_ks_20,
                     'oot2_score_ks_20': oot2_score_ks_20
                     }
xgb_dict['04_model_importance'] = model_importance.reset_index()
xgb_dict['05_model_params'] = df_params
xgb_dict['06_ranking_result'] = ranking_result
#xgb_dict['07_data_scored'] = data_scored_all
#xgb_dict['08_data_prediction'] = all_pred
xgb_dict['09_woe_iv_train'] = woe_iv_df
#xgb_dict['09_woe_ivtest'] = woe_iv_df_test
#xgb_dict['09_woe_iv_testnew'] = woe_iv_df_testnew
xgb_dict['10_var_dist_badRate'] = var_dist_badRate_by_time_all

from datetime import datetime
import utils3.filing as fl


FILE_NAME = "grid_%d_%s"%(len(features_used), datetime.now().strftime('%y%m%d-%H-%M'))

best_xgb.save_model(os.path.join(result_path, FILE_NAME+'.model'))

fl.ModelSummary2Excel(result_path = result_path, fig_path= result_path, file_name = FILE_NAME +".xlsx", data_dic = xgb_dict).run()

data_scored_all.order_no = data_scored_all.order_no.astype(str)
data_scored_all = data_scored_all.merge(x_with_y[['loan_id','extend_times','applied_at','sample_set2']], left_on = 'order_no', right_index = True, how = 'left')
data_scored_all.to_excel(os.path.join(result_path, FILE_NAME + 'score' + '.xlsx'))


data_scored_all = score_no_modify.copy()

""" Lift Charts """
import utils3.plot_tools as pt
import matplotlib.pyplot as plt

#分train, test, oot 看模型的效果
FIG_PATH = os.path.join(result_path, 'figure', 'lifechart59')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

# lift_chart - train test oot
train_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'train'], 'y_pred','Y', n_bins = 10, feature_label='train')
test_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'test'], 'y_pred','Y', n_bins = 10, feature_label='test')
oot1_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'oot1'], 'y_pred','Y', n_bins = 10, feature_label='oot1')
oot2_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'oot2'], 'y_pred','Y', n_bins = 10, feature_label='oot2')
path = os.path.join(FIG_PATH, "LiftChart_" + '55' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

#
data_scored_all['week'] = pd.to_datetime(data_scored_all.loc[data_scored_all.sample_set.isin(['oot1', 'oot2', 'test']), 'applied_at']).dt.week
data_scored_all.groupby(['week'])['applied_at'].min()

data_scored_all = score_xuchao.copy()

pt.lift_chart_by_time(data_scored_all.loc[data_scored_all.sample_set.isin(['oot1', 'oot2','test'])],'y_pred','Y', n_bins = 10, by="week")
path = os.path.join(FIG_PATH, "LiftChart_" + 'by_week_plan_one' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

# pt.lift_chart_by_time(online_score.loc[online_score.sample_set.isin(['oot1', 'oot2','test'])],'prob','Y', n_bins = 10, by="week")
# path = os.path.join(FIG_PATH, "LiftChart_" + 'v2_by_week' + ".png")
# plt.savefig(path, format='png', dpi=100)
# plt.close()

#和线上模型对比效果
online_score = load_data_from_pickle('D:/Model/indn/202004_uku_old_model/01_data','v2score.pkl')
online_score.columns

online_score.loan_id = online_score.loan_id.astype(str)
data_scored_all.loan_id = data_scored_all.loan_id.astype(str)

score_has_modify.order_no = score_has_modify.order_no.astype(str)

online_score = data_scored_all[['loan_id','Y','sample_set','applied_at']].merge(online_score[['loan_id', 'score', 'prob']], left_on = 'loan_id', right_on = 'loan_id')

#online_score = score_has_modify[['order_no','Y','sample_set']].merge(online_score[['loan_id', 'score', 'prob']], left_on = 'order_no', right_on = 'loan_id')


#和V2对比

test_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'test'], 'y_pred','Y', n_bins = 10, feature_label='test')
oot1_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'oot1'], 'y_pred','Y', n_bins = 10, feature_label='oot1')
oot2_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'oot2'], 'y_pred','Y', n_bins = 10, feature_label='oot2')
testv2_lc = pt.show_result_new(online_score.loc[online_score.sample_set == 'test'], 'prob','Y', n_bins = 10, feature_label='v2model_test')
oot1v2_lc = pt.show_result_new(online_score.loc[online_score.sample_set == 'oot1'], 'prob','Y', n_bins = 10, feature_label='v2model_oot1')
oot2v2_lc = pt.show_result_new(online_score.loc[online_score.sample_set == 'oot2'], 'prob','Y', n_bins = 10, feature_label='v2model_oot2')
path = os.path.join(FIG_PATH, "LiftChart_" + '59_compare_onlinemodel' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()





#####################################################################################
def univariate_chart_oot(df, feature, y_true, n_bins=10,
                     default_value=-99999,
                     dftrain=None, dftest=None, dfoot1 = None,dfoot2 = None,
                     draw_all=True, draw_train_test=False):
    '''
    在样本集上画出指定变量的单变量分析图.
    :param df: 数据集, DataFrame.
               至少包含特征和标签列.
    :param feature: 要画的特征, str.
    :param y_true: 标签列, string.
                   仅仅包含0/1.
    :param n_bins: 最大分成的箱数, int.
                   默认为10, 仅仅对于包含数值的特征.
    :param default_value: 缺省值, int.
                          默认-99999.
    :param dftrain: 训练集, DataFrame.
    :param dftest: 测试集, Dataframe.
    :param draw_all: 是否在全样本上作图, bool.
                     默认为True.
    :param draw_train_test: 是否分别在训练集和测试集上作图, bool.
                            默认为False.
    :return: 单变量分析图, fig.
    '''

    idx_not_default = (df[feature] != default_value)  # 非缺省数据位置.

    if n_bins > df[feature].nunique():  # 数据元素种类过少.
        y_mean_all, y_mean_train, y_mean_test, y_mean_oot1, y_mean_oot2 = [], [], [], [], []
        count_all, count_train, count_test, count_oot1, count_oot2 = [], [], [], [], []

        feature_grid = sorted(df.loc[idx_not_default, feature].unique().tolist())
        for feature_value in feature_grid:  # 对全样本记录每个bin的信息.
            y_mean_all.append(df.loc[df[feature] == feature_value, y_true].mean())
            count_all.append(df.loc[df[feature] == feature_value, y_true].count())
        y_mean_all = np.round(y_mean_all, 3)

        if draw_train_test:  # 对训练集和测试集记录每个bin的信息.
            for feature_value in feature_grid:
                y_mean_train.append(dftrain.loc[dftrain[feature] == feature_value, y_true].mean())
                y_mean_test.append(dftest.loc[dftest[feature] == feature_value, y_true].mean())
                y_mean_oot1.append(dfoot1.loc[dfoot1[feature] == feature_value, y_true].mean())
                y_mean_oot2.append(dfoot2.loc[dfoot2[feature] == feature_value, y_true].mean())

                count_train.append(dftrain.loc[dftrain[feature] == feature_value, y_true].count())
                count_test.append(dftest.loc[dftest[feature] == feature_value, y_true].count())
                count_oot1.append(dfoot1.loc[dfoot1[feature] == feature_value, y_true].count())
                count_oot2.append(dfoot2.loc[dfoot2[feature] == feature_value, y_true].count())

            y_mean_train = np.round(y_mean_train, 3)
            y_mean_test = np.round(y_mean_test, 3)
            y_mean_oot1 = np.round(y_mean_oot1, 3)
            y_mean_oot2 = np.round(y_mean_oot2, 3)


        # 画图.
        fig = plt.figure()
        plt.figure(figsize=(8, 6))
        x_index = list(range(1, len(feature_grid) + 1))
        if draw_all:  # 画全样本.
            plt.plot(x_index, y_mean_all, 'bo-', label='%s' % 'all')
            plt.gcf().text(0.6, 0.60, 'All data sample: %s' % count_all, fontsize=9)


        if draw_train_test:
            plt.plot(x_index, y_mean_train, 'co-', label='%s' % 'train')
            plt.plot(x_index, y_mean_test, 'mo-', label='%s' % 'test')
            plt.plot(x_index, y_mean_oot1, 'go-', label='%s' % 'oot1')
            plt.plot(x_index, y_mean_oot2, 'yo-', label='%s' % 'oot2')
            plt.gcf().text(0.6, 0.55, 'Train data sample: %s' % count_train, fontsize=9)
            plt.gcf().text(0.6, 0.50, 'Train data eventR: %s' % y_mean_train, fontsize=9)
            plt.gcf().text(0.6, 0.45, 'Test data sample: %s' % count_test, fontsize=9)
            plt.gcf().text(0.6, 0.40, 'Test data eventR: %s' % y_mean_test, fontsize=9)
            plt.gcf().text(0.6, 0.35, 'Oot1 data sample: %s' % count_oot1, fontsize=9)
            plt.gcf().text(0.6, 0.30, 'Oot1 data eventR: %s' % y_mean_oot1, fontsize=9)
            plt.gcf().text(0.6, 0.25, 'Oot2 data sample: %s' % count_oot2, fontsize=9)
            plt.gcf().text(0.6, 0.20, 'Oot2 data eventR: %s' % y_mean_oot2, fontsize=9)

        plt.axhline(y=df[y_true].mean(), color='k', linestyle='-.', label='eventR_all')
        plt.axhline(y=df.loc[df[feature] == default_value, y_true].mean(), color='r', linestyle='--', label='eventR_dft')
        plt.gcf().text(0.6, 0.7, 'Categorical value:', fontsize=9)
        plt.gcf().text(0.6, 0.65, 'Feature grid: %s' % [str(int(x)) for x in feature_grid], fontsize=9)
        plt.subplots_adjust(right=0.59)


    else:  # 特征元素比较多的时候,采用等频分箱.
        feature_grid = sorted(list(set(df.loc[idx_not_default, feature].
                                       describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9])[3:].values)))
        #feature_grid[-1] = feature_grid[-1] + 1
        feature_grid.append(max(df[feature]))
        df['bins'] = -99999
        bins = pd.cut(df.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
        df.loc[idx_not_default, 'bins'] = bins
        df.loc[idx_not_default, 'bin_no'] = bins.cat.codes
        g_df = df[idx_not_default].groupby(['bins', 'bin_no'],observed=True)[y_true].agg({'mean', 'count', 'sum'})
        g_df.rename(columns={'mean': 'allEvntR', 'count': 'allSpl', 'sum': 'allEvnt'}, inplace=True)

        if draw_train_test:
            # Train sample
            dftrain['bins'] = -99999
            bins = pd.cut(dftrain.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
            dftrain.loc[idx_not_default, 'bins'] = bins
            dftrain.loc[idx_not_default, 'bin_no'] = bins.cat.codes
            g_dtrain = dftrain[idx_not_default].groupby(['bins', 'bin_no'])[y_true].agg({'mean', 'count', 'sum'})
            g_dtrain.rename(columns={'mean': 'trEvntR', 'count': 'trSpl', 'sum': 'trEvnt'}, inplace=True)

            # Test sample
            dftest['bins'] = -99999
            bins = pd.cut(dftest.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
            dftest.loc[idx_not_default, 'bins'] = bins
            dftest.loc[idx_not_default, 'bin_no'] = bins.cat.codes
            g_dtest = dftest[idx_not_default].groupby(['bins', 'bin_no'])[y_true].agg({'mean', 'count', 'sum'})
            g_dtest.rename(columns={'mean': 'teEvntR', 'count': 'teSpl', 'sum': 'teEvnt'}, inplace=True)

            # Oot sample
            dfoot1['bins'] = -99999
            bins = pd.cut(dfoot1.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
            dfoot1.loc[idx_not_default, 'bins'] = bins
            dfoot1.loc[idx_not_default, 'bin_no'] = bins.cat.codes
            g_doot1 = dfoot1[idx_not_default].groupby(['bins', 'bin_no'])[y_true].agg({'mean', 'count', 'sum'})
            g_doot1.rename(columns={'mean': 'oot1EvntR', 'count': 'oot1Spl', 'sum': 'oot1Evnt'}, inplace=True)

            # Oot2 sample
            dfoot2['bins'] = -99999
            bins = pd.cut(dfoot2.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
            dfoot2.loc[idx_not_default, 'bins'] = bins
            dfoot2.loc[idx_not_default, 'bin_no'] = bins.cat.codes
            g_doot2 = dfoot2[idx_not_default].groupby(['bins', 'bin_no'])[y_true].agg({'mean', 'count', 'sum'})
            g_doot2.rename(columns={'mean': 'oot2EvntR', 'count': 'oot2Spl', 'sum': 'oot2Evnt'}, inplace=True)

            g_all = pd.concat([g_df, g_dtrain, g_dtest, g_doot1, g_doot2], axis=1)
        else:
            g_all = g_df
        g_all = g_all.sort_index(level=1)
        #g_all = g_all.sortlevel(1)  # 按区间排序.

        if len(feature_grid) != len(g_all['allEvntR']) + 1:
            strss = '\n有的分段内没有数据！！！-----------------------------------'
        else:
            strss = '\n'
        print(strss)

        # 画图.
        fig = plt.figure(1)
        x_index = list(g_all.index.get_level_values('bin_no'))
        if draw_all:
            plt.plot(x_index, g_all['allEvntR'], 'bo-', label='%s' % 'all')

        if draw_train_test:
            plt.plot(x_index, g_all['trEvntR'], 'co-', label='%s' % 'train')
            plt.plot(x_index, g_all['teEvntR'], 'mo-', label='%s' % 'test')
            plt.plot(x_index, g_all['oot1EvntR'], 'go-', label='%s' % 'oot1')
            plt.plot(x_index, g_all['oot2EvntR'], 'yo-', label='%s' % 'oot2')


        plt.axhline(y=df[y_true].mean(), color='k', linestyle='-.', label='eventR_all')
        plt.axhline(y=df.loc[df[feature] == default_value, y_true].mean(), color='r', linestyle='--', label='eventR_dft')
        plt.gcf().text(0.6, 0.7, '%s' % strss, fontsize=10)
        plt.gcf().text(0.6, 0.3, '%s' % g_all, fontsize=10)
        plt.subplots_adjust(right=0.59)
        plt.subplots_adjust(right=0.59)

    plt.title('Univariate Chart of %s' % feature)
    plt.ylabel('event r ate')
    plt.legend(fontsize=10, loc=4, framealpha=0.5)
    plt.grid()
    #plt.show()
    #return fig


def get_feature_importance(df_importance):
    feature_importance = df_importance.sort_values("importance", ascending=False).reset_index()
    f_imp_sorted_list = feature_importance['varName'].values.tolist()

    x = feature_importance.loc[:19, 'varName']
    y = feature_importance.loc[:19, 'importance']

    plt.bar(x, y)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("f_imp.png")

    return f_imp_sorted_list
