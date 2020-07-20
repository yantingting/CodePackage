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
import utils3.data_processing as dp

data_path = 'D:/Model/indn/202001_mvp_model/01_data/'
result_path = 'D:/Model/indn/202001_mvp_model/02_result/0326/'
result_path = 'D:/Model/indn/202001_mvp_model/02_result/0422/'

from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score


""" 读入数据 """
x_with_y = load_data_from_pickle(data_path,'x_with_y_v5.pkl') #修复了avg_extend_time变量
x_with_y.avg_extend_times = x_with_y.avg_extend_times.astype(float)

x_with_y.shape #25935
x_with_y.sample_set.value_counts()
x_with_y.index = x_with_y.loan_id


"""数据准备"""
x_with_y.rename(columns = {'effective_date':'applied_at', 'flag7':'label'}, inplace=True)
x_with_y['applied_from'] = 'UKU'
x_with_y['passdue_day'] = 0
x_with_y['applied_type'] = 0

x_with_y = x_with_y.loc[x_with_y.label.isin([0,1])]

#字典
var_dict = pd.read_excel('D:/Model/indn/202001_mvp_model/建模代码可用变量字典.xlsx')

y_cols = list(set(x_with_y.columns) - set(var_dict.指标英文)) #19
x_col = list(set(x_with_y.columns).intersection(var_dict.指标英文))

useless_var = list(var_dict.loc[var_dict['是否可以直接使用'] == '否','指标英文'])

x_with_y.phoneverify = x_with_y.phoneverify.replace('NOT_FOUND', -1 ).replace('INVALID_PHONE_NUMBER', -1 ).replace('INVALID_ID_NUMBER', -1 )

#增加APP子模型
app_score = pd.read_csv('D:/Model/indn/202001_mvp_model/APP_model/grid_58_200120_145938/grid_58_200120_145938_data_scored_all.csv')
app_score = load_data_from_pickle('D:/Model/indn/202001_mvp_model/APP_model/grid_58_200120_145938/','grid_58_200120_145938_data_scored_all.pkl')
app_score.shape
app_score.rename(columns = {'y_pred':'app_pred'}, inplace=True)

x_with_y = x_with_y.reset_index(drop=True)
x_with_y = x_with_y.merge(app_score[['order_no','app_pred']], left_on = 'loan_id', right_on = 'order_no')

x_with_y = x_with_y.drop(['order_no','app_pred'], 1)

"""""""""EDA"""""""""
X = x_with_y[x_col]

ss.eda(X = X, var_dict=var_dict, data_path = result_path, useless_vars = [],exempt_vars = [], save_label ='all', uniqvalue_cutoff=0.97)
summary = pd.read_excel(os.path.join(result_path, 'all_variables_summary.xlsx'), encoding='utf-8')

exclude_after_eda = summary.loc[summary.exclusion_reason == '缺失NA比例大于0.97', '指标英文'].tolist()
exclude_after_eda = summary.loc[summary.exclusion_reason.notnull(), '指标英文'].tolist()

#划分训练和验证
train_df = x_with_y[x_with_y.sample_set == 'train']
test_df = x_with_y[x_with_y.sample_set == 'test']
oot_df = x_with_y[x_with_y.sample_set == 'oot']

train_df.shape
train_df.label.value_counts(dropna = False)

test_df.shape
test_df.label.value_counts(dropna = False)

oot_df.shape
#去掉EDA变量
y_cols2 = [i for i in y_cols if i != 'label']

to_drop_cols = list(var_dict.loc[var_dict.数据源.isin(['program_device','program_gps','izi_phoneverify','izi_phoneinquiries']),'指标英文'])
len(to_drop_cols)

drop_var = list(set(useless_var).union(set(exclude_after_eda)).union(set(to_drop_cols)))

train = train_df.drop(y_cols2,1).drop(drop_var, 1)
test = test_df.drop(y_cols2,1).drop(drop_var,1)
oot = oot_df.drop(y_cols2,1).drop(drop_var,1)

X_train = train.drop(['label'], 1)
X_test = test.drop(['label'], 1)
X_oot = oot.drop(['label'], 1)

X_train = train.drop(['label','bins','bin_no'], 1)
X_test = test.drop(['label','bins','bin_no'], 1)
X_oot = oot.drop(['label','bins','bin_no'], 1)


set(X_train.columns) - set(var_dict['指标英文'])
set(X_test.columns) - set(var_dict['指标英文'])
set(X_oot.columns) - set(var_dict['指标英文'])

y_train = train.label
y_test = test.label
y_oot = oot.label

set(X_train.columns) - set(var_dict.指标英文)
set(var_dict.指标英文) - set(X_train.columns)

#检查数据量是否一致

X_train.shape[0] == y_train.shape[0]
X_test.shape[0] == y_test.shape[0]
X_test.shape[0] == y_test.shape[0]

for i in oot_df.columns:
    try:
        oot_df[i] = oot_df[i].astype(float)
    except:
        print(i)
        pass

"""""""Univariate Chart"""""""
import utils3.plot_tools as pt
import matplotlib.pyplot as plt

FIG_PATH = os.path.join(result_path, 'figure', 'UniVarChart_0401_everx')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

x_with_y[['cnt_loan','everx']]

for i in features_used['指标英文']:
    #try:
        x_with_y[i] = x_with_y[i].astype(float)
        train_df[i] = train_df[i].astype(float)
        test_df[i] = test_df[i].astype(float)
        oot_df[i] = oot_df[i].astype(float)
        univariate_chart_oot(x_with_y, i,  'everx', n_bins=10,default_value=-1,dftrain=train_df, dftest=test_df, dfoot=oot_df,
                         draw_all=True, draw_train_test=True)
        path = os.path.join(FIG_PATH,"uniVarChart_"+i+".png")
        plt.savefig(path,format='png', dpi=100)
        plt.close()
        print(i+': done')
    except:
        print(i+': fail')
        pass

save_data_to_pickle(x_with_y,'D:/Model/indn/202001_mvp_model/02_result/0326/','data_for_sj.pkl')
features_used['指标英文'].to_excel('D:/Model/indn/202001_mvp_model/02_result/0326/features.xlsx')

oot_df['everx']

oot_df['cnt_loan90']

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

woe_iv_df.to_excel(os.path.join(result_path,'woe_iv_df.xlsx'))
ranking_result.to_excel(os.path.join(result_path,'ranking_result.xlsx'))
#save_data_to_pickle(rebin_spec,result_path,'rebin_spec.pkl')
save_data_to_pickle(rebin_spec_bin_adjusted,result_path,'rebin_spec_bin_adjusted.pkl')

#save_data_to_pickle(X_cat_train,result_path,'X_cat_train.pkl')

bin_obj = mt.BinWoe()
X_cat_train = bin_obj.convert_to_category(X_train, var_dict, rebin_spec_bin_adjusted)
X_cat_test = bin_obj.convert_to_category(X_test, var_dict, rebin_spec_bin_adjusted)
X_cat_oot = bin_obj.convert_to_category(X_oot, var_dict, rebin_spec_bin_adjusted)


X_cat_test['14d']

"""按月/按sample_set检查变量的稳定性和有效性"""

## train
train_df['appmon'] = '0_train'

## test
test_df['appmon'] = '1_test'

#oot
oot_df['appmon'] = '2_oot'
## all
all_cat = pd.concat([X_cat_train,X_cat_test, X_cat_oot])
all_cat = all_cat.reset_index()

app_data = pd.concat([train_df[['label','appmon']],test_df[['label','appmon']], oot_df[['label','appmon']]])
X_cat_with_y_appmon_all = pd.merge(all_cat,app_data[['label','appmon']] ,left_index=True,right_index=True)

var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,x_cols,'appmon','label')
var_dist_badRate_by_time_all.to_excel(os.path.join(result_path, 'var_dist_badRate_by_sample_0209.xlsx'))

x_with_y.phone_age.value_counts()
x_with_y.loc[ x_with_y.customer_id == 344285282438258688].T

# PSI
var_psi = pf.variable_psi(X_cat_train, X_cat_test, var_dict)
var_psi.loc[:, 'psi_rank'] = var_psi.PSI.rank(ascending=False)
var_psi.head()

# 汇总各项指标
ranking_result = ranking_result.merge(var_psi, on='指标英文', how='left').merge(var_importance, on='指标英文', how='left')
ranking_result.sort_values('importance', ascending= False)
ranking_result.loc[ranking_result.指标英文 == 'appmon']

""" 变量筛选 """
train = train.drop(['bins','bin_no'],axis=1)
test = test.drop(['bins','bin_no'],axis=1)
oot = oot.drop(['bins','bin_no'],axis=1)

f_rmv = fs.feature_remove(train,test, ranking_result, result_path, psi = -1, iv = 0, imp = 0.004, corr = 0.75, slope = 'TRUE')

try:
    train = train.drop(['slope'], axis=1)
    test = test.drop(['slope'], axis=1)
except:
    pass

train_2 = train.drop(f_rmv, axis=1)
test_2 = test.drop(f_rmv, axis=1)
oot_2 = oot.drop(f_rmv, axis=1)

X_train = train_2.drop(['label','bins','bin_no','order_no'], axis=1)
y_train = train_2.label
X_test = test_2.drop(['label','bins','bin_no','order_no'], axis=1)
y_test= test_2.label
X_oot = oot_2.drop(['label','bins','bin_no','order_no'], axis=1)
y_oot = oot_2.label

# X_train = train_2.drop(['label'], axis=1)
# y_train = train_2.label
# X_test = test_2.drop(['label'], axis=1)
# y_test= test_2.label
# X_oot = oot_2.drop(['label'], axis=1)
# y_oot = oot_2.label

features_used = X_train.columns.tolist()

features_used = pd.read_excel(os.path.join('D:/Model/indn/202001_mvp_model/02_result/0205/','grid_46_200205-17.xlsx'), sheet_name = '04_model_importance')[['入模顺序','指标英文']]
features_used = features_used.loc[features_used['指标英文']!='lastapply_curds']
features_used = features_used.loc[features_used['指标英文']!='izi_telcom_AXIS']
features_used = features_used.loc[~features_used['指标英文'].isin(['cnt_loan30'])]


# features_used = features_used.sort_index(by = '入模顺序', ascending= True)
#
# features_used.append('app_pred')
#
X_train = train[list(features_used['指标英文'])]
y_train = train.label
X_test = test[list(features_used['指标英文'])]
y_test= test.label
X_oot = oot[list(features_used['指标英文'])]
y_oot = oot.label


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
model_importance['varName'] = features_used['指标英文']
model_importance['importance'] = best_importance



y_train_pred = best_xgb.predict_proba(X_train)[:,1]
y_test_pred = best_xgb.predict_proba(X_test)[:,1]
y_oot_pred = best_xgb.predict_proba(X_oot)[:,1]
# ### 打分&KS

train_pred = train_df.copy()
train_pred['y_pred'] = y_train_pred

test_pred = test_df.copy()
test_pred['y_pred'] = y_test_pred
train_pred.head()

oot_pred = oot_df.copy()
oot_pred['y_pred'] = y_oot_pred

reload(pf)
reload(mt)

import utils3.metrics as mt
pf = mt.Performance()
bw = mt.BinWoe()

data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, data_scored_test, test_proba_ks, test_proba_ks_20, test_score_ks, test_score_ks_20 = pf.data_score_KS(train_pred, test_pred, 'y_pred')
data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, data_scored_oot, oot_proba_ks, oot_proba_ks_20, oot_score_ks, oot_score_ks_20 = pf.data_score_KS(train_pred, oot_pred, 'y_pred')


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
auc_oot, acc_oot = auc_acc_table(oot_pred)

""" PDP(all) """

import utils3.plot_tools as pt
import matplotlib.pyplot as plt
import utils3.generate_report as gr

FIG_PATH = os.path.join(result_path, 'figure', 'PDP_43')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

f_imp_list = gr.get_feature_importance(model_importance)

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
auc_list = [auc_train, auc_test, auc_oot]
acc_list = [acc_train, acc_test, acc_oot]
split_list = ['train','test', 'oot']

df_auc_acc = pd.DataFrame({"sample_set":split_list,"auc":auc_list,"accuracy":acc_list})

## df_prediction
all_pred = pd.concat([train_pred, test_pred, oot_pred])
all_pred['loan_id'] = all_pred['loan_id'].astype(str)

## data_scored
data_scored_train['sample_set'] = "train"
data_scored_test['sample_set'] = "test"
data_scored_oot['sample_set'] = "oot"
data_scored_all = pd.concat([data_scored_train, data_scored_test, data_scored_oot])
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
                     'oot_proba_ks':oot_proba_ks,
                     'train_score_ks':train_score_ks,
                     'test_score_ks':test_score_ks,
                     'oot_score_ks':oot_score_ks,
                     'train_proba_ks_20':train_proba_ks_20,
                     'test_proba_ks_20':test_proba_ks_20,
                     'oot_proba_ks_20':oot_proba_ks_20,
                     'train_score_ks_20':train_score_ks_20,
                     'test_score_ks_20':test_score_ks_20,
                     'oot_score_ks_20':oot_score_ks_20
                      }
xgb_dict['04_model_importance'] = model_importance.reset_index()
xgb_dict['05_model_params'] = df_params
#xgb_dict['06_ranking_result'] = ranking_result
#xgb_dict['07_data_scored'] = data_scored_all
#xgb_dict['08_data_prediction'] = all_pred
#xgb_dict['09_woe_iv_train'] = woe_iv_df
#xgb_dict['09_woe_ivtest'] = woe_iv_df_test
#xgb_dict['09_woe_iv_testnew'] = woe_iv_df_testnew
#xgb_dict['10_var_dist_badRate'] = var_dist_badRate_by_time_all

from datetime import datetime
import utils3.filing as fl


FILE_NAME = "grid_%d_%s"%(len(features_used), datetime.now().strftime('%y%m%d-%H-%M'))
#FILE_NAME = "grid_%d"%(len(features_used))

best_xgb.save_model(os.path.join(result_path, FILE_NAME+'.model'))

fl.ModelSummary2Excel(result_path = result_path, fig_path= result_path, file_name = FILE_NAME +".xlsx", data_dic = xgb_dict).run()

data_scored_all.order_no = data_scored_all.order_no.astype(str)

data_scored_all = data_scored_all.merge(x_with_y[['loan_id','extend_times','applied_at']], left_on = 'order_no', right_index = True, how = 'left')

data_scored_all.to_excel(os.path.join(result_path, FILE_NAME + 'score' + '.xlsx'))

#data_scored_all = pd.read_excel(os.path.join(result_path, 'grid_46_200225-20score.xlsx'))
#data_scored_all = pd.read_excel(os.path.join(result_path, 'grid_46_200209-11score.xlsx'))


""" Lift Charts """
import utils3.plot_tools as pt
import matplotlib.pyplot as plt

#分train, test, oot 看模型的效果
FIG_PATH = os.path.join(result_path, 'figure', 'lifechart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

# lift_chart - train test oot
train_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'train'], 'y_pred','Y', n_bins = 10, feature_label='train')
test_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'test'], 'y_pred','Y', n_bins = 10, feature_label='test')
oot_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'oot'], 'y_pred','Y', n_bins = 10, feature_label='oot')
path = os.path.join(FIG_PATH, "LiftChart_" + '44' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()


pt.lift_chart_by_time(data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])],'y_pred','Y', n_bins = 10, by="week")
path = os.path.join(FIG_PATH, "LiftChart_" + 'by_week' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

#和线上模型对比效果
online_score = pd.read_excel('D:/Model/indn/202001_mvp_model/00_前期分析文档/all_score.xlsx')
online_score.dtypes
online_score.to_excel('D:/Model/indn/202001_mvp_model/00_前期分析文档/all_score.xlsx')

online_score['prob'] = mt.Performance().score_to_p(online_score['score'], PDO=20.0, Base=600, Ratio=1.0)

online_score.loan_id = online_score.loan_id.astype(str)
data_scored_all.loan_id = data_scored_all.loan_id.astype(str)

online_score = data_scored_all[['loan_id','Y','sample_set']].merge(online_score, left_on = 'loan_id', right_on = 'loan_id')
data_scored_all = online_score[['loan_id','customer_type']].merge(data_scored_all, left_on = 'loan_id', right_on = 'loan_id', how = 'right')

#和有Izi phoneverify的对比
grid_92_200205_17score = pd.read_excel(os.path.join(result_path, 'grid_92_200205-17score.xlsx'))
grid_46_200225_20score = pd.read_excel('D:/Model/indn/202001_mvp_model/02_result/0205/grid_46_200225-20score.xlsx')
grid_46_200225_20score.columns

#和V1对比
#test_92_17 = pt.show_result_new(grid_92_200205_17score.loc[grid_92_200205_17score.sample_set == 'test'], 'y_pred','Y', n_bins = 10, feature_label='new_model_test_phoneinquir')
#oot_92_17 = pt.show_result_new(grid_92_200205_17score.loc[grid_92_200205_17score.sample_set == 'oot'], 'y_pred','Y', n_bins = 10, feature_label='new_model_oot_phoneinquir')
test_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'test'], 'y_pred','Y', n_bins = 10, feature_label='new_model_test')
oot_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'oot'], 'y_pred','Y', n_bins = 10, feature_label='new_model_oot')
test_online_lc = pt.show_result_new(grid_46_200225_20score.loc[grid_46_200225_20score.sample_set == 'test'], 'y_pred','Y', n_bins = 10, feature_label='onlinev2model_test')
oot_online_lc = pt.show_result_new(grid_46_200225_20score.loc[grid_46_200225_20score.sample_set == 'oot'], 'y_pred','Y', n_bins = 10, feature_label='onlinev2model_oot')
path = os.path.join(FIG_PATH, "LiftChart_" + '46_onlinev2model' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

x_with_y.loc[x_with_y.loan_id == '381367605369864192']

#一键复贷
new_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])
                & (data_scored_all.customer_type == 'ONE_KEY_LOAN')], 'y_pred','Y', n_bins = 10, feature_label='new_model_ONE_KEY_LOAN')
old_lc = pt.show_result_new(online_score.loc[online_score.sample_set.isin(['oot','test'])
                & (online_score.customer_type == 'ONE_KEY_LOAN')], 'prob','Y', n_bins = 10, feature_label='old_model_ONE_KEY_LOAN')
new_modify_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])
                & (data_scored_all.customer_type != 'ONE_KEY_LOAN')], 'y_pred','Y', n_bins = 10, feature_label='new_model_modify')
old_modify_lc = pt.show_result_new(online_score.loc[online_score.sample_set.isin(['oot','test'])
                & (online_score.customer_type != 'ONE_KEY_LOAN')], 'prob','Y', n_bins = 10, feature_label='old_model_modify')
path = os.path.join(FIG_PATH, "LiftChart_" + 'one_key_loan' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

new_onekey_ks = mt.Performance().calculate_ks_by_decile(data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])
                & (data_scored_all.customer_type == 'ONE_KEY_LOAN')]['score']
                , data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])
                & (data_scored_all.customer_type == 'ONE_KEY_LOAN')]['Y'],'decile', q = 10)
new_onekey_20ks = mt.Performance().calculate_ks_by_decile(data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])
                & (data_scored_all.customer_type == 'ONE_KEY_LOAN')]['score']
                , data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])
                & (data_scored_all.customer_type == 'ONE_KEY_LOAN')]['Y'],'decile', q = 20)

new_modify_ks = mt.Performance().calculate_ks_by_decile(data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])
                & (data_scored_all.customer_type != 'ONE_KEY_LOAN')]['score'],
                data_scored_all.loc[data_scored_all.sample_set.isin(['oot', 'test']) & (data_scored_all.customer_type != 'ONE_KEY_LOAN')]['Y'],'decile', q = 10)

new_modify_20ks = mt.Performance().calculate_ks_by_decile(data_scored_all.loc[data_scored_all.sample_set.isin(['oot','test'])
                & (data_scored_all.customer_type != 'ONE_KEY_LOAN')]['score'],
                data_scored_all.loc[data_scored_all.sample_set.isin(['oot', 'test']) & (data_scored_all.customer_type != 'ONE_KEY_LOAN')]['Y'],'decile', q = 20)


writer = pd.ExcelWriter(os.path.join(result_path,'compare_ks_score_0331_18.xlsx'))
new_onekey_ks.append(new_onekey_20ks).to_excel(writer,'new_onekey_ks')
new_modify_ks.append(new_modify_20ks).to_excel(writer,'new_modify_ks')
writer.save()
