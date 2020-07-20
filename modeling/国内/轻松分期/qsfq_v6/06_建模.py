"""
201908轻松分期建模数据清理
@author: yuexin, mingqing
"""
import datetime
import os
import pandas as pd
import numpy as np
import sys
from jinja2 import Template
import  logging

sys.path.append('/Users/Mint/Desktop/repos/genie')

import matplotlib
matplotlib.use('TkAgg')
import utils3.plotting as pl
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *
from utils3.data_io_utils import *

data_path = 'D:/Model/201908_qsfq_model/01_data/'
result_path = 'D:/Model/201908_qsfq_model/02_result/'

x_with_y.order_no = x_with_y.order_no.astype(str)
x_with_y4 = pd.read_excel(os.path.join(data_path, 'x_with_y_v4.xlsx'))
x_with_y4.to_excel(os.path.join(data_path,'x_with_y_v4.xlsx'))

x_with_y2.index = x_with_y2.order_no
x_with_y2 = x_with_y4.loc[x_with_y4.sample_set != 'train_new']

x_with_y2.shape

x_with_y2.sample_set.value_counts()

var_dict = pd.read_excel('D:/Model/201908_qsfq_model/建模代码可用变量字典.xlsx')
var_dict.columns

set(x_with_y2.columns) - set(var_dict.指标英文.unique())
set(var_dict.指标英文.unique()) - set(x_with_y.columns)
x_cols = list(set(x_with_y2.columns).intersection(set(var_dict.指标英文.unique())))
len(x_cols)

y_cols = list(set(x_with_y2.columns) - set(var_dict.指标英文.unique()))
len(y_cols)

x_with_y2[x_cols] = x_with_y2[x_cols].astype(float)

"""
EDA
"""
X = x_with_y2[x_cols]
X.dtypes.value_counts()


len(x_cols)

ss.eda(X, var_dict, useless_vars=[], exempt_vars=[],data_path=result_path, save_label='all')

summary = pd.read_excel(os.path.join('D:/Model/201908_qsfq_model/02_result_all/all_variables_summary_add_yl.xlsx'), encoding='utf-8')
set(summary.指标英文.unique()) - set(X.columns)

exclude_after_eda = summary.loc[summary.exclusion_reason.notnull(), '指标英文'].tolist()
len(exclude_after_eda)

#
x_with_y2.order_no = x_with_y2.order_no.astype(str)
x_with_y2 = x_with_y2.replace(-1,-8887)
x_with_y2 = x_with_y2.replace(-8887,-1)

(x_with_y2 == -8887).sum().sum()

"""
划分训练&验证
"""

x_with_y2.perf_flag.value_counts(dropna = False)

data_train = x_with_y2[x_with_y2.sample_set == 'train']
data_test = x_with_y2[x_with_y2.sample_set == 'test']
data_test_new = x_with_y2[x_with_y2.sample_set == 'test_new']


"""
子模型
"""
result_path = 'D:/Model/201908_qsfq_model/02_result_td/'
result_path = 'D:/Model/201908_qsfq_model/02_result_xy/'
result_path = 'D:/Model/201908_qsfq_model/02_result_jxl/'
result_path = 'D:/Model/201908_qsfq_model/02_result_yl/'

#基本信息, 新颜, 聚信立,天启, 腾讯—天御分, 同盾, 银联
td_cols = list(set(var_dict.loc[var_dict.数据源 == '同盾','指标英文']) - set(exclude_after_eda))
len(td_cols)
xy_cols = list(set(var_dict.loc[var_dict.数据源 == '新颜','指标英文']) - set(exclude_after_eda))
len(xy_cols)
jxl_cols = list(set(var_dict.loc[var_dict.数据源 == '聚信立','指标英文']) - set(exclude_after_eda))
len(jxl_cols)
base_cols = list(set(var_dict.loc[var_dict.数据源 == '基本信息','指标英文']) - set(exclude_after_eda))

yl_cols = list(set(var_dict.loc[var_dict.数据源 == '银联','指标英文']) - set(exclude_after_eda))






#train
X_train_df = data_train.loc[data_train.perf_flag.isin([0,1])].drop(y_cols,1)
X_train_df = X_train_df[yl_cols]

y_train_df = data_train.loc[data_train.perf_flag.isin([0,1])].perf_flag

X_train_df.shape #2264
y_train_df.shape




#test
X_test_df = data_test.drop(y_cols,1)[yl_cols]

y_test_df = data_test.perf_flag.replace(9,0)
X_test_df.shape #1250

#test new
X_test_df_new = data_test_new.drop(y_cols,1)[jxl_cols]
y_test_df_new = data_test_new.perf_flag


X_test_df_new.shape #3723

#
X_train = X_train_df.values
y_train = y_train_df.values

X_test = X_test_df.values
X_test.shape
y_test = y_test_df.values

X_test_new = X_test_df_new.values
X_test_new.shape
y_test_new = y_test_df_new.values


"""
IV
"""
args_dict = {
    'random_forest': {
        'grid_search': False,#选择了True则会进行网格筛选速度会比较慢
        'param': None
    },
    'xgboost': {
        'grid_search': False,
        'param': None
    }
}
methods = [
    #'random_forest',
    #'lasso',
    #'xgboost'
]

fs_obj = fs.FeatureSelection()
logging.basicConfig(filename=os.path.join(result_path, 'test_log.log'), level=logging.INFO, filemode='w')
LOG = logging.getLogger(__name__)


X_cat_train,X_transformed, woe_iv_df, rebin_spec, ranking_result = fs_obj.overall_ranking(X_train_df, y_train_df, var_dict, args_dict, methods, num_max_bins=5)

#rebin_spec = mu.convert_rebin_spec2XGB_rebin_spec(rebin_spec)

woe_iv_df.to_excel(os.path.join(result_path,'woe_iv_df_add_yl.xlsx'))
ranking_result.to_excel(os.path.join(result_path,'ranking_result_add_yl.xlsx'))
save_data_to_pickle(rebin_spec,result_path,'rebin_spec_add_yl.pkl')

save_data_to_pickle(X_cat_train,result_path,'X_cat_train_add_yl.pkl')

bin_obj = mt.BinWoe()
X_cat_test = bin_obj.convert_to_category(X_test_df, var_dict, rebin_spec)
X_cat_test.shape
X_cat_train = bin_obj.convert_to_category(X_train_df, var_dict, rebin_spec)

X_cat_test_new = bin_obj.convert_to_category(X_test_df_new, var_dict, rebin_spec)


"""
按训练验证看稳定性
"""

#data_train['appmon'] = data_train.effective_date.apply(lambda x:str(x)[0:7])
data_train['appmon'] = '0'
data_train.appmon.value_counts()

X_cat_train_with_y_appmon = pd.merge(X_cat_train,data_train.loc[data_train.perf_flag.isin([0,1])][['perf_flag','appmon']] ,left_index=True,right_index=True)

var_dist_badRate_by_time = ss.get_badRate_and_dist_by_time(X_cat_train_with_y_appmon,x_cols,'appmon','perf_flag')
#var_dist_badRate_by_time.to_excel(os.path.join(result_path,'var_dist_badRate_by_time_train.xlsx'))

#test
data_test['appmon'] = data_test.effective_date.apply(lambda x:str(x)[0:7])
data_test['appmon'] = '1'
data_test.appmon.value_counts()
X_cat_test_with_y_appmon = pd.merge(X_cat_test,data_test[['perf_flag','appmon']].replace(9,0) ,left_index=True,right_index=True)
var_dist_badRate_by_time_test = ss.get_badRate_and_dist_by_time(X_cat_test_with_y_appmon,x_cols,'appmon','perf_flag')

#var_dist_badRate_by_time_test.to_excel(os.path.join(result_path,'var_dist_badRate_by_time_test.xlsx'))

#test_new
data_test_new['appmon'] = data_test_new.effective_date.apply(lambda x:str(x)[0:7])
data_test_new['appmon'] = '2'
data_test_new.appmon.value_counts()
X_cat_test_new_with_y_appmon = pd.merge(X_cat_test_new,data_test_new[['perf_flag','appmon']].replace(9,0) ,left_index=True,right_index=True)

var_dist_badRate_by_time_test_new = ss.get_badRate_and_dist_by_time(X_cat_test_new_with_y_appmon,x_cols,'appmon','perf_flag')

all_cat = pd.concat([X_cat_train,X_cat_test,X_cat_test_new])
app_data = pd.concat([data_train[['perf_flag','appmon']],data_test[['perf_flag','appmon']].replace(9,0), data_test_new[['perf_flag','appmon']]])

all_cat = pd.concat([X_cat_train,X_cat_test])
app_data = pd.concat([data_train[['perf_flag','appmon']],data_test[['perf_flag','appmon']].replace(9,0)])

X_cat_with_y_appmon_all = pd.merge(all_cat,app_data[['perf_flag','appmon']] ,left_index=True,right_index=True)

var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,x_cols,'appmon','perf_flag')
var_dist_badRate_by_time_all.to_excel(os.path.join(result_path,'var_dist_badRate_by_time_all_add_yl.xlsx'))


# CALCULATE PSI
# var_dist_badRate_by_time_all = pd.read_excel(os.path.join(result_path,'var_dist_badRate_by_time_all.xlsx'))
# var_dist_badRate_by_time_all.head()
#
# var_dist_badRate_by_time_all['PSI_index'] = (var_dist_badRate_by_time_all['dist_train']- var_dist_badRate_by_time_all['dist_test'])*np.log(var_dist_badRate_by_time_all['dist_train']/ var_dist_badRate_by_time_all['dist_test'])
#
# var_psi = pd.DataFrame(var_dist_badRate_by_time_all.groupby(['varName'])['PSI_index'].agg('sum')).reset_index().rename(columns = {'PSI_index':'PSI'})
# var_psi.head()
#
# #features_stable = list(var_psi[var_psi['PSI']<0.1]['varName'])
# features_unstable = list(var_psi[var_psi['PSI']>=0.1]['varName'])
# len(features_unstable) #40
# features_unstable
#
# var_dist_badRate_by_time_all = var_dist_badRate_by_time_all.merge(var_psi, on = 'varName')
# var_dist_badRate_by_time_all.head()
# var_dist_badRate_by_time_all.to_excel(os.path.join(result_path,'var_dist_badRate_by_time_all.xlsx'))
# var_dist_badRate_by_time_all = pd.read_excel(os.path.join(result_path,'var_dist_badRate_by_time_all.xlsx'))
# features_unstable = list(set(var_dist_badRate_by_time_all[var_dist_badRate_by_time_all['PSI']>=0.1]['varName']))


"""
相关性
"""
corr_threshold = 0.7 # 可调
vif_threshold = 10 # 可调

X_transformed_train = X_transformed

vif_result = fs.Colinearity(X_transformed_train,result_path)
vif_result.run(ranking_result)
corr_exclude = vif_result.corr_exclude_vars
len(corr_exclude)
len(vif_result.vif_dropvars)


##################################
#初筛变量                         #
#################################

#fit model on taining data
model = XGBClassifier()
model.fit(X_train, y_train)
print(model)

#make prediction for test data
y_pred = model.predict(X_test)
predictions = [round(value) for value in y_pred]

#evaluate predictions
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0))  #73.23  61.66

#print(model.feature_importances_)

#plot_importance(model, max_num_features =30)
#pyplot.show()

#利用特征重要性筛去一部分无用的变量

all_features = list(X_train_df.columns)

#feature_importance = model.feature_importances_

# features_in_model = all_features
# while(min(feature_importance)<0.001):
#     features_in_model = [features_in_model[i] for i in range(len(feature_importance)) if feature_importance[i] > 0.001]

#all_features = [i for i in all_features if i not in  y_cols and not in drop_cols]


feature_importance = model.feature_importances_
features_in_model = all_features
len(feature_importance)
len(features_in_model)

len(set(features_in_model))

feature_importance_df = pd.DataFrame(feature_importance).rename(columns = {0:'importance'})
features_in_model_df = pd.DataFrame(features_in_model).rename(columns = {0:'feature'})

variable_importance = pd.concat([features_in_model_df,feature_importance_df],1)
variable_importance = variable_importance.merge(ranking_result, left_on = 'feature', right_on = '指标英文',how = 'left')

ranking_result = pd.read_excel('D:/Model/201908_qsfq_model/02_result/ranking_result.xlsx')
ranking_result = pd.read_excel('D:/Model/201908_qsfq_model/02_result_yl/ranking_result_add_yl.xlsx')

variable_importance.to_excel(os.path.join(result_path,'feature_importance_0924_v2.xlsx'))
variable_importance = variable_importance.sort_values('importance', ascending = False)
variable_importance.tail()

top_n = set(variable_importance[(variable_importance.IV> 0.003)]['feature']) #143 final

top_n = set(variable_importance[(variable_importance.importance> 0.001)]['feature']) #83

top_n = variable_importance.sort_values('IV')['指标英文'].iloc[:100].tolist()

#top_n = set(variable_importance[(variable_importance.importance> 0.001) & (variable_importance.IV>0.04)]['指标英文']) #25 , 76var
top_n = set(variable_importance[(variable_importance.importance> 0.001) & (variable_importance.IV>0.0)]['指标英文']) #100 15%, 9%

len(top_n)

#聚信立rm unstabale变量

jxl_var_psi = pd.read_excel('D:/Model/201908_qsfq_model/02_result_all/var_dist_badRate_by_time_all.xlsx', sheet_name = 'Sheet3')
unstable_jxl = list(jxl_var_psi.loc[(jxl_var_psi['PSI_train_test_new'] > 0.5) & (jxl_var_psi.数据源=='聚信立')]['指标英文']) #22

td_rm = ['r47580334_m_industry_1_ct','r47580344_m_industry_22_ct','r47580344_m_industry_1_ct','r47580334_id_industry_1_ct','r47580344_id_industry_22_ct'  #变量缺失太高
         ,'r47580334_id_industry_15_ct', 'r47580334_m_industry_15_ct']

td_var = ['r47580334_m_platform_ct'
,'r47580344_m_industry_18_ct'
,'r47580344_id_industry_15_ct'
,'r47580344_m_industry_9_ct'
,'r47580344_id_industry_9_ct'
,'r47580334_id_platform_ct'
,'r47580344_m_industry_15_ct'
,'r47580344_id_industry_18_ct'
,'r47580334_id_industry_9_ct'
,'r47580344_m_platform_ct'
,'r47580334_m_industry_9_ct'
,'r47580334_id_industry_18_ct'
,'r47580334_m_industry_18_ct'
,'r47580344_id_platform_ct'
]

xy_rm = ['loans_cash_count']

yl_rm = ['YLZC035','TSJY033','YLZC053','TSJY009','TSJY020']

features_in_model = [i for i in top_n if i ]
features_in_model = [i for i in top_n if i not in unstable_jxl]

#features_in_model= [i for  i in cols_filter['final_kept'] if  i not in to_remove]
len(features_in_model)


# pd.DataFrame(features_in_model).to_excel(os.path.join(result_path,'features_in_model.xlsx'))
# var_dist = pd.read_excel(os.path.join(result_path_c,'var_dist_badRate_by_time_all.xlsx'))
# var_dist_in_model = var_dist.loc[var_dist.varName.isin(features_in_model)]
# var_dist_in_model.to_excel(os.path.join(result_path_c,'var_dist_in_model_all_v2.xlsx'))

##################################
#         SAVE FEATURES          #
##################################
#feature_importance_list = list(zip(all_features, model.feature_importances_))
#pd.DataFrame(feature_importance_list).to_csv(filepath_out + "feature_importance.csv")
#pd.DataFrame(features_in_model).to_csv(filepath_out + "features_in_model.csv",index=False,sep=',')


##################################
#调参                            #
#################################

data_train_lean = X_train_df[features_in_model].values
data_test_lean = X_test_df[features_in_model].values
data_test_lean_new = X_test_df_new[features_in_model].values

#data_test.flag_credit.value_counts()

# X_train = data_train_lean.values
# X_test = data_test_lean.values
data_train_lean.shape
param_test1 = {'max_depth':range(2,7,2)}
#param_test1 = {'max_depth':range(1,2,3)}
gsearch1 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, gamma=0, subsample=0.8,
                                                  colsample_bytree=0.8, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test1, scoring='precision',n_jobs=4,iid=False,cv=5)
                        #param_grid = param_test1, scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch1.fit(data_train_lean,y_train)
best_max_depth = gsearch1.best_params_['max_depth']
best_max_depth
#best_max_depth = 1

param_test10 = {'min_child_weight':range(1,6,2)}
#param_test10 = {'min_child_weight':range(1,3)}

gsearch10 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, gamma=0, subsample=0.8,
                                                 max_depth =4, colsample_bytree=0.8, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test10, scoring='precision',n_jobs=4,iid=False,cv=5)
gsearch10.fit(data_train_lean,y_train)
best_min_child_weight = gsearch10.best_params_['min_child_weight']
best_min_child_weight
#best_min_child_weight = 5

param_test2 = {'gamma':[i/10.0 for i in range(0,5)]}
#param_test2 = {'gamma':[0,0.05,0.1]}

gsearch2 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, subsample=0.8, colsample_bytree=0.8, max_depth= best_max_depth,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test2,scoring='precision',n_jobs=4,iid=False,cv=5)
gsearch2.fit(data_train_lean,y_train)
best_gamma = gsearch2.best_params_['gamma']
best_gamma
#best_gamma = 0.3

param_test3 = {'subsample':[i/10.0 for i in range(5,8)]}
gsearch3 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test3,scoring='auo',n_jobs=4,iid=False,cv=5)
gsearch3.fit(data_train_lean,y_train)
best_subsample = gsearch3.best_params_['subsample']
best_subsample
best_subsample = 1
#best_subsample = 0.5


param_test30 = {'colsample_bytree':[i/10.0 for i in range(6,10)]}
gsearch30 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test30,scoring='precision',n_jobs=4,iid=False,cv=5)
gsearch30.fit(data_train_lean,y_train)
best_colsample_bytree = gsearch30.best_params_['colsample_bytree']
best_colsample_bytree
best_colsample_bytree = 1

param_test4 = {'reg_alpha':[0.1,1,10,50, 100]}
gsearch4 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test4,scoring='precision',n_jobs=4,iid=False,cv=5)
gsearch4.fit(data_train_lean,y_train)
best_reg_alpha = gsearch4.best_params_['reg_alpha']
best_reg_alpha
#best_reg_alpha = 0.3

param_test5 = {'n_estimators':range(50,500,50)}
gsearch5 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1,  max_depth= best_max_depth, gamma=best_gamma,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,reg_alpha=best_reg_alpha,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test5,scoring='precision',n_jobs=4,iid=False,cv=5)
gsearch5.fit(data_train_lean,y_train)
best_n_estimators = gsearch5.best_params_['n_estimators']       #300
best_n_estimators
#best_n_estimators = 400

param_test6 = {'learning_rate':[0.01,0.1,0.2,0.3]}
param_test6 = {'learning_rate':[0.01,0.03,0.05,0.1,0.2,0.3]}
gsearch6 = GridSearchCV(estimator = XGBClassifier(max_depth= best_max_depth, gamma=best_gamma, n_estimators = best_n_estimators,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,reg_alpha=best_reg_alpha,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test6,scoring='precision',n_jobs=4,iid=False,cv=5)
gsearch6.fit(data_train_lean,y_train)
best_learning_rate = gsearch6.best_params_['learning_rate']
best_learning_rate
#best_learning_rate = 0.1

#用获取得到的最优参数再次训练模型
best_xgb = XGBClassifier(learning_rate =best_learning_rate, n_estimators=best_n_estimators, max_depth= best_max_depth, gamma=best_gamma,
                         colsample_bytree = best_colsample_bytree, subsample = best_subsample, reg_alpha=best_reg_alpha,
                         min_child_weight=best_min_child_weight,
                         objective= 'binary:logistic',nthread=4,scale_pos_weight=1
                         ,eval_metric='auc', seed=27)
best_xgb.fit(data_train_lean,y_train)

##################################
#             TRAIN              #
#################################

#data_train = data_all[pd.to_datetime(data_all['effective_date'])<='2019-01-08']
#data_train.shape

#data_train_lean = data_train[features_in_model]
#data_train_lean.shape

#data_train.loc[data_train.ever30_flag4.isin([1]),'Y'] = 1
#data_train.loc[data_train.ever0_flag4.isin([1]),'Y'] = 0
#data_train.loc[data_train.Y.isnull(),'Y'] = 9

#X_train = data_train_lean.values
#y_train = data_train.Y.astype(int)   # 2 for credit, 3 for fraud
#y_train = data_train.flag_fraud.astype(int)   # 2 for credit, 3 for fraud
#X_train.shape; len(y_train)

data_train_lean = data_train[features_in_model].values
y_train = data_train.flag_credit.replace(9,0)

y_train_pred = best_xgb.predict_proba(data_train_lean)[:,1]
roc_auc_score(y_train, y_train_pred)   #0.90  0.9917  1

predictions = [round(value) for value in y_train_pred]
accuracy = accuracy_score(y_train, predictions)
print("Accuracy: %.4f%%" % (accuracy * 100.0))  # 81.46%  93.5053%

score_train = [round(Prob2Score(value, 600, 20)) for value in y_train_pred]

decile_score = pd.cut(pd.DataFrame(score_train)[0], bins = train_bin, precision=0).astype(str)
decile_score = pd.qcut(pd.DataFrame(score_train)[0], q=10, duplicates='drop', precision=0).astype(str)
decile_score.value_counts().sort_index()


#data_scored_train = pd.DataFrame([data_train.values[:,0], data_train.values[:,5], y_train, score_train]).T
data_scored_train = pd.DataFrame([data_train.index, data_train.effective_date, y_train, score_train,y_train_pred,decile_score]).T

data_scored_train = data_scored_train.rename(columns = {0:'order_no',1:'effective_date',2:'Y',3:'score',4:'y_pred',5:'score_bin'})
data_scored_train['score'] = data_scored_train['score'].astype(float)
data_scored_train['Y'] = data_scored_train['Y'].astype(float)

train_ks = calculate_ks_by_decile(data_scored_train['score'],data_scored_train['Y'],'decile', q=10)
train_ks
train_ks.分箱.sort_index()
decile_score.value_counts().sort_index()
data_scored_train.head()
#data_scored_train.to_csv(result_path + "data_scored_train_td_mx_cc.csv",index=False,sep=',')
data_scored_train.score_bin.value_counts().sort_index()
##################################
#             TEST               #
##################################
y_test_pred = best_xgb.predict_proba(data_test_lean)[:,1]
roc_auc_score(y_test, y_test_pred)   #0.67   0.6827

predictions = [round(value) for value in y_test_pred]
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.4f%%" % (accuracy * 100.0))  # 71.39%   70.9020% 62

score_test = [round(Prob2Score(value, 600, 20)) for value in y_test_pred]

data_scored_test = pd.DataFrame([data_test.index, data_test.effective_date, y_test, y_test_pred, score_test]).T
#data_scored_test = pd.DataFrame([data_test.values[:,0], data_test.effective_date, data_test.ever30, score_test]).T
data_scored_test = data_scored_test.rename(columns = {0:'order_no',1:'effective_date',2:'Y', 3:'y_pred' ,4:'score'})

train_ks.分箱.sort_index()

train_bin = [400, 570, 590, 602, 613, 621,627,632,638,647, 750]
train_bin = [400, 601, 608, 614, 616, 619,620,621,622, 623,750]
train_bin = [400, 564, 588, 600, 610, 620,627,633,641, 652,750]

data_scored_test['score_bin'] = pd.cut(data_scored_test['score'], bins = train_bin)

data_scored_test['score'] = data_scored_test['score'].astype(float)
data_scored_test['Y'] = data_scored_test['Y'].astype(float)
data_scored_test.head()
data_scored_test.score.min();data_scored_test.score.max()


test_ks = calculate_ks_by_decile(data_scored_test.score,data_scored_test.Y,'decile', manual_cut_bounds = train_bin)
test_ks

#######################
#       test_new      #
#######################
y_test_pred_new = best_xgb.predict_proba(data_test_lean_new)[:,1]
roc_auc_score(y_test_new, y_test_pred_new)   #0.67   0.6827

predictions = [round(value) for value in y_test_pred_new]
accuracy = accuracy_score(y_test_new, predictions)
print("Accuracy: %.4f%%" % (accuracy * 100.0))  # 71.39%   70.9020% 62

score_test_new = [round(Prob2Score(value, 600, 20)) for value in y_test_pred_new]

data_scored_test_new = pd.DataFrame([data_test_new.index, data_test_new.effective_date, y_test_new, y_test_pred_new,score_test_new]).T

#data_scored_test = pd.DataFrame([data_test.values[:,0], data_test.effective_date, data_test.ever30, score_test]).T
data_scored_test_new = data_scored_test_new.rename(columns = {0:'order_no',1:'effective_date',2:'Y', 3:'y_pred',4:'score'})

data_scored_test_new['score_bin'] = pd.cut(data_scored_test_new['score'], bins = train_bin).astype(str)
data_scored_test_new['score_bin_decile'] = pd.qcut(data_scored_test_new['score'], q = 10,duplicates='drop', precision=0).astype(str)

data_scored_test_new['score'] = data_scored_test_new['score'].astype(float)
data_scored_test_new['Y'] = data_scored_test_new['Y'].astype(float)
data_scored_test_new.head()
data_scored_test_new.score.min();data_scored_test_new.score.max()


test_ks_new = calculate_ks_by_decile(data_scored_test_new.score,data_scored_test_new.Y,'decile', manual_cut_bounds = train_bin)
test_ks_new

test_ks_new_self = calculate_ks_by_decile(data_scored_test_new.score,data_scored_test_new.Y,'decile', q=10)
test_ks_new_self


pd.DataFrame(features_in_model).to_csv(os.path.join(result_path, 'features_in_model.csv'))

#OUTPUT
data_scored_train.score_bin = data_scored_train.score_bin.astype(str)
data_scored_test.score_bin = data_scored_test.score_bin.astype(str)
data_scored_test_new.score_bin = data_scored_test_new.score_bin.astype(str)

#best predictor 变量importance
feature_importance_best = best_xgb.feature_importances_
len(feature_importance_best)


feature_importance_df_best = pd.DataFrame(feature_importance_best).rename(columns = {0:'importance'})
features_in_model_df_best = pd.DataFrame(features_in_model).rename(columns = {0:'feature'})

variable_importance_best = pd.concat([features_in_model_df_best,feature_importance_df_best],1)


writer = pd.ExcelWriter(os.path.join(result_path, 'result_0924_23.xlsx'))
train_ks.to_excel(writer, 'train_ks', index=True)
test_ks.to_excel(writer, 'test_ks', index=True)
test_ks_new.to_excel(writer, 'test_ks_new', index=True)
test_ks_new_self.to_excel(writer, 'test_ks_new_self', index=True)
data_scored_train.to_excel(writer, 'data_scored_train', index=True)
data_scored_test.to_excel(writer, 'data_scored_test',  index=True)
data_scored_test_new.to_excel(writer, 'data_scored_test_new',  index=True)
pd.DataFrame(features_in_model).to_excel(writer, 'features_in_model',  index=True)
variable_importance.to_excel(writer, 'variable_importance',  index=True)
variable_importance_best.to_excel(writer, 'variable_importance_best',  index=True)
writer.save()


##################################
# SAVE MODEL                     #
##################################
best_xgb.save_model(result_path + 'dc 0924 23.model')
best_xgb.save_model(result_path + 'dc 20190909 100.model')
best_xgb.save_model(result_path + 'dc 20190909 74.model')
best_xgb.save_model(result_path + 'dc 20190909 42.model')
best_xgb.save_model(result_path + 'dc 20190719 52v2.model')
best_xgb.save_model(result_path + 'dc 20190719 45.model')

def calculate_ks_by_decile(score, y, job, q=10, score_bin_size = 25, manual_cut_bounds=[], score_type='raw'):
        if score_type == 'raw':
            score = np.round(score)
            if job == 'decile':
                if len(manual_cut_bounds) == 0:
                    decile_score = pd.qcut(score, q=q, duplicates='drop', precision=0).astype(str) #, labels=range(1,11))
                else:
                    decile_score = pd.cut(score, manual_cut_bounds, precision=0).astype(str)
            if job == 'runbook':
                if len(manual_cut_bounds) == 0:
                    min_score = int(np.round(min(score)))
                    max_score = int(np.round(max(score)))
                    score_bin_bounardies = list(range(min_score, max_score, score_bin_size))
                    score_bin_bounardies[0] = min_score - 0.001
                    score_bin_bounardies[-1] = max_score
                    decile_score = pd.cut(score, score_bin_bounardies, precision=0).astype(str)
                else:
                    decile_score = pd.cut(score, manual_cut_bounds, precision=0).astype(str)
        else:
            decile_score = score.astype(str)
        r = pd.crosstab(decile_score, y).rename(columns={0: 'N_nonEvent', 1: 'N_Event'})
        if 'N_Event' not in r.columns:
            r.loc[:, 'N_Event'] = 0
        if 'N_nonEvent' not in r.columns:
            r.loc[:, 'N_nonEvent'] = 0
        r.index.name = None
        r.loc[:, 'N_sample'] = decile_score.value_counts()
        r.loc[:, 'EventRate'] = r.N_Event * 1.0 / r.N_sample
        r.loc[:, 'BadPct'] = r.N_Event * 1.0 / sum(r.N_Event)
        r.loc[:, 'GoodPct'] = r.N_nonEvent * 1.0 / sum(r.N_nonEvent)
        r.loc[:, 'CumBadPct'] = r.BadPct.cumsum()
        r.loc[:, 'CumGoodPct'] = r.GoodPct.cumsum()
        r.loc[:, 'KS'] = np.round(r.CumBadPct - r.CumGoodPct, 4)
        r.loc[:, 'odds(good:bad)'] = np.round(r.N_nonEvent*1.0 / r.N_Event, 1)
        r = r.reset_index().rename(columns={'index': '分箱',
                                            'N_sample': '样本数',
                                            'N_nonEvent': '好样本数',
                                            'N_Event': '坏样本数',
                                            'EventRate': '逾期率',
                                            'BadPct': 'Bad分布占比',
                                            'GoodPct': 'Good分布占比',
                                            'CumBadPct': '累积Bad占比',
                                            'CumGoodPct': '累积Good占比'
                                            })
        reorder_cols = ['分箱', '样本数', '好样本数', '坏样本数', '逾期率',\
                        'Bad分布占比', 'Good分布占比', '累积Bad占比', '累积Good占比',\
                        'KS', 'odds(good:bad)']
        result = r[reorder_cols]
        result.loc[:, '分箱'] = result.loc[:, '分箱'].astype(str)
        return result



def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))
#.map(lambda x: int(x))

round(Prob2Score(0.090300090611, 600, 20))