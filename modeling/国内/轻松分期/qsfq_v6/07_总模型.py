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

data_path = 'D:/Model/201908_qsfq_model/01_data/'
result_path = 'D:/Model/201908_qsfq_model/02_result/'

x_with_y4 = pd.read_excel(os.path.join(data_path, 'x_with_y_v4.xlsx'))
x_with_y6.to_excel(os.path.join(data_path,'x_with_y_v6.xlsx'))

br = pd.read_excel(os.path.join(data_path, 'bairong_0926.xlsx')).rename(columns = {'cus_num':'order_no'})

x_with_y2 = x_with_y4.merge(br, on = 'order_no', how = 'left')

x_with_y2.index = x_with_y2.order_no
x_with_y2 = x_with_y2.loc[x_with_y2.sample_set != 'train_new']

x_with_y2.shape

x_with_y2.sample_set.value_counts()

var_dict = pd.read_excel('D:/Model/201908_qsfq_model/建模代码可用变量字典.xlsx')
var_dict.columns

set(x_with_y2.columns) - set(var_dict.指标英文.unique())
x_cols = list(set(x_with_y2.columns).intersection(set(var_dict.指标英文.unique())))
len(x_cols)

y_cols = list(set(x_with_y2.columns) - set(var_dict.指标英文.unique()))
len(y_cols)

x_with_y2[x_cols] = x_with_y2[x_cols].astype(float)
x_with_y2 = x_with_y2.fillna(-1)
x_with_y2.isnull().sum().sum()

"""
划分训练&验证
"""

x_with_y6.perf_flag.value_counts(dropna = False)

data_train = x_with_y6[x_with_y6.sample_set == 'train']
data_test = x_with_y6[x_with_y6.sample_set == 'test']
data_test_new = x_with_y6[x_with_y6.sample_set == 'test_new']


"""
子模型
"""
result_path = 'D:/Model/201908_qsfq_model/02_result_td/'
result_path = 'D:/Model/201908_qsfq_model/02_result_xy/'
result_path = 'D:/Model/201908_qsfq_model/02_result_jxl/'
result_path = 'D:/Model/201908_qsfq_model/02_result_yl/'
result_path = 'D:/Model/201908_qsfq_model/02_result_all/'

#基本信息, 新颜, 聚信立,天启, 腾讯—天御分, 同盾, 银联
td_cols = list(set(var_dict.loc[var_dict.数据源 == '同盾','指标英文']) - set(exclude_after_eda))
len(td_cols)
xy_cols = list(set(var_dict.loc[var_dict.数据源 == '新颜','指标英文']) - set(exclude_after_eda))
len(xy_cols)
jxl_cols = list(set(var_dict.loc[var_dict.数据源 == '聚信立','指标英文']) - set(exclude_after_eda))
len(jxl_cols)
base_cols = list(set(var_dict.loc[var_dict.数据源 == '基本信息','指标英文']) - set(exclude_after_eda))

yl_cols = list(set(var_dict.loc[var_dict.数据源 == '银联','指标英文']) - set(exclude_after_eda))

tq_cols = list(set(var_dict.loc[var_dict.数据源 == '天启','指标英文']) - set(exclude_after_eda))

ty_cols = list(set(var_dict.loc[var_dict.数据源 == '腾讯—天御分','指标英文']) - set(exclude_after_eda))


########################################################################################################
#                              总模型                                                                  #
########################################################################################################

base_var = x_with_y2[base_cols].reset_index()
tq_var = x_with_y2[tq_cols].reset_index()
ty_var = x_with_y2[ty_cols].reset_index()

#ly
ly_train = pd.read_excel('D:/Model/201908_qsfq_model/02_result_yl/result_0919_18.xlsx', sheet_name = 'data_scored_train')[['order_no','y_pred']]
ly_test = pd.read_excel('D:/Model/201908_qsfq_model/02_result_yl/result_0919_18.xlsx', sheet_name = 'data_scored_test')[['order_no','y_pred']]
ly_var = pd.concat([ly_train, ly_test]).rename(columns = {'y_pred':'ly_pre'})

# ly_train = pd.read_excel('D:/Model/201908_qsfq_model/02_result_yl/result_0919_23.xlsx', sheet_name = 'data_scored_train')[['order_no','y_pred']]
# ly_test = pd.read_excel('D:/Model/201908_qsfq_model/02_result_yl/result_0919_23.xlsx', sheet_name = 'data_scored_test')[['order_no','y_pred']]
# ly_var = pd.concat([ly_train, ly_test]).rename(columns = {'y_pred':'ly_pre'})

#jxl
jxl_train = pd.read_excel('D:/Model/201908_qsfq_model/02_result_jxl/result_0919_95.xlsx', sheet_name = 'data_scored_train')[['order_no','y_pred']]
jxl_test = pd.read_excel('D:/Model/201908_qsfq_model/02_result_jxl/result_0919_95.xlsx', sheet_name = 'data_scored_test')[['order_no','y_pred']]
jxl_seq = list(pd.read_excel('D:/Model/201908_qsfq_model/02_result_jxl/result_0919_95.xlsx', sheet_name = 'features_in_model')['features_in_model'])
jxl_var = pd.concat([jxl_train, jxl_test]).rename(columns = {'y_pred':'jxl_pre'})

jxl_train = pd.read_excel('D:/Model/201908_qsfq_model/02_result_jxl/result_0919_29.xlsx', sheet_name = 'data_scored_train')[['order_no','y_pred']]
jxl_test = pd.read_excel('D:/Model/201908_qsfq_model/02_result_jxl/result_0919_29.xlsx', sheet_name = 'data_scored_test')[['order_no','y_pred']]
jxl_seq = list(pd.read_excel('D:/Model/201908_qsfq_model/02_result_jxl/result_0919_29.xlsx', sheet_name = 'features_in_model')['features_in_model'])
jxl_var = pd.concat([jxl_train, jxl_test]).rename(columns = {'y_pred':'jxl_pre'})

#xy
xy_train = pd.read_excel('D:/Model/201908_qsfq_model/02_result_xy/result_0919_17.xlsx', sheet_name = 'data_scored_train')[['order_no','y_pred_v2']].rename(columns = {'y_pred_v2':'y_pred'})
xy_test = pd.read_excel('D:/Model/201908_qsfq_model/02_result_xy/result_0919_17.xlsx', sheet_name = 'data_scored_test')[['order_no','y_pred']]
xy_seq = pd.read_excel('D:/Model/201908_qsfq_model/02_result_xy/result_0919_17.xlsx', sheet_name = 'features_in_model')['features_in_model']
xy_var = pd.concat([xy_train, xy_test]).rename(columns = {'y_pred':'xy_pre'})


#合并
all_var = x_with_y2[['order_no','effective_date','perf_flag','sample_set']].merge(base_var, on = 'order_no').merge(tq_var, on = 'order_no').merge(ty_var, on = 'order_no')\
            .merge(jxl_var, on = 'order_no').merge(xy_var, on = 'order_no').merge(ly_var, on = 'order_no')

all_var.shape

y_cols_new = ['order_no','effective_date','perf_flag','sample_set']

"""
融合模型
"""
result_path = 'D:/Model/201908_qsfq_model/02_result_0927/'
X = x_with_y2.loc[x_with_y2.sample_set.isin(['train','test']), x_cols]
X.dtypes.value_counts()


len(x_cols)

ss.eda(X, var_dict, useless_vars=[], exempt_vars=[],data_path=result_path, save_label='all')

summary = pd.read_excel(os.path.join(result_path,'all_variables_summary.xlsx'), encoding='utf-8')

#summary = pd.read_excel(os.path.join('D:/Model/201908_qsfq_model/02_result/all_add_br_variables_summary.xlsx'), encoding='utf-8')
exclude_after_eda = summary.loc[summary.exclusion_reason.notnull(), '指标英文'].tolist()
len(exclude_after_eda) #252

x_with_y2 = x_with_y2.drop(exclude_after_eda,1)
x_with_y2 = x_with_y2.replace(-1, -8887)

#train
X_train_df = x_with_y2.loc[(x_with_y2.perf_flag.isin([0,1])) & (x_with_y2.sample_set == 'train')].drop(y_cols,1)

y_train_df = x_with_y2.loc[x_with_y2.perf_flag.isin([0,1]) & (x_with_y2.sample_set == 'train') ].perf_flag

X_train_df.shape #2264
y_train_df.shape


#test
X_test_df = x_with_y2.loc[x_with_y2.sample_set == 'test'].drop(y_cols,1)

y_test_df = x_with_y2.loc[x_with_y2.sample_set == 'test'].perf_flag.replace(9,0)
X_test_df.shape #1250
y_test_df.shape
# #test new

X_test_df_new = data_test_new.drop(y_cols,1)
y_test_df_new = data_test_new.perf_flag


#X_test_df_new.shape #3723

#
X_train = X_train_df.values
y_train = y_train_df.values

X_test = X_test_df.values
X_test.shape
y_test = y_test_df.values

# X_test_new = X_test_df_new.values
# X_test_new.shape
# y_test_new = y_test_df_new.values


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
    'random_forest',
    'lasso',
    #'xgboost'
]

fs_obj = fs.FeatureSelection()
logging.basicConfig(filename=os.path.join(result_path, 'test_log.log'), level=logging.INFO, filemode='w')
LOG = logging.getLogger(__name__)


X_cat_train,X_transformed, woe_iv_df, rebin_spec, ranking_result = fs_obj.overall_ranking(X_train_df, y_train_df, var_dict, args_dict, methods, num_max_bins=5)

#rebin_spec = mu.convert_rebin_spec2XGB_rebin_spec(rebin_spec)

woe_iv_df.to_excel(os.path.join(result_path,'woe_iv_df.xlsx'))
ranking_result.to_excel(os.path.join(result_path,'ranking_result.xlsx'))
save_data_to_pickle(rebin_spec,result_path,'rebin_spec.pkl')

save_data_to_pickle(X_cat_train,result_path,'X_cat_train.pkl')

bin_obj = mt.BinWoe()
X_cat_test = bin_obj.convert_to_category(X_test_df, var_dict, rebin_spec)
X_cat_test.shape
X_cat_train = bin_obj.convert_to_category(X_train_df, var_dict, rebin_spec)

X_cat_test_new = bin_obj.convert_to_category(X_test_df_new, var_dict, rebin_spec)


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
#ranking_result = pd.read_excel('D:/Model/201908_qsfq_model/02_result_all/ranking_result.xlsx')

variable_importance.to_excel(os.path.join(result_path,'feature_importance_0925_rmtd.xlsx'))
variable_importance = variable_importance.sort_values('importance', ascending = False)
variable_importance.tail()

top_n = set(variable_importance[(variable_importance.importance> 0.001)]['feature']) #143 final

top_n = set(variable_importance[(variable_importance.importance> 0.03)]['feature']) #83

#top_n = variable_importance.sort_values('IV')['指标英文'].iloc[:100].tolist()
top_n = set(variable_importance[(variable_importance.importance> 0.001) & (variable_importance.IV>0.01)]['指标英文']) #100 15%, 9%

len(top_n)

var_dist = pd.read_excel('D:/Model/201908_qsfq_model/02_result_all/var_dist_badRate_by_time_all.xlsx', sheetname= 'Sheet3')

jxl_rm = list(var_dist.loc[(var_dist.数据源=='聚信立') & (var_dist.PSI_train_test_new > 1),'指标英文'])

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

features_in_model = [
    #'ty_riskscore',
'xy_pre',
'age_marital',
'education',
'ly_pre',
'id_city_level',
'jxl_pre',
'monthlyincome']

to_rm = ['YLZC297','YLZC053']

features_in_model = [i for i in top_n ]

features_in_model = [i for i in top_n if i and i not in jxl_rm]

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
exclusion_cols = vif_result.corr_exclude_vars + vif_result.vif_dropvars
len(exclusion_cols)

# 确定需要留下的排名靠前变量的数量
# n = 300
# top_n = ranking_result.sort_values('overall_rank')[u'指标英文'].iloc[:n].tolist()
# 
# # 决定从top_n中剔除的字段名称
# exclusion = []
# # 决定模型一定要有的字段，即使可能不显著
# start_from = []
# # 通过综合排序选中的
# selected = list(set(top_n) - set(exclusion_cols))
# len(selected)
##################################
#调参                            #
#################################

features_in_model = [i for i in top_n if i not in exclusion_cols and i not in jxl_rm ]
len(features_in_model)

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
                        param_grid=param_test1, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
                        #param_grid = param_test1, scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch1.fit(data_train_lean,y_train)
best_max_depth = gsearch1.best_params_['max_depth']
best_max_depth
#best_max_depth = 1

param_test10 = {'min_child_weight':range(1,6,2)}
#param_test10 = {'min_child_weight':range(1,3)}

gsearch10 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, gamma=0, subsample=0.8,
                                                 max_depth =4, colsample_bytree=0.8, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test10, scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch10.fit(data_train_lean,y_train)
best_min_child_weight = gsearch10.best_params_['min_child_weight']
best_min_child_weight
#best_min_child_weight = 5

param_test2 = {'gamma':[i/10.0 for i in range(0,5)]}
#param_test2 = {'gamma':[0,0.05,0.1]}

gsearch2 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, subsample=0.8, colsample_bytree=0.8, max_depth= best_max_depth,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test2,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch2.fit(data_train_lean,y_train)
best_gamma = gsearch2.best_params_['gamma']
best_gamma
#best_gamma = 0.3

param_test3 = {'subsample':[i/10.0 for i in range(5,8)]}
gsearch3 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test3,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch3.fit(data_train_lean,y_train)
best_subsample = gsearch3.best_params_['subsample']
best_subsample
#best_subsample = 1
#best_subsample = 0.5


param_test30 = {'colsample_bytree':[i/10.0 for i in range(6,10)]}
gsearch30 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test30,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch30.fit(data_train_lean,y_train)
best_colsample_bytree = gsearch30.best_params_['colsample_bytree']
best_colsample_bytree
#best_colsample_bytree = 1

param_test4 = {'reg_alpha':[0.1,1,10,50, 100]}
gsearch4 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1, n_estimators=100, max_depth= best_max_depth, gamma=best_gamma,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test4,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch4.fit(data_train_lean,y_train)
best_reg_alpha = gsearch4.best_params_['reg_alpha']
best_reg_alpha
#best_reg_alpha = 0.3

param_test5 = {'n_estimators':range(50,500,50)}
gsearch5 = GridSearchCV(estimator = XGBClassifier(learning_rate =0.1,  max_depth= best_max_depth, gamma=best_gamma,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,reg_alpha=best_reg_alpha,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test5,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch5.fit(data_train_lean,y_train)
best_n_estimators = gsearch5.best_params_['n_estimators']       #300
best_n_estimators
#best_n_estimators = 400

param_test6 = {'learning_rate':[0.01,0.1,0.2,0.3]}
#param_test6 = {'learning_rate':[0.01,0.03,0.05,0.1,0.2,0.3]}
gsearch6 = GridSearchCV(estimator = XGBClassifier(max_depth= best_max_depth, gamma=best_gamma, n_estimators = best_n_estimators,
                                                  colsample_bytree = best_colsample_bytree, subsample = best_subsample,reg_alpha=best_reg_alpha,
                                                  min_child_weight=best_min_child_weight, objective= 'binary:logistic',nthread=4,scale_pos_weight=1,seed=27),
                        param_grid = param_test6,scoring='roc_auc',n_jobs=4,iid=False,cv=5)
gsearch6.fit(data_train_lean,y_train)
best_learning_rate = gsearch6.best_params_['learning_rate']
best_learning_rate
#best_learning_rate = 0.1

#用获取得到的最优参数再次训练模型
best_xgb = XGBClassifier(learning_rate =best_learning_rate, n_estimators=best_n_estimators, max_depth= best_max_depth, gamma=best_gamma,
                         colsample_bytree = best_colsample_bytree, subsample = best_subsample, reg_alpha=best_reg_alpha,
                         min_child_weight=best_min_child_weight,
                         objective= 'binary:logistic',nthread=4,scale_pos_weight=1,eval_metric='auc', seed=27)
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

data_train_lean = x_with_y2.loc[(x_with_y2.sample_set == 'train')][features_in_model].values
y_train = x_with_y2.loc[(x_with_y2.sample_set == 'train')]['perf_flag'].replace(9,0).values

# data_train_lean = X_train_df[features_in_model].values
# y_train = y_train_df.values

y_train_pred = best_xgb.predict_proba(data_train_lean)[:,1]
roc_auc_score(y_train, y_train_pred)   #0.90  0.9917  1

predictions = [round(value) for value in y_train_pred]
accuracy = accuracy_score(y_train, predictions)
print("Accuracy: %.4f%%" % (accuracy * 100.0))  # 81.46%  93.5053%

score_train = [round(Prob2Score(value, 600, 20)) for value in y_train_pred]

decile_score = pd.cut(pd.DataFrame(score_train)[0], bins = train_bin, precision=0).astype(str)
decile_score = pd.qcut(pd.DataFrame(score_train)[0], q=10, duplicates='drop', precision=0).astype(str)
decile_score_20 = pd.qcut(pd.DataFrame(score_train)[0], q=20,duplicates ='drop', precision=0).astype(str)
decile_score.value_counts().sort_index()

data_scored_train = pd.DataFrame([data_train.index, data_train.effective_date, y_train, score_train,y_train_pred.T,decile_score,decile_score_20]).T

data_scored_train = data_scored_train.rename(columns = {0:'order_no',1:'effective_date',2:'Y',3:'score',4:'y_pred',5:'score_bin', 6:'score_bin_20'})
data_scored_train['score'] = data_scored_train['score'].astype(float)
data_scored_train['Y'] = data_scored_train['Y'].astype(float)

train_ks = calculate_ks_by_decile(data_scored_train['score'],data_scored_train['Y'].replace(9,0),'decile', q=10)
train_ks

train_ks_20 = calculate_ks_by_decile(data_scored_train['score'],data_scored_train['Y'].replace(9,0),'decile', q=20)
train_ks_20


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
data_scored_test = data_scored_test.rename(columns = {0:'order_no',1:'effective_date',2:'Y', 3:'y_pred' ,4:'score'})

train_ks.分箱.sort_index()
data_scored_train.score_bin_20.value_counts().sort_index()

train_bin = [400, 561, 586, 602, 612, 621,628,636,645,655, 750]
train_bin = [400, 559, 584, 600, 610, 621,630,638,647, 658,750]
train_bin = [400, 574, 589, 602, 612, 619,624,630,636, 644,750]
train_bin_20 = [400, 546, 564, 578, 587, 595, 601,606,610,615, 619,623,627,630,634,638,643,648,653,660,750]

train_bin = mt.BinWoe().obtain_boundaries(train_ks[u'分箱'])['cut_boundaries']
train_bin_20 = mt.BinWoe().obtain_boundaries(train_ks_20[u'分箱'])['cut_boundaries']

data_scored_test['score_bin'] = pd.cut(data_scored_test['score'], bins = train_bin)
data_scored_test['score_bin_20'] = pd.cut(data_scored_test['score'], bins = train_bin_20)

data_scored_test['score'] = data_scored_test['score'].astype(float)
data_scored_test['Y'] = data_scored_test['Y'].astype(float)
data_scored_test.head()
data_scored_test.score.min();data_scored_test.score.max()


test_ks = calculate_ks_by_decile(data_scored_test.score,data_scored_test.Y,'decile', manual_cut_bounds = train_bin)
test_ks

test_ks_20 = calculate_ks_by_decile(data_scored_test.score,data_scored_test.Y,'decile', manual_cut_bounds = train_bin_20)
test_ks_20
calculate_ks_by_decile(data_scored_test.score,data_scored_test.Y,'decile', q = 10)

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
data_scored_train.score_bin_20 = data_scored_train.score_bin_20.astype(str)
data_scored_test.score_bin_20 = data_scored_test.score_bin_20.astype(str)
#data_scored_test_new.score_bin = data_scored_test_new.score_bin.astype(str)

#best predictor 变量importance
feature_importance_best = best_xgb.feature_importances_
len(feature_importance_best)


feature_importance_df_best = pd.DataFrame(feature_importance_best).rename(columns = {0:'importance'})
features_in_model_df_best = pd.DataFrame(features_in_model).rename(columns = {0:'feature'})

variable_importance_best = pd.concat([features_in_model_df_best,feature_importance_df_best],1)


writer = pd.ExcelWriter(os.path.join(result_path, 'result_0925_50.xlsx'))
train_ks.to_excel(writer, 'train_ks', index=True)
test_ks.to_excel(writer, 'test_ks', index=True)
#test_ks_new.to_excel(writer, 'test_ks_new', index=True)
#test_ks_new_self.to_excel(writer, 'test_ks_new_self', index=True)
data_scored_train.to_excel(writer, 'data_scored_train', index=True)
data_scored_test.to_excel(writer, 'data_scored_test',  index=True)
#data_scored_test_new.to_excel(writer, 'data_scored_test_new',  index=True)
pd.DataFrame(features_in_model).to_excel(writer, 'features_in_model',  index=True)
variable_importance.to_excel(writer, 'variable_importance',  index=True)
variable_importance_best.to_excel(writer, 'variable_importance_best',  index=True)
writer.save()

##################################
# SAVE MODEL                     #
##################################
best_xgb.save_model(result_path + 'dc 0925 50.model')
best_xgb.save_model(result_path + 'dc 20190909 100.model')
best_xgb.save_model(result_path + 'dc 20190909 74.model')
best_xgb.save_model(result_path + 'dc 20190909 42.model')
best_xgb.save_model(result_path + 'dc 20190719 52v2.model')
best_xgb.save_model(result_path + 'dc 20190719 45.model')


"""""聚信立重新打分"""
import xgboost as xgb
from xgboost import DMatrix


# LOAD MODEL
mymodel = xgb.Booster()
mymodel.load_model("D:/Model/201908_qsfq_model/02_result_jxl/dc 0919 81.model")
mymodel.load_model("D:/Model/201908_qsfq_model/02_result_xy/dc 0919 17.model")


# PREPARE DATA
data_rescore = x_with_y2.loc[(x_with_y2.sample_set == 'test_new')][jxl_seq]

data_rescore = x_with_y2.loc[(x_with_y2.sample_set == 'train')][xy_seq]
data_rescore = x_with_y2.loc[(x_with_y2.sample_set == 'test_new')][xy_seq]


# PREDICT SCORES
data_rescore2 = DMatrix(data_rescore)
ypred = mymodel.predict(data_rescore2)

score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([data_rescore.index, score, ypred]).T.rename(columns = {0:'order_no',1:'score',2:'y_pred'})

data_scored.score = data_scored.score.astype(float)

bin = [400, 594, 601, 606, 610, 614, 617, 621, 625, 629, 750]

bin = [400, 597, 604, 606, 608, 611, 615, 619, 624, 630, 750] #xy

data_scored['score_bin'] = pd.cut(data_scored.score, bins = bin)

data_scored.score_bin = data_scored.score_bin.astype(str)
data_scored.order_no = data_scored.order_no.astype(str)
data_scored.to_excel("D:/Model/201908_qsfq_model/02_result_jxl/test_new_scored_81.xlsx",index=False)
data_scored.to_excel("D:/Model/201908_qsfq_model/02_result_xy/test_new_scored_17.xlsx",index=False)

"""
总模型打分
"""
# LOAD MODEL
mymodel = xgb.Booster()
mymodel.load_model("D:/Model/201908_qsfq_model/02_result_all/dc 0919 16.model")

xy_test_new = pd.read_excel('D:/Model/201908_qsfq_model/02_result_xy/test_new_scored_17.xlsx').rename(columns = {'y_pred':'xy_pre'})
jxl_test_new = pd.read_excel('D:/Model/201908_qsfq_model/02_result_jxl/test_new_scored_81.xlsx').rename(columns = {'y_pred':'jxl_pre'})
var_new = x_with_y2.loc[x_with_y2.sample_set == 'test_new'][base_cols + ty_cols + tq_cols]


var_seq = list(pd.read_excel('D:/Model/201908_qsfq_model/02_result_all/result_0919_16.xlsx', sheet_name = 'features_in_model')['features_in_model'])
data_new = x_with_y2.loc[x_with_y2.sample_set == 'test_new'][['order_no','perf_flag']].merge(xy_test_new, on = 'order_no').merge(jxl_test_new, on = 'order_no')\
    .merge(var_new, left_on = 'order_no', right_index = True)

data_new = data_new[var_seq]

# PREDICT SCORES
data_new2 = DMatrix(data_new)
ypred = mymodel.predict(data_new2)

score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([data_rescore.index, score, ypred]).T.rename(columns = {0:'order_no',1:'score',2:'y_pred'})

data_scored.score = data_scored.score.astype(float)

bin = [400, 594, 601, 606, 610, 614, 617, 621, 625, 629, 750]

bin = [400, 574, 590, 599, 606, 613, 620, 629, 639, 653, 750] #xy

data_scored['score_bin'] = pd.cut(data_scored.score, bins = bin)
data_scored['score_bin_self'] = pd.qcut(data_scored.score, q=10 )

data_scored.score_bin_self = data_scored.score_bin_self.astype(str)
data_scored.order_no = data_scored.order_no.astype(str)

data_scored = x_with_y2.loc[x_with_y2.sample_set == 'test_new'][['order_no','perf_flag']].merge(data_scored, on = 'order_no')

data_scored.to_excel("D:/Model/201908_qsfq_model/02_result_all/test_new_scored_16.xlsx",index=False)



