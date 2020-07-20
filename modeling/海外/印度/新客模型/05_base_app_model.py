#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 05_base_app_model.py
@Time    : 2020-07-01 20:37
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""



import numpy as np
import pandas as pd
import sys
import os
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score
from hyperopt import hp
import matplotlib.pyplot as plt
from datetime import datetime

sys.path.append('/Users/yantingting/Documents/MintechJob/newgenie1/utils3/')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
import utils3.xgboost_model as xm
from data_io_utils import *
import utils3.filing as fl
import plot_tools as pt

fs_obj = fs.FeatureSelection()
pf = mt.Performance()
bw = mt.BinWoe()


data_path = '/Users/yantingting/Seafile/风控/模型/15 印度/V4模型/01_Data'
result_path = '/Users/yantingting/Seafile/风控/模型/15 印度/V4模型/03_base_app'
if not os.path.exists(result_path):
    os.makedirs(result_path)

# '''读入数据'''
x_with_y = pd.read_excel(os.path.join(data_path,'df_base_app.xlsx'))
x_with_y.head()
x_with_y.shape


# """数据准备"""
# 必须要有applied_at 和 label这两列
x_with_y.rename(columns = {'effective_date':'applied_at', 'label':'label'}, inplace=True)
x_with_y = x_with_y.loc[x_with_y.label.isin([0,1])]
x_with_y.shape

# """字典"""
# var_dict 里只能存放备选变量
var_dict = pd.read_excel(os.path.join(data_path,'dict_base_app.xlsx'))
y_cols = list(set(x_with_y.columns) - set(var_dict.指标英文))
x_col = list(set(x_with_y.columns).intersection(var_dict.指标英文))
useless_vars = []


# """EDA"""
# 删除异常值比例较高的变量
X = x_with_y[x_col]
eda_result = ss.eda(X = X, var_dict=var_dict, useless_vars =useless_vars,exempt_vars = [], uniqvalue_cutoff=0.85)
eda_result.to_excel(os.path.join(result_path, 'all_variable_summary.xlsx'))

summary = pd.read_excel(os.path.join(result_path, 'all_variable_summary.xlsx'), encoding='utf-8')
exclude_after_eda = summary.loc[summary.exclusion_reason.notnull(), '指标英文'].tolist()


#划分训练和验证
x_with_y.index = x_with_y.order_no
x_with_y.head()

train_df = x_with_y[x_with_y.sample_set.isin(['train'])]
test_df = x_with_y[x_with_y.sample_set.isin(['test'])]
oot_df = x_with_y[x_with_y.sample_set == 'oot']

train_df.label.value_counts(dropna = False)
test_df.label.value_counts(dropna = False)

#去掉EDA变量
y_cols2 = [i for i in y_cols if i != 'label']
drop_var = list(set(useless_vars).union(set(exclude_after_eda)))
len(drop_var)

train = train_df.drop(y_cols2,1).drop(drop_var, 1)
train.head()
test = test_df.drop(y_cols2,1).drop(drop_var,1)
oot = oot_df.drop(y_cols2,1).drop(drop_var,1)

X_train = train.drop(['label'], 1)
X_test = test.drop(['label'], 1)
X_oot = oot.drop(['label'], 1)
set(X_train.columns) - set(var_dict['指标英文'])
set(X_test.columns) - set(var_dict['指标英文'])
set(X_oot.columns) - set(var_dict['指标英文'])

y_train = train.label
y_test = test.label
y_oot = oot.label

# #Univariate Chart
# # univariate_chart要求数据中 必须有一列是sample_type  这里可以在代码中加上判断分数据集的列名
# x_with_y.head()
# wrong_list = []
# for cols in X_train.columns.tolist():
#     print('{} is in processing'.format(cols))
#     try:
#         pt.univariate_chart(x_with_y,cols,'label',sample_set='sample_set',n_bins = 10,
#             default_value = -1,draw_all = True,draw_list = ['train', 'test','oot'],result_path = result_path)
#     except IndexError:
#         wrong_list.append(cols)
#         print('wrong!')
#
# print(wrong_list)



"""计算XGB importance"""
## fit model
model = XGBClassifier()
model.fit(X_train, y_train)

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


# """ overall_ranking """
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
save_data_to_pickle(rebin_spec_bin_adjusted,result_path,'rebin_spec_bin_adjusted.pkl')

bin_obj = mt.BinWoe()
X_cat_train = bin_obj.convert_to_category(X_train, var_dict, rebin_spec_bin_adjusted)
X_cat_test = bin_obj.convert_to_category(X_test, var_dict, rebin_spec_bin_adjusted)
X_cat_oot = bin_obj.convert_to_category(X_oot, var_dict, rebin_spec_bin_adjusted)


"""按月/按sample_set检查变量的稳定性和有效性"""

## train
train_df['appmon'] = '0_train'

## test
test_df['appmon'] = '1_test'

#oot
oot_df['appmon'] = '2_oot1'

## all
all_cat = pd.concat([X_cat_train,X_cat_test, X_cat_oot])
all_cat.head()

app_data = pd.concat([train_df[['label','appmon']],test_df[['label','appmon']], oot_df[['label','appmon']]])
X_cat_with_y_appmon_all = pd.merge(all_cat,app_data[['label','appmon']] ,left_index=True,right_index=True)
X_cat_with_y_appmon_all.head()

x_cols2 = list(set(X_cat_with_y_appmon_all.columns).intersection(x_col))
len(x_cols2)
X_cat_with_y_appmon_all.shape

var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,x_cols2,'appmon','label')
var_dist_badRate_by_time_all.to_excel(os.path.join(result_path, 'var_dist_badRate_by_sample.xlsx'))

# PSI
var_psi = pf.variable_psi(X_cat_train, X_cat_test, var_dict)
var_psi.loc[:, 'psi_rank'] = var_psi.PSI.rank(ascending=False)
var_psi.head()

# 汇总各项指标
ranking_result = ranking_result.merge(var_psi, on='指标英文', how='left').merge(var_importance, on='指标英文', how='left')
ranking_result.to_excel(os.path.join(result_path, 'ranking_result.xlsx'))

# 变量筛选
time1 = time.time()
train_df.head()
f_rmv = fs.feature_remove(train,test, ranking_result, result_path, psi = -1, iv = 0.001,
                          imp = 0, corr = 0.7, slope = True)
try:
    train = train.drop(['slope'], axis=1)
    test = test.drop(['slope'], axis=1)
except:
    pass

feature_used = list(set(X_train.columns.tolist()) - set(f_rmv))
len(feature_used)
X_train = train_df[feature_used]
X_test = test_df[feature_used]
X_oot = oot_df[feature_used]
time2 = time.time()
print('run_time: ', time2 - time1)


# ''' *************************** 调参1:XGB GridSearch  ***************************'''
param = {
        'eta': [i / 10.0 for i in range(0, 10)],
        'max_depth': range(2, 6, 1),
        'gamma': [i for i in range(5)],
        'min_child_weight':range(1, 8, 1),
        'subsample': [i / 10.0 for i in range(5, 10)],
        'colsample_bytree':  [i / 10.0 for i in range(5, 10)],
        'n_estimators': range(50, 500, 50)
        }

model, df_params, model_importance = xm.xgboost_randomgridsearch(X_train, y_train, X_test, y_test, NFOLD=5, param=None)
model
best_xgb= model

## model变量重要性
best_importance = best_xgb.feature_importances_
model_importance = pd.DataFrame(columns=["varName", 'importance'])
model_importance['varName'] = feature_used
model_importance['importance'] = best_importance

y_train_pred = best_xgb.predict_proba(X_train)[:,1]
y_test_pred = best_xgb.predict_proba(X_test)[:,1]
y_oot_pred = best_xgb.predict_proba(X_oot)[:,1]

# ### 打分&KS
train_pred = train_df.copy()
train_pred['y_pred'] = y_train_pred
test_pred = test_df.copy()
test_pred['y_pred'] = y_test_pred
oot_pred = oot_df.copy()
oot_pred['y_pred'] = y_oot_pred

data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, data_scored_test, test_proba_ks, test_proba_ks_20, test_score_ks, test_score_ks_20 = pf.data_score_KS(train_pred, test_pred, 'y_pred')
data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, data_scored_oot, oot_proba_ks, oot_proba_ks_20, oot_score_ks, oot_score_ks_20 = pf.data_score_KS(train_pred, oot_pred, 'y_pred')

# AUC ACCURACY
auc_train, acc_train = pf.auc_acc_table(train_pred)
auc_test, acc_test = pf.auc_acc_table(test_pred)
auc_oot, acc_oot = pf.auc_acc_table(oot_pred)


# ''' ***************************  PDP(all)  ***************************'''
FIG_PATH = os.path.join(result_path, 'figure', 'PDP')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

f_imp_list = feature_used
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
    plt.savefig(path, format='png', dpi=300, bbox_inches = 'tight',pad_inches = 0.1)
    plt.close()
    n += 9
print('end')


# ''' ***************************  输出Exccel合集结果  ***************************'''
## 变量分布
var_dist_badRate_by_time_all['used_in_model'] = var_dist_badRate_by_time_all.varName.apply(lambda x: x in feature_used)
ranking_result['used_in_model'] = ranking_result.指标英文.apply(lambda x: x in feature_used)

auc_list = [auc_train, auc_test, auc_oot]
acc_list = [acc_train, acc_test, acc_oot]
split_list = ['train','test', 'oot']
df_auc_acc = pd.DataFrame({"sample_set":split_list,"auc":auc_list,"accuracy":acc_list})

all_pred = pd.concat([train_pred, test_pred, oot_pred])
all_pred['order_no'] = all_pred['order_no'].astype(str)

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
                     'oot1_score_ks_20':oot_score_ks_20
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

FILE_NAME = "grid_%d_%s"%(len(feature_used), datetime.now().strftime('%y%m%d-%H-%M'))

best_xgb.save_model(os.path.join(result_path, FILE_NAME+'.model'))

fl.ModelSummary2Excel(result_path = result_path, fig_path= result_path, file_name = FILE_NAME +".xlsx", data_dic = xgb_dict).run()
data_scored_all.order_no = data_scored_all.order_no.astype(str)
data_scored_all = data_scored_all.merge(x_with_y[['order_no','applied_at']], left_on = 'order_no', right_index = True, how = 'left')
data_scored_all.to_excel(os.path.join(result_path, FILE_NAME + 'score' + '.xlsx'))
data_scored_all.head()


# ''' ***************************  lift_chart_train test oot  ***************************'''

FIG_PATH = os.path.join(result_path, 'figure', 'lifechart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)
train_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'train'], 'y_pred','Y', n_bins = 10, feature_label='train')
test_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'test'], 'y_pred','Y', n_bins = 10, feature_label='test')
oot_lc = pt.show_result_new(data_scored_all.loc[data_scored_all.sample_set == 'oot'], 'y_pred','Y', n_bins = 10, feature_label='oot')
path = os.path.join(FIG_PATH, "LiftChart" + ".png")
plt.savefig(path, format='png', dpi=300, bbox_inches = 'tight',pad_inches = 0.1)
plt.close()


# ''' ***************************  lift_chart_bytime  ***************************'''

data_scored_all['week'] = pd.to_datetime(data_scored_all.loc[data_scored_all.sample_set.isin(['train', 'oot', 'test']), 'applied_at']).dt.week
data_scored_all.groupby(['week'])['applied_at'].min()
pt.lift_chart_by_time(data_scored_all.loc[data_scored_all.sample_set.isin(['train', 'oot', 'test'])],'y_pred','Y', n_bins = 10, by="week")
path = os.path.join(FIG_PATH, "LiftChart_" + 'by_week' + ".png")
plt.savefig(path, format='png', dpi=300, bbox_inches = 'tight',pad_inches = 0.1)
plt.close()
