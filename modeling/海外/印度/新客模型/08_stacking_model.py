#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 08_stacking_model.py
@Time    : 2020-07-02 18:31
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""



import numpy as np
import os
import sys
import pandas as pd
pd.set_option('display.max_columns', None)
import matplotlib.pyplot as plt
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


data_path = '/Users/yantingting/Seafile/风控/15 印度/V4模型/'
result_path = '/Users/yantingting/Seafile/风控/15 印度/V4模型/06_stacking_model2/'
if not os.path.exists(result_path):
    os.makedirs(result_path)


df_experian = pd.read_excel(os.path.join(data_path,'02_experian/grid_23_200702-01-18score.xlsx'))
rename_list = ['y_pred', 'score', 'score_bin', 'score_bin_20', 'proba_bin', 'proba_bin_20',]
rename_exp = ['exp_' + i for i in rename_list]
df_experian.rename(columns = dict(zip(rename_list,rename_exp)), inplace= True)
df_experian.head()


df_base_app = pd.read_excel(os.path.join(data_path,'05_base/grid_45_200702-18-31score.xlsx'))
rename_base = ['base_' + i for i in rename_list]
df_base_app.rename(columns = dict(zip(rename_list,rename_base)), inplace= True)
df_base_app.head()

df_all = df_experian.merge(df_base_app[['order_no'] + rename_base])
df_all.head()
df_all.to_excel(os.path.join(result_path,'df_all.xlsx'))



df_model = df_all[['order_no', 'Y', 'exp_score', 'base_score', 'sample_set','applied_at']]
df_model.index = df_model.order_no
df_model['intercept'] = [1] * df_model.shape[0]
df_model.rename(columns = {'Y':'label'}, inplace= True)
df_model.head()

train_df = df_model[df_model['sample_set'] == 'train']
test_df = df_model[df_model['sample_set'] == 'test']
oot_df = df_model[df_model['sample_set'] == 'oot']

x_col = ['exp_score','base_score','intercept']
X_train = train_df[x_col]
X_test = test_df[x_col]
X_oot = oot_df[x_col]

y_train = train_df.label
y_test = test_df.label
y_oot = oot_df.label



# ''' *************************** LR ***************************'''

result_path = '/Users/yantingting/Seafile/风控/模型/15 印度/V4模型/06_stacking_model2/LR'
if not os.path.exists(result_path):
    os.makedirs(result_path)

import statsmodels.api as sm
LR = sm.Logit(y_test,X_test).fit()
print(LR.summary())
y_train_pred = LR.predict(X_train)
y_test_pred = LR.predict(X_test)
y_oot_pred = LR.predict(X_oot)

#
# # ''' *************************** DT ***************************'''
# result_path = '/Users/yantingting/Seafile/风控/模型/15 印度/V4模型/06_stacking_model2/DT'
# if not os.path.exists(result_path):
#     os.makedirs(result_path)
#
# from sklearn import tree
# from six import StringIO
# from IPython.display import Image
# import pydot
# import pydotplus
#
# best_clf = tree.DecisionTreeClassifier(criterion='gini', splitter='best', max_depth = 4,
#                                     min_samples_split=0.05,min_samples_leaf =0.05,
#                                     min_weight_fraction_leaf=0.0, max_features=None,
#                                     random_state=None, max_leaf_nodes=None,class_weight=None, presort=False)
# best_clf.fit(X_test, y_test)
#
# var_tree = ['exp_score', 'base_score']
# var_rank = dict(zip(range(len(var_tree)+1),var_tree))
# clf = best_clf
#
# clf_train = clf.fit(X_train, y_train)
# clf_train
# dot_data = StringIO()
# tree.export_graphviz(clf_train, out_file=dot_data, proportion=True,
#                      impurity = False,precision=2)
# graph = pydotplus.graph_from_dot_data(dot_data.getvalue())
# Image(graph.create_png())
# graph.write_png(os.path.join(result_path , 'tree_train.png'))
#
#
# clf_test = clf.fit(X_test, y_test)
# clf_test
#
# dot_data = StringIO()
# tree.export_graphviz(clf_test, out_file=dot_data, proportion=True,
#                      impurity = False,precision=2)
# graph = pydotplus.graph_from_dot_data(dot_data.getvalue())
# Image(graph.create_png())
# graph.write_png(os.path.join(result_path , 'tree_test.png'))
#
#
# y_train_pred = best_clf.predict_proba(X_train)[:,1]
# y_test_pred = best_clf.predict_proba(X_test)[:,1]
# y_oot_pred = best_clf.predict_proba(X_oot)[:,1]


# ''' *************************** 评价 ***************************'''
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


data_scored_train['sample_set'] = "train"
data_scored_test['sample_set'] = "test"
data_scored_oot['sample_set'] = "oot"
data_scored_all = pd.concat([data_scored_train, data_scored_test, data_scored_oot])

data_scored_all = data_scored_all.merge(df_model['applied_at'],on='order_no',how ='left')
data_scored_all.head()
data_scored_all.to_excel(os.path.join(result_path, 'score' + '.xlsx'))




# ''' ***************************  KS  ***************************'''

df1 = train_proba_ks.copy()
df1.columns = ['train_' + str(i) for i in df1.columns]

df2 = oot_proba_ks.copy()
df2.columns = ['oot_' + str(i) for i in df2.columns]
df3 = df1.merge(df2,left_index = True, right_index = True, how = 'left')



# plt.plot(df3.index, df3['train_累积Bad占比'],)
# plt.plot(df3.index, df3['train_累积Good占比'])
# plt.xlabel('group')
# plt.ylabel('cum')
# plt.title('KS train vs. oot')
# plt.savefig(os.path.join(result_path,'ks.png'))
#

import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['STFangsong']
plt.rcParams['font.family']=['STFangsong']
# plt.rcParams['font.family']=['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

fig = plt.figure()
# ax1 = fig.add_subplot(121)
ax1 = df3[['train_样本分布占比', 'oot_样本分布占比']].plot.bar()
plt.legend(loc= 'upper right')
plt.ylabel('分布占比')
ax2 = ax1.twinx()   #组合图必须加这个
ax2.plot(df3['train_逾期率'] )
ax2.plot(df3['oot_逾期率'] )
plt.xlabel('group')
plt.ylabel('逾期率')
plt.title('score_v1 in 10 Quantiles')
plt.savefig(os.path.join(result_path,'rete.png'))





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


