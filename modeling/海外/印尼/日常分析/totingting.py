# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 14:05:42 2020

@author: Mint
"""

import pandas as pd
pd.set_option('display.max_columns', None)
import os
sys.path.append('/Users/yantingting/Documents/MintechModel/newgenie/utils3/')
import utils3.feature_selection as fs
fs_obj = fs.FeatureSelection()
import utils3.metrics as mt
pf = mt.Performance()
bw = mt.BinWoe()
import utils3.misc_utils as mu
# import utils3.model_training as tr
# import utils3.filing as fl
import utils3.summary_statistics as ss


file_path = '/Users/yantingting/Desktop/SJ/'

df2 = pd.read_excel(os.path.join(file_path,'df2.xlsx'))
data_all = pd.read_excel(os.path.join(file_path,'data_all.xlsx'))
var_dict = pd.read_excel(os.path.join(file_path,'同盾Carpo数据字典.xlsx'))
var_dict.head()
var_dict['数据类型'].value_counts()
var_list = var_dict.loc[var_dict['数据类型'].isin(['int', 'float']) ,'指标英文'].tolist()
var_list.append('loan_id')
var_list.append('flag7')
var_list.append('effective_date')
# del df_all

dat = df2.drop(['random'], axis=1).merge(data_all[['loan_id','effective_date']], left_on='order_no', right_on ='loan_id', how='left')
dat2 = dat.drop(['loan_id'], axis=1).set_index('order_no')
dat2.effective_date.value_counts(dropna=False)

dat2.effective_date = dat2.effective_date.astype(str)
dat2['group'] = dat2.effective_date.apply(lambda x:'before' if str(x)=='2020-02-11' 
    else ('during' if str(x)=='2020-03-04' else 'after')
    )
dat2.group.value_counts()

dat3 = dat2.drop(['sample_set','effective_date'], axis=1)

before = dat3[dat3.group =='before']
during = dat3[dat3.group =='during']
after = dat3[dat3.group =='after']

before1 = before.drop(['group'], axis=1)
during1 = during.drop(['group'], axis=1)
after1 = after.drop(['group'], axis=1)

X_train = before1.drop(['label'], axis=1)
y_train = before1.label
X_test = during1.drop(['label'], axis=1)
y_test = during1.label
X_oot = after1.drop(['label'], axis=1)
y_oot = after1.label

features_var_dict = X_train.columns.tolist()

## Ranking
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
# Train 分箱
X_train.dtypes
X_cat_train, X_transformed_train, woe_iv_df_train, rebin_spec_train, ranking_result_train = fs_obj.overall_ranking(X_train, y_train,
                                                                                        var_dict, args_dict,
                                                                                        methods=[], 
                                                                                        num_max_bins=5,
                                                                                        n_clusters=15)



rebin_spec = mu.convert_rebin_spec2XGB_rebin_spec(rebin_spec_train)
rebin_spec_bin_adjusted = {k: v for k, v in rebin_spec.items() if k in features_var_dict}
# 输出变量的分箱
X_cat_train = bw.convert_to_category(X_train, var_dict, rebin_spec_bin_adjusted)
X_cat_test = bw.convert_to_category(X_test, var_dict, rebin_spec_bin_adjusted)
X_cat_oot = bw.convert_to_category(X_oot, var_dict, rebin_spec_bin_adjusted)

# 变量分布
## before
before1['appmon'] = '0_before'
var_cols = set(X_train.columns).intersection(var_dict.指标英文)
X_cat_train_with_y_appmon = pd.merge(X_cat_train,before1[['label','appmon']] ,left_index=True,right_index=True)
var_dist_badRate_by_time = ss.get_badRate_and_dist_by_time(X_cat_train_with_y_appmon,var_cols,'appmon','label')

## during
during1['appmon'] = '1_during'
X_cat_test_with_y_appmon = pd.merge(X_cat_test,during1[['label','appmon']],left_index=True,right_index=True)
var_dist_badRate_by_time_test = ss.get_badRate_and_dist_by_time(X_cat_test_with_y_appmon,var_cols,'appmon','label')

## after
after1['appmon'] = '2_after'
X_cat_oot_with_y_appmon = pd.merge(X_cat_oot,after1[['label','appmon']],left_index=True,right_index=True)
var_dist_badRate_by_time_oot = ss.get_badRate_and_dist_by_time(X_cat_oot_with_y_appmon,var_cols,'appmon','label')

## all
all_cat = pd.concat([X_cat_train,X_cat_test,X_cat_oot])
app_data = pd.concat([before1[['label','appmon']],during1[['label','appmon']],after1[['label','appmon']]])
X_cat_with_y_appmon_all = pd.merge(all_cat,app_data[['label','appmon']] ,left_index=True,right_index=True)
var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,var_cols,'appmon','label')
var_dist_badRate_by_time_all.head()
var_dist_badRate_by_time_all.to_excel(os.path.join(file_path,'var_dist_badRate_by_time_all.xlsx'))




