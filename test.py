#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : test.py
@Time    : 2020-09-11 16:46
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

# 1)把主键设置成index ；2）在代码前期声名y； 3）train-test模式和train-test-OOT模式

import sys
sys.path.append('/Users/yantingting/Documents/Code/CodePackage/')
from utils3.summary_statistics import *
from utils3.data_bins import *
from utils3.common_fun import *
import pandas as pd
pd.set_option('display.max_columns', None)
import os


data_path = '/Users/yantingting/Documents/Code/'
result_path = '/Users/yantingting/Documents/Code/result'
if not os.path.exists(result_path):
    os.makedirs(result_path)

df = pd.read_excel(os.path.join(data_path, 'sample_data.xlsx'))
df.set_index(['company_name_md5'], inplace=True)
df.head()
df['flag'].sum()/df['flag'].count()

# step 1:生成数据字典，方便后期管理建模可用的变量
my_dict = DataSummary().create_dict(df , is_available = 'Y', data_sorce='TYC', data_type='三方', useless_vars = ['company_name_md5'])
my_dict.to_csv(os.path.join(result_path, 'my_dict.csv'), index = False)

# step 2:数据EDA，初步分析数据的分布
my_eda= DataSummary().eda(df, useless_vars = [], special_value = [np.nan, 0], var_dict = my_dict, result_path = result_path, save_label = 'test', cutoff=0.9)





y_header = 'flag'
# step3: 拆分数据集，随机分，分层随机分，按时间分
df_all = DataSummary().split_df(df= df, test_size=0.3, random_state=2, shuffle = True, stratify = 'flag' )
df_all
df_train = df_all['train']
print(df_train.head())
df_test = df_all['test']
print(df_test.head())



# step4:数据分箱并计算IV，KS等值（开始分箱的类）

# 单变量分箱调整(如果braek_list 非空则按照指定的分箱来，否则按照默认分箱）
# breaks_list = {
#     'overdue_15d_cnt_6m': [3, 6, 12, 20]
#     , 'discount_vs_cate_ratio_1m': [1, 1.5, 1.7]
# }

bins = woebin(df, y=y_header,  min_perc_coarse_bin=0.02)
bins
df_bins = convert_bins_to_df(bins)
df_bins.to_csv(os.path.join(result_path, 'df_bins.csv'), index = False)

df_iv = get_ivlist_from_bins(bins)


# step5:变量筛选（低IV，高PSI，不单调）




# step6:需保留数据替换WOE
list_col_by_iv = ['company_age',y_header]  #这个列表需要包含y，或者是不是也可以
df_train_woe = woebin_ply(df_train[list_col_by_iv], bins)
df_test_woe = woebin_ply(df_test[list_col_by_iv], bins)

y_train = df_train_woe.loc[:, y_header]
x_train = df_train_woe.loc[:, df_train_woe.columns != y_header]
y_test = df_test_woe.loc[:, y_header]
x_test = df_test_woe.loc[:, df_test_woe.columns != y_header]
x_train.head()


# step7: 回归筛变量 适用于逻辑回归模型


# 输出所有结果  尽量能放在同一个Excel里

# 之前的结果比较好，把所有变量的各个指标统计在一起



