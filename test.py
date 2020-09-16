#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : test.py
@Time    : 2020-09-11 16:46
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import sys
sys.path.append('/Users/yantingting/Documents/Code/CodePackage/')
from utils3.summary_statistics import *
from utils3.data_io_utils import *
import pandas as pd
pd.set_option('display.max_columns', None)
import os


data_path = '/Users/yantingting/Documents/Code/'
result_path = '/Users/yantingting/Documents/Code/result'
if not os.path.exists(result_path):
    os.makedirs(result_path)

df = pd.read_excel(os.path.join(data_path, 'sample_data.xlsx'))
df.head()

# step 1:生成数据字典，方便后期管理建模可用的变量
my_dict = DataSummary().create_dict(df , is_available = 'Y', data_sorce='TYC', data_type='三方', useless_vars = ['company_name_md5'])
my_dict

# step 2:数据EDA，初步分析数据的分布
my_eda= eda(df, useless_vars = ['company_name_md5'], special_value = [np.nan, 0], var_dict = my_dict, result_path = result_path, save_label = 'test', cutoff=0.9)

# step3: 拆分数据集，随机分，分层随机分，按时间分



