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
df['flag'].sum()/df['flag'].count()

# step 1:生成数据字典，方便后期管理建模可用的变量
my_dict = DataSummary().create_dict(df , is_available = 'Y', data_sorce='TYC', data_type='三方', useless_vars = ['company_name_md5'])
my_dict

# step 2:数据EDA，初步分析数据的分布
my_eda= eda(df, useless_vars = ['company_name_md5'], special_value = [np.nan, 0], var_dict = my_dict, result_path = result_path, save_label = 'test', cutoff=0.9)

# step3: 拆分数据集，随机分，分层随机分，按时间分




# my_dict = {'b':'c','a':'b',  'c':'d', 'd':'e', 's':'t', 't':'m'}


temp = pd.read_excel(r'/Users/yantingting/Desktop/text.xlsx')
temp = temp[~temp['property2'].isna()]
temp['property2'] = temp['property2'].astype(str)
temp['id'] = temp['id'].astype(str)
my_dict = dict(zip(temp['id'], temp['property2']))
my_dict
def convert_dict(my_dict):
    all_keys = my_dict.keys()
    all_keys_1 = [value for value in all_keys]
    all_values = my_dict.values()
    all_values_1 = [value for value in all_values]
    my_dict_1 = {}
    mylist_temp = []
    for k,v in my_dict.items():
        if k in all_values_1:
            continue
        else:
            temp1 = v
            while v in all_keys_1:
                temp1 = ','.join([temp1, my_dict.get(v)])
                v = my_dict.get(v)
            my_dict_1[k] = temp1


    return my_dict_1, mylist_temp


dict1, mylist_temp = convert_dict(my_dict)
# print(mylist_temp)
# dict1
# pd.DataFrame([dict1]).T

df = pd.DataFrame.from_dict(dict1, orient='index', columns = ['his_name_list'])
df = df.reset_index().rename(columns = {'index':'name'})






def search_used_name(root, used_name, lines):
    child_counter = 0
    for line in lines:
        if root == line[1]:
            child_counter += 1
            used_name.append(line[0])
            used_name = search_used_name(line[0], used_name, lines)

            if child_counter == 0:
                return used_name

    return used_name

lines = my_dict
for line in lines:
    if not line[1]:
        used_name = search_used_name(line[0], [], lines)
        print(line[0],used_name)