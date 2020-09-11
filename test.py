#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : test.py
@Time    : 2020-09-11 16:46
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import pandas as pd
pd.set_option('display.max_columns', None)
import os

data_path = '/Users/yantingting/Documents/Code/'
result_path = '/Users/yantingting/Documents/Code/result'
if not os.path.exists(result_path):
    os.makedirs(result_path)


df = pd.read_excel(os.path.join(data_path, 'sample_data.xlsx'))
print(df.shape)
df.head()

var_list = []
for cols, rows in df.iterrows():
    print(rows)
    c = tuple(rows)
    var_list.append(c)

