#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 03_Advance AI.py
@Time    : 2020-05-26 15:16
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import sys
import os
import pandas as pd
pd.set_option('display.max_columns', None)
sys.path.append('/Users/yantingting/Documents/MintechModel/newgenie/')
from utils3.data_io_utils import *


file_path = r'/Users/yantingting/Seafile/风控/模型/10 印尼/三方数据/Advance AI'
if not os.path.exists(file_path):
    os.makedirs(file_path)


'''取样本'''

query0 = '''
select id as loanid,customer_id,effective_date
from rt_t_gocash_core_loan 
where ((effective_date>'2020-02-12'  and effective_date<='2020-02-18')
or (effective_date > '2020-03-10' and effective_date<='2020-03-20') )
and return_flag = 'false'
'''
df0 = DataBase().get_df_from_pg(query0)
df0.shape
df0.head()
df0['loanid'] = df0['loanid'].astype('str')
df0['customer_id'] = df0['customer_id'].astype('str')
df0.to_excel(os.path.join(file_path,'Advance AI0526.xlsx'), index = False)
