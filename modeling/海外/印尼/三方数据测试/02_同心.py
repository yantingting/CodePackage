#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 02_同心.py
@Time    : 2020-05-26 09:49
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import sys
import os
import pandas as pd
pd.set_option('display.max_columns', None)
sys.path.append('/Users/yantingting/Documents/MintechModel/newgenie/utils3/')
from data_io_utils import *


file_path = r'/Users/yantingting/Seafile/风控/模型/10 印尼/三方数据/09 同心'
if not os.path.exists(file_path):
    os.makedirs(file_path)

'''取样本'''
# 不支持回溯
# 密码：mintq123
query0 = '''
select t1.id as loanid,t2.id_card_name,md5(t2.id_card_no) as md5_id_no,md5(t2.cell_phone) as md5_cell
from rt_t_gocash_core_loan t1 
left join rt_t_gocash_core_customer t2 
on t1.customer_id = t2.id
where effective_date>='2020-05-01' and effective_date<='2020-05-25' and return_flag = 'false'
order by effective_date desc
limit 1000;
'''
df0 = DataBase().get_df_from_pg(query0)
df0.shape
df0.head()
df0['loanid'] = df0['loanid'].astype('str')
df0.to_excel(os.path.join(file_path,'同心样本0526.xlsx'), index = False)




'''分析'''








