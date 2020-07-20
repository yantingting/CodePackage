#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : v3_temp.py
@Time    : 2020-07-03 13:15
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import os
import sys
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.2f' % x)
import datetime
import numpy as np
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)
sys.path.append('/Users/yantingting/Documents/MintechJob/newgenie1/utils3/')
from data_io_utils import *

today_date = datetime.date.today().strftime('%m%d')
file_path = '/Users/yantingting/Seafile/风控/10 印尼/01_模型文档/202004 老用户模型 V3'


query0 = '''
SELECT loan_id, customerid,createtime,ruleresult as score ,datasources
FROM (SELECT id as loan_id
    FROM rt_t_gocash_core_loan
    WHERE return_flag = 'true' and apply_time>='2020-07-11'  and product_id = 1) t1
INNER JOIN (SELECT id as loanid, customerid,createtime,ruleresult,datasources, orderno
           FROM rt_risk_mongo_gocash_riskreport r
           WHERE ruleresultname in('oldUserModelV3Score') and businessid='uku') t2
ON t1.loan_id :: varchar = t2.orderno;
'''

df0 = DataBase().get_df_from_pg(query0)
df0.shape
df0.head()
df0.info()
df0['loan_id'] = df0['loan_id'].astype(str)
df0['score'] = df0['score'].astype(float)
df0['score'].describe()

df0['group'] = pd.qcut(df0['score'], q=10)
df0['group'].value_counts()
df0['score'].value_counts().sort_index()



def json_to_var(data):
    # 解析sql从线上取的数
    data_json= from_json(from_json(data, 'datasources'), 'vars')
    data_json= data_json.drop(['dataSourceName'],1)
    # 变为一个变量一列
    data_prepared = data_json.set_index(['loan_id','customerid','createtime','score','varName']).unstack('varName')
    data_prepared.columns=[col[1] for col in data_prepared.columns]
    data_prepared=data_prepared.reset_index()
    data_prepared = data_prepared.fillna(-1).replace('', -1)
    return (data_prepared)

df_var1= json_to_var(df0)
df_var1.shape
df_var1.head()
df_var1['loan_id'] = df_var1['loan_id'].astype(str)
df_var1.to_excel(os.path.join(file_path,'oldV3.xlsx'))

a = ['bankCodeBni']
