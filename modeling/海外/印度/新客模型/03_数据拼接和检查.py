#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 03_数据拼接和检查.py
@Time    : 2020-06-05 16:16
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import sys
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
import os
from collections import Counter
sys.path.append('/Users/yantingting/Documents/MintechJob/newgenie1/utils3/')
from data_io_utils import  *

file_path = '/Users/yantingting/Seafile/风控/模型/15 印度/V4模型/01_Data'


# '''--------------------- 基本信息变量衍生 ---------------------'''
df_baseinfo = pd.read_excel(os.path.join(file_path,'baseinfo.xlsx'))
df_baseinfo.info()
df_baseinfo.head()
df_baseinfo['model'] = df_baseinfo['model'].apply(lambda x: x.lower())
df_baseinfo.columns.tolist()

dummy_list = ['user_sexual_distinction',
 'user_marital_status',
'ind_residential_status',
'ind_spoken_language',
'user_usage_of_loan',
'state',
'model',
'user_years_in_company',
 'user_salary',
 'user_educational_level',
 'user_career',
 'user_receipt_bank_name',
 'state1',
'mobile_version'
 ]
frame1 = df_baseinfo[['order_no']].copy()
for cols in dummy_list:
    temp_list = df_baseinfo[cols].value_counts().index.tolist()
    temp_dummy = pd.get_dummies(df_baseinfo[cols])[temp_list]
    temp_dummy.columns = [cols + '_' + str(col) for col in temp_dummy.columns]
    frame1 = frame1.join(temp_dummy)
df_baseinfo_var = df_baseinfo.merge(frame1,on ='order_no')
df_baseinfo_var.to_excel(os.path.join(file_path,'df_baseinfo_var.xlsx'),index = False)



# ''' ***************************  experian 变量  ***************************'''

df_experian = pd.read_excel(os.path.join(file_path,'date_Experian.xlsx'))
df_experian.shape
df_experian.head()
df_experian.info()
dict_experian = create_dict(df_experian,data_sorce='experian',useless_vars=['order_no','effective_date','label','sample_set'])
dict_experian.head()
dict_experian.to_excel(os.path.join(file_path,'dict_experian.xlsx'),index = False)



# ''' ***************************  基本信息+app 变量  ***************************'''
df_baseinfo = pd.read_excel(os.path.join(file_path,'df_baseinfo_var.xlsx'))
df_baseinfo.shape
df_baseinfo.head()
dict_baseinfo = create_dict(df_baseinfo,data_sorce='baseinfo',useless_vars=['order_no','start_date','flag1','flag2', 'flag_extend'])
dict_baseinfo.head()
dict_baseinfo.to_excel(os.path.join(file_path,'dict_baseinfo.xlsx'),index = False)


df_app = pd.read_csv(os.path.join(file_path,'var_app_tfidf.csv'))
df_app1 = pd.read_csv(os.path.join(file_path,'var_app_freq(1).csv'))
df_app = df_app.merge(df_app1,on = 'order_no', how = 'left')
df_app.shape
df_app.head()
dict_app = create_dict(df_app,data_sorce='app',useless_vars=['order_no'])
dict_app.head()
dict_app.to_excel(os.path.join(file_path,'dict_app.xlsx'),index = False)



df_base_app = df_baseinfo.drop(['start_date','flag1','flag2', 'flag_extend'],axis = 1)\
                         .merge(df_app, how='left', on='order_no')
df_base_app.head()
df_use = pd.read_excel(os.path.join(file_path,'date_Experian.xlsx'),usecols=['order_no','effective_date','label','sample_set'])
df_use.head()
df_base_app = df_use.merge(df_base_app, how='left', on='order_no')
df_base_app.head()
df_base_app.to_excel(os.path.join(file_path,'df_base_app.xlsx'),index = False)
dict_base_app = pd.concat([dict_app,dict_baseinfo], axis= 0)
dict_base_app.to_excel(os.path.join(file_path,'dict_base_app.xlsx'),index = False)


