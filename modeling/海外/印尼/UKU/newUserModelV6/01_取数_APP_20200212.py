# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 16:34:45 2019

@author: Mint
"""
# 更新.pyc文件
import compileall
compileall.compile_dir(r'D:/_Tools/newgenie/utils3')

import sys
sys.path.append('D:/_Tools/newgenie/')
sys.setrecursionlimit(100000)

import utils3.data_processing as dp

import os
import numpy as np
import pandas as pd

# json解析函数
def from_json(data, var_name: str):
    """

    :param data: dataframe
    :param var_name: column name of json in dataframe, json object: dict or [dict]
    :return:
    """

    a1 = data.copy()
    a1 = a1[~a1[var_name].isna()].reset_index(drop=True)
    other_col_list = list(a1.columns)
    other_col_list.remove(var_name)

    a1[var_name] = a1[var_name].map(lambda x: json.loads(x) if isinstance(x, str) else x)

    if not isinstance(a1[var_name][0], dict) or not isinstance(a1[var_name][1], dict):
        a1[var_name] = a1[var_name].map(lambda x: [{'temp': None}] if len(x) == 0 else x)
        list_len = list(map(len, a1[var_name].values))
        newvalues = np.hstack((np.repeat(a1[other_col_list].values, list_len, axis=0),
                               np.array([np.concatenate(a1[var_name].values)]).T))
        a1 = pd.DataFrame(data=newvalues, columns=other_col_list + [var_name])

    start = time.time()
    # 新增一列'columns'用于存储每一列的json串的字段名
    a1['columns'] = a1[str(var_name)].map(
        lambda x: list(x.keys()) if isinstance(x, dict) else list(json.loads(x).keys()))
    print('new columns done')
    # 获取json串中的所有字段名称
    add_columns_list = list(set(list(itertools.chain(*a1['columns']))))
    for columns in add_columns_list:
        # 将json串展开
        a1[str(columns)] = a1[str(var_name)].map(
            lambda x: x.get(str(columns)) if isinstance(x, dict) else json.loads(x).get(str(columns)))
        print(str(columns))
    if 'temp' in a1.columns:
        del a1['temp']
    del a1['columns'], a1[str(var_name)]
    end = time.time()
    print("run time = {}".format(end - start))

    return a1

import psycopg2
def get_df_from_pg(SQL):
    usename = "postgres"
    password = "Mintq2019"
    db = "risk_dm"
    host = "192.168.2.19"
    port = "5432"
    try:
        conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)
        print("Opened database successfully")
    except Exception as e:
        print(e)
    cur = conn.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    df = pd.DataFrame(rows,columns=[i.name for i in cur.description])
    df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
    return df



os.getcwd()
os.chdir('D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212')

#------------------
#------ 取数 ------
#------------------
app_sql = """
select * 
from (select a.*, b.userpackages, row_number() over(partition by a.loan_id order by b.create_time desc) rn
from (SELECT id as loan_id
        , apply_time
        , effective_date
        , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE return_flag = 'false' and device_approval <> 'IOS' 
and (apply_time::date between '2020-01-07' and '2020-01-20'))a
left join gocash_loan_risk_program_packages b
on a.customer_id::text = b.customer_id
where a.apply_time > b.create_time) t
where t.rn = 1
"""
app_data = get_df_from_pg(app_sql)

app_data.rename(columns = {'userpackages':'packages'}, inplace = True)

print(app_data.shape)  #(65373, 6)
print(app_data.loan_id.nunique()) #65373
app_data.head()

app_data.to_csv('app_json_0107_0120.csv', index=False)

app_data = pd.read_csv('D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212/app_json_0107_0120.csv.csv')

#----------------------
#------ 解析json ------
#----------------------

# app list原始json
import json
import time
import itertools

app_data2 = app_data[['loan_id','packages']] 
x = app_data2.copy() 

for row in range(app_data2.shape[0]):
    #print(row)
    try:
        json.loads(app_data2.iloc[row, 1])
    except:
        x.drop(index=row, inplace=True)
        print(row)
x.shape   # 27462
x.reset_index(drop=True, inplace=True) 
app_data_analysis = from_json(x, 'packages') 
#app_data3 = app_data_analysis.loc[:, ['loan_id', 'appName']]
#app_data3.to_csv('app_0627_1106.csv')
print(app_data_analysis.shape) 
# print(app_data_analysis.loan_id.nunique())
app_data_analysis.head()
app_data_analysis.to_csv('app_0107_0120.csv', index=False)

# app_data_analysis = pd.read_csv('app_0107_0120.csv')

#------------------------
#------ 计算tf idf ------
#------------------------

# 1) 生成频数字典（安装率超过5% 的app作为未来tf-idf考虑的app)
#import pickle
#with open('x_with_y_1206_v4.pkl', 'rb') as infile:
#    traintest = pickle.load(infile)
# traintest=pd.read_csv('r_all2.csv')

app_freq = pd.DataFrame(app_data_analysis['appName'].value_counts()).reset_index()
print('训练集的不同app数量: ' , app_freq.shape[0])
app_freq.rename(columns={'index': 'app', 'appName': 'freq'}, inplace=True)
cnt_loan = app_data_analysis['loan_id'].nunique()
app_freq['rate_freq'] = app_freq['freq'].apply(lambda x: x/cnt_loan)

app_for_tfidf = app_freq[app_freq['rate_freq']>=0.05]
print('可用于tf_idf的app总数是(安装率超过5%)： ', app_for_tfidf.shape[0])

app_for_tfidf.to_csv('app_for_tfidf_0107_0120.csv', index = False)
app_for_tfidf = pd.read_csv('app_for_tfidf_0107_0120.csv')

#训练集的不同app数量:  49562
#可用于tf_idf的app总数是(安装率超过5%)：  1092

high = pd.Series(app_freq['freq'].unique()).quantile(0.98)
mid = pd.Series(app_freq['freq'].unique()).quantile(0.93)
high
mid

min(app_freq.rate_freq)
max(app_freq.rate_freq)

app_freq['high_freq_app'] = app_freq['rate_freq'].apply(lambda x: 1 if x >= 0.3 else 0)
print('高频APP个数', app_freq['high_freq_app'].sum())
app_freq['mid_freq_app'] = app_freq['rate_freq'].apply(lambda x: 1 if (0.2 <= x < 0.3) else 0)
print('中频APP个数', app_freq['mid_freq_app'].sum())
app_freq['low_freq_app'] = app_freq['freq'].apply(lambda x: 1 if x <= 1 else 0)
print('低频APP个数(仅一人安装)', app_freq['low_freq_app'].sum())
# 高频APP个数 18
# 中频APP个数 10
# 低频APP个数(仅一人安装) 38354
app_freq.to_csv('app_freq_train_dict_0107_0120.csv', index=False)


# 频数变量
import time
time.sleep(0.1)

app_sql = """
select loan_id, packages
from (select a.*, b.packages, row_number() over(partition by a.loan_id order by b.create_time desc) rn
from (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
)a
left join gocash_loan_risk_program_packages b on a.customer_id::text = b.customer_id
where a.apply_time > b.create_time) t
where t.rn = 1
"""
app_data_use = get_df_from_pg(app_sql)

print(app_data_use.shape)  
print(app_data_use.loan_id.nunique()) 

app_data_use.to_csv('app_json_1220_0126.csv', index = False)

# app list原始json
import json
import time
import itertools

app_data2 = app_data_use[['loan_id','packages']] 
x = app_data2.copy() 

for row in range(app_data2.shape[0]):
    #print(row)
    try:
        json.loads(app_data2.iloc[row, 1])
    except:
        x.drop(index=row, inplace=True)
        print(row)
x.shape   # 27462
x.reset_index(drop=True, inplace=True) 
app_data_analysis2 = from_json(x, 'packages') 
#app_data3 = app_data_analysis2.loc[:, ['loan_id', 'appName']]
#app_data3.to_csv('app_0627_1106.csv')
print(app_data_analysis2.shape) 
# print(app_data_analysis2.loan_id.nunique())
app_data_analysis2.head()
app_data_analysis2.to_csv('app_1220_0126.csv', index=False)

app_data_analysis2 = pd.read_csv('app_1220_0126.csv')

df_app = app_data_analysis2.copy()


from collections import Counter
count_app = Counter(df_app['loan_id'])
#app_freq = pd.read_csv('app_freq_train_dict.csv')
train1 = pd.merge(df_app, app_freq, left_on='appName', right_on='app', how = 'left')
var_app = train1.groupby(['loan_id'])['high_freq_app', 'mid_freq_app', 'low_freq_app'].agg('sum').reset_index()
var_app['cnt_app'] = var_app['loan_id'].map(lambda x: count_app[x])

list1 = ['high_freq_app', 'mid_freq_app', 'low_freq_app']
for i in list1:
    var_app['rate_' + i] = var_app.apply(lambda x: x[i]/x['cnt_app'], axis=1)

var_app.loan_id = var_app.loan_id.astype(str)
var_app.to_csv('var_app_freq_1220_0126.csv', index=False)

var_app = pd.read_csv('var_app_freq_1220_0126.csv', dtype={'loan_id':str})

# var_dict_app_freq = dp.VarDict(var_app.drop('loan_id', axis = 1), data_sorce='app_freq')
# var_dict_app_freq.to_csv('var_dict_app_freq_0917_1106.csv', index = False)
# print('app频数变量: ', var_app.shape)
var_app.head()
var_app.shape


# 2) 生成idf 字典
time.sleep(0.5)
import math

app_idf_data = app_data_analysis[['loan_id','appName']].copy()
app_idf = pd.pivot_table(app_idf_data.drop_duplicates(), index = 'appName', values = 'loan_id', aggfunc = 'count').reset_index()
app_idf.rename(columns={'loan_id': 'cnt_idf'}, inplace=True)
cnt_document = app_idf_data['loan_id'].nunique()
app_idf['idf'] = app_idf['cnt_idf'].apply(lambda x: math.log10(cnt_document/(x+1)))

app_idf.shape
app_idf.head(10)

app_idf[['appName', 'idf']].to_csv('app_idf_train_dict_0107_0120.csv', index=False)
# app_idf[['appName', 'idf']].to_excel('app_idf_train_dict_0917_1106.xlsx', index=False)

# 3) 生成TF-IDF相关的变量

time.sleep(0.5)
from collections import Counter
dict_tfidf = pd.read_csv('app_idf_train_dict_0107_0120.csv')
#dict_tfidf = pd.read_excel('app_idf_train_dict.xlsx')
#dict_tfidf2.tail(20)
#dict_tfidf.tail(20)
app_tfidf_data = app_data_analysis2.copy()

# 计算每个loan的app个数
cnt_app = Counter(app_tfidf_data['loan_id'])

# 计算TF
app_tfidf =  pd.pivot_table(app_tfidf_data, index=['loan_id', 'appName'], values='packageName',aggfunc='count' ,fill_value=0).reset_index()
app_tfidf.rename(columns = {'packageName': 'cnt_tf'},  inplace=True)
app_tfidf['cnt_app'] = app_tfidf['loan_id'].map(lambda x: cnt_app[x])
app_tfidf['app_tf'] = app_tfidf.apply(lambda x: x['cnt_tf']/x['cnt_app'], axis = 1)
app_tfidf.head(10)

# 计算IDF
app_tfidf = pd.merge(app_tfidf, dict_tfidf, on='appName', how = 'left')
app_tfidf['tf_idf'] = app_tfidf.apply(lambda x: x['app_tf']*x['idf'], axis=1)
app_tfidf['tf_idf2'] = app_tfidf.apply(lambda x: x['cnt_tf']*x['idf'], axis=1) # tf*idf 不除以分母


# app_for_tfidf = pd.read_csv('app_for_tfidf_0917_1106.csv')
app_tfidf_list = app_for_tfidf['app'].values.tolist()
app_tfidf1=app_tfidf[app_tfidf['appName'].isin(app_tfidf_list)]

# tf*idf 除以分母
var_app_tfidf = pd.pivot_table(app_tfidf1, index='loan_id', columns = 'appName',values='tf_idf', aggfunc='sum', fill_value=0).reset_index()
var_app_tfidf = pd.merge(app_data_analysis2[['loan_id']].drop_duplicates(),var_app_tfidf, on='loan_id' , how='left')
print(var_app_tfidf.shape)   
var_app_tfidf.loan_id = var_app_tfidf.loan_id.astype(str)

var_app_tfidf.to_csv('var_app_tfidf_1220_0126.csv', index = False)

# tf*idf 不除以分母
var_app_tfidf2 = pd.pivot_table(app_tfidf1, index='loan_id', columns = 'appName',values='tf_idf2', aggfunc='sum', fill_value=0).reset_index()
var_app_tfidf2 = pd.merge(app_data_analysis2[['loan_id']].drop_duplicates(),var_app_tfidf2, on='loan_id' , how='left')
print(var_app_tfidf2.shape)   
var_app_tfidf2.loan_id = var_app_tfidf2.loan_id.astype(str)

var_app_tfidf2.to_csv('var_app_tfidf2_0917_1106.csv', index = False)

#var_app_tfidf = pd.read_csv('var_app_tfidf.csv')
# var_dict_app_tfidf = dp.VarDict(var_app_tfidf.drop('loan_id', axis = 1), data_sorce='app_freq')
# var_dict_app_tfidf.to_csv('var_dict_app_tfidf_0917_1106.csv', index = False)


## 拼接变量
# var_app = pd.read_csv('var_app_freq_0917_1106.csv')
# var_app_tfidf = pd.read_csv('var_app_tfidf_0917_1106.csv')

# tf*idf 除以分母
var_app_freq_w_tfidf = pd.merge(var_app, var_app_tfidf, on = 'loan_id', how = 'inner')
var_app_freq_w_tfidf.shape
var_app_freq_w_tfidf.isnull().sum()

var_app_freq_w_tfidf.loan_id = var_app_freq_w_tfidf.loan_id.astype(str)
var_app_freq_w_tfidf.to_csv('var_app_freq_w_tfidf_1220_0126.csv', index=False)

# tf*idf 不除以分母
var_app_freq_w_tfidf2 = pd.merge(var_app, var_app_tfidf2, on = 'loan_id', how = 'inner')
var_app_freq_w_tfidf2.shape
var_app_freq_w_tfidf2.isnull().sum()

var_app_freq_w_tfidf2.loan_id = var_app_freq_w_tfidf2.loan_id.astype(str)
var_app_freq_w_tfidf2.to_csv('var_app_freq_w_tfidf2_1220_0126.csv', index=False)







