# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 11:09:29 2020

@author: yuexin
"""
import os
import time
import pandas as pd
import numpy as np
import psycopg2
import pickle as pickle

def load_data_from_pickle(file_path, file_name):
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'rb') as infile:
        result = pickle.load(infile)
    return result

path_rawdata = 'D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212/'
path = 'D:/Model Development/202001 IDN new v6/01 Data/'

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


''' **************************** GPS **************************** '''
sql = '''
	select 
	loan_id, customer_id, grouping, apply_time, effective_date, ruleresultname, ruleresult, createtime 
	from (
		select a.id::text as loan_id, a.customer_id::text, a."grouping", a.apply_time, a.effective_date, b.ruleresultname, b.ruleresult, b.createtime
		from rt_t_gocash_core_loan a
		inner join rt_risk_mongo_gocash_riskreport b on a.id::text = b.orderno and b.ruleresultname = 'newUserModelResultV5'
		where a.apply_time::date between '2020-02-21' and '2020-02-22' and a.grouping like '%Test%'
	) t 
'''
r_score = get_df_from_pg(sql)
print(r_score.shape)
print(r_score.loan_id.nunique())

r_score.ruleresult.value_counts(dropna=False)


''' **************************** GPS **************************** '''
sql_gps = '''
select loan_id, jailbreak_status, case when substring(gps,1,1) = '{' then gps::json #>> '{altitude}' end as altitude
from (
select loan_id, a.effective_date, b.*,
row_number() over(partition by loan_id order by b.create_time desc) as rn 
from (
	select 
	loan_id, customer_id, grouping, apply_time, effective_date, ruleresultname, ruleresult, createtime 
	from (
		select a.id::text as loan_id, a.customer_id::text, a."grouping", a.apply_time, a.effective_date, b.ruleresultname, b.ruleresult, b.createtime
		from rt_t_gocash_core_loan a
		inner join rt_risk_mongo_gocash_riskreport b on a.id::text = b.orderno and b.ruleresultname = 'newUserModelResultV5'
		where a.apply_time::date between '2020-02-21' and '2020-02-22' and a.grouping like '%Test%'
	) t 
) a 
left join ( 
	select customer_id, create_time, 
    case when jailbreak_status in ('True','true') then 1 when jailbreak_status in ('False','false') then 0 else -1 end as jailbreak_status, 
    case when simulator_status in ('True','true') then 1 when simulator_status in ('False','false') then 0 else -1 end as simulator_status, gps
--	case when substring(gps,1,1) = '{' then gps::json #>> '{latitude}' end as latitude, 
--	case when substring(gps,1,1) = '{' then gps::json #>> '{longitude}' end as longitude, 
--	case when substring(gps,1,1) = '{' then gps::json #>> '{direction}' end as direction, 
--	case when substring(gps,1,1) = '{' then gps::json #>> '{speed}' end as speed, 
--	case when substring(gps,1,1) = '{' then gps::json #>> '{altitude}' end as altitude 
--	case when substring(gps,1,1) = '{' then gps::json #>> '{accuracy}' end as accuracy
	from gocash_loan_risk_program_baseinfo) b on a.customer_id = b.customer_id and a.apply_time::timestamp >= b.create_time::timestamp
) t 
where rn = 1
'''
r_gps = get_df_from_pg(sql_gps)
print(r_gps.shape)
print(r_gps.loan_id.nunique())



sql_baseinfo = '''
WITH loan as (
	select 
	loan_id, customer_id, grouping, apply_time, effective_date, ruleresultname, ruleresult, createtime 
	from (
		select a.id::text as loan_id, a.customer_id::text, a."grouping", a.apply_time, a.effective_date, b.ruleresultname, b.ruleresult, b.createtime
		from rt_t_gocash_core_loan a
		inner join rt_risk_mongo_gocash_riskreport b on a.id::text = b.orderno and b.ruleresultname = 'newUserModelResultV5'
		where a.apply_time::date between '2020-02-21' and '2020-02-22' and a.grouping like '%Test%'
	) t 
),
baseinfo as 
(
SELECT id as customer_id
    , update_time
    , cell_phone
    , mail
    , id_card_address
    ,marital_status
    ,religion
    ,education
    ,channel
FROM dw_gocash_go_cash_loan_gocash_core_customer
UNION
SELECT customer_id
    , update_time
    , cell_phone
    , mail
    , id_card_address
    , marital_status
    , religion
    ,education
    ,channel
FROM dw_gocash_go_cash_loan_gocash_core_customer_history 
)
SELECT loan_id, 
case when education='SENIOR_HIGH_SCHOOL' then 1 else 0 end as education_SENIOR_HIGH_SCHOOL,
case when education='REGULAR_COLLEGE_COURSE' then 1 else 0 end as education_REGULAR_COLLEGE_COURSE
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , baseinfo.*
            , row_number() over(partition by loan.customer_id order by baseinfo.update_time desc) as rn
      FROM loan
      LEFT JOIN baseinfo  ON loan.customer_id = baseinfo.customer_id::text
      WHERE loan.apply_time::timestamp >= baseinfo.update_time::timestamp
) t
WHERE rn = 1
'''
r_base = get_df_from_pg(sql_baseinfo)
print(r_base.shape)
print(r_base.loan_id.nunique())

'''******************* + advanceAI *******************'''

r_adv_multi = pd.read_excel('D:/Model Development/000001 IDN External Data/07 AdvanceAI/02 回溯数据/返回数据 20200223.xlsx', 
                          dtype = {'loan_id': str}, sheet_name = 'multiplatformscore')

r_adv_multi.rename(columns = {'score': 'multiscore'}, inplace=True)

r_adv_multi = r_adv_multi.drop(['name','idNumber','phoneNumber','createTimestamp'],1)


'''******************* APP *******************'''
app_sql = """
select loan_id, packages
from (select a.*, b.packages, row_number() over(partition by a.loan_id order by b.create_time desc) rn
from (
	select 
	loan_id, customer_id, grouping, apply_time, effective_date, ruleresultname, ruleresult, createtime 
	from (
		select a.id::text as loan_id, a.customer_id::text, a."grouping", a.apply_time, a.effective_date, b.ruleresultname, b.ruleresult, b.createtime
		from rt_t_gocash_core_loan a
		inner join rt_risk_mongo_gocash_riskreport b on a.id::text = b.orderno and b.ruleresultname = 'newUserModelResultV5'
		where a.apply_time::date between '2020-02-21' and '2020-02-22' and a.grouping like '%Test%'
	) t 
) a
left join (select customer_id, create_time, packages from gocash_loan_risk_program_packages) b on a.customer_id::text = b.customer_id
where a.apply_time::timestamp > b.create_time::timestamp) t
where t.rn = 1
"""
app_data_use = get_df_from_pg(app_sql)
print(app_data_use.shape)  
print(app_data_use.loan_id.nunique()) 


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
# app_data_analysis2.to_csv('app_1220_0126.csv', index=False)

# app_data_analysis2 = pd.read_csv('app_1220_0126.csv')

df_app = app_data_analysis2.copy()


from collections import Counter
count_app = Counter(df_app['loan_id'])
app_freq = pd.read_csv('D:\\Model Development\\202001 IDN new v6\\01 Data\\raw data 20200212\\app_freq_train_dict_0107_0120.csv')
train1 = pd.merge(df_app, app_freq, left_on='appName', right_on='app', how = 'left')
var_app = train1.groupby(['loan_id'])['high_freq_app', 'mid_freq_app', 'low_freq_app'].agg('sum').reset_index()
var_app['cnt_app'] = var_app['loan_id'].map(lambda x: count_app[x])

list1 = ['high_freq_app', 'mid_freq_app', 'low_freq_app']
for i in list1:
    var_app['rate_' + i] = var_app.apply(lambda x: x[i]/x['cnt_app'], axis=1)

var_app.loan_id = var_app.loan_id.astype(str)
# var_app.to_csv('var_app_freq_1220_0126.csv', index=False)

# var_app = pd.read_csv('var_app_freq_1220_0126.csv', dtype={'loan_id':str})

# var_dict_app_freq = dp.VarDict(var_app.drop('loan_id', axis = 1), data_sorce='app_freq')
# var_dict_app_freq.to_csv('var_dict_app_freq_0917_1106.csv', index = False)
# print('app频数变量: ', var_app.shape)
var_app.head()
var_app.shape


# 2) 生成idf 字典

# 3) 生成TF-IDF相关的变量

time.sleep(0.5)
from collections import Counter
dict_tfidf = pd.read_csv('D:\\Model Development\\202001 IDN new v6\\01 Data\\raw data 20200212\\app_idf_train_dict_0107_0120.csv')
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


app_for_tfidf = pd.read_csv('D:\\Model Development\\202001 IDN new v6\\01 Data\\raw data 20200212\\app_for_tfidf_0107_0120.csv')
app_tfidf_list = app_for_tfidf['app'].values.tolist()
app_tfidf1=app_tfidf[app_tfidf['appName'].isin(app_tfidf_list)]

# tf*idf 除以分母
# var_app_tfidf = pd.pivot_table(app_tfidf1, index='loan_id', columns = 'appName',values='tf_idf', aggfunc='sum', fill_value=0).reset_index()
# var_app_tfidf = pd.merge(app_data_analysis2[['loan_id']].drop_duplicates(),var_app_tfidf, on='loan_id' , how='left')
# print(var_app_tfidf.shape)   
# var_app_tfidf.loan_id = var_app_tfidf.loan_id.astype(str)

# var_app_tfidf.to_csv('var_app_tfidf_1220_0126.csv', index = False)

# tf*idf 不除以分母
var_app_tfidf2 = pd.pivot_table(app_tfidf1, index='loan_id', columns = 'appName',values='tf_idf2', aggfunc='sum', fill_value=0).reset_index()
var_app_tfidf2 = pd.merge(app_data_analysis2[['loan_id']].drop_duplicates(),var_app_tfidf2, on='loan_id' , how='left')
print(var_app_tfidf2.shape)   
var_app_tfidf2.loan_id = var_app_tfidf2.loan_id.astype(str)

# var_app_tfidf2.to_csv('var_app_tfidf2_0917_1106.csv', index = False)


## 拼接变量

# tf*idf 除以分母
# var_app_freq_w_tfidf = pd.merge(var_app, var_app_tfidf, on = 'loan_id', how = 'inner')
# var_app_freq_w_tfidf.shape
# var_app_freq_w_tfidf.isnull().sum()

# var_app_freq_w_tfidf.loan_id = var_app_freq_w_tfidf.loan_id.astype(str)
# var_app_freq_w_tfidf.to_csv('var_app_freq_w_tfidf_1220_0126.csv', index=False)

# tf*idf 不除以分母
var_app_freq_w_tfidf2 = pd.merge(var_app, var_app_tfidf2, on = 'loan_id', how = 'inner')
var_app_freq_w_tfidf2.shape
var_app_freq_w_tfidf2.isnull().sum()

var_app_freq_w_tfidf2.loan_id = var_app_freq_w_tfidf2.loan_id.astype(str)

var_app_freq_w_tfidf2.to_csv('D:\\Model Development\\202001 IDN new v6\\01 Data\\raw data 20200212\\var_app_freq_w_tfidf2_0221_0222.csv', index=False)

var_app_freq_w_tfidf2 = pd.read_csv('D:\\Model Development\\202001 IDN new v6\\01 Data\\raw data 20200212\\var_app_freq_w_tfidf2_0221_0222.csv', 
                                    dtype = {'loan_id':str})




'''************************** IZI **************************'''
sql = '''
select loan_id, topup_0_30_min, topup_30_60_min, topup_360_720_min
from (
select loan_id, a.effective_date, b.*,
row_number() over(partition by loan_id order by b.create_time desc) as rn 
from (
	select 
	loan_id, customer_id, grouping, apply_time, effective_date, ruleresultname, ruleresult, createtime 
	from (
		select a.id::text as loan_id, a.customer_id::text, a."grouping", a.apply_time, a.effective_date, b.ruleresultname, b.ruleresult, b.createtime
		from rt_t_gocash_core_loan a
		inner join rt_risk_mongo_gocash_riskreport b on a.id::text = b.orderno and b.ruleresultname = 'newUserModelResultV5'
		where a.apply_time::date between '2020-02-21' and '2020-02-22' and a.grouping like '%Test%'
	) t 
) a 
inner join (
	select customer_id, create_time, 
	case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,min}' end as topup_0_30_min,
	case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,min}' end as topup_30_60_min,
	case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,min}' end as topup_360_720_min
	from gocash_oss_to_pup) b on a.customer_id = b.customer_id::text and a.apply_time::timestamp + '8 hour'  >= b.create_time::timestamp
) t 
where rn = 1
'''
r_izitopup = get_df_from_pg(sql)
print(r_izitopup.shape)
print(r_izitopup.loan_id.nunique())

import json
import numpy as np
import pandas as pd
from imp import reload
import itertools
import sys
sys.path.append('D:/_Tools/newgenie-master 20200217')

# from utils3.data_io_utils import *

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
    a1['columns'] = a1[str(var_name)].map(lambda x: list(x.keys()) if isinstance(x, dict) else list(json.loads(x).keys()))
    print('new columns done')
    # 获取json串中的所有字段名称
    add_columns_list = list(set(list(itertools.chain(*a1['columns']))))
    for columns in add_columns_list:
        # 将json串展开
        a1[str(columns)] = a1[str(var_name)].map(lambda x: x.get(str(columns)) if isinstance(x, dict) else json.loads(x).get(str(columns)))
        print(str(columns))
    if 'temp' in a1.columns:
        del a1['temp']
    del a1['columns'], a1[str(var_name)]
    end = time.time()
    print("run time = {}".format(end - start))

    return a1
    
# 从json/dict形式提取出所有日期

def extract_date(dict_ori):
    s=''
    for i in dict_ori.values():
        s = s+","+','.join(i)
    string = s.lstrip(',')
    lst = string.split(',')
    return lst
    
import datetime

def date_to_daysdiff(row, length, col):
    if row[col] == ['']:
        date_diff=[]
    elif row[col] == []:
        date_diff=[]
    else:
        date_diff=[]
        for i in range(length):
            start = row[col][i]
            end = row['apply_time']
            d1 = datetime.datetime.strptime(start, '%Y%m%d')
            d2 = datetime.datetime.strptime(str(end), '%Y%m%d')
            diff = (d2-d1).days
            #print(diff)
            date_diff.append(str(diff))
            #print(date_diff)
            #date_d = date_diff.astype(str)
    return date_diff

    
izi_sql = """
select loan_id
, concat(year, month, day) as apply_time
, date(apply_time) - interval '1 day' as pre_dt
, date(create_time) as create_time, message
from (
select a.*, b.*,
row_number() over(partition by loan_id order by b.create_time desc) as rn 
from (
	select 
	loan_id, customer_id, grouping, apply_time, effective_date, ruleresultname, ruleresult, createtime ,
	to_char(apply_time, 'YYYY') as year , to_char(apply_time, 'MM') as month, to_char(apply_time, 'DD') as day
	from (
		select a.id::text as loan_id, a.customer_id::text, a."grouping", a.apply_time, a.effective_date, b.ruleresultname, b.ruleresult, b.createtime
		from dw_gocash_go_cash_loan_gocash_core_loan a
		inner join rt_risk_mongo_gocash_riskreport b on a.id::text = b.orderno and b.ruleresultname = 'newUserModelResultV5'
		where a.apply_time::date between '2020-02-21' and '2020-02-22' and a.grouping like '%Test%'
	) t 
) a 
inner join (
	select customer_id, create_time, message
	from gocash_oss_inquiries_v4 where status='OK') b on a.customer_id = b.customer_id::text and a.apply_time::timestamp + '8 hour'  >= b.create_time::timestamp
) t 
where rn = 1
"""
izi_df = get_df_from_pg(izi_sql)

try1= from_json(izi_df, 'message')
try2= from_json(try1, 'detail')

try3 = try2.copy()
try3['AA'] = try3['A'].apply(lambda x: extract_date(x))
try3['BB'] = try3['B'].apply(lambda x: extract_date(x))
try3['CC'] = try3['C'].apply(lambda x: extract_date(x))
try3.head()

try4 = try3.copy()

try4.pre_dt = try4.pre_dt.astype(str)


for index, row in try4.iterrows():
    try:
        try4.loc[index, 'AA'] = str(try4.loc[index,'AA']).replace('Within24hours',try4.loc[index, 'pre_dt'].replace('-',''))
        try4.loc[index, 'BB'] = str(try4.loc[index,'BB']).replace('Within24hours',try4.loc[index, 'pre_dt'].replace('-',''))
        try4.loc[index, 'CC'] = str(try4.loc[index,'CC']).replace('Within24hours',try4.loc[index, 'pre_dt'].replace('-',''))
    except:
        print(index)
        pass

import ast

try4['AA'] = try4['AA'].apply(lambda x:ast.literal_eval(x))
try4['BB'] = try4['BB'].apply(lambda x:ast.literal_eval(x))
try4['CC'] = try4['CC'].apply(lambda x:ast.literal_eval(x))


for row in range(try4.shape[0]):
    #print(row)
    a=try4.iloc[row]
    print(a[['AA']])
    print(a[['BB']])
    print(a[['CC']])
    try4.loc[row,'A_daysdiff']= ','.join(date_to_daysdiff(a, len(a['AA']), 'AA'))
    try4.loc[row,'B_daysdiff']= ','.join(date_to_daysdiff(a, len(a['BB']), 'BB'))
    try4.loc[row,'C_daysdiff']= ','.join(date_to_daysdiff(a, len(a['CC']), 'CC'))


try4.A_daysdiff = try4.A_daysdiff.replace('', '-9999')
try4.B_daysdiff = try4.B_daysdiff.replace('', '-9999')
try4.C_daysdiff = try4.C_daysdiff.replace('', '-9999')

try5 = try4.copy()
try5['A_3d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['A_7d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['A_14d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['A_21d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['A_30d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['A_60d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['A_90d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['A_180d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['A_360d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

try5['B_3d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['B_7d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['B_14d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['B_21d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['B_30d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['B_60d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['B_90d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['B_180d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['B_360d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

try5['C_3d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['C_7d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['C_14d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['C_21d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['C_30d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['C_60d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['C_90d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['C_180d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['C_360d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

r_izi = try5[['loan_id','A_3d',
'A_7d',
'A_14d',
'A_21d',
'A_30d',
'A_60d',
'A_90d',
'A_180d',
'A_360d',
'B_3d',
'B_7d',
'B_14d',
'B_21d',
'B_30d',
'B_60d',
'B_90d',
'B_180d',
'B_360d',
'C_3d',
'C_7d',
'C_14d',
'C_21d',
'C_30d',
'C_60d',
'C_90d',
'C_180d',
'C_360d']]


'''************************** MERGE **************************'''
r_all_0221_0222 = r_gps.merge(r_base, on ='loan_id', how='left')
r_all_0221_0222 = r_all_0221_0222.merge(r_izi, on ='loan_id', how='left')
r_all_0221_0222 = r_all_0221_0222.merge(r_izitopup, on ='loan_id', how='left')
r_all_0221_0222 = r_all_0221_0222.merge(r_adv_multi, on ='loan_id', how='left')
r_all_0221_0222 = r_all_0221_0222.merge(var_app_freq_w_tfidf2, on ='loan_id', how='left')

r_all_0221_0222.to_csv(path_rawdata + 'r_all_0221_0222.xlsx', index=False)


'''###################### 打分 ######################'''
import xgboost as xgb
import pandas as pd
import numpy as np
from pandas import read_csv
from pandas import read_excel
from xgboost import DMatrix

# LOAD FUNCTION
def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))
#.map(lambda x: int(x))
    
''' 读取数据 '''
r_all = pd.read_csv(path_rawdata + 'r_all_0221_0222.csv', dtype = {'loan_id':str})

r_all.rename(columns={'education_senior_high_school':'education_SENIOR_HIGH_SCHOOL',
                      'education_regular_college_course':'education_REGULAR_COLLEGE_COURSE'}, inplace = True)

mydata = r_all.copy()

mydata = mydata.fillna(-1)
mydata = mydata.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])
print(mydata.shape)
print(mydata.dtypes)


# LOAD MODEL
mymodel = xgb.Booster() 
mymodel.load_model("D:\\Model Development\\202001 IDN new v6\\04 Document\\开发文档\\model_perf_65_200221_142420.model")  


# LOAD VARIABLES 
myvar = pd.read_excel('D:\\Model Development\\202001 IDN new v6\\04 Document\\印尼UKU产品新客V6模型文档.xlsx',\
                      sheet_name='03_model_importance') 
print(myvar.dtypes)

# PREDICT SCORES
data_features = mydata[list(myvar['指标英文'])]

var_to_float = list(myvar['指标英文'])
for x in var_to_float:
    data_features[x] = data_features[x].astype(float)
print(data_features.dtypes)

data_lean = DMatrix(data_features)
ypred = mymodel.predict(data_lean)


score = [round(Prob2Score(value, 600, 20)) for value in ypred]


bin_prob = [-np.Inf,
0.081456308,
0.105051515,
0.129186535,
0.158755821,
0.195552707,
0.237709829,
0.292495287,
0.37013545,
0.496628916,
np.Inf]

bin_prob20 = [-np.Inf, 0.067612745,
0.081456308,
0.092987199,
0.105051515,
0.116783314,
0.129186535,
0.142778564,
0.158755821,
0.176262865,
0.195552707,
0.216169348,
0.237709829,
0.262765861,
0.292495287,
0.326998889,
0.37013545,
0.426103395,
0.496628916,
0.598211849,
np.Inf]


decile_prob = pd.cut(pd.DataFrame(ypred)[0], bin_prob, precision=0).astype(str)
decile_prob20 = pd.cut(pd.DataFrame(ypred)[0], bin_prob20, precision=0).astype(str)

data_scored = pd.DataFrame([mydata['loan_id'].values, 
                            ypred, score, decile_prob, decile_prob20]).T
data_scored.columns= ['loan_id', 
                      'y_pred', 'score', 'proba_bin', 'proba_bin_20']

print(data_scored.dtypes)
print(data_scored.shape)

data_scored = data_scored.merge(r_score, on ='loan_id', how = 'left')


data_scored.to_excel('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219\\data_scored_0221_0222.xlsx',
                          index=False)
data_scored = pd.read_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219\\data_scored_0221_0222.csv'
                          )

data_scored['score_bin_10'] = pd.qcut(data_scored.score, q=10, duplicates='drop', precision=0).astype(str)
data_scored.head()
data_scored.to_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219\\data_scored_0221_0222 1.csv',
                          index=False)


bin_prob20 = [0.067612745,
0.081456308,
0.092987199,
0.105051515,
0.116783314,
0.129186535,
0.142778564,
0.158755821,
0.176262865,
0.195552707,
0.216169348,
0.237709829,
0.262765861,
0.292495287,
0.326998889,
0.37013545,
0.426103395,
0.496628916,
0.598211849]

for i in bin_prob20:
    print(round(Prob2Score(i, 600, 20)))

# CHECK MISSING 

import utils3.summary_statistics as ss

var_dict = pd.DataFrame(columns= ['数据源', '指标英文', '指标中文', '数据类型'])
var_dict['数据源'] = list(myvar['指标英文'])
var_dict['指标英文'] = list(myvar['指标英文'])
var_dict['指标中文'] = list(myvar['指标英文'])
var_dict['数据类型'] = 'float'

eda = ss.eda(X = data_features, var_dict=var_dict, useless_vars = [],\
       exempt_vars = [], uniqvalue_cutoff=0.97)

eda.to_excel('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219\\eda_0221_0222.xlsx',
           index =False)

