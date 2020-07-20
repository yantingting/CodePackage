# -*- coding: utf-8 -*-
"""
Created on Sun Dec  8 10:57:47 2019

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

''' **************************** FLAG **************************** '''
sql = ''' 
    select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, b.pipelineid, b.newusermodelscorev5::float,
	a.extend_times, a.approved_principal, a.approved_period, a.product_id, 
	case when extend_times>3 then 0
	when paid_off_time::Date-due_date>=3 then 1 
	when loan_status='COLLECTION' and current_date::Date-due_date<3 then 0
	when loan_status='COLLECTION' and current_date::Date-due_date>=3 then 1
	when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then 0
	when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then 0
	else 0 end as flag7
    from dw_gocash_go_cash_loan_gocash_core_loan a 
	inner join risk_mongo_gocash_installmentriskcontrolresult b on a.id::text = b.loanid and pipelineid = '479' and newusermodelscorev5::float <=618
	where effective_date between '2020-01-23' and '2020-01-29'
	union 
	select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, b.pipelineid, b.newusermodelscorev5::float,
	a.extend_times, a.approved_principal, a.approved_period, a.product_id, 
	case when extend_times>3 then 0
	when paid_off_time::Date-due_date>=3 then 1 
	when loan_status='COLLECTION' and current_date::Date-due_date<3 then 0
	when loan_status='COLLECTION' and current_date::Date-due_date>=3 then 1
	when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then 0
	when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then 0
	else 0 end as flag7    
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolresult b on a.id::text = b.loanid and pipelineid = '480' 
	where effective_date between '2020-01-27' and '2020-01-29'

'''
r_flag = get_df_from_pg(sql)
print(r_flag.shape)
print(r_flag.loan_id.nunique())

print(r_flag.flag7.value_counts(dropna=False))

r_flag.groupby('effective_date')['flag7'].sum()



''' **************************** GPS **************************** '''
sql_gps = '''
select loan_id, jailbreak_status, case when substring(gps,1,1) = '{' then gps::json #>> '{altitude}' end as altitude
from (
select loan_id, a.effective_date, b.*,
row_number() over(partition by loan_id order by b.create_time desc) as rn 
from (
	select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, b.pipelineid, b.newusermodelscorev5::float
	from dw_gocash_go_cash_loan_gocash_core_loan a 
	inner join risk_mongo_gocash_installmentriskcontrolresult b on a.id::text = b.loanid and pipelineid = '479' and newusermodelscorev5::float <=618
	where effective_date between '2020-01-23' and '2020-01-29'
	union 
	select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, b.pipelineid, b.newusermodelscorev5::float
	from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolresult b on a.id::text = b.loanid and pipelineid = '480' 
	where effective_date between '2020-01-27' and '2020-01-29'
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
	select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, b.pipelineid, b.newusermodelscorev5::float
	from dw_gocash_go_cash_loan_gocash_core_loan a 
	inner join risk_mongo_gocash_installmentriskcontrolresult b on a.id::text = b.loanid and pipelineid = '479' and newusermodelscorev5::float <=618
	where effective_date between '2020-01-23' and '2020-01-29'
	union 
	select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, b.pipelineid, b.newusermodelscorev5::float
	from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolresult b on a.id::text = b.loanid and pipelineid = '480' 
	where effective_date between '2020-01-27' and '2020-01-29'
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


app_sql = """
select loan_id, packages
from (select a.*, b.packages, row_number() over(partition by a.loan_id order by b.create_time desc) rn
from (
	select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, b.pipelineid, b.newusermodelscorev5::float
	from dw_gocash_go_cash_loan_gocash_core_loan a 
	inner join risk_mongo_gocash_installmentriskcontrolresult b on a.id::text = b.loanid and pipelineid = '479' and newusermodelscorev5::float <=618
	where effective_date between '2020-01-23' and '2020-01-29'
	union 
	select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, b.pipelineid, b.newusermodelscorev5::float
	from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolresult b on a.id::text = b.loanid and pipelineid = '480' 
	where effective_date between '2020-01-27' and '2020-01-29'
)a
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
var_app_freq_w_tfidf2.to_csv('D:\\Model Development\\202001 IDN new v6\\01 Data\\raw data 20200212\\var_app_freq_w_tfidf2_0227_0229.csv', index=False)

var_app_freq_w_tfidf2 = pd.read_csv('D:\\Model Development\\202001 IDN new v6\\01 Data\\raw data 20200212\\var_app_freq_w_tfidf2_0227_0229.csv', 
                                    dtype = {'loan_id':str})


''' **************************** IZI NEW **************************** '''
import json
import pandas as pd
from pandas.api.types import is_dict_like
import numpy as np
import itertools
import time
import os

r_izinew3 = pd.read_excel('D:\\Model Development\\000001 IDN External Data\\01 izidata\\02 返回数据\\20200213\\新izi回溯返回_原始结果 20200225.xlsx', 
                        dtype={'loan_id': str})

izi = r_izinew3[['loan_id','apply_time', '身份证多头v4_result']]
izi['身份证多头v4_result'] = izi['身份证多头v4_result'].str.replace("\'","\"")

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

#解析一层
try1= from_json(izi, '身份证多头v4_result')
try1.head()

#解析两层
try2= from_json(try1, 'detail')
try2.head()

# 从json/dict形式提取出所有日期
def extract_date(dict_ori):
    s=''
    for i in dict_ori.values():
        s = s+","+','.join(i)
    string = s.lstrip(',')
    lst = string.split(',')
    return lst

try3 = try2.copy()
try3['AA'] = try3['A'].apply(lambda x: extract_date(x))
try3['BB'] = try3['B'].apply(lambda x: extract_date(x))
try3['CC'] = try3['C'].apply(lambda x: extract_date(x))
try3.head()

#将日期与申请日期相减计算相差的天数
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

try4 = try3.copy()
for row in range(try4.shape[0]):
    #print(row)
    a=try4.iloc[row]
    try4.loc[row,'A_daysdiff']= ','.join(date_to_daysdiff(a, len(a['AA']), 'AA'))
    try4.loc[row,'B_daysdiff']= ','.join(date_to_daysdiff(a, len(a['BB']), 'BB'))
    try4.loc[row,'C_daysdiff']= ','.join(date_to_daysdiff(a, len(a['CC']), 'CC'))


# 将相差日期衍生成变量
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


try5.loan_id = try5.loan_id.astype(str)

# try5.to_csv('izi_origin_ys.csv', index=False)
# try5.to_excel('izi_origin_ys.xlsx')


izi_ys = try5.drop(['A','B','C','AA','BB','CC','A_daysdiff','B_daysdiff','C_daysdiff'],1)
izi_ys.head()

izi_ys.loan_id = izi_ys.loan_id.astype(str)

# izi_ys.to_csv('izi_ys.csv', index=False)
# izi_ys.to_excel('izi_ys.xlsx')

# 拼接izi其他数据和衍生后的多头

izi_all_new = r_izinew3.merge(izi_ys.drop(['apply_time'],1), on='loan_id',how='left')
izi_all_new.shape


izi_all_new.head()

# izi_all_new = izi_all_new.iloc[:,1:]
izi_all_new.head()
izi_all_new.shape
izi_all_new.columns
izi_all3 = izi_all_new[['loan_id', 
'topup_0_30_avg',
'topup_0_30_times',
'topup_0_30_max',
'topup_0_30_min',
'topup_0_60_avg',
'topup_0_60_times',
'topup_0_60_max',
'topup_0_60_min',
'topup_0_90_avg',
'topup_0_90_times',
'topup_0_90_max',
'topup_0_90_min',
'topup_0_180_avg',
'topup_0_180_times',
'topup_0_180_max',
'topup_0_180_min',
'topup_0_360_avg',
'topup_0_360_times',
'topup_0_360_max',
'topup_0_360_min',
'topup_30_60_avg',
'topup_30_60_times',
'topup_30_60_max',
'topup_30_60_min',
'topup_60_90_avg',
'topup_60_90_times',
'topup_60_90_max',
'topup_60_90_min',
'topup_90_180_avg',
'topup_90_180_times',
'topup_90_180_max',
'topup_90_180_min',
'topup_180_360_avg',
'topup_180_360_times',
'topup_180_360_max',
'topup_180_360_min',
'topup_360_720_avg',
'topup_360_720_times',
'topup_360_720_max',
'topup_360_720_min',
'A_3d',
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


print(izi_all3.shape)
print(izi_all3.loan_id.nunique())
print(izi_all3.columns)
print(izi_all3.dtypes)

izi_all3.to_csv(path_rawdata + 'r_izinew_0227_0229.csv', index=False)


'''******************* + advanceAI *******************'''

r_adv_multi = pd.read_excel('D:/Model Development/000001 IDN External Data/07 AdvanceAI/02 回溯数据/返回数据 20200225.xlsx', 
                          dtype = {'loan_id': str}, sheet_name = 'multiplatformscore')

r_adv_multi.rename(columns = {'score': 'multiscore'}, inplace=True)

r_adv_multi = r_adv_multi.drop(['name','idNumber','phoneNumber','createTimestamp'],1)

    


'''################## 合并准备 ####################'''

''' MERGE ALL '''
r_all = r_flag.merge(r_gps, how='left', on='loan_id')
r_all = r_all.merge(r_base, how='left', on='loan_id')
r_all = r_all.merge(var_app_freq_w_tfidf2, how='left', on='loan_id')
r_all = r_all.merge(r_adv_multi, how='left', on='loan_id')
r_all = r_all.merge(izi_all3, how='left', on='loan_id')

print(r_all.shape)
print(r_all.loan_id.nunique())


r_all.to_csv(path_rawdata + 'r_all_0227_0229.xlsx', index = False)



''' CHECK MISSING '''
# check = r_all.copy()

# check = check.fillna(-1)
# check = check.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999, -1111],[-1,  -1, -1, -1, -1, -1, -1, -1, -1])

# # var = list(check.iloc[:,3:].columns).remove('sample_set')

# var = list(check.columns)
# var = [x for x in var if x not in {'loan_id', 'flag7', 'effective_date', 'sample_set','customer_id', 'apply_time', 'curr_date', 'paid_off_time', 'due_date',
#  'loan_status'}]

# check_missingpct = pd.DataFrame(check.groupby(['effective_date']).size(), columns=['total'])
# for x in var:
#     check[x] = check[x].mask(check[x].ne(-1))
#     check_missing = (check.groupby(['effective_date'])[x].count())
#     check_total = (check.groupby(['effective_date']).size())
#     check_missingpct = check_missingpct.merge(pd.DataFrame(check_missing/check_total, columns=[x]), how='left',left_index=True, right_index=True)

# check_missingpct.to_excel(path_rawdata + 'check_missing_all.xlsx')



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
r_all = pd.read_csv(path_rawdata + 'r_all_0227_0229.xlsx', dtype = {'loan_id':str})

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
                            mydata['flag7'].values, 
                            ypred, score, decile_prob, decile_prob20,                            
                            mydata['newusermodelscorev5'].values,
                            mydata['effective_date'].values]).T
data_scored.columns= ['order_no', 'Y', 
                      'y_pred', 'score', 'proba_bin', 'proba_bin_20', 'scorev5','effective_date']
data_scored['sample_set'] = 'oot2'
data_scored['gt618'] = data_scored.scorev5.apply(lambda x: 1 if x > 618 else 0)

print(data_scored.dtypes)
print(data_scored.shape)

data_scored_all = load_data_from_pickle('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219',
                                        'model_perf_65_200221_142420_data_scored_all.pkl')

data_scored_concat = pd.concat([data_scored, data_scored_all[data_scored_all.sample_set =='oot']],0)
data_scored_concat.Y = data_scored_concat.Y.astype(int)
print(data_scored_concat.shape)

data_scored_concat.to_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219\\data_scored_0123_0129.csv',
                          index=False)



'''************************** LIFT CHART **************************'''
import compileall
compileall.compile_dir(r'D:/_Tools/newgenie-master 20200217')

import pandas as pd
import numpy as np
import os
import pickle
import datetime
import sys
sys.path.append('D:/_Tools/newgenie-master 20200217')
sys.setrecursionlimit(100000)
import utils3.model_training as tr
import utils3.filing as fl
import utils3.plot_tools as pt
import matplotlib.pyplot as plt


'''************************** 0223-0226 **************************'''
g1 = data_scored_concat[(data_scored_concat.sample_set == 'oot') ] # 0223-0226 gt618
g2 = data_scored_concat[(data_scored_concat.sample_set == 'oot') |  # 0223-0226 gt600
                        ((data_scored_concat.gt618 == 0) & 
                         (pd.to_datetime(data_scored_concat.effective_date) <= pd.to_datetime('2020-02-26')))]

FIG_PATH = os.path.join('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219', 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

score_v5_lc = pt.show_result_new(g1, 'y_pred','Y', n_bins = 10, feature_label='0223-0226 gt618')
score_v6_lc = pt.show_result_new(g2, 'y_pred','Y', n_bins = 10, feature_label='0223-0226 gt600')

path = os.path.join(FIG_PATH,"lift_chart_plus600_oot.png")
plt.savefig(path, format='png', dpi=100)
plt.close()

'''************************** 0223-0229 **************************'''
g1 = data_scored_concat[(data_scored_concat.sample_set == 'oot') |  # 0223-0229 gt618
                        (data_scored_concat.gt618 == 1)]
g2 = data_scored_concat.copy()  # 0223-0229 gt600


FIG_PATH = os.path.join('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219', 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

score_v5_lc = pt.show_result_new(g1, 'y_pred','Y', n_bins = 10, feature_label='0223-0229 gt618')
score_v6_lc = pt.show_result_new(g2, 'y_pred','Y', n_bins = 10, feature_label='0223-0229 gt600')

path = os.path.join(FIG_PATH,"lift_chart_plus600_oot3.png")
plt.savefig(path, format='png', dpi=100)
plt.close()





