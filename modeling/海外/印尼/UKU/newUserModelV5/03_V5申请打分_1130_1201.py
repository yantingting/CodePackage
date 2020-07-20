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


usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)


import xgboost as xgb
import pandas as pd
import numpy as np
from pandas import read_csv
from pandas import read_excel
from xgboost import DMatrix

path_applydata = 'D:/Model Development/201912 IDN new v5/01 Data/apply data/'


'''###################### 申请取数 ######################'''
sql_apply = '''
select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01' 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_apply)
r_apply = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_apply.shape)
print(r_apply.loan_id.nunique())
print(r_apply.dtypes)

r_apply = r_apply.rename(columns={'iziphoneageage': 'phone_age'})


sql_izi1 = '''
WITH loan as 
(select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01' 
),
izi as (
SELECT customerid as customer_id
    , createtime
    , "07d"
    , "14d"
    , "21d"
    , "30d"
    , "60d"
    , "90d"
    , total
FROM risk_gocash_mongo_iziinquiriesbytype
)
SELECT loan_id
    , "07d"
    , "14d"
    , "21d"
    , "30d"
    , "60d"
    , "90d"
    , total
FROM (SELECT loan.loan_id
            , loan.apply_time
            , izi.*
            , row_number() over(partition by loan.loan_id order by izi.createtime desc) as rn
      FROM loan
      LEFT join izi on loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_izi1)
r_izi1 = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_izi1.shape)
print(r_izi1.loan_id.nunique())
print(r_izi1.dtypes)


sql_izi2 = '''
WITH loan as 
(select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01' 
),
izi as (
SELECT customerid as customer_id
    , createtime
    , result
FROM risk_gocash_mongo_iziphoneverify
)
SELECT loan_id, case when result='MATCH' then 1 when result = 'NOT_MATCH' then 0 else null end as result
FROM (SELECT loan.loan_id
            , loan.apply_time
            , izi.*
            , row_number() over(partition by loan.loan_id order by izi.createtime desc) as rn
      FROM loan
      LEFT join izi on loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_izi2)
r_izi2 = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_izi2.shape)
print(r_izi2.loan_id.nunique())
print(r_izi2.dtypes)


sql_izi3 = """
WITH loan as 
(
select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01' 
),
izi as 
(
SELECT customerid as customer_id
    , createtime
    , age as phone_age
FROM risk_gocash_mongo_iziphoneage
--WHERE age is not null
)
SELECT loan_id, phone_age
FROM (SELECT loan.loan_id
            , loan.apply_time
            , izi.*
            , row_number() over(partition by loan.customer_id order by izi.createtime desc) as rn
      FROM loan
      LEFT join izi ON loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
conn.rollback()
cur = conn.cursor()
cur.execute(sql_izi3)
r_izi3 = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_izi3.shape)
print(r_izi3.loan_id.nunique())
print(r_izi3.dtypes)


sql_refer = '''
WITH apply as 
(
select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01' 
),
refer as 
(
SELECT customer_id
    , create_time
    , refer_type
--    , case when refer_type = 'BROTHERS_AND_SISTERS' then 1 else 0 end as refer_bro_sis 
--    , case when refer_type = 'PARENTS' then 1 else 0 end as refer_parents
    , case when refer_type = 'SPOUSE' then 1 else 0 end as refer_spouse    
FROM dw_gocash_go_cash_loan_gocash_core_customer_refer
WHERE create_time != '1970-01-01' and refer_type not in ('SELF','KINSFOLK','FRIEND')
)
SELECT loan_id, max(refer_spouse) as refer_spouse
FROM (SELECT apply.customer_id
            , apply.loan_id
            , apply.apply_time
            , refer.*
            , dense_rank() over(partition by apply.loan_id order by refer.create_time desc) as rn
      FROM apply
      LEFT JOIN refer  ON apply.customer_id = refer.customer_id
      WHERE apply.apply_time:: timestamp + '8 hour'  >= refer.create_time) t
WHERE rn = 1
GROUP BY loan_id
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_refer)
r_refer = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_refer.shape)
print(r_refer.loan_id.nunique())
print(r_refer.dtypes)



sql_cust = '''
WITH loan as 
(select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01'
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
SELECT loan_id, case religion when 'ISLAM' then 1 else 0 end as religion_ISLAM,
case education when 'SENIOR_HIGH_SCHOOL' then 1 else 0 end as education_SENIOR_HIGH_SCHOOL,
case education when 'REGULAR_COLLEGE_COURSE' then 1 else 0 end as education_REGULAR_COLLEGE_COURSE
FROM (SELECT loan.loan_id
            , loan.apply_time
            , baseinfo.*
            , row_number() over(partition by loan.loan_id order by baseinfo.update_time desc) as rn
      FROM loan
      LEFT JOIN baseinfo  ON loan.customer_id = baseinfo.customer_id
      WHERE loan.apply_time::timestamp >= baseinfo.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_cust)
r_cust = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_cust.shape)
print(r_cust.loan_id.nunique())
print(r_cust.dtypes)

r_cust = r_cust.rename(columns={'religion_islam': 'religion_ISLAM', 'education_senior_high_school': 'education_SENIOR_HIGH_SCHOOL',\
                       'education_regular_college_course': 'education_REGULAR_COLLEGE_COURSE'})
   
  
sql_cust2 = '''
WITH loan as 
(select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01'
),
baseinfo as 
(
SELECT customer_id
    , update_time
     --计算年龄和性别
    , case when substring(id_card_no, 7,2)::int > 40 then 0 else 1 end as gender --female生日 = 1-31 + 40; male生日=1-31
    , case when substring(id_card_no, 11,2)::int >19 --说明是19XX年出生
        then 119 - substring(id_card_no, 11,2)::int else 19 - substring(id_card_no, 11,2)::int  end as age
FROM dw_gocash_go_cash_loan_gocash_core_customer_history 
)
SELECT loan_id, gender, age
FROM (SELECT loan.loan_id
            , loan.apply_time
            , baseinfo.*
            , row_number() over(partition by loan.loan_id order by baseinfo.update_time desc) as rn
      FROM loan
      LEFT JOIN baseinfo  ON loan.customer_id = baseinfo.customer_id
      WHERE loan.apply_time::timestamp >= baseinfo.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_cust2)
r_cust2 = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_cust2.shape)
print(r_cust2.loan_id.nunique())
print(r_cust2.dtypes)


sql_bank = '''
WITH loan as 
(select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01'
),
bank as (
SELECT customer_id
    , create_time as update_time
    , bank_code
FROM dw_gocash_go_cash_loan_gocash_core_customer_account
UNION
SELECT customer_id
    , update_time
    , bank_code
FROM dw_gocash_go_cash_loan_gocash_core_customer_account_history 
)
SELECT loan_id, case bank_code when 'BCA' then 1 else 0 end as bank_code_BCA, case bank_code when 'BRI' then 1 else 0 end as bank_code_BRI
FROM (SELECT loan.loan_id
            , loan.apply_time
            , bank.*
            , row_number() over(partition by loan.loan_id order by bank.update_time desc) as rn
      FROM loan
      LEFT join bank ON loan.customer_id = bank.customer_id
      WHERE loan.apply_time::timestamp >= bank.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_bank)
r_bank = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_bank.shape)
print(r_bank.loan_id.nunique())
print(r_bank.dtypes)

r_bank = r_bank.rename(columns={'bank_code_bca': 'bankcode_BCA', 'bank_code_bri': 'bankcode_BRI'})


sql_prof = '''
WITH loan as 
(select id::text as loan_id, customer_id, apply_time, effective_date
from dw_gocash_go_cash_loan_gocash_core_loan 
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01'
),
prof as 
(
SELECT customer_id
    , update_time
    , occupation_type
    , job
    , industry_involved
    , monthly_salary
    , company_area
    , employee_number
    , jobless_time_income
    , monthly_income_resource
    , pre_work_industry
    , pre_work_income
FROM dw_gocash_go_cash_loan_gocash_core_cusomer_profession
UNION
SELECT customer_id
    , update_time
    , occupation_type
    , job
    , industry_involved
    , monthly_salary
    , company_area
    , employee_number
    , jobless_time_income
    , monthly_income_resource
    , pre_work_industry
    , pre_work_income
FROM dw_gocash_go_cash_loan_gocash_core_cusomer_profession_history 
)
SELECT loan_id, case when occupation_type in ('OFFICE') and job='MANAJER' then 1 else 0 end as job_MANAJER
FROM (SELECT loan.loan_id
            , loan.apply_time
            , prof.*
            , row_number() over(partition by loan.loan_id order by prof.update_time desc) as rn
      FROM loan
      LEFT JOIN prof ON loan.customer_id = prof.customer_id
      WHERE loan.apply_time::timestamp >= prof.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_prof)
r_prof = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_prof.shape)
print(r_prof.loan_id.nunique())
print(r_prof.dtypes)

r_prof = r_prof.rename(columns={'job_manajer': 'job_MANAJER'})


sql_device = '''
select loan_id, 
case when round(heightpixels::float)=1424 and round(widthpixels::float)=720 then 1 else 0 end as screen_1424_720,
case when upper(brand) = 'SAMSUNG' then 1 else 0 end as brand_SAMSUNG,
case when upper(model) = 'SH-04H' then 1 else 0 end as model_SH04H
from (
select a.id::text as loan_id, b.*, row_number() over(partition by a.id order by b.create_time desc) as rn 
from dw_gocash_go_cash_loan_gocash_core_loan a
left join ( 
	select customer_id, create_time, 
    device_info::json #>> '{brand}' as brand, device_info::json #>> '{Model}' as model,
    device_info::json #>> '{manufacturer}' as manufacturer, device_info::json #>> '{version}' as version, 
    device_info::json #>> '{heightPixels}' as heightpixels, device_info::json #>> '{widthPixels}' as widthpixels
	from gocash_loan_risk_program_baseinfo 
	where device_info <>'' ) b on a.customer_id::text = b.customer_id and a.apply_time::timestamp >= b.create_time
where a.return_flag = 'false' and a.apply_time::date between '2019-11-29' and '2019-12-01'
) t 
where rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_device)
r_device = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_device.shape)
print(r_device.loan_id.nunique())
print(r_device.dtypes)

r_device = r_device.rename(columns={'screen_1424_720': 'screen_(1424.0, 720.0)', 'brand_samsung': 'brand_SAMSUNG', 'model_sh04h': 'model_SH-04H'})


'''###########################  APP变量 ########################### '''
# 更新.pyc文件
import compileall
compileall.compile_dir(r'D:/_Tools/newgenie/utils3')

import sys
sys.path.append('D:/_Tools/newgenie/')
sys.setrecursionlimit(100000)

import utils3.data_processing as dp

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

#------------------
#------ 取数 ------
#------------------

sql_package = '''
select tt1.loan_id, tt2.packages  
from (
select loan_id, id 
from (select a.*, b.id, row_number() over(partition by a.loan_id order by b.create_time desc) rn
from (SELECT id::text as loan_id, customer_id, apply_time
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE return_flag = 'false' and apply_time::date between '2019-11-29' and '2019-12-01') a
left join gocash_loan_risk_program_packages b on a.customer_id::text = b.customer_id and a.apply_time::timestamp >= b.create_time) t
where t.rn = 1) tt1 
left join gocash_loan_risk_program_packages tt2 on tt1.id = tt2.id
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_package)
r_package = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_package.shape)  # (11929, 2)
print(r_package.loan_id.nunique())
print(r_package.dtypes)

app_data = r_package.copy()
app_data.to_csv(path_applydata + 'app_data.csv', index=False)

# app_data = pd.read_csv(path_applydata + 'app_data.csv')
# app_data = app_data.reset_index()
# app_data = app_data.drop(['index'],1)


#----------------------
#------ 解析json ------
#----------------------

# app list原始json
import json
import time
import itertools

x = app_data.copy() 

for row in range(app_data.shape[0]):
    #print(row)
    try:
        json.loads(app_data.iloc[row, 1])
    except:
        x.drop(index=row, inplace=True)
        print(row)
x.shape   # 27462
x.reset_index(drop=True, inplace=True) 
app_data_analysis = from_json(x, 'packages') 
#app_data3 = app_data_analysis.loc[:, ['loan_id', 'appName']]
#app_data3.to_csv('app_0627_1106.csv')
app_data_analysis.shape  # (3043097, 5)
app_data_analysis.head()
app_data_analysis.to_csv(path_applydata + 'app_data_analysis.csv', index=False)


#app_data_analysis = pd.read_csv('app_0917_1106.csv')


#------------------------
#------ 计算tf idf ------
#------------------------

# 频数变量
import time
time.sleep(0.1)
df_app = app_data_analysis.copy()

from collections import Counter
count_app = Counter(df_app['loan_id'])
app_freq = pd.read_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\raw data\\app-0917-1106\\app_freq_train_dict_0917_1106.csv')

train1 = pd.merge(df_app, app_freq, left_on='appName', right_on='app', how = 'left')
var_app = train1.groupby(['loan_id'])['high_freq_app', 'mid_freq_app', 'low_freq_app'].agg('sum').reset_index()
var_app['cnt_app'] = var_app['loan_id'].map(lambda x: count_app[x])
list1 = ['high_freq_app', 'mid_freq_app', 'low_freq_app']
for i in list1:
    var_app['rate_' + i] = var_app.apply(lambda x: x[i]/x['cnt_app'], axis=1)
var_app.loan_id = var_app.loan_id.astype(str)

# var_dict_app_freq = dp.VarDict(var_app.drop('loan_id', axis = 1), data_sorce='app_freq')
# var_dict_app_freq.to_csv('var_dict_app_freq_0917_1106.csv', index = False)
# print('app频数变量: ', var_app.shape)
var_app.head()
var_app.shape

# 2) 生成idf 字典

# 3) 生成TF-IDF相关的变量

time.sleep(0.5)
from collections import Counter
dict_tfidf = pd.read_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\raw data\\app-0917-1106\\app_idf_train_dict_0917_1106.csv')
#dict_tfidf = pd.read_excel('app_idf_train_dict.xlsx')
#dict_tfidf2.tail(20)
#dict_tfidf.tail(20)
app_tfidf_data = app_data_analysis.copy()

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


app_tfidf_list = pd.read_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800\\grid_64_191213_120304.xlsx',\
                      sheet_name='05_model_importance', index = False) 
app_tfidf_list = app_tfidf_list.varName[25:64]
    
app_tfidf1=app_tfidf[app_tfidf['appName'].isin(app_tfidf_list)]
var_app_tfidf = pd.pivot_table(app_tfidf1, index='loan_id', columns = 'appName',values='tf_idf', aggfunc='sum', fill_value=0).reset_index()
var_app_tfidf = pd.merge(app_data_analysis[['loan_id']].drop_duplicates(),var_app_tfidf, on='loan_id' , how='left')
print(var_app_tfidf.shape)   # (11427, 40)
var_app_tfidf.loan_id = var_app_tfidf.loan_id.astype(str)


# 合并频数变量和tfidf变量
var_app_freq_w_tfidf = pd.merge(var_app, var_app_tfidf, on = 'loan_id', how = 'inner')
var_app_freq_w_tfidf.shape
var_app_freq_w_tfidf.isnull().sum()




data_apply = r_apply.merge(r_izi1, on = 'loan_id', how = 'left')
data_apply = data_apply.merge(r_izi2, on = 'loan_id', how = 'left')
data_apply = data_apply.merge(r_izi3, on = 'loan_id', how = 'left')
data_apply = data_apply.merge(r_refer, on = 'loan_id', how = 'left')
data_apply = data_apply.merge(r_cust, on = 'loan_id', how = 'left')
data_apply = data_apply.merge(r_cust2, on = 'loan_id', how = 'left')
data_apply = data_apply.merge(r_bank, on = 'loan_id', how = 'left')
data_apply = data_apply.merge(r_prof, on = 'loan_id', how = 'left')
data_apply = data_apply.merge(r_device, on = 'loan_id', how = 'left')
data_apply = data_apply.fillna(0)
print(data_apply.shape)
data_apply = data_apply.merge(var_app_freq_w_tfidf, on = 'loan_id', how = 'left')
print(data_apply.shape)

'''###################### 申请打分 ######################'''
# LOAD FUNCTION
def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))
#.map(lambda x: int(x))
    
''' 读取数据 '''
# mydata = pd.read_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\raw data\\r_all3.csv')
# mydata.loan_id = mydata.loan_id.astype(str)

mydata = data_apply.copy()

mydata = mydata.fillna(-1)
mydata = mydata.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])
print(mydata.shape)
print(mydata.dtypes)


# LOAD MODEL
mymodel = xgb.Booster() 
mymodel.load_model("D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800 python3.6.8\\grid_64_191219_145432.model")  

# LOAD VARIABLES 
myvar = pd.read_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800\\grid_64_191213_120304.xlsx',\
                      sheet_name='05_model_importance') 
print(myvar.dtypes)

# PREDICT SCORES
data_features = mydata[list(myvar['varName'])]

var_to_float = list(myvar['varName'])
for x in var_to_float:
    data_features[x] = data_features[x].astype(float)
print(data_features.dtypes)

data_lean = DMatrix(data_features)
ypred = mymodel.predict(data_lean)


score = [round(Prob2Score(value, 600, 20)) for value in ypred]


bin_prob = [-np.Inf, 0.1484033465,
0.1936467737,
0.2336990833,
0.2756111026,
0.3217975348,
0.3770893812,
0.4499350786,
0.5520563126,
0.6899961531,
np.Inf]

bin_prob20 = [-np.Inf, 0.1183571722,
0.1484033465,
0.172218129,
0.1936467737,
0.2133354135,
0.2336990833,
0.2539385557,
0.2756111026,
0.296992287,
0.3217975348,
0.3475448564,
0.3770893812,
0.4105168805,
0.4499350786,
0.4977725521,
0.5520563126,
0.6125063747,
0.6899961531,
0.8118900061,
np.Inf]

bin_score = [-np.Inf, 577.0,
594.0,
606.0,
614.0,
622.0,
628.0,
634.0,
641.0,
650.0,
np.Inf]

bin_score20 = [-np.Inf,558.0,
577.0,
587.0,
594.0,
600.0,
606.0,
610.0,
614.0,
618.0,
622.0,
625.0,
628.0,
631.0,
634.0,
638.0,
641.0,
645.0,
650.0,
658.0,
np.Inf]

decile_prob = pd.cut(pd.DataFrame(ypred)[0], bin_prob, precision=0).astype(str)
decile_prob20 = pd.cut(pd.DataFrame(ypred)[0], bin_prob20, precision=0).astype(str)
decile_score = pd.cut(pd.DataFrame(score)[0], bin_score, precision=0).astype(str)
decile_score20 = pd.cut(pd.DataFrame(score)[0], bin_score20, precision=0).astype(str)

data_scored = pd.DataFrame([mydata['loan_id'].values, 
                            mydata['customer_id'].values,
                            mydata['apply_time'].values, 
                            mydata['effective_date'].values,
                            score, decile_score, decile_score20, ypred, decile_prob, decile_prob20]).T
data_scored.columns= ['loan_id', 'customer_id', 'apply_time', 'effective_date',
                      'score', 'decile_score', 'decile_score20', 'prob', 'decile_prob', 'decile_prob20']
data_scored.customer_id = data_scored.customer_id.astype(str)

print(data_scored.dtypes)
print(data_scored.shape)

writer = pd.ExcelWriter('D:\\Model Development\\201912 IDN new v5\\03 Result\\score_apply_1129_1201.xlsx')
data_scored.to_excel(writer, 'data_scored')
writer.save()


select loan_id, apply_time, effective_date,
installspecifyapp,
deniedbyoccupation,
invalidage,
blacklist,
installdangerapp,
denyiziphoneage,
denyiziinquiriesbytypetotal,
newusermodelv4result,
newusermodelv4score
from (
select a.id::text as loan_id, apply_time, effective_date, b.*, row_number() over(partition by a.id order by b.createtime desc) as rn
from dw_gocash_go_cash_loan_gocash_core_loan a 
left join risk_gocash_mongo_riskcontrolresult b on a.customer_id::text = b.customerid and a.apply_time::timestamp + '8 hour' >= b.createtime::timestamp
where return_flag='false' and apply_time::date between '2019-11-29' and '2019-12-01' 
) t 
where rn=1


