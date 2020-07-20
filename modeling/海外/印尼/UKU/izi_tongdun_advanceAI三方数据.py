import os
import time
import pandas as pd
import numpy as np
import psycopg2
import pickle as pickle

os.getcwd()
RESULT_PATH = 'C:\\Users\\Mint\\Documents\\Python Scripts\\取数\\izi\\'

''' 从pg库取数 '''
import pandas as pd
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

''' 检查缺失比例 '''
def check_missingpct(df, date_col='effective_date'):
    check = df.copy()
    check = check.fillna(-1)
    check = check.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])
    var = list(check.iloc[:,2:].columns)
    check_missingpct = pd.DataFrame(check.groupby([date_col]).size(), columns=['total'])
    for x in var:
        check[x] = check[x].mask(check[x].ne(-1))
        check_missing = (check.groupby([date_col])[x].count())
        check_total = (check.groupby([date_col]).size())
        check_missingpct = check_missingpct.merge(pd.DataFrame(check_missing/check_total, columns=[x]), how='left',left_index=True, right_index=True)
    return check_missingpct

''' 解析json '''
import json
#from pandas.api.types import is_dict_like
import itertools
import time
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


####################
## tongdun credit guard
####################

sql_td ='''
with loan as (
SELECT id::text as loan_id
,customer_id::text
, apply_time::timestamp
, effective_date
FROM dw_gocash_go_cash_loan_gocash_core_loan
where effective_date between '2020-03-01' and '2020-03-10'
and return_flag = 'false'
and device_approval = 'ANDROID'
),
guard as (
select customer_id::text as cus_id, create_time, result_desc
,row_number() over(partition by customer_id order by create_time desc)as num
from gocash_oss_credit_guard_go_no
where result_desc is not null
and result_desc<>''
and business_id='uku'
and create_time::date between '2020-02-29' and '2020-03-10'
)
select loan.*,
guard.*
from loan
left join guard
on loan.customer_id::text = guard.cus_id
and loan.apply_time::timestamp + '8 hour' >= guard.create_time::timestamp
and loan.apply_time::timestamp <= guard.create_time::timestamp
and guard.num = 1
'''

r_td = get_df_from_pg(sql_td)
print(r_td.shape)
print(r_td.loan_id.nunique())
print(r_td.dtypes)
r_td.loan_id = r_td.loan_id.astype(str)
r_td['apply_date']=r_td['apply_time'].apply(lambda x: str(x)[0:10].replace('-','')).astype(int)

data_guard = r_td[['loan_id', 'result_desc']]
try1= from_json(from_json(from_json(from_json(from_json(data_guard, 'result_desc'),'ANTIFRAUD'),'risk_items'),'risk_detail'),'risk_details')
try1.risk_name.value_counts()
try1.risk_name.nunique()
try1.loan_id.nunique()

rule_list=[
'Borrower_applied_multiplatform_in3day',
'Borrower_applied_multiplatform_in7day',
'Borrower_applied_multiplatform_in14day',
'Borrower_applied_multiplatform_in30day',
'Borrower_applied_multiplatform_in90day',
'Borrower_applied_multiplatform_in180day',
'Borrower_nightapplied_multiplatform_in30day',
'Borrower_nightapplied_multiplatform_in90day',
'Borrower_nightapplied_multiplatform_in180day',
'Borrower_applied_times_in3day',
'Borrower_applied_times_in7day',
'Borrower_applied_times_in14day',
'Borrower_applied_times_in30day',
'Borrower_applied_times_in90day',
'Borrower_applied_times_in180day',
'Borrower_nightapplied_times_in30day',
'Borrower_nightapplied_times_in90day',
'Borrower_nightapplied_times_in180day',
'Borrower_apply_cycle_in30day',
'Borrower_apply_cycle_in90day',
'Borrower_apply_cycle_in180day',
'Borrower_granted_multiplatform_in3day',
'Borrower_granted_multiplatform_in7day',
'Borrower_granted_multiplatform_in14day',
'Borrower_granted_multiplatform_in30day',
'Borrower_granted_multiplatform_in90day',
'Borrower_granted_multiplatform_in180day',
'Borrower_nightgranted_multiplatform_in30day',
'Borrower_nightgranted_multiplatform_in90day',
'Borrower_nightgranted_multiplatform_in180day',
'Borrower_granted_times_in3day',
'Borrower_granted_times_in7day',
'Borrower_granted_times_in14day',
'Borrower_granted_times_in30day',
'Borrower_granted_times_in90day',
'Borrower_granted_times_in180day',
'Borrower_nightgranted_times_in30day',
'Borrower_nightgranted_times_in90day',
'Borrower_nightgranted_times_in180day',
'Borrower_grant_cycle_in30day',
'Borrower_grant_cycle_in90day',
'Borrower_grant_cycle_in180day'
]
var_list = [
'multiplatform_apply_3day_count',
'multiplatform_apply_7day_count',
'multiplatform_apply_14day_count',
'multiplatform_apply_1month_count',
'multiplatform_apply_3month_count',
'multiplatform_apply_6month_count',
'multiplatform_apply_1month_nightapply_count',
'multiplatform_apply_3month_nightapply_count',
'multiplatform_apply_6month_nightapply_count',
'multiplatform_apply_3day_total',
'multiplatform_apply_7day_total',
'multiplatform_apply_14day_total',
'multiplatform_apply_1month_total',
'multiplatform_apply_3month_total',
'multiplatform_apply_6month_total',
'multiplatform_apply_1month_nightapply_total',
'multiplatform_apply_3month_nightapply_total',
'multiplatform_apply_6month_nightapply_total',
'multiplatform_apply_1month_applycycle',
'multiplatform_apply_3month_applycycle',
'multiplatform_apply_6month_applycycle',
'multiplatform_grant_3day_count',
'multiplatform_grant_7day_count',
'multiplatform_grant_14day_count',
'multiplatform_grant_1month_count',
'multiplatform_grant_3month_count',
'multiplatform_grant_6month_count',
'multiplatform_grant_1month_nightgrant_count',
'multiplatform_grant_3month_nightgrant_count',
'multiplatform_grant_6month_nightgrant_count',
'multiplatform_grant_3day_total',
'multiplatform_grant_7day_total',
'multiplatform_grant_14day_total',
'multiplatform_grant_1month_total',
'multiplatform_grant_3month_total',
'multiplatform_grant_6month_total',
'multiplatform_grant_1month_nightgrant_total',
'multiplatform_grant_3month_nightgrant_total',
'multiplatform_grant_6month_nightgrant_total',
'multiplatform_grant_1month_grantcycle',
'multiplatform_grant_3month_grantcycle',
'multiplatform_grant_6month_grantcycle'
]
mapping = dict(zip(rule_list,var_list))

try2 = try1.loc[try1.risk_name.isin(rule_list),['loan_id','risk_name','mh_value']]
try2 = try2[~try2.mh_value.isnull()]
try2.risk_name.value_counts()
var_guard = try2.set_index(['loan_id','risk_name']).unstack().reset_index()
var_guard.columns=[col[1] for col in var_guard.columns]
var_guard.rename(columns={'':'loan_id'}, inplace=True)
var_guard.loan_id=var_guard.loan_id.astype(str)
var_guard = var_guard.rename(columns = mapping)


''' 检查缺失比例 '''
check_guard= check_missingpct(var_guard, date_col='effective_date')
check_guard.to_excel(RESULT_PATH + 'check_missingpct_td_guard.xlsx')

var_guard = var_guard.drop('effective_date', 1)
var_guard.to_csv(RESULT_PATH + 'r_td_guard.csv',index=False)


####################
## tongdun full vision
####################
sql_fv ='''
with loan as (
SELECT id::text as loan_id
,customer_id::text
, apply_time::timestamp
, effective_date
FROM dw_gocash_go_cash_loan_gocash_core_loan
where effective_date between '2020-03-01' and '2020-03-10'
and return_flag = 'false'
and device_approval = 'ANDROID'
),
fv as (
select customer_id::text as cus_id, create_time as create_time, data
,row_number() over(partition by customer_id order by create_time desc)as num
from gocash_oss_full_version
where data is not null
and data<>''
and business_id='uku'
and create_time::date between '2020-02-12' and '2020-03-06'
)
select loan.*,
fv.*
from loan
left join fv
on loan.customer_id::text = fv.cus_id
and loan.apply_time::timestamp + '8 hour' >= fv.create_time2::timestamp
and loan.apply_time::timestamp <= fv.create_time::timestamp
and fv.num = 1
'''

r_fv = get_df_from_pg(sql_fv)
print(r_fv.shape)
print(r_fv.loan_id.nunique())
print(r_fv.dtypes)
r_fv.loan_id = r_fv.loan_id.astype(str)

data_fv = r_fv[['loan_id','data']]
try1= from_json(from_json(from_json(data_fv, 'data'),'task_data'),'LoanRegcheckObj')
var_fv = try1.drop(['created_time', 'channel_code', 'mobile', 'lost_data',
       'user_name', 'channel_type', 'channel_attr', 'channel_src',
       'mobile_country_code'],1)

''' 检查缺失比例 '''
check_fv= check_missingpct(var_fv, date_col='effective_date')
check_fv.to_excel(RESULT_PATH + 'check_missingpct_td_fv.xlsx')

var_fv = var_fv.drop('effective_date', 1)
var_fv.to_csv(RESULT_PATH + 'r_td_fv.csv',index=False)




#----------------#
# izi: phoneage
#----------------#

sql_izi1 = '''
select loan_id, effective_date
,coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> 'age' as int) end, age) as izi_phoneage
from (
    select 
    *, row_number() over(partition by a.loan_id order by coalesce(b.create_time::timestamp, c.createtime::timestamp) desc) as rn
    from (
        SELECT id::text as loan_id
        , customer_id::text
        , apply_time::timestamp
        , effective_date
        FROM dw_gocash_go_cash_loan_gocash_core_loan
        where effective_date between '2020-03-01' and '2020-03-10'
        and return_flag = 'false'
        and device_approval = 'ANDROID'
    ) a 
    left join gocash_oss_phone_age b on a.customer_id = b.customer_id and a.effective_date >= b.create_time::date
    left join risk_gocash_mongo_iziphoneage c on a.customer_id = c.customerid::text and a.effective_date >= c.createtime::date
) t 
where rn = 1 
'''
r_izi1 = get_df_from_pg(sql_izi1)
print(r_izi1.shape)
print(r_izi1.loan_id.nunique())
print(r_izi1.dtypes)
r_izi1.loan_id = r_izi1.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi1 = check_missingpct(r_izi1, date_col='effective_date')
check_izi1.to_excel(RESULT_PATH + 'check_missingpct_izi_phoneage.xlsx')

r_izi1 = r_izi1.drop('effective_date', 1)
r_izi1.to_csv(RESULT_PATH + 'r_izi_phoneage.csv',index=False)


#-----------------------#
# izi: phone inquiry
#-----------------------#

sql_izi2 = '''
select loan_id, effective_date
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> 'total' as int) end, total) as izi_total
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '07d' as int) end, "07d") as izi_07d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '14d' as int) end, "14d") as izi_14d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '21d' as int) end, "21d") as izi_21d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '30d' as int) end, "30d") as izi_30d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '60d' as int) end, "60d") as izi_60d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '90d' as int) end, "90d") as izi_90d
from (
    select 
    *, row_number() over(partition by a.loan_id order by coalesce(b.create_time::timestamp, c.createtime::timestamp) desc) as rn
    from (
        SELECT id::text as loan_id
        ,customer_id::text
        , apply_time::timestamp
        , effective_date
        FROM dw_gocash_go_cash_loan_gocash_core_loan
        where effective_date between '2020-03-01' and '2020-03-10'
        and return_flag = 'false'
        and device_approval = 'ANDROID'
    ) a 
    left join gocash_oss_inquiries_cell_phone b on a.customer_id = b.customer_id and a.effective_date >= b.create_time::date
    left join risk_gocash_mongo_iziinquiriesbytype c on a.customer_id = c.customerid::text and a.effective_date >= c.createtime::date
) t 
where rn = 1 
'''
r_izi2 = get_df_from_pg(sql_izi2)
print(r_izi2.shape)
print(r_izi2.loan_id.nunique())
print(r_izi2.dtypes)
r_izi2.loan_id = r_izi2.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi2 = check_missingpct(r_izi2, date_col='effective_date')
check_izi2.to_excel(RESULT_PATH + 'check_missingpct_izi_phoneinquiry.xlsx')

r_izi2 = r_izi2.drop('effective_date', 1)
r_izi2.to_csv(RESULT_PATH + 'r_izi_phoneinquiry.csv',index=False)



#----------------------#
# izi: phone verify
#----------------------#

sql_izi3 = '''
select loan_id, effective_date
,case when coalesce(message, result)='MATCH' then 1 when coalesce(message, result) ='NOT_MATCH' then 0 else -1 end as izi_phoneverify
from (
    select 
    *, row_number() over(partition by a.loan_id order by coalesce(b.create_time::timestamp, c.createtime::timestamp) desc) as rn
    from (
        SELECT id::text as loan_id
        ,customer_id::text
        , apply_time::timestamp
        , effective_date
        FROM dw_gocash_go_cash_loan_gocash_core_loan
        where effective_date between '2020-03-01' and '2020-03-10'
        and return_flag = 'false'
        and device_approval = 'ANDROID'
    ) a 
    left join gocash_oss_phone_verify b on a.customer_id = b.customer_id and a.effective_date >= b.create_time::date
    left join risk_gocash_mongo_iziphoneverify c on a.customer_id = c.customerid::text and a.effective_date >= c.createtime::date
) t 
where rn = 1 
'''
r_izi3 = get_df_from_pg(sql_izi3)
print(r_izi3.shape)
print(r_izi3.loan_id.nunique())
print(r_izi3.dtypes)
r_izi3.loan_id = r_izi3.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi3 = check_missingpct(r_izi3, date_col='effective_date')
check_izi3.to_excel(RESULT_PATH + 'check_missingpct_izi_phoneverify.xlsx')

r_izi3 = r_izi3.drop('effective_date', 1)
r_izi3.to_csv(RESULT_PATH + 'r_izi_phoneverify.csv',index=False)


#----------------#
# izi-topup
#----------------#

sql_izi4 = '''
select loan_id, effective_date,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,times}' end as topup_0_30_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,min}' end as topup_0_30_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,max}' end as topup_0_30_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,avg}' end as topup_0_30_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_60,times}' end as topup_0_60_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_60,min}' end as topup_0_60_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_60,max}' end as topup_0_60_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_60,avg}' end as topup_0_60_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,times}' end as topup_0_90_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,min}' end as topup_0_90_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,max}' end as topup_0_90_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,avg}' end as topup_0_90_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,times}' end as topup_0_180_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,min}' end as topup_0_180_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,max}' end as topup_0_180_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,avg}' end as topup_0_180_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,times}' end as topup_0_360_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,min}' end as topup_0_360_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,max}' end as topup_0_360_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,avg}' end as topup_0_360_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,times}' end as topup_30_60_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,min}' end as topup_30_60_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,max}' end as topup_30_60_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,avg}' end as topup_30_60_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,times}' end as topup_60_90_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,min}' end as topup_60_90_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,max}' end as topup_60_90_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,avg}' end as topup_60_90_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,times}' end as topup_90_180_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,min}' end as topup_90_180_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,max}' end as topup_90_180_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,avg}' end as topup_90_180_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,times}' end as topup_180_360_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,min}' end as topup_180_360_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,max}' end as topup_180_360_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,avg}' end as topup_180_360_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,times}' end as topup_360_720_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,min}' end as topup_360_720_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,max}' end as topup_360_720_max,
case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,avg}' end as topup_360_720_avg
from (
    select *, row_number() over(partition by a.loan_id order by b.create_time::timestamp desc) as rn
    from (
        SELECT id::text as loan_id
        ,customer_id::text
        , apply_time::timestamp
        , effective_date
        FROM dw_gocash_go_cash_loan_gocash_core_loan
        where effective_date between '2020-03-01' and '2020-03-10'
        and return_flag = 'false'
    	and device_approval = 'ANDROID'
    ) a
    left join gocash_oss_to_pup b 
    on a.customer_id = b.customer_id 
    and a.effective_date >= b.create_time::date
)t
where rn = 1
'''

r_izi4 = get_df_from_pg(sql_izi4)
print(r_izi4.shape)
print(r_izi4.loan_id.nunique())
print(r_izi4.dtypes)
r_izi4.loan_id = r_izi4.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi4 = check_missingpct(r_izi4, date_col='effective_date')
check_izi4.to_excel(RESULT_PATH + 'check_missingpct_izi_topup.xlsx')

r_izi4.to_csv(RESULT_PATH + 'r_izi_topup.csv',index=False)



#----------------#
# izi-idinquiry
#----------------#

sql_izi5 ='''
with loan as (
SELECT id::text as loan_id
,customer_id::text
, apply_time::timestamp
, effective_date
FROM dw_gocash_go_cash_loan_gocash_core_loan
where effective_date between '2020-03-01' and '2020-03-10'
and return_flag = 'false'
	and device_approval = 'ANDROID'
),
izi_II as (
select customer_id::text as cus_id2, create_time as create_time2, message
,row_number() over(partition by customer_id order by create_time desc)as num
from gocash_oss_inquiries_v4
where message is not null
and message<>''
and status='OK'
and business_id='uku'
and create_time::date between '2020-02-29' and '2020-03-10'
)
select loan.loan_id, loan.apply_time, loan.effective_date,
izi_II.message
from loan
inner join izi_II
on loan.customer_id::text = izi_II.cus_id2
and loan.apply_time::timestamp + '8 hour' >= izi_II.create_time2::timestamp
and loan.apply_time::timestamp <= izi_II.create_time2::timestamp
and izi_II.num = 1
'''

r_izi5 = get_df_from_pg(sql_izi5)
print(r_izi5.shape)
print(r_izi5.loan_id.nunique())
print(r_izi5.dtypes)
r_izi5.loan_id = r_izi5.loan_id.astype(str)


#解析一层
r_izi5['apply_date']=r_izi5['apply_time'].apply(lambda x: str(x)[0:10].replace('-','')).astype(int)
#r_izi5_2 = r_izi5[r_izi5.message !="This md5 can't be found in our system"]
try1= from_json(from_json(r_izi5, 'message'),'detail')

# 从json/dict形式提取出所有日期
def extract_date(dict_ori):
    if dict_ori=={}:
        lst=[]
    else:
        s=''
        for i in dict_ori.values():
            s = s+","+','.join(i)
        string = s.lstrip(',')
        lst = string.split(',')
    return lst

try2 = try1.copy()
try2['AA'] = try2['A'].apply(lambda x: extract_date(x))
try2['BB'] = try2['B'].apply(lambda x: extract_date(x))
try2['CC'] = try2['C'].apply(lambda x: extract_date(x))

# within24hours替换为createtime日期or申请日期，并去除引号
try2['AA'] = try2.apply(lambda x: list(str(x['apply_date']) if i=='Within24hours' else i for i in x['AA']), axis=1)
try2['BB'] = try2.apply(lambda x: list(str(x['apply_date']) if i=='Within24hours' else i for i in x['BB']), axis=1)
try2['CC'] = try2.apply(lambda x: list(str(x['apply_date']) if i=='Within24hours' else i for i in x['CC']), axis=1)


#将日期与申请日期相减计算相差的天数
import datetime
def date_to_daysdiff(row, length, col):
    if row[col] == ['']:
        date_diff=[]
    elif row[col] == []:
        date_diff=[]
    elif row[col] == '[]':
        date_diff=[]
    else:
        date_diff=[]
        for i in range(length):
            start = row[col][i]
            end = row['apply_date']
            d1 = datetime.datetime.strptime(start, '%Y%m%d')
            d2 = datetime.datetime.strptime(str(end), '%Y%m%d')
            diff = (d2-d1).days
            #print(diff)
            date_diff.append(str(diff))
            #print(date_diff)
            #date_d = date_diff.astype(str)
    return date_diff

try3=try2.copy()
for row in range(try3.shape[0]):
    print(row)
    a=try3.iloc[row]
    try3.loc[row,'A_daysdiff']= ','.join(date_to_daysdiff(a, len(a['AA']), 'AA'))
    try3.loc[row,'B_daysdiff']= ','.join(date_to_daysdiff(a, len(a['BB']), 'BB'))
    try3.loc[row,'C_daysdiff']= ','.join(date_to_daysdiff(a, len(a['CC']), 'CC'))

# 将相差日期衍生成变量
try4 = try3.copy()
try4.A_daysdiff = try4.A_daysdiff.replace('', '-9999')
try4.B_daysdiff = try4.B_daysdiff.replace('', '-9999')
try4.C_daysdiff = try4.C_daysdiff.replace('', '-9999')

try5 = try4.copy()
try5['A_II_3d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['A_II_7d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['A_II_14d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['A_II_21d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['A_II_30d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['A_II_60d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['A_II_90d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['A_II_180d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['A_II_360d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))
try5['B_II_3d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['B_II_7d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['B_II_14d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['B_II_21d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['B_II_30d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['B_II_60d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['B_II_90d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['B_II_180d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['B_II_360d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))
try5['C_II_3d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['C_II_7d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['C_II_14d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['C_II_21d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['C_II_30d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['C_II_60d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['C_II_90d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['C_II_180d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['C_II_360d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

izi_ys = try5.drop(['A','B','C','AA','BB','CC','A_daysdiff','B_daysdiff','C_daysdiff'],1)
izi_ys.loan_id = izi_ys.loan_id.astype(str)
izi_ys = izi_ys.drop(['apply_time','apply_date'], 1)

''' 检查缺失比例 '''
check_izi5 = check_missingpct(izi_ys, date_col='effective_date')
check_izi5.to_excel(RESULT_PATH + 'check_missingpct_izi_idinquiry.xlsx')

izi_ys = izi_ys.drop('effective_date', 1)
izi_ys.to_csv(RESULT_PATH + 'r_izi_idinquiry.csv',index=False)


#----------------------#
# izi: phone score
#----------------------#

sql_izi6 = '''
select loan_id, effective_date
,case when substring(message,1,1) = '{' then message::json ->> 'score' end as phone_score
from (
    select 
    *, row_number() over(partition by a.loan_id order by b.create_time::timestamp desc) as rn
    from (
        SELECT id::text as loan_id
        ,customer_id::text
        , apply_time::timestamp
        , effective_date
        FROM dw_gocash_go_cash_loan_gocash_core_loan
        where effective_date between '2020-03-01' and '2020-03-10'
        and return_flag = 'false'
        and device_approval = 'ANDROID'
    ) a 
    left join gocash_oss_phone_score b 
    on a.customer_id = b.customer_id 
    and a.effective_date >= b.create_time::date
) t 
where rn = 1 
'''
r_izi6 = get_df_from_pg(sql_izi6)
print(r_izi6.shape)
print(r_izi6.loan_id.nunique())
print(r_izi6.dtypes)
r_izi6.loan_id = r_izi6.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi6 = check_missingpct(r_izi6, date_col='effective_date')
check_izi6.to_excel(RESULT_PATH + 'check_missingpct_izi_phonescore.xlsx')

r_izi6 = r_izi6.drop('effective_date', 1)
r_izi6.to_csv(RESULT_PATH + 'r_izi_phonescore.csv',index=False)


#----------------------#
# izi: preference
#----------------------#

sql_izi7 = '''
select loan_id, effective_date
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{bank,060d}' end as preference_bank_60d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{bank,090d}' end as preference_bank_90d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{bank,180d}' end as preference_bank_180d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{bank,270d}' end as preference_bank_270d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{game,060d}' end as preference_game_60d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{game,090d}' end as preference_game_90d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{game,180d}' end as preference_game_180d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{game,270d}' end as preference_game_270d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{ecommerce,060d}' end as preference_ecommerce_60d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{ecommerce,090d}' end as preference_ecommerce_90
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{ecommerce,180d}' end as preference_ecommerce_180d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{ecommerce,270d}' end as preference_ecommerce_270d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{lifestyle,060d}' end as preference_lifestyle_60d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{lifestyle,090d}' end as preference_lifestyle_90d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{lifestyle,180d}' end as preference_lifestyle_180d
,case when status='OK' and substring(message,1,1) = '{' then message::json #>>'{lifestyle,270d}' end as preference_lifestyle_270d
from (
    select 
    *, row_number() over(partition by a.loan_id order by b.create_time::timestamp desc) as rn
    from (
        SELECT id::text as loan_id
        ,customer_id::text
        , apply_time::timestamp
        , effective_date
        FROM dw_gocash_go_cash_loan_gocash_core_loan
        where effective_date between '2020-03-01' and '2020-03-10'
        and return_flag = 'false'
        and device_approval = 'ANDROID'
    ) a 
    left join gocash_oss_preference b 
    on a.customer_id = b.customer_id 
    and a.effective_date >= b.create_time::date
) t 
where rn = 1 

'''
r_izi7 = get_df_from_pg(sql_izi7)
print(r_izi7.shape)
print(r_izi7.loan_id.nunique())
print(r_izi7.dtypes)
r_izi7.loan_id = r_izi7.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi7 = check_missingpct(r_izi7, date_col='effective_date')
check_izi7.to_excel(RESULT_PATH + 'check_missingpct_izi_preference.xlsx')

r_izi7 = r_izi7.drop('effective_date', 1)
r_izi7.to_csv(RESULT_PATH + 'r_izi_preference.csv',index=False)


#----------------------#
# izi: operator
#----------------------#

sql_izi8 = '''
select loan_id, effective_date
,case when status='ok' and substring(message,1,1) = '{' then message::json #>>'{carrier,en}' end as izi_operator
from (
    select 
    *, row_number() over(partition by a.loan_id order by b.create_time::timestamp desc) as rn
    from (
        SELECT id::text as loan_id
        ,customer_id::text
        , apply_time::timestamp
        , effective_date
        FROM dw_gocash_go_cash_loan_gocash_core_loan
        where effective_date between '2020-03-01' and '2020-03-10'
        and return_flag = 'false'
        and device_approval = 'ANDROID'
    ) a 
    left join gocash_oss_phone_operator b 
    on a.customer_id = b.customer_id 
    and a.effective_date >= b.create_time::date
) t 
where rn = 1 

'''
r_izi8 = get_df_from_pg(sql_izi8)
print(r_izi8.shape)
print(r_izi8.loan_id.nunique())
print(r_izi8.dtypes)
r_izi8.loan_id = r_izi8.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi8 = check_missingpct(r_izi8, date_col='effective_date')
check_izi8.to_excel(RESULT_PATH + 'check_missingpct_izi_operator.xlsx')

r_izi8 = r_izi8.drop('effective_date', 1)
r_izi8.to_csv(RESULT_PATH + 'r_izi_operator.csv',index=False)


#----------------------#
# 拼接全部izi数据
#----------------------#
r_izi_all = izi_1.merge(izi_2, on='loan_id', how='left')\
.merge(izi_3, on='loan_id', how='left')\
.merge(izi_4, on='loan_id', how='left')\
.merge(izi_5, on='loan_id', how='left')\
.merge(izi_6, on='loan_id', how='left')\
.merge(izi_7, on='loan_id', how='left')\
.merge(izi_8, on='loan_id', how='left')


pickle.dump(r_izi_all, open(os.path.join(RESULT_PATH,'r_izi_all.pkl'), "wb"))



#----------------------#
# advanceAI: multiplatform score
#----------------------#

sql_adv = '''
select loan_id, effective_date
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{score}' end as adv_multiscore
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_104}' end as GD_M_104
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_237}' end as GD_M_237
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_105}' end as GD_M_105
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_106}' end as GD_M_106
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_337}' end as GD_M_337
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_348}' end as GD_M_348
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_227}' end as GD_M_227
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_107}' end as GD_M_107
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_109}' end as GD_M_109
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_72}' end as GD_M_72
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_41}' end as GD_M_41
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_57}' end as GD_M_57
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_89}' end as GD_M_89
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_261}' end as GD_M_261
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_164}' end as GD_M_164
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_120}' end as GD_M_120
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_121}' end as GD_M_121
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_210}' end as GD_M_210
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_177}' end as GD_M_177
,case when message='OK' and substring(data,1,1) = '{' then data::json #>>'{features, GD_M_320}' end as GD_M_320
from (
    select 
    *, row_number() over(partition by a.loan_id order by b.create_time::timestamp desc) as rn
    from (
        SELECT id::text as loan_id
        ,customer_id::text
        , apply_time::timestamp
        , effective_date
        FROM dw_gocash_go_cash_loan_gocash_core_loan
        where effective_date between '2020-03-01' and '2020-03-10'
        and return_flag = 'false'
        and device_approval = 'ANDROID'
    ) a 
    left join gocash_oss_multi_platform_score b 
    on a.customer_id = b.customer_id 
    and a.effective_date >= b.create_time::date
) t 
where rn = 1 

'''
r_adv = get_df_from_pg(sql_adv)
print(r_adv.shape)
print(r_adv.loan_id.nunique())
print(r_adv.dtypes)
r_adv.loan_id = r_adv.loan_id.astype(str)

''' 检查缺失比例 '''
check_adv = check_missingpct(r_adv, date_col='effective_date')
check_adv.to_excel(RESULT_PATH + 'check_missingpct_adv.xlsx')

r_adv = r_adv.drop('effective_date', 1)
r_adv.to_csv(RESULT_PATH + 'r_adv.csv',index=False)

