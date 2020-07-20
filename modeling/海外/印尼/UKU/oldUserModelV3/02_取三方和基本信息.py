import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/newgenie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *

data_path = 'D:/Model/indn/202004_uku_old_model/01_data/'

#izi数据

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

#----------------#
# izi: phoneage
#----------------#

sql_izi1 = """
select loan_id, effective_date
,coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> 'age' as int) end, age) as izi_phoneage
from (
    select 
    *, row_number() over(partition by a.loan_id order by coalesce(b.create_time::timestamp, c.createtime::timestamp) desc) as rn
    --from temp_uku_oldmodelv3_sample a 
    from (select id :: text as loan_id, apply_time, effective_date, apply_time, customer_id :: text 
          from dw_gocash_go_cash_loan_gocash_core_loan
          where return_flag = 'true' and product_id = 1
                and effective_date between '2020-03-04' and '2020-05-07' 
          ) a
    left join gocash_oss_phone_age b on a.customer_id = b.customer_id and a.effective_date :: date >= b.create_time::date
    left join risk_gocash_mongo_iziphoneage c on a.customer_id = c.customerid::text and a.effective_date :: date >= c.createtime::date
) t 
where rn = 1 
"""

r_izi1 = get_df_from_pg(sql_izi1)
r_izi1.shape[0] == r_izi1.loan_id.nunique() #71361
print(r_izi1.dtypes)
r_izi1.loan_id = r_izi1.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi1 = check_missingpct(r_izi1, date_col='effective_date')
check_izi1.to_excel(data_path + '数据检查/' + 'check_missingpct_izi_phoneage.xlsx')


r_izi1 = r_izi1.drop('effective_date', 1)
save_data_to_pickle(r_izi1, data_path, 'x_izi_phoneage.pkl')


#-----------------------#
# izi: phone inquiry 效果一般
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
   from (select id :: text as loan_id, apply_time, effective_date, customer_id :: text 
          from dw_gocash_go_cash_loan_gocash_core_loan
          where return_flag = 'true' and product_id = 1
                and effective_date between '2020-03-04' and '2020-05-07' 
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
check_izi2.to_excel(data_path + '数据检查/' + 'check_missingpct_izi_phoneinquiry.xlsx')

r_izi2 = r_izi2.drop('effective_date', 1)
save_data_to_pickle(r_izi2, data_path, 'x_izi_phoneinquiry.pkl')


#----------------------#
# izi: phone verify 效果一般 暂时不使用
#----------------------#

# sql_izi3 = '''
# select loan_id, effective_date
# ,case when coalesce(message, result)='MATCH' then 1 when coalesce(message, result) ='NOT_MATCH' then 0 else -1 end as izi_phoneverify
# from (
#     select
#     *, row_number() over(partition by a.loan_id order by coalesce(b.create_time::timestamp, c.createtime::timestamp) desc) as rn
#     from (
#         SELECT id::text as loan_id
#         ,customer_id::text
#         , apply_time::timestamp
#         , effective_date
#         FROM dw_gocash_go_cash_loan_gocash_core_loan
#         where effective_date between '2020-03-01' and '2020-03-10'
#         and return_flag = 'false'
#         and device_approval = 'ANDROID'
#     ) a
#     left join gocash_oss_phone_verify b on a.customer_id = b.customer_id and a.effective_date >= b.create_time::date
#     left join risk_gocash_mongo_iziphoneverify c on a.customer_id = c.customerid::text and a.effective_date >= c.createtime::date
# ) t
# where rn = 1
# '''
# r_izi3 = get_df_from_pg(sql_izi3)
# print(r_izi3.shape)
# print(r_izi3.loan_id.nunique())
# print(r_izi3.dtypes)
# r_izi3.loan_id = r_izi3.loan_id.astype(str)
#
# ''' 检查缺失比例 '''
# check_izi3 = check_missingpct(r_izi3, date_col='effective_date')
# check_izi3.to_excel(RESULT_PATH + 'check_missingpct_izi_phoneverify.xlsx')
#
# r_izi3 = r_izi3.drop('effective_date', 1)
# r_izi3.to_csv(RESULT_PATH + 'r_izi_phoneverify.csv',index=False)
#

#----------------#
# izi-topup
#----------------#

sql_izi4 = """
select loan_id
    , effective_date
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,times}' end as topup_0_30_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,min}' end as topup_0_30_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,max}' end as topup_0_30_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,avg}' end as topup_0_30_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_60,times}' end as topup_0_60_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_60,min}' end as topup_0_60_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_60,max}' end as topup_0_60_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_60,avg}' end as topup_0_60_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,times}' end as topup_0_90_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,min}' end as topup_0_90_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,max}' end as topup_0_90_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,avg}' end as topup_0_90_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,times}' end as topup_0_180_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,min}' end as topup_0_180_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,max}' end as topup_0_180_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,avg}' end as topup_0_180_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,times}' end as topup_0_360_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,min}' end as topup_0_360_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,max}' end as topup_0_360_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,avg}' end as topup_0_360_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,times}' end as topup_30_60_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,min}' end as topup_30_60_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,max}' end as topup_30_60_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,avg}' end as topup_30_60_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,times}' end as topup_60_90_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,min}' end as topup_60_90_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,max}' end as topup_60_90_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,avg}' end as topup_60_90_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,times}' end as topup_90_180_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,min}' end as topup_90_180_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,max}' end as topup_90_180_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,avg}' end as topup_90_180_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,times}' end as topup_180_360_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,min}' end as topup_180_360_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,max}' end as topup_180_360_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,avg}' end as topup_180_360_avg
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,times}' end as topup_360_720_times
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,min}' end as topup_360_720_min
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,max}' end as topup_360_720_max
    , case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,avg}' end as topup_360_720_avg
from (
    select *, row_number() over(partition by a.loan_id order by b.create_time::timestamp desc) as rn
    --from temp_uku_oldmodelv3_sample a
    from (select id :: text as loan_id, effective_date, apply_time, customer_id :: text 
          from dw_gocash_go_cash_loan_gocash_core_loan
          where return_flag = 'true' and product_id = 1
                and effective_date between '2020-03-04' and '2020-05-07' 
   ) a
    left join gocash_oss_to_pup b on a.customer_id = b.customer_id 
    and a.effective_date :: date >= b.create_time::date
    --and a.apply_time::timestamp + '8 hour' >= b.create_time::timestamp
    --and a.apply_time::timestamp <= b.create_time::timestamp
)t
where rn = 1
"""

r_izi4 = get_df_from_pg(sql_izi4)
print(r_izi4.shape)
print(r_izi4.loan_id.nunique())
print(r_izi4.dtypes)
r_izi4.loan_id = r_izi4.loan_id.astype(str)

''' 检查缺失比例 '''
check_izi4 = check_missingpct(r_izi4, date_col='effective_date')
check_izi4.to_excel(data_path + '数据检查/'  + 'check_missingpct_izi_topup.xlsx')
save_data_to_pickle(r_izi4, data_path, 'x_izi_topup.pkl')

#----------------#
# izi-idinquiry
#----------------#

sql_izi5 ='''
with loan as (
--select *
--from temp_uku_oldmodelv3_sample
select id :: text as loan_id
        , effective_date
        , apply_time
        , customer_id :: text 
from dw_gocash_go_cash_loan_gocash_core_loan
where return_flag = 'true' and product_id = 1
and effective_date between '2020-03-04' and '2020-05-07' 
        
),
izi_II as (
select customer_id::text as cus_id2
    , create_time as create_time2
    , message
    ,row_number() over(partition by customer_id order by create_time desc)as num
from gocash_oss_inquiries_v4
where message is not null and message<>'' and status='OK' and business_id='uku'
and create_time::date between '2020-03-03' and '2020-05-08'
)
select *
from (select loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , izi_II.message
            , row_number() over(partition by loan.loan_id order by izi_II.create_time2::timestamp desc) as rn
     from loan
     left join izi_II on loan.customer_id::text = izi_II.cus_id2
     and loan.effective_date :: date >= izi_II.create_time2::date
) t
where rn = 1
--select loan.loan_id, loan.apply_time, loan.effective_date,
--izi_II.message
--from loan
--inner join izi_II
--on loan.customer_id::text = izi_II.cus_id2
--and loan.apply_time::timestamp + '8 hour' >= izi_II.create_time2::timestamp
--and loan.apply_time::timestamp <= izi_II.create_time2::timestamp
--and izi_II.num = 1
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
check_izi4.to_excel(data_path + '数据检查/'  + 'check_missingpct_izi_topup.xlsx')

check_izi5 = check_missingpct(izi_ys, date_col='effective_date')
check_izi5.to_excel(data_path + '数据检查/' + 'check_missingpct_izi_idinquiry.xlsx')

izi_ys = izi_ys.drop('effective_date', 1)
save_data_to_pickle(izi_ys, data_path, 'x_izi_idinquiry.pkl')

#----------------------#
# izi: phone score
#----------------------#

sql_izi6 = '''
select loan_id, effective_date
    ,case when substring(message,1,1) = '{' then message::json ->> 'score' end as phone_score
from (select *, row_number() over(partition by a.loan_id order by b.create_time::timestamp desc) as rn
      --from temp_uku_oldmodelv3_sample a 
      from (select id :: text as loan_id
                 , effective_date
                 , apply_time
                 , customer_id :: text 
            from dw_gocash_go_cash_loan_gocash_core_loan
            where return_flag = 'true' and product_id = 1
            and effective_date between '2020-03-04' and '2020-05-07' 
            ) a
      left join gocash_oss_phone_score b on a.customer_id = b.customer_id and a.effective_date :: date >= b.create_time::date
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
check_izi6.to_excel(data_path + '数据检查/' + 'check_missingpct_izi_phonescore.xlsx')

r_izi6 = r_izi6.drop('effective_date', 1)
r_izi6.to_csv(RESULT_PATH + 'r_izi_phonescore.csv',index=False)
save_data_to_pickle(r_izi6, data_path, 'x_izi_phonescore.pkl')

#----------------------#
# izi: tel
#----------------------#

sql_izi7 = '''
select loan_id
    ,carrier
--    ,case when carrier = 'Telkomsel' then 1 else 0 end as izi_Telkomsel
--    ,case when carrier = 'IM3' then 1 else 0 end as izi_IM3
--    ,case when carrier = 'XL' then 1 else 0 end as izi_XL
--    ,case when carrier = '3' then 1 else 0 end as izi_3
--    ,case when carrier = 'AXIS' then 1 else 0 end as izi_AXIS
--    ,case when carrier = 'Hutchison' then 1 else 0 end as izi_Hutchison    
from (select *, row_number() over(partition by a.loan_id order by b.create_time::timestamp desc) as rn
      --from temp_uku_oldmodelv3_sample a 
      from (select id :: text as loan_id
                 , effective_date
                 , apply_time
                 , customer_id :: text 
            from dw_gocash_go_cash_loan_gocash_core_loan
            where return_flag = 'true' and product_id = 1
            and effective_date between '2020-03-04' and '2020-05-07' 
            ) a
      inner join (select *,cast(message::json ->> 'carrier' as json) ->> 'en' as carrier
                 from gocash_oss_phone_operator
                 where status = 'ok'
                  )b on a.customer_id = b.customer_id and a.effective_date :: date >= b.create_time::date
) t 
where rn = 1 
'''

r_izi7 = get_df_from_pg(sql_izi7)

print(r_izi7.shape)
print(r_izi7.loan_id.nunique())
print(r_izi7.dtypes)
r_izi7.loan_id = r_izi7.loan_id.astype(str)
save_data_to_pickle(r_izi7, data_path, 'x_izi_tel.pkl')


"""回溯数据和线上数据一致性检查"""
izi_hs = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/01_data/izi_td/izi回溯样本 20200506_result.xlsx')
izi_hs.loan_id = izi_hs.loan_id.astype(str)
izi_hs.index = izi_hs.loan_id
#topup
topup_cols = [i for i in izi_hs if 'topup' in i]
izi_hs[topup_cols]

temp1 = r_izi4.merge(izi_hs[topup_cols], left_on = 'loan_id', right_index = True)

cum_list = pd.DataFrame()

writer = pd.ExcelWriter('D:/Model/indn/202004_uku_old_model/01_data/izi_td/izi_topup_compare_v2.xlsx')
for i in topup_cols:
    if i not in ['loan_id', 'apply_time', 'effective_date', 'rn', 'apply_date']:
        try:
            temp1['%s_x' %i] = temp1['%s_x' %i].astype(float)
            temp1['%s_y' %i] = temp1['%s_y' %i].astype(float)
            temp1['compare_%s'%i] = temp1['%s_x' %i] - temp1['%s_y' %i]
            cum_list= temp1.groupby(['compare_%s'%i])['loan_id'].count().reset_index()
            cum_list['pct'] = cum_list['loan_id'] / cum_list['loan_id'].sum()
            cum_list.to_excel(writer, '%s'%i)
        except:
            pass
temp1.to_excel(writer, 'raw_df')
writer.save()

for i in topup_cols:
    try:
        temp1['%s_x' %i] = temp1['%s_x' %i].astype(float)
        temp1['%s_y' %i] = temp1['%s_y' %i].astype(float)
        temp1['compare_%s'%i] = temp1['%s_x' %i] - temp1['%s_y' %i]
    except:
        pass

temp1.columns

temp1['topup_60_90_avg_x'] - temp1['topup_60_90_avg_y']

temp1.to_excel('D:/Model/indn/202004_uku_old_model/01_data/izi_td/izi_topup_compare.xlsx')

#phonescore

temp2 = r_izi6.merge(izi_hs['phonescore'], left_on = 'loan_id', right_index = True)
temp2.to_excel('D:/Model/indn/202004_uku_old_model/01_data/izi_td/izi_phonescore_compare.xlsx')

#身份证多头V4
"""izi回溯数据解析"""
izi_hs_dt = izi_hs[['loan_id','apply_time', '身份证多头v4_result']]

# json只认“”不认‘’
izi_hs_dt['身份证多头v4_result'] = izi_hs_dt['身份证多头v4_result'].str.replace("\'","\"")

izi_hs_dt.apply_time = izi_hs_dt.apply_time.apply(lambda x:str(x)[0:10])
izi_hs_dt.apply_time = izi_hs_dt.apply_time.apply(lambda x:x.replace('-', ''))
izi_hs_dt.apply_time.value_counts()

#解析一层
try1= from_json(izi_hs_dt, '身份证多头v4_result')

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

try4.head()
# 将相差日期衍生成变量
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

try5 = try5.drop(['apply_time', 'B', 'A', 'C', 'AA', 'BB', 'CC', 'A_daysdiff','B_daysdiff','C_daysdiff'], 1)

temp3 = izi_ys.merge(try5, on = 'loan_id')
temp3.columns

dt_list = list(set([i.replace('_x','').replace('_y', '') for i in temp3.columns]))
dt_list.sort()

cum_list = pd.DataFrame()

writer = pd.ExcelWriter('D:/Model/indn/202004_uku_old_model/01_data/izi_td/izi_inquiries_compare_v2.xlsx')
for i in dt_list:
    if i not in ['loan_id', 'apply_time', 'effective_date', 'rn', 'apply_date']:
        temp3['%s_x' %i] = temp3['%s_x' %i].astype(float)
        temp3['%s_y' %i] = temp3['%s_y' %i].astype(float)
        temp3['compare_%s'%i] = temp3['%s_x' %i] - temp3['%s_y' %i]
        cum_list= temp3.groupby(['compare_%s'%i])['loan_id'].count().reset_index()
        cum_list['pct'] = cum_list['loan_id'] / cum_list['loan_id'].sum()
        cum_list.to_excel(writer, '%s'%i)
temp3.to_excel(writer, 'raw_df')
writer.save()



#合并回溯和线上数据(izi)

#phoneage
izi_phoneage_hs = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/01_data/izi_td/izi回溯样本_20200506_result_phoneage.xlsx')[['loan_id', 'phoneage']]
izi_phoneage_hs.loan_id = izi_phoneage_hs.loan_id.astype(str)

izi_phoneage = load_data_from_pickle(data_path, 'x_izi_phoneage.pkl')
izi_phoneage.rename(columns = {'izi_phoneage':'phoneage'}, inplace = True)

izi_phoneage_f = pd.concat([izi_phoneage, izi_phoneage_hs])
izi_phoneage_f.shape

#phonescore
izi_phonescore = load_data_from_pickle(data_path, 'x_izi_phonescore.pkl')
izi_phonescore.rename(columns = {'phone_score':'phonescore'}, inplace = True)

izi_phonescore.columns

izi_phonescore_f = pd.concat([izi_phonescore, izi_hs[['loan_id', 'phonescore']]])
izi_phonescore_f.shape

#idinquiry
izi_idinquiry = load_data_from_pickle(data_path, 'x_izi_idinquiry.pkl').drop(['rn'],1)
izi_idinquiry.dtypes

izi_idinquiry_f = pd.concat([izi_idinquiry, try5])
izi_idinquiry_f.shape

#izi_tel
izi_hs[['loan_id', 'telcom']].dtypes

r_izi7.rename(columns = {'carrier': 'telcom'}, inplace=True)


izi_tel_f = pd.concat([r_izi7, izi_hs[['loan_id', 'telcom']]])
izi_tel_f.isnull().sum()
izi_tel_f = izi_tel_f.fillna(-1)
izi_tel_f = izi_tel_f.reset_index(drop = True)

izi_tel_f.index = izi_tel_f.loan_id

tel_dummy = pd.get_dummies(izi_tel_f['telcom'])
tel_dummy.head()
tel_dummy.columns = ['tel_' + str(col) for col in tel_dummy.columns]
tel_dummy = tel_dummy.reset_index()

izi_tel_f = izi_tel_f.merge(tel_dummy, on = 'loan_id', how = 'left')
izi_tel_f.head()

#合并izi_topup
izi_topup = load_data_from_pickle(data_path, 'x_izi_topup.pkl').drop('effective_date',1)

izi_hs.index = izi_hs.loan_id

topup_cols = [i for i in izi_hs.columns if 'topup' in i ]
topup_cols.remove('topup_status')

izi_topup_f = pd.concat([izi_topup, izi_hs[topup_cols].reset_index()])
izi_topup_f.head()

#合并izi

izi_f = izi_phoneage_f.merge(izi_phonescore_f, on  = 'loan_id', how = 'left').merge(izi_idinquiry_f, on  = 'loan_id', how = 'left')\
    .merge(izi_tel_f, on  = 'loan_id', how = 'left').merge(izi_topup_f, on  = 'loan_id', how = 'left')
izi_f.shape
izi_f.columns

save_data_to_pickle(izi_f, data_path, 'x_izi_0124to0508.pkl')




"""同盾"""
####################
## tongdun credit guard
####################

sql_td ='''
with loan as (
select id :: text as loan_id
        , effective_date
        , apply_time
        , customer_id :: text 
from dw_gocash_go_cash_loan_gocash_core_loan
where return_flag = 'true' and product_id = 1
and effective_date between '2020-04-01' and '2020-05-07' 
),
guard as (
select customer_id::text as cus_id
        , create_time
        , result_desc
from gocash_oss_credit_guard_go_no
where result_desc is not null
and result_desc<>''
and business_id='uku'
and create_time::date between '2020-03-29' and '2020-05-08'
)
select *
from (select loan.*,guard.*, row_number() over(partition by loan.loan_id order by guard.create_time::timestamp desc) as rn
      from loan
      left join guard on loan.customer_id::text = guard.cus_id
      and loan.effective_date :: date >= guard.create_time::date
)t 
where rn = 1
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
try1.risk_name.nunique()#32
try1.loan_id.nunique()

len(rule_list)
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

save_data_to_pickle(var_guard, data_path, 'x_td_guard.pkl')
var_guard.columns
var_guard[['loan_id', 'multiplatform_apply_7day_count']]


"""基础信息"""

baseinfo_sql = """
WITH baseinfo as 
(
SELECT id as customer_id
    , create_time
    , update_time
    , mail
    , marital_status
    , religion
    , education
FROM dw_gocash_go_cash_loan_gocash_core_customer
UNION
SELECT customer_id
    , create_time
    , update_time
    , mail
    , marital_status
    , religion
    , education
FROM dw_gocash_go_cash_loan_gocash_core_customer_history 
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , baseinfo.*
            , row_number() over(partition by loan.loan_id order by baseinfo.update_time desc) as rn
      FROM  (SELECT id :: text as loan_id
                , apply_time
                , effective_date
                , customer_id :: text
            FROM rt_t_gocash_core_loan
            WHERE effective_date between '2020-03-04' and '2020-05-08' and return_flag = 'true' and product_id = 1
            UNION 
            SELECT loan_id
                , apply_time
                , effective_date
                , customer_id :: text                
            FROM temp_uku_oldmodelv3_sample
            ) loan
      LEFT JOIN baseinfo  ON loan.customer_id :: varchar = baseinfo.customer_id :: varchar
      --WHERE loan.apply_time::timestamp >= baseinfo.update_time
      WHERE loan.apply_time::timestamp >= baseinfo.create_time
) t
WHERE rn = 1
"""
base_data = get_df_from_pg(baseinfo_sql)
base_data.shape

base_data.customer_id.value_counts()

#base_data.loc[base_data.loan_id == '434795091339943936'].T
#base_data.loc[base_data.loan_id == '448864410264829952'].T

base_data['mail_address1'] = base_data.mail.apply(lambda x: x.split('@'))
base_data['mail_address2'] = base_data.mail_address1.apply(lambda x: x[-1])
base_data['mail_address3'] = base_data.mail_address2.apply(lambda x: x.lower().split('.')[0])
base_data['mail_address'] = base_data.mail_address3.apply(lambda x: 'gmail' if x in ['gmail'] else 'yahoo'  if x in ['yahoo','ymail','rocketmail'] else 'others')
base_data['mail_address'].value_counts()
base_data.index = base_data.loan_id
base_data.columns #无缺失

mail_dummy = pd.get_dummies(base_data['mail_address'])
mail_dummy.head()
mail_dummy.columns = ['mail_' + str(col) for col in mail_dummy.columns]

marital_dummy = pd.get_dummies(base_data['marital_status'])
marital_dummy.head()
marital_dummy.columns = ['marital_' + str(col) for col in marital_dummy.columns]

religion_dummy = pd.get_dummies(base_data['religion'])
religion_dummy.columns = ['religion_' + str(col) for col in religion_dummy.columns]

education_dummy = pd.get_dummies(base_data['education'])
education_dummy.columns = ['education_' + str(col) for col in education_dummy.columns]

base_data_f = base_data.merge(mail_dummy,  left_index = True, right_index = True).merge(marital_dummy, left_index = True, right_index = True)\
            .merge(religion_dummy, left_index = True, right_index = True).merge(education_dummy,left_index = True, right_index = True)

base_data_f = base_data_f.reset_index(drop = True)

save_data_to_pickle(base_data_f, data_path, 'x_base_0124to0508.pkl')
base_data = load_data_from_pickle(data_path, 'base_0808to1130.pkl')


baseinfo_sql2 = """
WITH baseinfo as (
SELECT customer_id
    , update_time
    --计算年龄和性别
    , case when substring(id_card_no, 7,2)::int > 40 then 'female' else 'male' end as gender --female生日 = 1-31 + 40; male生日=1-31
    , case when substring(id_card_no, 11,2)::int >20 --说明是19XX年出生
        then 120 - substring(id_card_no, 11,2)::int else 20 - substring(id_card_no, 11,2)::int  end as age   
FROM public.dw_gocash_go_cash_loan_gocash_core_customer_history
where id_card_no <> ''
UNION
SELECT id as customer_id
    , create_time as update_time
    --计算年龄和性别
    , case when substring(id_card_no, 7,2)::int > 40 then 'female' else 'male' end as gender --female生日 = 1-31 + 40; male生日=1-31
    , case when substring(id_card_no, 11,2)::int >20 --说明是19XX年出生
        then 120 - substring(id_card_no, 11,2)::int else 20 - substring(id_card_no, 11,2)::int  end as age   
FROM public.dw_gocash_go_cash_loan_gocash_core_customer
where id_card_no <> ''
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , baseinfo.*
            , row_number() over(partition by loan.loan_id order by baseinfo.update_time desc) as rn
      FROM (SELECT id :: text as loan_id
                , apply_time
                , effective_date
                , customer_id :: text
            FROM rt_t_gocash_core_loan
            WHERE effective_date between '2020-03-04' and '2020-05-08' and return_flag = 'true' and product_id = 1
            UNION 
            SELECT loan_id
                , apply_time
                , effective_date
                , customer_id :: text                
            FROM temp_uku_oldmodelv3_sample
            ) loan
      LEFT join baseinfo ON loan.customer_id :: varchar = baseinfo.customer_id :: varchar
      WHERE loan.apply_time :: timestamp >= baseinfo.update_time :: timestamp
) t
WHERE rn = 1
"""
base_data2 = get_df_from_pg(baseinfo_sql2)

base_data2.columns
base_data2 = base_data2[['loan_id','gender','age']]

base_data2.gender = base_data2.gender.replace('male', 1).replace('female', 0)
base_data2.shape
base_data2.loc[base_data2.loan_id == '436170323791544320']

save_data_to_pickle(base_data2, data_path, 'x_base2_0124to0508.pkl')

base_data3 = load_data_from_pickle(data_path, 'x_base2_0124to0508.pkl')


"""职业信息"""
prof_sql = """
WITH prof as 
(
SELECT customer_id
    , update_time
    , occupation_type
    , job
    , industry_involved
    , monthly_salary
FROM dw_gocash_go_cash_loan_gocash_core_cusomer_profession
UNION
SELECT customer_id
    , update_time
    , occupation_type
    , job
    , industry_involved
    , monthly_salary
FROM dw_gocash_go_cash_loan_gocash_core_cusomer_profession_history 
)
SELECT loan_id
    , apply_time
    , effective_date
    , customer_id
    , occupation_type
    , job
    , industry_involved
    , monthly_salary 
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , prof.*
            , row_number() over(partition by loan.loan_id order by prof.update_time desc) as rn
      FROM (SELECT loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM temp_uku_oldmodelv3_sample
            UNION
            SELECT id :: text as loan_id
                , apply_time :: varchar
                , effective_date :: varchar
                , customer_id :: text
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            WHERE effective_date between '2020-03-04' and '2020-05-08' and return_flag = 'true'
            ) loan
      LEFT JOIN prof ON loan.customer_id = prof.customer_id :: text
      WHERE loan.apply_time :: timestamp >= prof.update_time
) t
WHERE rn = 1
"""
prof_data = get_df_from_pg(prof_sql)
prof_data.index = prof_data.loan_id

occ_dummy = pd.get_dummies(prof_data['occupation_type'])
occ_dummy.columns = ['occupation_' + str(col) for col in occ_dummy.columns]
occ_dummy.head()

job_dummy = pd.get_dummies(prof_data['job'])
job_dummy.columns = ['job_' + str(col) for col in job_dummy.columns]

industry_dummy = pd.get_dummies(prof_data['industry_involved'])
industry_dummy.columns = ['industry_' + str(col) for col in industry_dummy.columns]

industry_dummy.head()

prof_data = prof_data.reset_index(drop=True)

prof_data_f = prof_data.merge(occ_dummy, left_on = 'loan_id', right_index = True).merge(job_dummy, left_on = 'loan_id', right_index = True).merge(industry_dummy, left_on = 'loan_id', right_index = True)
prof_data_f = prof_data_f.drop(['apply_time','effective_date','customer_id','occupation_type','job','industry_involved','job_','industry_'],1)

save_data_to_pickle(prof_data_f, data_path, 'x_prof_0124to0508.pkl')

"""银行卡信息"""
bank_sql = """
WITH bank as (
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
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , bank.*
            , row_number() over(partition by loan.loan_id order by bank.update_time desc) as rn
      FROM (SELECT loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM temp_uku_oldmodelv3_sample
            UNION
            SELECT id :: text as loan_id
                , apply_time :: varchar
                , effective_date :: varchar
                , customer_id :: text
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            WHERE effective_date between '2020-03-04' and '2020-05-08' and return_flag = 'true'
            ) loan
      LEFT join bank ON loan.customer_id = bank.customer_id :: text
      WHERE loan.apply_time :: timestamp >= bank.update_time
) t
WHERE rn = 1
"""
bank_data = get_df_from_pg(bank_sql)

bank_data.shape
bank_data.index = bank_data.loan_id

bank_dummy = pd.get_dummies(bank_data['bank_code'])

bank_dummy.columns = ['bankcode_' + str(col) for col in bank_dummy.columns]
bank_dummy.head()
bank_data_f = bank_data.merge(bank_dummy, left_index = True, right_index = True)
bank_data_f.shape

bank_data_f = bank_data_f.reset_index(drop=True)
bank_data_f.head()

save_data_to_pickle(bank_data_f, data_path, 'x_bank_0124to0508.pkl')

"""紧急联系人"""

refer_sql = """
WITH refer as 
(
SELECT customer_id
    , create_time
    , refer_type
    , case when refer_type = 'BROTHERS_AND_SISTERS' then 1 else 0 end as refer_bro_sis 
    , case when refer_type = 'PARENTS' then 1 else 0 end as refer_parents
    , case when refer_type = 'SPOUSE' then 1 else 0 end as refer_spouse    
FROM dw_gocash_go_cash_loan_gocash_core_customer_refer
WHERE create_time != '1970-01-01' and refer_type not in ('SELF','KINSFOLK','FRIEND')
)
SELECT loan_id
    , customer_id
    , max(date_part('day', interval_value) * 24 * 60 + date_part('hour', interval_value) * 60 +  date_part('minute', interval_value)) as refer_update_minutes
    , max(refer_bro_sis) as refer_bro_sis
    , max(refer_parents) as refer_parents
    , max(refer_spouse) as refer_spouse
FROM (SELECT loan.loan_id
            , refer.*
            , (apply_time :: timestamp - create_time:: timestamp) as interval_value
            , dense_rank() over(partition by loan.loan_id order by refer.create_time desc) as rn
      FROM (SELECT loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM temp_uku_oldmodelv3_sample
            UNION
            SELECT id :: text as loan_id
                , apply_time :: varchar
                , effective_date :: varchar
                , customer_id :: text
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            WHERE effective_date between '2020-03-04' and '2020-05-08' and return_flag = 'true'
            ) loan
      LEFT JOIN refer  ON loan.customer_id = refer.customer_id :: text
      WHERE loan.apply_time :: timestamp >= refer.create_time) t
WHERE rn = 1
GROUP BY loan_id, customer_id
"""

refer_data = get_df_from_pg(refer_sql)

save_data_to_pickle(refer_data, data_path, 'x_refer_0124to0508.pkl')
