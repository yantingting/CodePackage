#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 老客召回_定时.py
@Time    : 2020-06-16 13:51
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

'''
方案：每周三上午12点之前把所有结清的老客取出来，样本给市场的修越超

'''



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
file_path = '/Users/yantingting/Seafile/风控/10 印尼/05_日常分析/召回项目/%s_印尼召回/'%today_date
if not os.path.exists(file_path):
    os.makedirs(file_path)


# ''' ---------------------- 额度是否影响贷后表现 ---------------------- '''
query0 = '''
with perf as (select t1.*,
case when loan_status = 'COLLECTION' then current_date-late_date
	else round(late_fee /(approved_principal*0.025))::int end as DPD
from (select id as loan_id,effective_date,loan_status,late_fee,request_principal,request_period,"grouping",approved_principal,
case when current_date::Date-effective_date::Date>=approved_period+6 then 1 else 0 end as due_f7
from rt_t_gocash_core_loan
WHERE  effective_date between '2020-02-01' and '2020-03-01'
and return_flag = 'true') t1 
left join (select * from 			
(select order_id,late_date,row_number() over(partition by order_id order by late_date asc) as num
from dw_gocash_gocash_collection_col_case 
where (order_status = 'COLLECTION_PAIDOFF' or order_status = 'COLLECTION')
and app_id not in ('Credits','KASANDAAI'))t
where num =1) t2 
on t1.loan_id = t2.order_id)
SELECT perf.loan_id,
perf.request_principal,
perf.request_period,
perf."grouping",
perf.effective_date,
perf.loan_status,
case when DPD >=1 then 1 else 0 end as  exerx,
case when DPD >=3 then 1 else 0 end as  exer3,
case when DPD >=7 then 1 else 0 end as  exer7,
case when loan_status = 'COLLECTION' then 1 else 0 end as flag
FROM perf
where due_f7 = 1;

'''

# df0 = DataBase().get_df_from_pg(query0)
# df0.shape
# df0.head()
# df0['grouping'].value_counts()
# df0['loan_id'] = df0['loan_id'].astype(str)
# df0.to_excel(os.path.join(file_path,'老客表现.xlsx'),index = False)
#





# ''' ---------------------- 取样本 ---------------------- '''
# 1）所有放过款的用户；
# 2）最后一次结清或者申请时间；
# 3）历史最大逾期天数；
# 4）最大逾期天数对应的应还日；

query1 = '''
with loan as (
select customer_id,count(distinct case when effective_date >'1970-01-01' then id else null end ) as cnt_loan,
max(paid_off_time) as max_paid_off_time
,max(apply_time) as max_apply_time
from rt_t_gocash_core_loan
group  by 1),
loan1 as (select * from (
select customer_id,loan_status,
row_number()over(partition by customer_id order by apply_time desc) as rank1
from rt_t_gocash_core_loan) t
where loan_status in ('ADVANCE_PAIDOFF','COLLECTION_PAIDOFF','DENIED','PAIDOFF','RESCIND')
and rank1 = 1),
collection as (select * from (
select customer_id,late_date,late_day,
row_number()over(partition by customer_id order by late_day desc ) as rank1
from dw_gocash_gocash_collection_col_case
where app_id = 'payday_uku') t 
where rank1 = 1),
customer as (select id as customer_id, cell_phone from rt_t_gocash_core_customer)
select loan.*,
collection.late_date,
case when collection.late_day >0 then late_day else 0 end as max_late_days,
case when loan.max_paid_off_time >= loan.max_apply_time then max_paid_off_time else max_apply_time  end as max_time,
customer.cell_phone
from loan
left join collection 
on loan.customer_id = collection.customer_id
inner join loan1 
on loan.customer_id  = loan1. customer_id
left join customer 
on loan1.customer_id = customer.customer_id
where cnt_loan>0 and max_paid_off_time >'1970-01-01 00:00:00';

'''

df1  = DataBase().get_df_from_pg(query1)
df1['last_now'] = df1['max_time'].apply(lambda x: (pd.to_datetime(datetime.date.today().strftime('%Y-%m-%d')) - pd.to_datetime(x)).days)

df1['recency'] = df1['last_now'].apply(lambda x: 1 if x<=25 else 0)
df1['risk'] = df1['max_late_days'].apply(lambda x : 1 if x>7 else 0)

df1['customer_id'] = df1['customer_id'].astype(str)
df1['cell_phone'] = df1['cell_phone'].astype(str)

df1['提额券'] = df1.apply(lambda x: '5w' if ((x['recency'] == 1) & (x['risk'] == 1)) else
                                  '15w' if ((x['recency'] == 1) & (x['risk'] == 0)) else
                                  '10w' if ((x['recency'] == 0) & (x['risk'] == 1)) else
                                  '20w', axis = 1)

df1['提额券'].value_counts()
df1[['customer_id','cell_phone','提额券']].to_excel(os.path.join(file_path,'样本%s.xlsx'%today_date))





# ''' ---------------------- 分析转化和额度差异 ---------------------- '''

df_sample= pd.read_excel(os.path.join(file_path,'召回0616.xlsx'),sheet_name='data')
df_sample.shape
df_sample.head()
DataBase().auto_upload_df_to_pg(df_sample,'zh_0613')

query2 = '''
with loan as (select * from rt_t_gocash_core_loan where apply_time>='2020-06-17 11:00:00'),
zh as (select * from zh_0613),
quota as (select * from rt_t_gocash_core_prize_grant_flow where date(create_time)= '2020-06-17'),
price as (select * from rt_t_gocash_risk_control_pricing),
risk as (select *
from (
with rc as (select distinct orderno,createtime,customerid,ruleresultname,ruleresult
from rt_risk_mongo_gocash_riskreport
where typeid !='dj' and createtime>='2020-06-17 12:00:00' and ruleresultname like '%Model%Score%' and ruleid not in ('222','246')),
dj as(select customerid,orderno,ruleresultname,ruleresult,createtime,risklevel as djlevel
from rt_risk_mongo_gocash_riskreport 
where typeid = 'dj' and createtime>='2020-06-17 12:00:00')
select dj.customerid,dj.orderno as djorder,dj.createtime as djtime,dj.ruleresultname as dj_ruleresultname,dj.ruleresult as dj_ruleresult,dj.djlevel,
rc.orderno as rcorder,rc.createtime as rctime,rc.ruleresultname,rc.ruleresult as score,
row_number()over(partition by dj.customerid order by dj.createtime desc) as rank1
from dj
left join rc 
on rc.customerid = dj.customerid
where rc.createtime>dj.createtime or rc.createtime is null) t 
where rank1 = 1),
customer as (select id as customer_id,cell_phone from rt_t_gocash_core_customer)
select zh.customer_id as customer_id,zh.risk as risk,zh.recency as recency,
quota.amount as quota_amount,quota.create_time as quota_create_time,quota.update_time as quota_updatetime,
quota.view_time as quota_view_time,quota.prize_status as quota_prize_status,
risk.*,
price.risk_rating,price.loan_quota,price.period,
loan.apply_time,loan.approved_principal,loan.effective_date,loan.return_flag,
customer.cell_phone
from zh 
left join quota on zh.customer_id = quota.customer_id
left join risk on zh.customer_id::varchar = risk.customerid
left join price on risk.djorder = price.trace_id
left join loan on risk.rcorder = loan.id::varchar
left join customer on zh.customer_id = customer.customer_id;
'''

df2 = DataBase().get_df_from_pg(query2)
df2.info()
df2.to_excel(os.path.join(file_path,'召回结果0623.xlsx'),index = False)


