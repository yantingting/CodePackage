#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 风控定价监测.py
@Time    : 2020-06-11 11:31
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
sys.path.append('/Users/yantingting/Documents/MIntechJob/newgenie1/utils3')
from data_io_utils import *

today_date = datetime.date.today().strftime('%m%d')
file_path = os.path.join('/Users/yantingting/Seafile/风控/10 印尼/05_日常分析/风控定价','监测%s'%today_date)
if not os.path.exists(file_path):
    os.makedirs(file_path)


query1 = '''
select t1.id as loan_id,date(t1.apply_time) as apply_time,t1.return_flag,
t1.device_approval,t1.request_principal,t1.approved_period,
case when t1.effective_date >'1970-01-01' then 1 else 0 end as fangkuan,
t2.rc_quota,t2.request_amount,t2.risk_rating,t2.period,
t3.prequota,t3.quota,t3.realquota,t3.groupquota
--,t5.customer_last_loan_status
from 
(select * from rt_t_gocash_core_loan 
where date(apply_time)>='2020-07-05' ) t1 
inner join rt_t_gocash_risk_control_pricing t2 
on t1.id = t2.loan_id
left join (	select	orderno,
ruleresultmap :: json ->> 'loanQuota' as loanquota,
ruleresultmap :: json ->> 'preQuota' as prequota,
ruleresultmap :: json ->> 'quota' as quota,
ruleresultmap :: json ->> 'realQuota' as realquota,
(ruleresultmap :: json ->> 'groupPrice')::json ->>'groupQuota' as groupquota,
ruleresultmap :: json ->> 'expectedQuota' as expectedquota
from public.rt_risk_mongo_gocash_riskreport
where typeid = 'dj'
and ruleresultmap::json ->>'isWhiteList' is null) t3 
on t2.trace_id = t3.orderno
/*left join (select *
from rt_t_gocash_core_increase_amount_record
where valid_flag = 'true'
and date(create_time)>='2020-05-01')t4
on t1.id = t4.loan_id
left join (select orderno2,
replace(split_part(table1,':',2),'"','') as customer_last_loan_status
from (
select orderno as orderno2,	
regexp_split_to_table(customerhistory,',') as table1
from public.risk_mongo_gocash_installmentriskcontrolparams
where rctype = 'dj'  and customerhistory is not null) t 
where table1 like '%customerLastLoanStatus%') t5 
on t2.trace_id= t5.orderno2*/;

'''

df1 = DataBase().get_df_from_pg(query1)
df1.shape
df1.head()
df1.info()
df1['loan_id'] = df1['loan_id'].astype(str)
convert = ['request_principal','rc_quota','request_amount',\
           'prequota','quota','realquota','groupquota']

for v in convert:
    df1[v] = df1[v].astype(float)
df1.to_excel(os.path.join(file_path,'Data_%s.xlsx'%today_date),index = False)






