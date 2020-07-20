#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 收入流水.py
@Time    : 2020-06-15 13:57
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
sys.path.append('/Users/yantingting/Documents/MintechModel/newgenie/utils3/')
from data_io_utils import *

today_date = datetime.date.today().strftime('%m%d')
file_path = '/Users/yantingting/Seafile/风控/模型/10 印尼/日常分析/收入流水'
if not os.path.exists(file_path):
    os.makedirs(file_path)


query0 = '''
with perf as (select t1.*,
case when loan_status = 'COLLECTION' then current_date-late_date
	else round(late_fee /(approved_principal*0.025))::int end as DPD
from (select id as loan_id,customer_id,apply_time,effective_date,loan_status,
late_fee,approved_principal,approved_period,return_flag,
case when current_date::Date-effective_date::Date>=approved_period then 1 else 0 end as due_f1
from rt_t_gocash_core_loan
where effective_date>='2020-05-01') t1 
left join (select * from 			
(select order_id,late_date,row_number() over(partition by order_id order by late_date asc) as num
from dw_gocash_gocash_collection_col_case 
where (order_status = 'COLLECTION_PAIDOFF' or order_status = 'COLLECTION')
and app_id not in ('Credits','KASANDAAI'))t
where num =1) t2 
on t1.loan_id = t2.order_id),
loan1 as (select * from 
(select t1.id as loan_id,t1.customer_id,t1.apply_time,t2.create_time,t2.salary_amount_flow,t2.bankbook_amount_flow,t2.approve_status,
row_number()over(partition by t1.customer_id order by t2.create_time desc ) as rank1
from rt_t_gocash_core_loan t1 
left join dw_gocash_go_cash_loan_t_gocash_core_customer_support_file_flow t2 
on t1.customer_id = t2.customer_id
where to_timestamp(t1.apply_time,'yyyy-MM-dd hh24:mi:ss')>=t2.create_time
)t
where rank1 = 1),
salary as (select customer_id,monthly_salary
from dw_gocash_go_cash_loan_gocash_core_cusomer_profession)
SELECT perf.loan_id,
loan1.loan_id,
perf.apply_time,
perf.effective_date,
perf.loan_status,
perf.return_flag,
perf.due_f1,
case when DPD >=1 then 1 else 0 end as  exerx,
case when DPD >=3 then 1 else 0 end as  exer3,
case when DPD >=7 then 1 else 0 end as  exer7,
case when loan_status = 'COLLECTION' then 1 else 0 end as flag,
loan1.salary_amount_flow,loan1.bankbook_amount_flow,loan1.approve_status,
salary.monthly_salary
FROM perf
left join loan1 
on perf.loan_id = loan1.loan_id
left join salary 
on loan1.customer_id = salary.customer_id

'''


df0 = DataBase().get_df_from_pg(query0)
df0.shape
df0.head()
df0['loan_id'] = df0['loan_id'].astype(str)
df0.to_excel(os.path.join(file_path,'流水.xlsx'))
df0['approve_status'].value_counts()



