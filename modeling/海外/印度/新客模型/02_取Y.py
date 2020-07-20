#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 02_取Y.py
@Time    : 2020-05-28 17:10
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import sys
import os
sys.path.append('/Users/yantingting/Documents/MintechModel/newgenie/utils3/')
from data_io_utils import *

file_path = '/Users/yantingting/Seafile/风控/模型/15 印度/V4模型/01_Data'


query0 = '''
with table_flag1 as 
(select t1.order_no,t1.customer_id,
t1.product_name,date(t1.start_date) as start_date,
case when t1.overdue_days >= 7 then 1 else 0 end as flag1
from dw_ind_supercore_prod_india_supercore_prod_repayment_schedules t1
left join ind_oss_creditreport t2 
on t1.customer_id = t2.customerid::int	
where customer_type in ('NewApplicationOrder','OrderTypeIndNewClient')
and extend_period = 0 
and date(start_date)>= '2020-01-02'
and date(start_date)<= '2020-03-19'
and t2.customerid is not null),
table_flag2 as (
with loan as (select order_no,start_date,due_date,actual_paid_off_date,extend_period,overdue_days
from dw_ind_supercore_prod_india_supercore_prod_repayment_schedules),
b as (select order_no,min(start_date) as first_start
from dw_ind_supercore_prod_india_supercore_prod_repayment_schedules
group by 1)
select loan.*,date(b.first_start) +55 as day1,
row_number()over(partition by loan.order_no order by loan.start_date desc) as rank1,
case when due_date>=date(b.first_start) +55 then 0
     when due_date<date(b.first_start) +55 and date(actual_paid_off_date) !='1970-01-01' and  actual_paid_off_date<=date(b.first_start) +55 then overdue_days
     when due_date<date(b.first_start) +55 and date(actual_paid_off_date) ='1970-01-01' then date_part('day',date(b.first_start) +55 - due_date)
     when due_date<date(b.first_start) +55 and actual_paid_off_date>date(b.first_start) +55 then date_part('day',date(b.first_start) +55 - due_date)
     end as lastoverdue_days
from loan 
left join b 
on loan.order_no = b.order_no
where start_date<=date(b.first_start) +55)
select 
table_flag1.*,
case when table_flag2.lastoverdue_days >= 7 then 1 else 0 end as flag2,
case when table_flag2.extend_period >0 then 1 else 0 end as flag_extend
from table_flag1
left join table_flag2
on table_flag1.order_no = table_flag2.order_no
where table_flag2.rank1 = 1
'''

time1 = time.time()
df0 =  DataBase().get_df_from_pg(query0)
time2 = time.time()
save_data_to_pickle(df0,file_path,'flag.pkl')
print('run_time: ', time2 - time1)
df0.shape
df0.head()




