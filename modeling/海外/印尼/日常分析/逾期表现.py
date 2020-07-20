#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 逾期表现.py
@Time    : 2020-06-30 11:55
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
sys.path.append('/Users/yantingting/Documents/MintechJob/newgenie1/utils3/')
from data_io_utils import *


file_path = os.path.join('/Users/yantingting/Seafile/风控/模型/10 印尼/日常分析/')

query0 = '''
with loan as (select * from (select id as loan_id,effective_date,approved_period,return_flag,
case when current_date::Date-effective_date::Date>approved_period+6 then 1 else 0 end as due_f7
from dw_gocash_go_cash_loan_gocash_core_loan 
where effective_date>='2019-11-01' ) t1
where due_f7 = 1),
repay as (select * from (select *,row_number()over(partition by loan_id order by create_time asc) as rank1
from dw_gocash_go_cash_loan_gocash_core_loan_pay_flow
where status = 'SUCCESS')t2
where rank1 = 1)
select loan.*,repay.paid_off_time,
case when date_part('days',repay.paid_off_time - date(loan.effective_date))>=loan.approved_period or repay.paid_off_time is null then 1 else 0 end as fst_flag,
case when date_part('days',repay.paid_off_time - date(loan.effective_date))>=loan.approved_period+6 or repay.paid_off_time is null then 1 else 0 end as dpd7_flag
from loan 
left join repay on loan.loan_id = repay.loan_id;
'''

df0 =DataBase().get_df_from_pg(query0)
df0.shape
df0.head()
df0['month'] = df0['effective_date'].apply(lambda x : x.month)
# df0.to_excel(os.path.join(file_path,'temp.xlsx'))
frame1 = pd.pivot_table(df0,index = ['return_flag','month'],aggfunc={'loan_id':'count', 'fst_flag':'sum','dpd7_flag':'sum'})
frame1['dpd1+'] = frame1.apply(lambda x: '{:.1f}%'.format(x['fst_flag']/x['loan_id']*100),axis = 1)
frame1['dpd7+'] = round(frame1['dpd7_flag']/frame1['loan_id'],3)
frame1



