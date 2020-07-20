import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos_v1/genie')
import matplotlib
matplotlib.use('TkAgg')
import utils3.plotting as pl
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *


#建模所用flag
perf_sql = """
select t1.id as loan_id
,customer_id
,approved_period
,effective_date
,paid_off_time
,due_date
,loan_status
,extend_times
,paid_off_time::Date-due_date as latedays
,return_flag
,current_date
,case when extend_times>3 then 0 
     when paid_off_time::Date-due_date>7 then 1 
     when loan_status='COLLECTION' and current_date::Date-due_date<=7 then -3 
     when loan_status='COLLECTION' and current_date::Date-due_date>7 then 1
     when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
     when current_date-effective_date <= approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
     else 0 end as flag_7
,case when extend_times>3 then 0 
     when paid_off_time::Date-due_date>15 then 1 
     when loan_status='COLLECTION' and current_date::Date-due_date<=15 then -3 
     when loan_status='COLLECTION' and current_date::Date-due_date>15 then 1
     when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
     when current_date-effective_date <= approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
     else 0 end as flag_15
from (select *
      from dw_gocash_go_cash_loan_gocash_core_loan 
      where effective_date!='1970-01-01' and effective_date<='2019-11-06' and effective_date >= '2019-06-27' and return_flag = 'false') t1
"""

perf_data = get_df_from_pg(perf_sql)
perf_data.loan_id.nunique() #110626

perf_data.effective_date = perf_data.effective_date.astype(str)
perf_data.customer_id = perf_data.customer_id.astype(str)
perf_data.loan_id = perf_data.loan_id.astype(str)
perf_data['effective_month'] = perf_data.effective_date.apply(lambda x:str(x)[5:7])

perf_data.to_excel('D:/Model/201911_uku_ios_model/01_data/flag_20191205.xlsx')

perf_data.loc[perf_data.flag.isin([0,1])].groupby(['effective_month'])['flag'].mean()


#之前印尼建模所用的取flag的代码, 两者计算出的7+逾期水平一致

new_perf_sql = """
select id,customer_id,effective_date,approved_period,due_date,loan_status,extend_times,
case when loan_status like '%COLLECTION' then 1 else 0 end as flag_f,
case when loan_status = 'COLLECTION' then 1 
     when loan_status like '%COLLECTION%' and loan_status != 'COLLECTION' 
     and date_part('day',paid_off_time-due_date)>7 
     then 1 else 0 end as flag from public.dw_gocash_go_cash_loan_gocash_core_loan
     where effective_date<='2019-11-28' and effective_date>='2019-05-01' and return_flag='false' 
"""
new_perf_data = get_df_from_pg(new_perf_sql)

new_perf_data.customer_id = new_perf_data.customer_id.astype(str)
new_perf_data.id = new_perf_data.id.astype(str)
new_perf_data['effective_month'] = new_perf_data.effective_date.apply(lambda x:str(x)[5:7])

new_perf_data.to_excel('D:/Model/201911_uku_ios_model/01_data/flag_20191202_v2.xlsx')



