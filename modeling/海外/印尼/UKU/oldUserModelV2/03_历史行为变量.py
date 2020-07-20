import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/newgenie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *
import utils3.data_processing as dp

data_path = 'D:/Model/202001_mvp_model/01_data/'
result_path = 'D:/Model/202001_mvp_model/02_result/'

perf_sql = """
SELECT loan_id
--近N天风控rc1通过次数(申请次数,可能不准)
--, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t2.id else null end) as cnt_apply30
--, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t2.id else null end) as cnt_apply60
--, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t2.id else null end) as cnt_apply90
--, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t2.id else null end) as cnt_apply180
--, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t2.id else null end) as cnt_apply360
--, count(distinct t2.id) as cnt_apply
					
--近N天放款次数
, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan30
, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan60
, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan90
, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan180
, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan360
, count(distinct case when date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan
					
--第一次/最近一次申请距离现在申请时间间隔
, max(date(t1.apply_time) - date(t2.apply_time)) as firstapply_curds
, min(date(t1.apply_time) - date(t2.apply_time)) as lastapply_curds

--第一次/最近一次放款距离现在申请时间间隔
, max(case when date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01' then date(t1.apply_time) - date(t2.effective_date) else null end) as firstloan_curds
, min(case when date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01' then date(t1.apply_time) - date(t2.effective_date) else null end) as last_loan_curds

--期限
, max(case when t2.effective_date != '1970-01-01' then t2.approved_period end) as  max_loan_period
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  max_loan_period30d
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  max_loan_period60d
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  max_loan_period90d
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  max_loan_period180d
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  max_loan_period360d

, min(case when t2.effective_date != '1970-01-01' then t2.approved_period end) as  min_loan_period
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  min_loan_period30d
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  min_loan_period60d
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  min_loan_period90d
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  min_loan_period180d
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.effective_date != '1970-01-01' then t2.approved_period end) as  min_loan_period360d

, 1.0 * sum(case when t2.effective_date != '1970-01-01' then t2.approved_period end)/count(case when t2.effective_date != '1970-01-01' then t2.id end)  as  avg_loan_period
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.id end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.approved_period end)
        /count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.id end) 
    else null end as  avg_loan_period30d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.id end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.approved_period end)
       /count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.id end) 
    else null end as  avg_loan_period60d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.approved_period end)
       /count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id end)
    else null end as  avg_loan_period90d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.id end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.approved_period end)
      /count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.id end) 
    else null end as  avg_loan_period180d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.effective_date != '1970-01-01' then t2.id end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.effective_date != '1970-01-01' then t2.approved_period end)
      /count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.effective_date != '1970-01-01' then t2.id end) 
    else null end as  avg_loan_period360d

--期限占比
, count(case when t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is15
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is15_30d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is15_60d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is15_90d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is15_180d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is15_360d

, count(case when t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is8
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is8_30d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is8_60d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is8_90d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is8_180d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_loan_period_is8_360d

----占比
, 1.0 * count(case when t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end)/count(case when t2.effective_date != '1970-01-01' then t2.id end) as rate_loan_period_is15
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30  and t2.effective_date != '1970-01-01' then t2.id else null end) > 0   
        then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.id else null end)
        else null end as rate_loan_period_is15_30d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0   
        then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01'then t2.id else null end) 
        else null end as rate_loan_period_is15_60d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0   
        then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90  and t2.effective_date != '1970-01-01' then t2.id else null end) 
        else null end as rate_loan_period_is15_90d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0   
        then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180  and t2.effective_date != '1970-01-01' then t2.id else null end) 
        else null end as rate_loan_period_is15_180d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0   
        then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.approved_period = 15 and t2.effective_date != '1970-01-01' then t2.id else null end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360  and t2.effective_date != '1970-01-01' then t2.id else null end) 
        else null end as rate_loan_period_is15_360d

, 1.0 * count(case when t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end)/count(case when t2.effective_date != '1970-01-01' then t2.id else null end) as rate_loan_period_is8
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0 
      then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end)/
                count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as rate_loan_period_is8_30d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0
      then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end)/
                count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60  and t2.effective_date != '1970-01-01' then t2.id else null end)
     else null end as rate_loan_period_is8_60d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0
      then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end)/
            count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as rate_loan_period_is8_90d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0
      then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.approved_period = 8 and t2.effective_date != '1970-01-01'  then t2.id else null end)/
            count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180  and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as rate_loan_period_is8_180d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360  and t2.effective_date != '1970-01-01' then t2.id else null end) > 0
      then 1.0 * count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.approved_period = 8 and t2.effective_date != '1970-01-01' then t2.id else null end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360  and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as rate_loan_period_is8_360d


----展期次数
, count(case when t2.extend_times >0 and t2.effective_date != '1970-01-01' then t2.id else null end) as cnt_extend_times
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.extend_times >0 and t2.effective_date != '1970-01-01'  then t2.id else null end) as cnt_extend_times_30d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.extend_times >0 and t2.effective_date != '1970-01-01'  then t2.id else null end) as cnt_extend_times_60d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.extend_times >0 and t2.effective_date != '1970-01-01'  then t2.id else null end) as cnt_extend_times_90d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.extend_times >0 and t2.effective_date != '1970-01-01'  then t2.id else null end) as cnt_extend_times_180d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.extend_times >0 and t2.effective_date != '1970-01-01'  then t2.id else null end) as cnt_extend_times_360d


, max(t2.extend_times) as max_extend_times
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t2.extend_times else null end) as max_extend_times_30d
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t2.extend_times else null end) as max_extend_times_60d
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t2.extend_times else null end) as max_extend_times_90d
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t2.extend_times else null end) as max_extend_times_180d
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t2.extend_times else null end) as max_extend_times_360d

, sum(t2.extend_times) as sum_extend_times
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t2.extend_times else null end) as sum_extend_times_30d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t2.extend_times else null end) as sum_extend_times_60d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t2.extend_times else null end) as sum_extend_times_90d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t2.extend_times else null end) as sum_extend_times_180d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t2.extend_times else null end) as sum_extend_times_360d

, 1.0 * sum(t2.extend_times)/ count(case when t2.effective_date != '1970-01-01'  then t2.id end) as avg_extend_times
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t2.extend_times else null end)/
    count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as avg_extend_times_30d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t2.extend_times else null end)/
    count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as avg_extend_times_60d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t2.extend_times else null end)/
    count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as avg_extend_times_90d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0 
    then 1.0 *  sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t2.extend_times else null end)/
    count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as avg_extend_times_180d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t2.extend_times else null end)/
    count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.effective_date != '1970-01-01'then t2.id else null end) 
    else null end as avg_extend_times_360d

----贷后
, count(case when t2.loan_status = 'ADVANCE_PAIDOFF' then t2.id else null end) as cnt_advance_paidoff
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.loan_status = 'ADVANCE_PAIDOFF' then t2.id else null end) as cnt_advance_paidoff30d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.loan_status = 'ADVANCE_PAIDOFF' then t2.id else null end) as cnt_advance_paidoff60d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.loan_status = 'ADVANCE_PAIDOFF' then t2.id else null end) as cnt_advance_paidoff90d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.loan_status = 'ADVANCE_PAIDOFF' then t2.id else null end) as cnt_advance_paidoff180d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.loan_status = 'ADVANCE_PAIDOFF' then t2.id else null end) as cnt_advance_paidoff360d

, count(case when t2.loan_status = 'COLLECTION_PAIDOFF' then t2.id else null end) as cnt_collection_paidoff
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.loan_status = 'COLLECTION_PAIDOFF' then t2.id else null end) as cnt_collection_paidoff30d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.loan_status = 'COLLECTION_PAIDOFF' then t2.id else null end) as cnt_collection_paidoff60d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.loan_status = 'COLLECTION_PAIDOFF' then t2.id else null end) as cnt_collection_paidoff90d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.loan_status = 'COLLECTION_PAIDOFF' then t2.id else null end) as cnt_collection_paidoff180d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.loan_status = 'COLLECTION_PAIDOFF' then t2.id else null end) as cnt_collection_paidoff360d

, count(case when t2.loan_status = 'PAIDOFF' then t2.id else null end) as cnt_paidoff
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.loan_status = 'PAIDOFF' then t2.id else null end) as cnt_paidoff30d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.loan_status = 'PAIDOFF' then t2.id else null end) as cnt_paidoff60d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.loan_status = 'PAIDOFF' then t2.id else null end) as cnt_paidoff90d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and t2.loan_status = 'PAIDOFF' then t2.id else null end) as cnt_paidoff180d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and t2.loan_status = 'PAIDOFF' then t2.id else null end) as cnt_paidoff360d

from (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            --WHERE effective_date between '2019-08-08' and '2019-11-30' and return_flag = 'true' --WHERE customer_id = '156821999134838784'
            WHERE apply_time >= '2020-02-27 15:00:00' and return_flag = 'true'
    )t1
left join dw_gocash_go_cash_loan_gocash_core_loan t2 on t1.customer_id = t2.customer_id
where t1.apply_time :: timestamp > t2.apply_time :: timestamp
group by loan_id
"""

perf_data = get_df_from_pg(perf_sql)

perf_data.shape
save_data_to_pickle(perf_data, data_path, 'perf_data.pkl')



perf_latedays_sql = """
SELECT loanid
--最大逾期天数
, max(late_days) as max_his_latedays
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then late_days else null end) as max_his_latedays_30d 
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then late_days else null end) as max_his_latedays_60d 
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then late_days else null end) as max_his_latedays_90d 
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then late_days else null end) as max_his_latedays_180d 
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then late_days else null end) as max_his_latedays_360d 

--最小逾期天数
, min(late_days) as min_his_latedays
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 	then late_days else null end) as min_his_latedays_30d 
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 	then late_days else null end) as min_his_latedays_60d 
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 	then late_days else null end) as min_his_latedays_90d 
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then late_days else null end) as min_his_latedays_180d 
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then late_days else null end) as min_his_latedays_360d 

----总共
, sum(late_days) as sum_his_latedays
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then late_days else null end) as sum_his_latedays_30d 
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then late_days else null end) as sum_his_latedays_60d 
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then late_days else null end) as sum_his_latedays_90d 
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then late_days else null end) as sum_his_latedays_180d 
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then late_days else null end) as sum_his_latedays_360d 

----平均
, sum(late_days)/count(distinct t2.id) as avg_his_latedays
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30  then late_days else null end)/
    count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t2.id else null end)as avg_his_latedays_30d 
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then late_days else null end)/
	count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t2.id else null end)as avg_his_latedays_60d 
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then late_days else null end)/
	count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t2.id else null end)as avg_his_latedays_90d 
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then late_days else null end)/
	count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t2.id else null end)as avg_his_latedays_180d 
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then late_days else null end)/
	count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t2.id else null end)as avg_his_latedays_360d 

--上一笔订单状态
--, max(case when rn = 1 then late_days else null end) as pre_latedays
--, max(case when rn = 1 then pre_extend_times else null end) as pre_extend_times 
FROM (select  id as loanid
            , customer_id
            , apply_time
            , effective_date
       FROM dw_gocash_go_cash_loan_gocash_core_loan
       --WHERE effective_date between '2019-09-17' and '2019-11-30' and return_flag = 'true'
       WHERE date(apply_time) between '2020-01-10' and '2020-02-09' and return_flag = 'true'
       )t1 -- and customer_id = 352418834367426560
LEFT JOIN (SELECT id, apply_time, customer_id FROM dw_gocash_go_cash_loan_gocash_core_loan WHERE apply_time < '2020-02-12 00:00:00') t2 on t1.customer_id = t2.customer_id
LEFT JOIN (SELECT * FROM public.dw_gocash_go_cash_loan_gocash_core_loan_pay_flow WHERE status = 'SUCCESS' and create_time < '2020-02-12 00:00:00') t3 ON t2.id = t3.loan_id 
WHERE t1.apply_time :: timestamp > t2.apply_time :: timestamp
group by 1
"""

perf_latedays_data = get_df_from_pg(perf_latedays_sql)

# perf_latedays_data.describe()
#
# perf_latedays_data.loc[perf_latedays_data.min_his_latedays <0]
#
# perf_latedays_data.rename(columns = {'loanid':'loan_id'}, inplace= True)
#
# save_data_to_pickle(perf_latedays_data, data_path, 'perf_latedays_data_0121.pkl')
# perf_latedays_data = load_data_from_pickle(data_path, 'perf_latedays_data_0121.pkl')
#
# perf_latedays_data.loc[perf_latedays_data.sum_his_latedays > 0].shape
# train_id = x_with_y.loc[x_with_y.sample_set == 'train','loan_id']
# test_id = x_with_y.loc[x_with_y.sample_set == 'test','loan_id']
# oot_id = x_with_y.loc[x_with_y.sample_set == 'oot','loan_id'] #9038
#
# perf_latedays_data.loc[perf_latedays_data.loanid.isin(train_id)]['sum_his_latedays'].value_counts().sort_index()
# (13865 + 3209)/21581 #79%
# perf_latedays_data.loc[perf_latedays_data.loanid.isin(test_id)]['sum_his_latedays'].value_counts().sort_index()
# (6239+1414)/9630 #79%
# perf_latedays_data.loc[perf_latedays_data.loanid.isin(oot_id)]['sum_his_latedays'].value_counts().sort_index()
# (5897 + 1323)/9038 #79%
#
# perf_latedays_data.loc[perf_latedays_data.sum_his_latedays <=1].shape
# 77132/97027


perf_paidhour_sql = """
SELECT loanid
, sum(case when date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end) as cnt_paidoffhour_0to5
, sum(case when date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end) as cnt_paidoffhour_6to12
, sum(case when date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end) as cnt_paidoffhour_13to18
, sum(case when date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end) as cnt_paidoffhour_19to24

, 1.0 * sum(case when date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end)/count(t3.paid_off_time) as rate_paidoffhour_0to5
, 1.0 * sum(case when date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end)/count(t3.paid_off_time) as rate_paidoffhour_6to12
, 1.0 * sum(case when date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end)/count(t3.paid_off_time) as rate_paidoffhour_13to18
, 1.0 * sum(case when date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end)/count(t3.paid_off_time) as rate_paidoffhour_19to24

, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end) as cnt_paidoffhour_0to5_30d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end) as cnt_paidoffhour_6to12_30d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end) as cnt_paidoffhour_13to18_30d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end) as cnt_paidoffhour_19to23_30d

, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end) as cnt_paidoffhour_0to5_60d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end) as cnt_paidoffhour_6to12_60d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end) as cnt_paidoffhour_13to18_60d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end) as cnt_paidoffhour_19to23_60d

, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end) as cnt_paidoffhour_0to5_90d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end) as cnt_paidoffhour_6to12_90d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end) as cnt_paidoffhour_13to18_90d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end) as cnt_paidoffhour_19to23_90d

, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end) as cnt_paidoffhour_0to5_180d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end) as cnt_paidoffhour_6to12_180d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end) as cnt_paidoffhour_13to18_180d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end) as cnt_paidoffhour_19to23_180d

, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end) as cnt_paidoffhour_0to5_360d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end) as cnt_paidoffhour_6to12_360d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end) as cnt_paidoffhour_13to18_360d
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end) as cnt_paidoffhour_19to23_360d

, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t3.paid_off_time else null end) > 0 
	    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_0to5_30d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t3.paid_off_time else null end ) 
        else null end as rate_paidoffhour_6to12_30d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_13to18_30d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t3.paid_off_time else null end) > 0 
        then  1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 then t3.paid_off_time else null end) 
        else null end as rate_paidoffhour_19to23_30d

, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t3.paid_off_time else null end) > 0 
	    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_0to5_60d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t3.paid_off_time else null end ) 
        else null end as rate_paidoffhour_6to12_60d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_13to18_60d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t3.paid_off_time else null end) > 0 
        then  1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t3.paid_off_time else null end) 
        else null end as rate_paidoffhour_19to23_60d

, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t3.paid_off_time else null end) > 0 
	    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_0to5_90d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t3.paid_off_time else null end ) 
        else null end as rate_paidoffhour_6to12_90d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_13to18_90d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t3.paid_off_time else null end) > 0 
        then  1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t3.paid_off_time else null end) 
        else null end as rate_paidoffhour_19to23_90d

, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t3.paid_off_time else null end) > 0 
	    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_0to5_180d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t3.paid_off_time else null end ) 
        else null end as rate_paidoffhour_6to12_180d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_13to18_180d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t3.paid_off_time else null end) > 0 
        then  1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t3.paid_off_time else null end) 
        else null end as rate_paidoffhour_19to23_180d

, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t3.paid_off_time else null end) > 0 
	    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date_part('hour', t3.paid_off_time) between 0 and 5 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_0to5_360d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date_part('hour', t3.paid_off_time) between 6 and 12 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t3.paid_off_time else null end ) 
        else null end as rate_paidoffhour_6to12_360d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t3.paid_off_time else null end) > 0 
        then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date_part('hour', t3.paid_off_time) between 13 and 18 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t3.paid_off_time else null end)
        else null end as rate_paidoffhour_13to18_360d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t3.paid_off_time else null end) > 0 
        then  1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 and date_part('hour', t3.paid_off_time) between 19 and 23 then 1 else 0 end)/
        count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then t3.paid_off_time else null end) 
        else null end as rate_paidoffhour_19to23_360d
FROM (select  id as loanid
            , customer_id
            , apply_time
            , effective_date
       FROM dw_gocash_go_cash_loan_gocash_core_loan
       --WHERE effective_date between '2019-09-17' and '2019-10-30' and return_flag = 'true'
       WHERE date(apply_time) between '2020-01-10' and '2020-02-09' and return_flag = 'true'
       )t1 -- and customer_id = 352418834367426560
LEFT JOIN (SELECT id, apply_time, customer_id FROM dw_gocash_go_cash_loan_gocash_core_loan WHERE apply_time < '2020-02-11 00:00:00') t2 on t1.customer_id = t2.customer_id
LEFT JOIN (SELECT * FROM public.dw_gocash_go_cash_loan_gocash_core_loan_pay_flow WHERE status = 'SUCCESS' and create_time < '2020-02-11 00:00:00') t3 ON t2.id = t3.loan_id 
WHERE t1.apply_time :: timestamp > t2.apply_time :: timestamp
group by 1
"""
perf_paidhour_data = get_df_from_pg(perf_paidhour_sql)
perf_paidhour_data.shape

save_data_to_pickle(perf_paidhour_data, data_path, 'perf_paidhour_data_0121.pkl')
perf_paidhour_data = load_data_from_pickle(data_path , 'perf_paidhour_data_0121.pkl')

perf_paidhour_data.rename(columns = {'loanid':'loan_id'}, inplace= True)

