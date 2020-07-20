import os
import sys
import json
import logging
import warnings
warnings.filterwarnings('ignore')

sys.path.append('/Users/Mint/Desktop/repos/genie')

import pandas as pd
import numpy as np
from jinja2 import Template

#python3种reload函数不能直接使用
from imp import reload

from utils3.data_io_utils import *
import utils3.misc_utils as mu
import utils3.summary_statistics as ss
import utils3.metrics as mt
import utils3.feature_selection as fs
import utils3.plotting as pl
import utils3.modeling as ml
from functools import reduce
import xgboost as xgb
from xgboost import DMatrix

########################  银联
yl_sql1 = """
with perf as (
select order_no
from t_loan_performance
where business_id in ('rong360') and effective_date between '2019-08-27' and '2019-10-18' and dt = '20191025'
),
--with apply as (
--select order_no
--, date(created_time) as apply_time
--, application_status
--from public.dw_dc_compensatory_cp_core_application
--where business_id = 'rong360' and date(created_time) between '2019-09-24' and '2019-10-07'
--),
--result as (
--select orderno, risklevel, pipelinename
--from ods_rsk_installmentriskcontrolresult
--where businessid = 'rong360'
--)
--,
related as (
  select orderno, messageno
  from risk_mongo_installmentmessagerelated
  where topicname in ('Application_thirdPart_unionpayloanbeforescore')
   and databasename in ('installmentUnionpayLoanBeforeScore')
   and businessid = 'rong360'
),
yl_score as (
select customerid, taskid,RMS002
from  (
		select taskid
		 ,customerid
		,json_array_elements( cast (oss::json #>> '{data}' as json)::json)->>'RMS002' as RMS002
		from risk_oss_yinlian_loanbeforescore
) t)
select t0.order_no
--, t0.apply_time
--, t0.application_status
--, risklevel
--, pipelinename
, RMS002
from perf t0 
--left join result t1 on t0.order_no = t1.orderno
left join related t2 on t0.order_no = t2.orderno
left join yl_score t3 on t2.messageno = t3.taskid
"""

yl_data1 =  get_df_from_pg(yl_sql1)
yl_data1 = yl_data1.drop_duplicates()

yl_hs = pd.read_excel('D:/seafile/Seafile/风控/模型/04 三方数据/01银联/圣泰信息_定制化变量_20191022.xlsx')[['order_no','借记卡得分']].rename(columns = {'借记卡得分':'rms002'})
yl_orderno = list(set(yl_hs.order_no).intersection(yl_data1.order_no))
yl_data1.loc[yl_data1.order_no.isin(yl_orderno)].rms002.isnull().sum()

yl_data1 = yl_data1.loc[~yl_data1.order_no.isin(yl_orderno)]
yl_data1 = pd.concat([yl_data1, yl_hs.loc[yl_hs.order_no.isin(yl_orderno)]])

# yl_sql2 = """
# with apply as (
# select order_no
# , date(created_time) as apply_time
# , application_status
# from public.dw_dc_compensatory_cp_core_application
# where business_id = 'rong360' and date(created_time) between '2019-09-24' and '2019-09-26'
# ),
# result as (
# select orderno, risklevel, pipelinename
# from ods_rsk_installmentriskcontrolresult
# where businessid = 'rong360'
# )
# ,
# related as (
#   select orderno, messageno
#   from risk_mongo_installmentmessagerelated
#   where topicname in ('Application_thirdPart_unionpayoverallscore')
#    and databasename in ('installmentUnionpayOverallScore')
# ),
# yl_var as (
# select taskid, customerid
# ,json_array_elements( cast (oss::json #>> '{data}' as json)::json)->>'YLZC037'  as YLZC037
# ,json_array_elements( cast (oss::json #>> '{data}' as json)::json)->>'YLZC013'   as YLZC013
# ,json_array_elements( cast (oss::json #>> '{data}' as json)::json)->>'YLZC053' as  YLZC053
# from risk_oss_yinlian_overallscore
# )
# select t0.order_no
# , t0.apply_time
# , t0.application_status
# , risklevel
# , pipelinename
# , ylzc037
# , ylzc013
# , ylzc053
# from apply t0
# left join result t1 on t0.order_no = t1.orderno
# left join related t2 on t0.order_no = t2.orderno
# left join yl_var t4 on t2.messageno = t4.taskid
# """
#
# yl_data2 =  get_df_from_pg(yl_sql2)
# yl_data2 = yl_data2.drop_duplicates()
# yl_data.loc[yl_data.order_no == '160347898859488259'].T
#
# yl_sql3 = """
# with apply as (
# select order_no
# , date(created_time) as apply_time
# , application_status
# from public.dw_dc_compensatory_cp_core_application
# where business_id = 'rong360' and date(created_time) between '2019-09-24' and '2019-09-26'
# ),
# result as (
# select orderno, risklevel, pipelinename
# from ods_rsk_installmentriskcontrolresult
# where businessid = 'rong360'
# ),
# related as (
#   select orderno, messageno
#   from risk_mongo_installmentmessagerelated
#   where topicname in ('Application_thirdPart_unionpayspecialexchangeimage')
#    and databasename in ('installmentUnionpaySpecialExchangeImage')
# )
# , yl_var2 as (
# select taskid, customerid
# ,json_array_elements( cast (oss::json #>> '{data}' as json)::json)->>'TSJY004'  as TSJY004
# ,json_array_elements( cast (oss::json #>> '{data}' as json)::json)->>'TSJY042'   as TSJY042
# ,json_array_elements( cast (oss::json #>> '{data}' as json)::json)->>'TSJY047' as  TSJY047
# from risk_oss_yinlian_specialexchangeimage
# )
# select t0.order_no
# , t0.apply_time
# , t0.application_status
# , risklevel
# , pipelinename
# , tsjy004
# , tsjy042
# , tsjy047
# from apply t0
# left join result t1 on t0.order_no = t1.orderno
# left join related t2 on t0.order_no = t2.orderno
# left join yl_var2 t5 on t2.messageno = t5.taskid
# """
#
# yl_data3 =  get_df_from_pg(yl_sql3)
# yl_data3 = yl_data3.drop_duplicates()

#yl_data = yl_data1.merge(yl_data2.drop(['apply_time','application_status','risklevel','pipelinename'],1), on = 'order_no').merge(yl_data3.drop(['apply_time','application_status','risklevel','pipelinename'],1), on = 'order_no')
#yl_data.columns

#######################百融#############################
br_sql1 = """
with perf as (
select order_no
from t_loan_performance
where business_id in ('rong360') and effective_date between '2019-08-27' and '2019-10-18' and dt = '20191025'
),
related as (
select orderno, messageno
from risk_mongo_installmentmessagerelated
where businessid in ('rong360') and topicname in ('Application_thirdPart_bairongdebtrepaystress')
and databasename = 'installmentBairongDebtRepayStress'
),
 br_stress as (
select taskid
, cast(oss::json #>> '{result}' as json)::json #>>'{DebtRepayStress,nodebtscore}' as drs_nodebtscore
from risk_oss_bairong_debt_repay_stress
)
select *
from perf t0
left join related t2 on t0.order_no = t2.orderno
left join br_stress t3 on t2.messageno = t3.taskid
"""
br_data1 = get_df_from_pg(br_sql1)
br_data1 = br_data1.drop(['orderno','messageno','taskid'],1)
br_data1.shape


# br_sql2 = """
# with apply as (
# select order_no
# , date(created_time) as apply_time
# , application_status
# from public.dw_dc_compensatory_cp_core_application
# where business_id = 'rong360' and date(created_time) between '2019-09-24' and '2019-09-26'
# ),
# result as (
# select orderno, risklevel, pipelinename
# from ods_rsk_installmentriskcontrolresult
# where businessid = 'rong360'
# ),
# related as (
# select orderno, messageno
# from risk_mongo_installmentmessagerelated
# where businessid in ('rong360') and topicname in ('Application_thirdPart_bairongnewloanbefore')
# and databasename = 'installmentBairongNewLoanBefore'
# ),
# br_alu as (
# select taskid
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,lst,id,inteday}' as alu_lst_id_inteday
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,cell,avg_monnum}' as alu_m12_cell_avg_monnum
# from risk_oss_bairong_new_loan_before
# )
# select *
# from apply t0
# left join result t1 on t0.order_no = t1.orderno
# left join related t2 on t0.order_no = t2.orderno
# left join br_alu t4 on t2.messageno = t4.taskid
# """
# br_data2 = get_df_from_pg(br_sql2)

br_data = br_data1.merge(br_data2.drop(['apply_time','application_status','orderno','risklevel','pipelinename','messageno','taskid'],1), on = 'order_no')

###############################基本信息
baseinfo_sql = """
with perf as (
select order_no
from t_loan_performance
where business_id in ('rong360') and effective_date between '2019-08-27' and '2019-10-18' and dt = '20191025'
)
select *
from perf t0
left join rong360_customer_history_result t2 on t0.order_no = t2.order_no
"""
base_data = get_df_from_pg(baseinfo_sql)
base_data.shape


###############新颜####################
xy_sql = """
with perf as (
select order_no
from t_loan_performance
where business_id in ('rong360') and effective_date between '2019-08-27' and '2019-10-18' and dt = '20191025'
),
xy as (
select order_no
, date(created_time) as apply_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_count}', oss::json #>> '{data,report_detail,loans_count}') as loans_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_long_time}', oss::json #>> '{data,report_detail,loans_long_time}') as loans_long_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,consfin_org_count}', oss::json #>> '{data,report_detail,consfin_org_count}') as consfin_org_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_cash_count}', oss::json #>> '{data,report_detail,loans_cash_count}') as loans_cash_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_six_month}', oss::json #>> '{data,report_detail,latest_six_month}') as latest_six_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,history_fail_fee}', oss::json #>> '{data,report_detail,history_fail_fee}') as history_fail_fee
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_three_month}', oss::json #>> '{data,report_detail,latest_three_month}') as latest_three_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month_fail}', oss::json #>> '{data,report_detail,latest_one_month_fail}') as latest_one_month_fail
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month}', oss::json #>> '{data,report_detail,latest_one_month}') as latest_one_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month_suc}', oss::json #>> '{data,report_detail,latest_one_month_suc}') as latest_one_month_suc
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_latest_time}', oss::json #>> '{data,report_detail,loans_latest_time}') as loans_latest_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_org_count}', oss::json #>> '{data,report_detail,loans_org_count}') as loans_org_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,history_suc_fee}', oss::json #>> '{data,report_detail,history_suc_fee}') as history_suc_fee
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_credibility}', oss::json #>> '{data,report_detail,loans_credibility}') as loans_credibility
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_score}', oss::json #>> '{data,report_detail,loans_score}') as loans_score
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_settle_count}', oss::json #>> '{data,report_detail,loans_settle_count}') as loans_settle_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_overdue_count}', oss::json #>> '{data,report_detail,loans_overdue_count}') as loans_overdue_count
from dc_xinyan_application  
where oss <>'' 
)
select t0.*
, loans_count
, loans_long_time
, consfin_org_count
, loans_cash_count
, latest_six_month
, history_fail_fee
, latest_three_month
, latest_one_month_fail
, latest_one_month
, latest_one_month_suc
, loans_org_count
, history_suc_fee
, loans_credibility
, loans_score
, loans_settle_count
, loans_overdue_count 
--, loans_latest_time
, case when loans_latest_time in ('-9999','-9998') then -9999
	  else apply_time - date(loans_latest_time) 
	  end as loans_latest_timediff
from perf t0
left join xy t2 on t0.order_no = t2.order_no
"""
xy_data = get_df_from_pg(xy_sql)

######################聚信立

jxl_sql	= """
with perf as (
select order_no
from t_loan_performance
where business_id in ('rong360') and effective_date between '2019-08-27' and '2019-10-18' and dt = '20191025'
)
select t0.*
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_org_cash}'  as d_30pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_cc}'  as d_30cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_org}'  as d_30cnt_org
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_org_all}'  as d_30pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt}'  as d_30cnt
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_org_cf}'  as d_30pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_cf}'  as d_30cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_cc}'  as d_30pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_org_cf}'  as d_30cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_cf}'  as d_30pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_cash}'  as d_30cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_cash}'  as d_30pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_org_cc}'  as d_30cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_org_cc}'  as d_30pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_all}'  as d_30pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_org_cash}'  as d_30cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_org_cash}'  as d_60pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_cc}'  as d_60cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_org}'  as d_60cnt_org
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_org_all}'  as d_60pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt}'  as d_60cnt
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_org_cf}'  as d_60pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_cf}'  as d_60cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_cc}'  as d_60pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_org_cf}'  as d_60cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_cf}'  as d_60pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_cash}'  as d_60cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_cash}'  as d_60pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_org_cc}'  as d_60cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_org_cc}'  as d_60pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_all}'  as d_60pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_org_cash}'  as d_60cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_org_cash}'  as d_90pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_cc}'  as d_90cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_org}'  as d_90cnt_org
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_org_all}'  as d_90pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt}'  as d_90cnt
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_org_cf}'  as d_90pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_cf}'  as d_90cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_cc}'  as d_90pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_org_cf}'  as d_90cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_cf}'  as d_90pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_cash}'  as d_90cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_cash}'  as d_90pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_org_cc}'  as d_90cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_org_cc}'  as d_90pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_all}'  as d_90pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_org_cash}'  as d_90cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_cash}'  as m_4pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_cc}'  as m_4cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_org}'  as m_4cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_all}'  as m_4pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt}'  as m_4cnt
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_cf}'  as m_4pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_cf}'  as m_4cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_cc}'  as m_4pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_org_cf}'  as m_4cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_cf}'  as m_4pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_cash}'  as m_4cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_cash}'  as m_4pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_org_cc}'  as m_4cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_cc}'  as m_4pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_all}'  as m_4pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_org_cash}'  as m_4cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_org_cash}'  as m_5pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_cc}'  as m_5cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_org}'  as m_5cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_org_all}'  as m_5pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt}'  as m_5cnt
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_org_cf}'  as m_5pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_cf}'  as m_5cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_cc}'  as m_5pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_org_cf}'  as m_5cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_cf}'  as m_5pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_cash}'  as m_5cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_cash}'  as m_5pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_org_cc}'  as m_5cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_org_cc}'  as m_5pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_all}'  as m_5pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_org_cash}'  as m_5cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_org_cash}'  as m_6pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_cc}'  as m_6cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_org}'  as m_6cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_org_all}'  as m_6pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt}'  as m_6cnt
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_org_cf}'  as m_6pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_cf}'  as m_6cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_cc}'  as m_6pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_org_cf}'  as m_6cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_cf}'  as m_6pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_cash}'  as m_6cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_cash}'  as m_6pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_org_cc}'  as m_6cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_org_cc}'  as m_6pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_all}'  as m_6pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_org_cash}'  as m_6cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_org_cash}'  as m_9pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_cc}'  as m_9cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org}'  as m_9cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_org_all}'  as m_9pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt}'  as m_9cnt
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_org_cf}'  as m_9pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_cf}'  as m_9cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_cc}'  as m_9pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org_cf}'  as m_9cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_cf}'  as m_9pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_cash}'  as m_9cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_cash}'  as m_9pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org_cc}'  as m_9cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_org_cc}'  as m_9pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_all}'  as m_9pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org_cash}'  as m_9cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_org_cash}'  as m_12pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_cc}'  as m_12cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org}'  as m_12cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_org_all}'  as m_12pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt}'  as m_12cnt
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_org_cf}'  as m_12pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_cf}'  as m_12cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_cc}'  as m_12pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org_cf}'  as m_12cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_cf}'  as m_12pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_cash}'  as m_12cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_cash}'  as m_12pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org_cc}'  as m_12cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_org_cc}'  as m_12pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_all}'  as m_12pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org_cash}'  as m_12cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_org_cash}'  as m_18pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_cc}'  as m_18cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org}'  as m_18cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_org_all}'  as m_18pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt}'  as m_18cnt
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_org_cf}'  as m_18pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_cf}'  as m_18cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_cc}'  as m_18pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org_cf}'  as m_18cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_cf}'  as m_18pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_cash}'  as m_18cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_cash}'  as m_18pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org_cc}'  as m_18cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_org_cc}'  as m_18pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_all}'  as m_18pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org_cash}'  as m_18cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_cash}'  as m_24pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_cc}'  as m_24cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org}'  as m_24cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_all}'  as m_24pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt}'  as m_24cnt
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_cf}'  as m_24pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_cf}'  as m_24cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_cc}'  as m_24pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org_cf}'  as m_24cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_cf}'  as m_24pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_cash}'  as m_24cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_cash}'  as m_24pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org_cc}'  as m_24cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_cc}'  as m_24pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_all}'  as m_24pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org_cash}'  as m_24cnt_org_cash
from perf t0 
left join (select * from dc_juxinli_application where oss <> '') t2 on t0.order_no = t2.order_no
"""

jxl_data = get_df_from_pg(jxl_sql)
jxl_data.shape
jxl_data.order_no.nunique()


###########################################天御
# tianyu_sql = """
# with apply as (
# select order_no
# , date(created_time) as apply_time
# , application_status
# from public.dw_dc_compensatory_cp_core_application
# where business_id = 'rong360' and date(created_time) between '2019-09-24' and '2019-09-26'
# ),
# tianyu as (
# select order_no
# , coalesce(oss::json #>> '{riskscore}', oss::json #>> '{riskScore}') as ty_riskscore
# , coalesce(json_array_elements(case when oss :: json #>> '{riskinfo}'= '[]' then '[null]' else cast(oss :: json #>> '{riskinfo}' as json) end)::json ->> 'riskcode',
#            json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskcode') as ty_riskcode
#
# from dc_tianyu_application
# where oss <>'')
# select t0.order_no, ty_riskscore
# from apply t0
# left join tianyu t1 on t0.order_no = t1.order_no
# """

tianyu_sql = """
with perf as (
select order_no
from t_loan_performance
where business_id in ('rong360') and effective_date between '2019-08-27' and '2019-10-18' and dt = '20191025'
),
tianyu as (
select order_no
, oss :: json #>> '{riskscore}' as riskscore_kfk
, json_array_elements(case when oss :: json #>> '{riskinfo}'= '[]' then '[null]' 
						   else cast(oss :: json #>> '{riskinfo}' as json) end)::json ->> 'riskcode' as riskcode_kfk
, json_array_elements(case when oss :: json #>> '{riskinfo}'= '[]' then '[null]' 
						   else cast(oss :: json #>> '{riskinfo}' as json) end)::json ->> 'riskcodevalue' as riskcodevalue_kfk
, oss :: json #>> '{riskScore}' as riskscore_oss
, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' 
						   else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCode' as riskcode_oss
, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' 
						   else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCodeValue' as riskcodevalue_oss
from dc_tianyu_application 
where oss <>''),
var as (
select  t0.order_no
--oss
, max(riskscore_oss) as ty_riskscore_oss
, max(case when riskcode_oss = '1' then riskcodevalue_oss else null end) as ty_riskcode_1_oss
, max(case when riskcode_oss = '2' then riskcodevalue_oss else null end) as ty_riskcode_2_oss
, max(case when riskcode_oss = '3' then riskcodevalue_oss else null end) as ty_riskcode_3_oss
, max(case when riskcode_oss = '4' then riskcodevalue_oss else null end) as ty_riskcode_4_oss
, max(case when riskcode_oss = '5' then riskcodevalue_oss else null end) as ty_riskcode_5_oss
, max(case when riskcode_oss = '6' then riskcodevalue_oss else null end) as ty_riskcode_6_oss
, max(case when riskcode_oss = '7' then riskcodevalue_oss else null end) as ty_riskcode_7_oss
, max(case when riskcode_oss = '8' then riskcodevalue_oss else null end) as ty_riskcode_8_oss
, max(case when riskcode_oss = '301' then riskcodevalue_oss else null end) as ty_riskcode_301_oss
, max(case when riskcode_oss = '503' then riskcodevalue_oss else null end) as ty_riskcode_503_oss
--kfk
, max(riskscore_kfk) as ty_riskscore_kfk
, max(case when riskcode_kfk = '1' then riskcodevalue_kfk else null end) as ty_riskcode_1_kfk
, max(case when riskcode_kfk = '2' then riskcodevalue_kfk else null end) as ty_riskcode_2_kfk
, max(case when riskcode_kfk = '3' then riskcodevalue_kfk else null end) as ty_riskcode_3_kfk
, max(case when riskcode_kfk = '4' then riskcodevalue_kfk else null end) as ty_riskcode_4_kfk
, max(case when riskcode_kfk = '5' then riskcodevalue_kfk else null end) as ty_riskcode_5_kfk
, max(case when riskcode_kfk = '6' then riskcodevalue_kfk else null end) as ty_riskcode_6_kfk
, max(case when riskcode_kfk = '7' then riskcodevalue_kfk else null end) as ty_riskcode_7_kfk
, max(case when riskcode_kfk = '8' then riskcodevalue_kfk else null end) as ty_riskcode_8_kfk
, max(case when riskcode_kfk = '301' then riskcodevalue_kfk else null end) as ty_riskcode_301_kfk
, max(case when riskcode_kfk = '503' then riskcodevalue_kfk else null end) as ty_riskcode_503_kfk
from perf t0
left join tianyu t1 on t1.order_no = t0.order_no
group by t0.order_no
)
select order_no
, case when ty_riskscore_kfk is null
	   then ty_riskscore_oss
	   else ty_riskscore_kfk
	   end as ty_riskscore
, case when ty_riskcode_1_kfk is null
	   then ty_riskcode_1_oss
	   else ty_riskcode_1_kfk
	   end as ty_riskcode_1
, case when ty_riskcode_2_kfk is null
	   then ty_riskcode_2_oss
	   else ty_riskcode_2_kfk
	   end as ty_riskcode_2
, case when ty_riskcode_3_kfk is null
	   then ty_riskcode_3_oss
	   else ty_riskcode_3_kfk
	   end as ty_riskcode_3
, case when ty_riskcode_4_kfk is null
	   then ty_riskcode_4_oss
	   else ty_riskcode_4_kfk
	   end as ty_riskcode_4
, case when ty_riskcode_5_kfk is null
	   then ty_riskcode_5_oss
	   else ty_riskcode_5_kfk
	   end as ty_riskcode_5
, case when ty_riskcode_6_kfk is null
	   then ty_riskcode_6_oss
	   else ty_riskcode_6_kfk
	   end as ty_riskcode_6
, case when ty_riskcode_7_kfk is null
	   then ty_riskcode_7_oss
	   else ty_riskcode_7_kfk
	   end as ty_riskcode_7
, case when ty_riskcode_8_kfk is null
	   then ty_riskcode_8_oss
	   else ty_riskcode_8_kfk
	   end as ty_riskcode_8
, case when ty_riskcode_301_kfk is null
	   then ty_riskcode_301_oss
	   else ty_riskcode_301_kfk
	   end as ty_riskcode_301
, case when ty_riskcode_503_kfk is null
	   then ty_riskcode_503_oss
	   else ty_riskcode_503_kfk
	   end as ty_riskcode_503
from var
"""

ty_data = get_df_from_pg(tianyu_sql)


#合并数据
base_data.index = base_data.iloc[:,0]
base_data = base_data.drop('order_no',1)
base_data = base_data.reset_index()
base_data = base_data[['order_no','education','monthlyincome']]

yl_data1.shape
jxl_data.shape
xy_data.shape
ty_data.shape
br_data1.shape


all_var = base_data.merge(yl_data1, on = 'order_no', how = 'left').merge(jxl_data, on = 'order_no' , how = 'left').merge(xy_data, on = 'order_no', how = 'left')\
.merge(ty_data, on = 'order_no', how = 'left').merge(br_data1, on = 'order_no', how = 'left')

all_var.index = all_var.order_no
all_var = all_var.rename(columns = {'rms002':'RMS002'})

all_var = all_var[features_in_model].reset_index()


save_data_to_pickle(all_var, data_path, 'rong360_var_1008to1009.pkl')


var_dict = pd.read_excel('D:/Model/201908_qsfq_model/建模代码可用变量字典.xlsx')

all_var = all_var.replace('null',-1).fillna(-1).replace('"null"',-1).replace('',-1)

for i in features_in_model:
    print (i)
    all_var[i] = all_var[i].astype(float)

all_var.index = all_var.order_no

"""
分流
"""

all_var = all_var.reset_index(drop=True)

all_var['abnormal'] = 0

for i in range(all_var.shape[0]):
    print (i)
    if all_var.loc[i,'drs_nodebtscore'] not in Interval(-1,100, closed= True):
        all_var.loc[i, 'drs_nodebtscore'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i,'d_30cnt_cash'] not in Interval(-1,49, closed= True):
        all_var.loc[i, 'd_30cnt_cash'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i,'loans_org_count'] not in Interval(-1,22, closed= True):
        all_var.loc[i, 'loans_org_count'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i, 'ty_riskscore'] not in Interval(-1, 110, closed=True):
        all_var.loc[i, 'ty_riskscore'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i, 'loans_settle_count'] not in Interval(-1, 64, closed=True):
        all_var.loc[i, 'loans_settle_count'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i, 'loans_count'] not in Interval(-1, 68, closed=True):
        all_var.loc[i, 'loans_count'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i, 'd_90cnt_org'] not in Interval(-1, 40, closed=True):
        all_var.loc[i, 'd_90cnt_org'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i, 'd_90pct_cnt_org_all'] not in Interval(-1, 1, closed=True):
        all_var.loc[i, 'd_90pct_cnt_org_all'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i, 'RMS002'] not in Interval(-1, 1317, closed=True):
        all_var.loc[i, 'RMS002'] = -1
        all_var.loc[i, 'abnormal'] = 1
    if all_var.loc[i, 'm_6pct_cnt_org_cc'] not in Interval(-1, 1, closed=True):
        all_var.loc[i, 'm_6pct_cnt_org_cc'] = -1
        all_var.loc[i, 'abnormal'] = 1




# all_var2 = all_var.merge(node_data,  on = 'order_no')
# all_var2.head()
# all_var2.index = all_var2.order_no
X = all_var2.loc[(all_var2.anti_fraud_rule_node == 'N')]

X.shape

ss.eda(X, var_dict, useless_vars=[], exempt_vars=[],data_path=result_path, save_label='all_1009')


#读入模型数据
result_path = 'D:/Model/201908_qsfq_model/02_result_0928/'

#features_in_model = list(pd.read_excel(os.path.join(result_path,'result_0926_54.xlsx'), sheet_name= 'features_in_model')[0])
features_in_model = list(pd.read_excel(os.path.join(result_path,'result_0926_52.xlsx'), sheet_name= 'features_in_model')[0])

len(features_in_model)

#重新打分
# LOAD MODEL
mymodel = xgb.Booster()
mymodel.load_model(result_path + 'dc 0926 52.model')



#mydata.rename(columns=dict(zip(myvar['变量名称'],myvar['线上变量名'])),inplace=True)
mydata = all_var[features_in_model]

mydata.columns

# PREPARE DATA
#data_lean = pd.DataFrame(mydata, columns=list(myvar.iloc[:,1]))
var_selected = all_var[features_in_model].values


# var_selected = pd.DataFrame(var_selected, columns=list(features)).astype(float)
# var_selected.head()


# PREDICT SCORES
var_selected = DMatrix(var_selected)
ypred = mymodel.predict(var_selected)
ypred

score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([mydata.index, score, ypred]).T
data_scored.head(200)
data_scored = data_scored.rename(columns = {0:'order_no',1:'model_score',2:'prob'})

score_with_var = all_var.merge(data_scored, on = 'order_no')

# train_bin = [-np.inf,557, 580, 596, 608, 620, 629, 638, 646, 657, np.inf]
# train_bin_20 = [-np.inf,543, 557, 570, 580, 589, 596, 602, 608, 614, 620, 624, 629, 633, 638, 642, 646, 651, 657, 664,np.inf]

train_bin = [-np.inf,566, 582, 600, 610, 618, 625, 633, 642, 652, np.inf]
train_bin_20 = [-np.inf,550, 566, 574, 582, 591, 600, 605, 610, 615, 618, 621, 625, 629, 633, 637, 642, 646, 652, 659,np.inf]
train_bin_30 = [-np.inf,541, 557, 566, 571, 576, 582, 588, 594, 600, 603, 607, 610, 613, 616, 618, 620, 623, 625, 628, 630, 633,636,639, 642,645,648,652,656, 662, np.inf]

hh_bin = [-np.inf,591, 599, 606, 610, 614, 618, 621, 626, 631, np.inf]
hh_bin_20 = [-np.inf, 585, 591, 595, 599,603, 606, 608,610, 612, 614, 616, 618, 619,621, 624, 626, 628, 631, 636, np.inf]



score_with_var['score_bin'] = pd.cut(score_with_var.model_score, bins = train_bin)
score_with_var['score_bin_20'] = pd.cut(score_with_var.model_score, bins = train_bin_20)
score_with_var['score_bin_30'] = pd.cut(score_with_var.model_score, bins = train_bin_30)

score_with_var_1009.dccreditscorev4score = score_with_var_1009.dccreditscorev4score.astype(float)
score_with_var_1009['score_bin_hh'] = pd.cut(score_with_var_1009.dccreditscorev4score, bins = hh_bin)
score_with_var_1009['score_bin_hh_20'] = pd.cut(score_with_var_1009.dccreditscorev4score, bins = hh_bin_20)

for i in score_with_var.columns:
    if 'score_bin' in i:
        print (i)
        score_with_var[i] = score_with_var[i].astype(str)

score_with_var.to_excel( "D:/Model/201908_qsfq_model/01_data/score_with_var_rong0824to1018_refill.xlsx",index=False)
perf_data.merge(score_with_var[['order_no','model_score','score_bin','score_bin_20','score_bin_30']]).to_excel( "D:/Model/201908_qsfq_model/01_data/rong_perf.xlsx",index=False)


data_scored2.loc[(data_scored2.created_time >= '2019-10-04 18:40:00') & (data_scored2.created_time <= '2019-10-06 19:30:00'),'has_br'] = 1


data_scored2.to_excel(data_path + "data_scored0924to1007.xlsx",index=False)
data_scored.model_score.min()
data_scored.model_score.max()



node_sql = """
select order_no
, created_time
, application_status
, anti_fraud_rule_node
, operator_anti_fraud_rule_node
, taobao_anti_fraud_rule_node
, model
, thirdpart_rule_node
, dccreditscorev4score
from (select order_no
              , created_time
            --, date(created_time) as apply_time
            --, date_part('hour', created_time) as hour
            , application_status
    from public.dw_dc_compensatory_cp_core_application
    where business_id = 'rong360' and date(created_time) = '2019-10-09') t0
left join ods_rsk_installmentriskcontrolresult t1 on t0.order_no = t1.orderno
"""
node_data = get_df_from_pg(node_sql)


X.loc[(X.drs_nodebtscore!= -1) & (X.alu_lst_id_inteday == -1)][['order_no','drs_nodebtscore','alu_lst_id_inteday']]


# LOAD FUNCTION
def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))
#.map(lambda x: int(x))

"""
看变量的分布情况
"""

rebin_spec = load_data_from_pickle(result_path, 'rebin_spec.pkl')

rebin_spec['drs_nodebtscore']

all_var3 = all_var[features_in_model].replace(-1,-8887)

bin_obj = mt.BinWoe()

X_cat = bin_obj.convert_to_category(all_var3, var_dict, rebin_spec)
X_cat.shape


all_var['perf_flag'] = 1


X_cat_train_with_y_appmon = pd.merge(X_cat,all_var[['perf_flag','apply_time']] ,left_index=True,right_index=True)

var_dist_badRate_by_time = ss.get_badRate_and_dist_by_time(X_cat_train_with_y_appmon,features_in_model,'apply_time','perf_flag')
var_dist_badRate_by_time.to_excel(os.path.join(data_path,'var_dist_0924to1007.xlsx'))

var_dist_badRate_by_time = pd.read_excel(os.path.join(data_path, 'var_dist_0924to1007.xlsx'))


woe_iv_df = pd.read_excel(os.path.join(result_path, 'woe_iv_df.xlsx'))

x_with_y = pd.read_excel(os.path.join(data_path, 'x_with_y_v6.xlsx'))

model_data = x_with_y.loc[x_with_y.sample_set.isin(['train'])][features_in_model]
model_data =model_data.replace(-1, -8887)
model_data = model_data.astype(float)

model_cat = bin_obj.convert_to_category(model_data, var_dict, rebin_spec)

model_cat['apply_time'] = 'train'
model_cat['perf_flag'] = 1

var_dist_badRate_by_time_train = ss.get_badRate_and_dist_by_time(model_cat,features_in_model,'apply_time','perf_flag')
var_dist_badRate_by_time_train.to_excel(os.path.join(data_path,'var_dist_train.xlsx'))
var_dist_badRate_by_time_train = pd.read_excel(os.path.join(data_path, 'var_dist_train.xlsx'))

all_dist = var_dist_badRate_by_time_train.merge(var_dist_badRate_by_time, on = ['varName','bins'], how = 'outer')
all_dist.to_excel(os.path.join(data_path,'all_dist.xlsx'))

"""
确认变量上限
"""
set(all_var3.loc[all_var3.loans_settle_count >= 64].index) - set(all_var2.loc[(all_var2.anti_fraud_rule_node == 'N') & (all_var2.loans_settle_count >= 64)].order_no)

# all_var3.loc[all_var3.loans_settle_count >= 64].shape[0]/len(all_var3)  #48, 0.0016482950448130215
# all_var3.loc[all_var3.loans_org_count >= 22].shape[0]/len(all_var3)  #253, 0.008687888465368634
# all_var3.loc[all_var3.loans_count >= 68].shape[0]/len(all_var3)  #72, 0.008687888465368634
#
# all_var3.loc[(all_var3.loans_settle_count >= 64)|(all_var3.loans_org_count >= 22)|(all_var3.loans_count >= 68)].shape[0]/len(all_var3)  #0.0016482950448130215

all_var.loc[all_var.order_no == '160348035224700930'].T


all_var2.loc[(all_var2.anti_fraud_rule_node == 'N') & (all_var2.loans_settle_count >= 64)].shape[0]/len(all_var2.loc[(all_var2.anti_fraud_rule_node == 'N')]) #44, 0.0019279642450267286
all_var2.loc[(all_var2.anti_fraud_rule_node == 'N') & (all_var2.loans_org_count >= 22)].shape[0]/len(all_var2.loc[(all_var2.anti_fraud_rule_node == 'N')]) #237, 0.010384716501621243
all_var2.loc[(all_var2.anti_fraud_rule_node == 'N') & (all_var2.loans_count >= 68)].shape[0]/len(all_var2.loc[(all_var2.anti_fraud_rule_node == 'N')]) #66, 0.002891946367540093

