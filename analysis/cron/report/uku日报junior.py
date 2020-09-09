# coding: utf-8
"""
Python 3.6.0
uku日报
"""
import sys
import os

#sys.path.append("C:\\Users\\wangj\\Documents\\genie")
sys.path.append('/home/ops/repos/newgenie/')

import numpy as np
import pandas as pd
from importlib import reload

from utils3.data_io_utils import get_df_from_pg
import utils3.send_email_with_table as se
from utils3.send_email import mailtest
reload(se)

"""0.检查表是否是空---------------------------------------------------------------------"""
loan_check = get_df_from_pg("""SELECT * FROM public.dw_gocash_go_cash_loan_gocash_core_loan LIMIT 10""")
col_check = get_df_from_pg("""SELECT * FROM public.dw_gocash_gocash_collection_col_case LIMIT 10""")
repay_check = get_df_from_pg("""SELECT * FROM public.dw_gocash_gocash_collection_col_repayment LIMIT 10""")
payflow_check = get_df_from_pg("""SELECT * FROM public.dw_gocash_go_cash_loan_gocash_core_loan_pay_flow LIMIT 10""")

if loan_check.shape[0]<10 or col_check.shape[0]<10 or repay_check.shape[0]<10 or payflow_check.shape[0]<10:
    send_email_elart = 1
    print('数据底层表无数据')
else:
    send_email_elart = 0
    print('数据底层表有数据')



"""1.放款情况--------------------------------------------------------------------------"""

FANGKUAN_SQL = """
set enable_nestloop=off;

SELECT
	to_char(effective_date, 'YYYY-MM-DD') as date,
	count(distinct case when return_flag = 'false' and approved_period = 8 then id else null end) as new8fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 8 then id else null end) as old8fangkuanliang,
	count(distinct case when return_flag = 'false' and approved_period = 15 then id else null end) as new15fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 15 then id else null end) as old15fangkuanliang,
    count(distinct case when return_flag = 'false' and approved_period = 28 then id else null end) as new28fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 28 then id else null end) as old28fangkuanliang,
	--count(distinct case when return_flag = 'false' and approved_period = 22 then id else null end) as new22fangkuanliang,
	--count(distinct case when return_flag = 'true' and approved_period = 22 then id else null end) as old22fangkuanliang,
    --count(distinct case when return_flag = 'false' and approved_period = 29 then id else null end) as new29fangkuanliang,
	--count(distinct case when return_flag = 'true' and approved_period = 29 then id else null end) as old29fangkuanliang,
	count(distinct id) as totalfangkuanliang,
	round(sum(case when return_flag = 'false' and approved_period = 8 then approved_principal else 0 end)/ 2000)::int as new8hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 8 then approved_principal else 0 end)/ 2000)::int as old8hetongjine,
	round(sum(case when return_flag = 'false' and approved_period = 15 then approved_principal else 0 end)/ 2000)::int as new15hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 15 then approved_principal else 0 end)/ 2000)::int as old15hetongjine,
    round(sum(case when return_flag = 'false' and approved_period = 28 then approved_principal else 0 end)/ 2000)::int as new28hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 28 then approved_principal else 0 end)/ 2000)::int as old28hetongjine,
	--round(sum(case when return_flag = 'false' and approved_period = 22 then approved_principal else 0 end)/ 2000)::int as new22hetongjine,
	--round(sum(case when return_flag = 'true' and approved_period = 22 then approved_principal else 0 end)/ 2000)::int as old22hetongjine,
    --round(sum(case when return_flag = 'false' and approved_period = 29 then approved_principal else 0 end)/ 2000)::int as new29hetongjine,
	--round(sum(case when return_flag = 'true' and approved_period = 29 then approved_principal else 0 end)/ 2000)::int as old29hetongjine,
	round(sum(approved_principal / 2000))::int as totalhetongjine
from
	public.dw_gocash_go_cash_loan_gocash_core_loan
where
	effective_date >= current_date-7
	and effective_date <= current_date-1
	and loan_status not in ('DENIED',
	'RESCIND',
	'APPROVING',
	'CREATED')
group by
	date
union
select
	'合计' as date,
	count(distinct case when return_flag = 'false' and approved_period = 8 then id else null end) as new8fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 8 then id else null end) as old8fangkuanliang,
	count(distinct case when return_flag = 'false' and approved_period = 15 then id else null end) as new15fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 15 then id else null end) as old15fangkuanliang,
    count(distinct case when return_flag = 'false' and approved_period = 28 then id else null end) as new28fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 28 then id else null end) as old28fangkuanliang,
	--count(distinct case when return_flag = 'false' and approved_period = 22 then id else null end) as new22fangkuanliang,
	--count(distinct case when return_flag = 'true' and approved_period = 22 then id else null end) as old22fangkuanliang,
    --count(distinct case when return_flag = 'false' and approved_period = 29 then id else null end) as new29fangkuanliang,
	--count(distinct case when return_flag = 'true' and approved_period = 29 then id else null end) as old29fangkuanliang,
	count(distinct id) as totalfangkuanliang,
	round(sum(case when return_flag = 'false' and approved_period = 8 then approved_principal else 0 end)/ 2000)::int as new8hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 8 then approved_principal else 0 end)/ 2000)::int as old8hetongjine,
	round(sum(case when return_flag = 'false' and approved_period = 15 then approved_principal else 0 end)/ 2000)::int as new15hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 15 then approved_principal else 0 end)/ 2000)::int as old15hetongjine,
    round(sum(case when return_flag = 'false' and approved_period = 28 then approved_principal else 0 end)/ 2000)::int as new28hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 28 then approved_principal else 0 end)/ 2000)::int as old28hetongjine,
	--round(sum(case when return_flag = 'false' and approved_period = 22 then approved_principal else 0 end)/ 2000)::int as new22hetongjine,
	--round(sum(case when return_flag = 'true' and approved_period = 22 then approved_principal else 0 end)/ 2000)::int as old22hetongjine,
    --round(sum(case when return_flag = 'false' and approved_period = 29 then approved_principal else 0 end)/ 2000)::int as new29hetongjine,
	--round(sum(case when return_flag = 'true' and approved_period = 29 then approved_principal else 0 end)/ 2000)::int as old29hetongjine,
	round(sum(approved_principal / 2000))::int as totalhetongjine
from
	public.dw_gocash_go_cash_loan_gocash_core_loan
where
	effective_date >= current_date-7
	and effective_date <= current_date-1
	and loan_status not in ('DENIED',
	'RESCIND',
	'APPROVING',
	'CREATED')
group by
	date
order by
	date
"""

try:
    fangkuan_df = get_df_from_pg(FANGKUAN_SQL)
    fangkuan_df.head()
    fangkuan_df.rename(columns={'date':'放款日期','new8fangkuanliang':'新客户8日','old8fangkuanliang':'老客户8日','new15fangkuanliang':'新客户15日','old15fangkuanliang':'老客户15日','new28fangkuanliang':'linkaja新28日','old28fangkuanliang':'linkaja老28日','totalfangkuanliang':'合计',\
        'new8hetongjine':'新客户8日','old8hetongjine':'老客户8日','new15hetongjine':'新客户15日','old15hetongjine':'老客户15日','new28hetongjine':'linkaja新28日','old28hetongjine':'linkaja老28日','totalhetongjine':'合计'},inplace=True)
    the_col = [('','放款日期'),('放款件数','新客户8日'),('放款件数','老客户8日'),('放款件数','新客户15日'),('放款件数','老客户15日'),('放款件数','linkaja新28日'),('放款件数','linkaja老28日'),('放款件数','合计')\
        ,('合同金额','新客户8日'),('合同金额','老客户8日'),('合同金额','新客户15日'),('合同金额','老客户15日'),('合同金额','linkaja新28日'),('合同金额','linkaja老28日'),('合同金额','合计')]
    # fangkuan_df.rename(columns={'date':'放款日期','new8fangkuanliang':'新客户8日','old8fangkuanliang':'老客户8日','new15fangkuanliang':'新客户15日','old15fangkuanliang':'老客户15日','new22fangkuanliang':'新客户22日','old22fangkuanliang':'老客户22日','new29fangkuanliang':'新客户29日','old29fangkuanliang':'老客户29日','totalfangkuanliang':'合计',\
    #     'new8hetongjine':'新客户8日','old8hetongjine':'老客户8日','new15hetongjine':'新客户15日','old15hetongjine':'老客户15日','new22hetongjine':'新客户22日','old22hetongjine':'老客户22日','new29hetongjine':'新客户29日','old29hetongjine':'老客户29日','totalhetongjine':'合计'},inplace=True)
    # the_col = [('','放款日期'),('放款件数','新客户8日'),('放款件数','老客户8日'),('放款件数','新客户15日'),('放款件数','老客户15日'),('放款件数','新客户22日'),('放款件数','老客户22日'),('放款件数','新客户29日'),('放款件数','老客户29日'),('放款件数','合计')\
    #     ,('合同金额','新客户8日'),('合同金额','老客户8日'),('合同金额','新客户15日'),('合同金额','老客户15日'),('合同金额','新客户22日'),('合同金额','老客户22日'),('合同金额','新客户29日'),('合同金额','老客户29日'),('合同金额','合计')]
    fangkuan_df.columns=pd.MultiIndex.from_tuples(the_col)
except:
    fangkuan_df = pd.DataFrame()
print('fangkuan_df')


"""2.入催情况--------------------------------------------------------------------------"""
RUCUI_SQL = """
set enable_nestloop=off;

SELECT
	to_char(due_date, 'YYYY-MM-DD') as date,
	sum(new_total_loan)::int as new_total_loan,
	sum(old_total_loan)::int as old_total_loan,
	sum(new_rucui_loan)::int as new_rucui_loan,
	sum(old_rucui_loan)::int as old_rucui_loan,
	case when sum(new_total_loan)=0 then '-' else concat(round(100 * sum(new_rucui_loan)/ sum(new_total_loan), 2), '%') end as new_rucui_rate,
	case when sum(old_total_loan)=0 then '-' else concat(round(100 * sum(old_rucui_loan)/ sum(old_total_loan), 2), '%') end as old_rucui_rate,
	case when sum(new_shoucidaoqi_loan)=0 then '-' else concat(round(100 * sum(new_shouyu_loan)/ sum(new_shoucidaoqi_loan), 2), '%') end as new_shouyu_loan_rate,
	case when sum(old_shoucidaoqi_loan)=0 then '-' else concat(round(100 * sum(old_shouyu_loan)/ sum(old_shoucidaoqi_loan), 2), '%') end as old_shouyu_loan_rate,
	case when sum(new_zhanqidaoqi_loan)=0 then '-' else concat(round(100 * sum(new_zhanqiyuqi_loan)/ sum(new_zhanqidaoqi_loan), 2), '%') end as new_zhanqiyuqi_loan_rate,
	case when sum(old_zhanqidaoqi_loan)=0 then '-' else concat(round(100 * sum(old_zhanqiyuqi_loan)/ sum(old_zhanqidaoqi_loan), 2), '%') end as old_zhanqiyuqi_loan_rate
from
	(
	select
		due_date,
		new_total_loan,
		old_total_loan,
		new_shoucidaoqi_loan,
		old_shoucidaoqi_loan,
		new_shouyu_loan,
		old_shouyu_loan,
		new_zhanqidaoqi_loan,
		old_zhanqidaoqi_loan,
		new_zhanqiyuqi_loan,
		old_zhanqiyuqi_loan,
		new_rucui_loan,
		old_rucui_loan
	from
		(
		select
			due_date::date,
			count(distinct case when return_flag = 'false' then ab.id else null end) as new_total_loan,
			count(distinct case when return_flag = 'true' then ab.id else null end) as old_total_loan,
			count(distinct case when due_date-effective_date+1=approved_period and return_flag = 'false' then ab.id else null end) as new_shoucidaoqi_loan,
			count(distinct case when due_date-effective_date+1=approved_period and return_flag = 'true' then ab.id else null end) as old_shoucidaoqi_loan,
			count(distinct case when due_date-effective_date+1=approved_period and ab.late_day>0 and order_status is not null and order_status <> 'PAIDOFF' and return_flag = 'false' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as new_shouyu_loan,
			count(distinct case when due_date-effective_date+1=approved_period and ab.late_day>0 and order_status is not null and order_status <> 'PAIDOFF' and return_flag = 'true' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as old_shouyu_loan,
			count(distinct case when due_date-effective_date+1>approved_period and return_flag = 'false' then ab.id else null end) as new_zhanqidaoqi_loan,
			count(distinct case when due_date-effective_date+1>approved_period and return_flag = 'true' then ab.id else null end) as old_zhanqidaoqi_loan,
			count(distinct case when due_date-effective_date+1>approved_period and late_day>0 and ab.order_status is not null and ab.order_status <> 'PAIDOFF' and return_flag = 'false' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as new_zhanqiyuqi_loan,
			count(distinct case when due_date-effective_date+1>approved_period and late_day>0 and ab.order_status is not null and ab.order_status <> 'PAIDOFF' and return_flag = 'true' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as old_zhanqiyuqi_loan,
            count(distinct case when ab.order_status is not null and ab.order_status <> 'PAIDOFF' and order_status != 'ABNORMAL' and return_flag = 'false' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as new_rucui_loan,
			count(distinct case when ab.order_status is not null and ab.order_status <> 'PAIDOFF' and order_status != 'ABNORMAL' and return_flag = 'true' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as old_rucui_loan
		from
			(
			select
				a.id,
				approved_principal,
				approved_period,
				effective_date,
				return_flag,
				due_date,
				order_status,
				late_day,
				late_date
			from
				(
				select
					id,
					approved_principal,
					approved_period,
					effective_date,
					return_flag,
					due_date
				from
					public.dw_gocash_go_cash_loan_gocash_core_loan
				where
					loan_status not in ('DENIED',
					'RESCIND',
					'APPROVING',
					'CREATED') )a
			left join (
				select
					id,
					order_id,
					order_status,
					late_day,
					late_date,
					row_number() over(partition by order_id
				order by
					create_time) as rk
				from
					public.dw_gocash_gocash_collection_col_case
				where
					app_id not in ('Credits','KASANDAAI'))b on
				a.id = b.order_id
		union
			select
				a1.loan_id as id,
				a2.approved_principal,
				a2.approved_period,
				a2.effective_date,
				a2.return_flag,
				a1.his_due_date as due_date,
				order_status,
				late_day,
				late_date
			from
				(
				select
					loan_id,
					his_due_date
				from
					dw_gocash_go_cash_loan_gocash_core_loan_pay_flow
				where
					status = 'SUCCESS'
					and extend_fee>0
					--and late_fee>0
					) a1
			left join (
				select
					id,
					approved_principal,
					effective_date,
					return_flag,
					approved_period
				from
					dw_gocash_go_cash_loan_gocash_core_loan
				where
					loan_status not in ('DENIED',
					'RESCIND',
					'APPROVING',
					'CREATED'))a2 on
				a1.loan_id = a2.id
			left join (
				select
					id,
					order_id,
					order_status,
					late_day,
					late_date,
					row_number() over(partition by order_id
				order by
					create_time) as rk
				from
					public.dw_gocash_gocash_collection_col_case
				where
					app_id not in ('Credits','KASANDAAI'))b on
				a1.loan_id = b.order_id)ab
		left join (
			select
				loan_id,
				sum(extend_fee) as extend_fee_sum
			from
				dw_gocash_go_cash_loan_gocash_core_loan_pay_flow
			where
				status = 'SUCCESS'
			group by
				1)d on
			ab.id = d.loan_id
		where
			due_date::date >= current_date-7
			and due_date::date <= current_date-1
		group by
			due_date::date)u)v
group by
	1
union
select
	'合计' as date,
	sum(new_total_loan)::int as new_total_loan,
	sum(old_total_loan)::int as old_total_loan,
	sum(new_rucui_loan)::int as new_rucui_loan,
	sum(old_rucui_loan)::int as old_rucui_loan,
	case when sum(new_total_loan)=0 then '-' else concat(round(100 * sum(new_rucui_loan)/ sum(new_total_loan), 2), '%') end as new_rucui_rate,
	case when sum(old_total_loan)=0 then '-' else concat(round(100 * sum(old_rucui_loan)/ sum(old_total_loan), 2), '%') end as old_rucui_rate,
	case when sum(new_shoucidaoqi_loan)=0 then '-' else concat(round(100 * sum(new_shouyu_loan)/ sum(new_shoucidaoqi_loan), 2), '%') end as new_shouyu_loan_rate,
	case when sum(old_shoucidaoqi_loan)=0 then '-' else concat(round(100 * sum(old_shouyu_loan)/ sum(old_shoucidaoqi_loan), 2), '%') end as old_shouyu_loan_rate,
	case when sum(new_zhanqidaoqi_loan)=0 then '-' else concat(round(100 * sum(new_zhanqiyuqi_loan)/ sum(new_zhanqidaoqi_loan), 2), '%') end as new_zhanqiyuqi_loan_rate,
	case when sum(old_zhanqidaoqi_loan)=0 then '-' else concat(round(100 * sum(old_zhanqiyuqi_loan)/ sum(old_zhanqidaoqi_loan), 2), '%') end as old_zhanqiyuqi_loan_rate
from
	(
	select
		due_date,
		new_total_loan,
		old_total_loan,
		new_shoucidaoqi_loan,
		old_shoucidaoqi_loan,
		new_shouyu_loan,
		old_shouyu_loan,
		new_zhanqidaoqi_loan,
		old_zhanqidaoqi_loan,
		new_zhanqiyuqi_loan,
		old_zhanqiyuqi_loan,
		new_rucui_loan,
		old_rucui_loan
	from
		(
		select
			due_date::date,
			count(distinct case when return_flag = 'false' then ab.id else null end) as new_total_loan,
			count(distinct case when return_flag = 'true' then ab.id else null end) as old_total_loan,
			count(distinct case when due_date-effective_date+1=approved_period and return_flag = 'false' then ab.id else null end) as new_shoucidaoqi_loan,
			count(distinct case when due_date-effective_date+1=approved_period and return_flag = 'true' then ab.id else null end) as old_shoucidaoqi_loan,
			count(distinct case when due_date-effective_date+1=approved_period and ab.late_day>0 and order_status is not null and order_status <> 'PAIDOFF' and return_flag = 'false' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as new_shouyu_loan,
			count(distinct case when due_date-effective_date+1=approved_period and ab.late_day>0 and order_status is not null and order_status <> 'PAIDOFF' and return_flag = 'true' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as old_shouyu_loan,
			count(distinct case when due_date-effective_date+1>approved_period and return_flag = 'false' then ab.id else null end) as new_zhanqidaoqi_loan,
			count(distinct case when due_date-effective_date+1>approved_period and return_flag = 'true' then ab.id else null end) as old_zhanqidaoqi_loan,
			count(distinct case when due_date-effective_date+1>approved_period and late_day>0 and ab.order_status is not null and ab.order_status <> 'PAIDOFF' and return_flag = 'false' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as new_zhanqiyuqi_loan,
			count(distinct case when due_date-effective_date+1>approved_period and late_day>0 and ab.order_status is not null and ab.order_status <> 'PAIDOFF' and return_flag = 'true' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as old_zhanqiyuqi_loan,
 count(distinct case when ab.order_status is not null and ab.order_status <> 'PAIDOFF' and order_status != 'ABNORMAL' and return_flag = 'false' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as new_rucui_loan,
			count(distinct case when ab.order_status is not null and ab.order_status <> 'PAIDOFF' and order_status != 'ABNORMAL' and return_flag = 'true' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as old_rucui_loan
		from
			(
			select
				a.id,
				approved_principal,
				approved_period,
				effective_date,
				return_flag,
				due_date,
				order_status,
				late_day,
				late_date
			from
				(
				select
					id,
					approved_principal,
					effective_date,
					return_flag,
					due_date,
					approved_period
				from
					public.dw_gocash_go_cash_loan_gocash_core_loan
				where
					loan_status not in ('DENIED',
					'RESCIND',
					'APPROVING',
					'CREATED') )a
			left join (
				select
					id,
					order_id,
					order_status,
					late_day,
					late_date,
					row_number() over(partition by order_id
				order by
					create_time) as rk
				from
					public.dw_gocash_gocash_collection_col_case
				where
					app_id not in ('Credits','KASANDAAI'))b on
				a.id = b.order_id
		union
			select
				a1.loan_id as id,
				a2.approved_principal,
				a2.approved_period,
				a2.effective_date,
				a2.return_flag,
				a1.his_due_date as due_date,
				order_status,
				late_day,
				late_date
			from
				(
				select
					loan_id,
					his_due_date
				from
					dw_gocash_go_cash_loan_gocash_core_loan_pay_flow
				where
					status = 'SUCCESS'
					and extend_fee>0
					--and late_fee>0
					) a1
			left join (
				select
					id,
					approved_principal,
					effective_date,
					return_flag,
					approved_period
				from
					dw_gocash_go_cash_loan_gocash_core_loan
				where
					loan_status not in ('DENIED',
					'RESCIND',
					'APPROVING',
					'CREATED'))a2 on
				a1.loan_id = a2.id
			left join (
				select
					id,
					order_id,
					order_status,
					late_day,
					late_date,
					row_number() over(partition by order_id
				order by
					create_time) as rk
				from
					public.dw_gocash_gocash_collection_col_case
				where
					app_id not in ('Credits','KASANDAAI'))b on
				a1.loan_id = b.order_id)ab
		left join (
			select
				loan_id,
				sum(extend_fee) as extend_fee_sum
			from
				dw_gocash_go_cash_loan_gocash_core_loan_pay_flow
			where
				status = 'SUCCESS'
			group by
				1)d on
			ab.id = d.loan_id
		where
			due_date::date >= current_date-7
			and due_date::date <= current_date-1
		group by
			due_date::date)u)v
group by
	1
order by
	date
"""

try:
    rucui_df = get_df_from_pg(RUCUI_SQL)
    rucui_df.head()
    rucui_df.rename(columns={'date':'到期日期','new_total_loan':'新客户','old_total_loan':'老客户','new_rucui_loan':'新客户','old_rucui_loan':'老客户'\
        ,'new_rucui_rate':'新客户','old_rucui_rate':'老客户','new_shouyu_loan_rate':'新客户','old_shouyu_loan_rate':'老客户','new_zhanqiyuqi_loan_rate':'新客户','old_zhanqiyuqi_loan_rate':'老客户'},inplace=True)
    the_col = [('','到期日期'),('到期件数','新客户'),('到期件数','老客户'),('入催件数','新客户'),('入催件数','老客户'),('入催比例','新客户'),('入催比例','老客户'),\
        ('首次入催比例','新客户'),('首次入催比例','老客户'),('展期后入催比例','新客户'),('展期后入催比例','老客户')]
    rucui_df.columns=pd.MultiIndex.from_tuples(the_col)
except:
    rucui_df = pd.DataFrame()
print('rucui_df')

"""3.1.入催（放款日8天&15天）情况--------------------------------------------------------------------------"""
RUCUI_FANGKUAN_SQL = """
set enable_nestloop=off;

select
	to_char(effective_date, 'YYYY-MM-DD') as date,
	new_cnt_num,
	old_cnt_num,
	total_cnt_num,
	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd1 / new_cnt_num, 2), '%') end as new_dpd1_rate,
	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd1 / old_cnt_num, 2), '%') end as old_dpd1_rate,
	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd1 / total_cnt_num, 2), '%') end as total_dpd1_rate,
	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd3 / new_cnt_num, 2), '%') end as new_dpd3_rate,
	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd3 / old_cnt_num, 2), '%') end as old_dpd3_rate,
	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd3 / total_cnt_num, 2), '%') end as total_dpd3_rate
from
	(
	select
		effective_date,
		count(distinct case when return_flag = 'false' then loan_id else null end) as new_cnt_num,
		count(distinct case when return_flag = 'true' then loan_id else null end) as old_cnt_num,
		count(distinct loan_id) as total_cnt_num,
		count(distinct case when late_date-effective_date = approved_period and return_flag = 'false' then loan_id end) as new_dpd1,
		count(distinct case when late_date-effective_date = approved_period and return_flag = 'true' then loan_id end) as old_dpd1,
		count(distinct case when late_date-effective_date = approved_period then loan_id end) as total_dpd1,
		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'false' then loan_id end) as new_dpd3,
		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'true' then loan_id end) as old_dpd3,
		count(distinct case when DPD >= 3 and effective_date <= current_date-18 then loan_id end) as total_dpd3
	from
		(
		select
			t1.id loan_id,
			effective_date,
			return_flag,
			loan_status,
			approved_period,
			late_date,
			case
				when loan_status = 'COLLECTION' then current_date-late_date
				else round(late_fee /(approved_principal*0.025))::int end as DPD,
				t1.approved_principal
			from
				(
				select
					*
				from
					dw_gocash_go_cash_loan_gocash_core_loan
				where
					effective_date is not null
					and effective_date >= '2019-01-01'
                    and approved_period in (8,15)
					and loan_status not in ('DENIED',
					'RESCIND',
					'APPROVING',
					'CREATED')) t1
			left join (
				select
					*
				from
					(
					select
						order_id,
						late_date,
						row_number() over(partition by order_id
					order by
						late_date asc) as num
					from
						dw_gocash_gocash_collection_col_case
					where
						(order_status = 'COLLECTION_PAIDOFF'
						or order_status = 'COLLECTION')
						and app_id not in ('Credits',
						'KASANDAAI'))t
				where
					num = 1) t2 on
				t1.id = t2.order_id
			where
				effective_date is not null
				and loan_status not in ('DENIED',
				'RESCIND',
				'APPROVING',
				'CREATED'))aa
	where
		effective_date >= current_date-21
		and effective_date <= current_date-15
	group by
		effective_date)u
union
select
	'合计' as date,
	new_cnt_num,
	old_cnt_num,
	total_cnt_num,
	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd1 / new_cnt_num, 2), '%') end as new_dpd1_rate,
	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd1 / old_cnt_num, 2), '%') end as old_dpd1_rate,
	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd1 / total_cnt_num, 2), '%') end as total_dpd1_rate,
	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd3 / new_cnt_num, 2), '%') end as new_dpd3_rate,
	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd3 / old_cnt_num, 2), '%') end as old_dpd3_rate,
	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd3 / total_cnt_num, 2), '%') end as total_dpd3_rate
from
	(
	select
		count(distinct case when return_flag = 'false' then loan_id else null end) as new_cnt_num,
		count(distinct case when return_flag = 'true' then loan_id else null end) as old_cnt_num,
		count(distinct loan_id) as total_cnt_num,
		count(distinct case when return_flag = 'false' and effective_date <= current_date-18 then loan_id else null end) as new_cnt_num3,
		count(distinct case when return_flag = 'true' and effective_date <= current_date-18 then loan_id else null end) as old_cnt_num3,
		count(distinct case when effective_date <= current_date-18 then loan_id else null end) as total_cnt_num3,
		count(distinct case when late_date-effective_date = approved_period and return_flag = 'false' then loan_id end) as new_dpd1,
		count(distinct case when late_date-effective_date = approved_period and return_flag = 'true' then loan_id end) as old_dpd1,
		count(distinct case when late_date-effective_date = approved_period then loan_id end) as total_dpd1,
		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'false' then loan_id end) as new_dpd3,
		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'true' then loan_id end) as old_dpd3,
		count(distinct case when DPD >= 3 and effective_date <= current_date-18 then loan_id end) as total_dpd3
	from
		(
		select
			t1.id loan_id,
			effective_date,
			return_flag,
			loan_status,
			approved_period,
			late_date,
			case
				when loan_status = 'COLLECTION' then current_date-late_date
				else round(late_fee /(approved_principal*0.025))::int end as DPD,
				t1.approved_principal
			from
				(
				select
					*
				from
					dw_gocash_go_cash_loan_gocash_core_loan
				where
					effective_date is not null
					and effective_date >= '2019-01-01'
                    and approved_period in (8,15)
					and loan_status not in ('DENIED',
					'RESCIND',
					'APPROVING',
					'CREATED')) t1
			left join (
				select
					*
				from
					(
					select
						order_id,
						late_date,
						row_number() over(partition by order_id
					order by
						late_date asc) as num
					from
						dw_gocash_gocash_collection_col_case
					where
						(order_status = 'COLLECTION_PAIDOFF'
						or order_status = 'COLLECTION')
						and app_id not in ('Credits',
						'KASANDAAI'))t
				where
					num = 1) t2 on
				t1.id = t2.order_id
			where
				effective_date is not null
				and loan_status not in ('DENIED',
				'RESCIND',
				'APPROVING',
				'CREATED'))aa
	where
		effective_date >= current_date-21
		and effective_date <= current_date-15)u
order by
	date
"""

try:
    rucui_fangkuan_df1 = get_df_from_pg(RUCUI_FANGKUAN_SQL)
    rucui_fangkuan_df1.head()
    rucui_fangkuan_df1.columns
    rucui_fangkuan_df1.rename(columns={'date':'放款日期','new_cnt_num':'新客户','old_cnt_num':'老客户'\
        ,'total_cnt_num':'整体','new_dpd1_rate':'新客户','old_dpd1_rate':'老客户','total_dpd1_rate':'整体'\
        ,'new_dpd3_rate':'新客户','old_dpd3_rate':'老客户','total_dpd3_rate':'整体'},inplace=True)
    the_col = [('','放款日期'),('放款件数','新客户'),('放款件数','老客户'),('放款件数','整体'),('首逾','新客户')\
        ,('首逾','老客户'),('首逾','整体'),('DPD3','新客户'),('DPD3','老客户'),('DPD3','整体')]
    rucui_fangkuan_df1.columns=pd.MultiIndex.from_tuples(the_col)
except:
    rucui_fangkuan_df1 = pd.DataFrame()
print('rucui_fangkuan_df1')

# """3.2.入催（放款日22天）情况--------------------------------------------------------------------------"""
# RUCUI_FANGKUAN_SQL = """
# select
# 	to_char(effective_date, 'YYYY-MM-DD') as date,
# 	new_cnt_num,
# 	old_cnt_num,
# 	total_cnt_num,
# 	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd1 / new_cnt_num, 2), '%') end as new_dpd1_rate,
# 	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd1 / old_cnt_num, 2), '%') end as old_dpd1_rate,
# 	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd1 / total_cnt_num, 2), '%') end as total_dpd1_rate,
# 	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd3 / new_cnt_num, 2), '%') end as new_dpd3_rate,
# 	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd3 / old_cnt_num, 2), '%') end as old_dpd3_rate,
# 	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd3 / total_cnt_num, 2), '%') end as total_dpd3_rate
# from
# 	(
# 	select
# 		effective_date,
# 		count(distinct case when return_flag = 'false' then loan_id else null end) as new_cnt_num,
# 		count(distinct case when return_flag = 'true' then loan_id else null end) as old_cnt_num,
# 		count(distinct loan_id) as total_cnt_num,
# 		count(distinct case when late_date-effective_date = approved_period and return_flag = 'false' then loan_id end) as new_dpd1,
# 		count(distinct case when late_date-effective_date = approved_period and return_flag = 'true' then loan_id end) as old_dpd1,
# 		count(distinct case when late_date-effective_date = approved_period then loan_id end) as total_dpd1,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'false' then loan_id end) as new_dpd3,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'true' then loan_id end) as old_dpd3,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 then loan_id end) as total_dpd3
# 	from
# 		(
# 		select
# 			t1.id loan_id,
# 			effective_date,
# 			return_flag,
# 			loan_status,
# 			approved_period,
# 			late_date,
# 			case
# 				when loan_status = 'COLLECTION' then current_date-late_date
# 				else round(late_fee /(approved_principal*0.025))::int end as DPD,
# 				t1.approved_principal
# 			from
# 				(
# 				select
# 					*
# 				from
# 					dw_gocash_go_cash_loan_gocash_core_loan
# 				where
# 					effective_date is not null
# 					and effective_date >= '2019-01-01'
#                     and approved_period =22
# 					and loan_status not in ('DENIED',
# 					'RESCIND',
# 					'APPROVING',
# 					'CREATED')) t1
# 			left join (
# 				select
# 					*
# 				from
# 					(
# 					select
# 						order_id,
# 						late_date,
# 						row_number() over(partition by order_id
# 					order by
# 						late_date asc) as num
# 					from
# 						dw_gocash_gocash_collection_col_case
# 					where
# 						(order_status = 'COLLECTION_PAIDOFF'
# 						or order_status = 'COLLECTION')
# 						and app_id not in ('Credits',
# 						'KASANDAAI'))t
# 				where
# 					num = 1) t2 on
# 				t1.id = t2.order_id
# 			where
# 				effective_date is not null
# 				and loan_status not in ('DENIED',
# 				'RESCIND',
# 				'APPROVING',
# 				'CREATED'))aa
# 	where
# 		effective_date >= current_date-31
# 		and effective_date <= current_date-25
# 	group by
# 		effective_date)u
# union
# select
# 	'合计' as date,
# 	new_cnt_num,
# 	old_cnt_num,
# 	total_cnt_num,
# 	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd1 / new_cnt_num, 2), '%') end as new_dpd1_rate,
# 	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd1 / old_cnt_num, 2), '%') end as old_dpd1_rate,
# 	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd1 / total_cnt_num, 2), '%') end as total_dpd1_rate,
# 	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd3 / new_cnt_num, 2), '%') end as new_dpd3_rate,
# 	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd3 / old_cnt_num, 2), '%') end as old_dpd3_rate,
# 	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd3 / total_cnt_num, 2), '%') end as total_dpd3_rate
# from
# 	(
# 	select
# 		count(distinct case when return_flag = 'false' then loan_id else null end) as new_cnt_num,
# 		count(distinct case when return_flag = 'true' then loan_id else null end) as old_cnt_num,
# 		count(distinct loan_id) as total_cnt_num,
# 		count(distinct case when return_flag = 'false' and effective_date <= current_date-18 then loan_id else null end) as new_cnt_num3,
# 		count(distinct case when return_flag = 'true' and effective_date <= current_date-18 then loan_id else null end) as old_cnt_num3,
# 		count(distinct case when effective_date <= current_date-18 then loan_id else null end) as total_cnt_num3,
# 		count(distinct case when late_date-effective_date = approved_period and return_flag = 'false' then loan_id end) as new_dpd1,
# 		count(distinct case when late_date-effective_date = approved_period and return_flag = 'true' then loan_id end) as old_dpd1,
# 		count(distinct case when late_date-effective_date = approved_period then loan_id end) as total_dpd1,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'false' then loan_id end) as new_dpd3,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'true' then loan_id end) as old_dpd3,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 then loan_id end) as total_dpd3
# 	from
# 		(
# 		select
# 			t1.id loan_id,
# 			effective_date,
# 			return_flag,
# 			loan_status,
# 			approved_period,
# 			late_date,
# 			case
# 				when loan_status = 'COLLECTION' then current_date-late_date
# 				else round(late_fee /(approved_principal*0.025))::int end as DPD,
# 				t1.approved_principal
# 			from
# 				(
# 				select
# 					*
# 				from
# 					dw_gocash_go_cash_loan_gocash_core_loan
# 				where
# 					effective_date is not null
# 					and effective_date >= '2019-01-01'
#                     and approved_period =22
# 					and loan_status not in ('DENIED',
# 					'RESCIND',
# 					'APPROVING',
# 					'CREATED')) t1
# 			left join (
# 				select
# 					*
# 				from
# 					(
# 					select
# 						order_id,
# 						late_date,
# 						row_number() over(partition by order_id
# 					order by
# 						late_date asc) as num
# 					from
# 						dw_gocash_gocash_collection_col_case
# 					where
# 						(order_status = 'COLLECTION_PAIDOFF'
# 						or order_status = 'COLLECTION')
# 						and app_id not in ('Credits',
# 						'KASANDAAI'))t
# 				where
# 					num = 1) t2 on
# 				t1.id = t2.order_id
# 			where
# 				effective_date is not null
# 				and loan_status not in ('DENIED',
# 				'RESCIND',
# 				'APPROVING',
# 				'CREATED'))aa
# 	where
# 		effective_date >= current_date-31
# 		and effective_date <= current_date-25)u
# order by
# 	date
# """
#
# try:
#     rucui_fangkuan_df2 = get_df_from_pg(RUCUI_FANGKUAN_SQL)
#     rucui_fangkuan_df2.head()
#     rucui_fangkuan_df2.columns
#     rucui_fangkuan_df2.rename(columns={'date':'放款日期','new_cnt_num':'新客户','old_cnt_num':'老客户'\
#         ,'total_cnt_num':'整体','new_dpd1_rate':'新客户','old_dpd1_rate':'老客户','total_dpd1_rate':'整体'\
#         ,'new_dpd3_rate':'新客户','old_dpd3_rate':'老客户','total_dpd3_rate':'整体'},inplace=True)
#     the_col = [('','放款日期'),('放款件数','新客户'),('放款件数','老客户'),('放款件数','整体'),('首逾','新客户')\
#         ,('首逾','老客户'),('首逾','整体'),('DPD3','新客户'),('DPD3','老客户'),('DPD3','整体')]
#     rucui_fangkuan_df2.columns=pd.MultiIndex.from_tuples(the_col)
# except:
#     rucui_fangkuan_df2 = pd.DataFrame()
#
#
# """3.3.入催（放款日29天）情况--------------------------------------------------------------------------"""
# RUCUI_FANGKUAN_SQL = """
# select
# 	to_char(effective_date, 'YYYY-MM-DD') as date,
# 	new_cnt_num,
# 	old_cnt_num,
# 	total_cnt_num,
# 	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd1 / new_cnt_num, 2), '%') end as new_dpd1_rate,
# 	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd1 / old_cnt_num, 2), '%') end as old_dpd1_rate,
# 	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd1 / total_cnt_num, 2), '%') end as total_dpd1_rate,
# 	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd3 / new_cnt_num, 2), '%') end as new_dpd3_rate,
# 	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd3 / old_cnt_num, 2), '%') end as old_dpd3_rate,
# 	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd3 / total_cnt_num, 2), '%') end as total_dpd3_rate
# from
# 	(
# 	select
# 		effective_date,
# 		count(distinct case when return_flag = 'false' then loan_id else null end) as new_cnt_num,
# 		count(distinct case when return_flag = 'true' then loan_id else null end) as old_cnt_num,
# 		count(distinct loan_id) as total_cnt_num,
# 		count(distinct case when late_date-effective_date = approved_period and return_flag = 'false' then loan_id end) as new_dpd1,
# 		count(distinct case when late_date-effective_date = approved_period and return_flag = 'true' then loan_id end) as old_dpd1,
# 		count(distinct case when late_date-effective_date = approved_period then loan_id end) as total_dpd1,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'false' then loan_id end) as new_dpd3,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'true' then loan_id end) as old_dpd3,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 then loan_id end) as total_dpd3
# 	from
# 		(
# 		select
# 			t1.id loan_id,
# 			effective_date,
# 			return_flag,
# 			loan_status,
# 			approved_period,
# 			late_date,
# 			case
# 				when loan_status = 'COLLECTION' then current_date-late_date
# 				else round(late_fee /(approved_principal*0.025))::int end as DPD,
# 				t1.approved_principal
# 			from
# 				(
# 				select
# 					*
# 				from
# 					dw_gocash_go_cash_loan_gocash_core_loan
# 				where
# 					effective_date is not null
# 					and effective_date >= '2019-01-01'
#                     and approved_period=29
# 					and loan_status not in ('DENIED',
# 					'RESCIND',
# 					'APPROVING',
# 					'CREATED')) t1
# 			left join (
# 				select
# 					*
# 				from
# 					(
# 					select
# 						order_id,
# 						late_date,
# 						row_number() over(partition by order_id
# 					order by
# 						late_date asc) as num
# 					from
# 						dw_gocash_gocash_collection_col_case
# 					where
# 						(order_status = 'COLLECTION_PAIDOFF'
# 						or order_status = 'COLLECTION')
# 						and app_id not in ('Credits',
# 						'KASANDAAI'))t
# 				where
# 					num = 1) t2 on
# 				t1.id = t2.order_id
# 			where
# 				effective_date is not null
# 				and loan_status not in ('DENIED',
# 				'RESCIND',
# 				'APPROVING',
# 				'CREATED'))aa
# 	where
# 		effective_date >= current_date-38
# 		and effective_date <= current_date-32
# 	group by
# 		effective_date)u
# union
# select
# 	'合计' as date,
# 	new_cnt_num,
# 	old_cnt_num,
# 	total_cnt_num,
# 	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd1 / new_cnt_num, 2), '%') end as new_dpd1_rate,
# 	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd1 / old_cnt_num, 2), '%') end as old_dpd1_rate,
# 	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd1 / total_cnt_num, 2), '%') end as total_dpd1_rate,
# 	case when new_cnt_num=0 then '-' else concat(round(100.00 * new_dpd3 / new_cnt_num, 2), '%') end as new_dpd3_rate,
# 	case when old_cnt_num=0 then '-' else concat(round(100.00 * old_dpd3 / old_cnt_num, 2), '%') end as old_dpd3_rate,
# 	case when total_cnt_num=0 then '-' else concat(round(100.00 * total_dpd3 / total_cnt_num, 2), '%') end as total_dpd3_rate
# from
# 	(
# 	select
# 		count(distinct case when return_flag = 'false' then loan_id else null end) as new_cnt_num,
# 		count(distinct case when return_flag = 'true' then loan_id else null end) as old_cnt_num,
# 		count(distinct loan_id) as total_cnt_num,
# 		count(distinct case when return_flag = 'false' and effective_date <= current_date-18 then loan_id else null end) as new_cnt_num3,
# 		count(distinct case when return_flag = 'true' and effective_date <= current_date-18 then loan_id else null end) as old_cnt_num3,
# 		count(distinct case when effective_date <= current_date-18 then loan_id else null end) as total_cnt_num3,
# 		count(distinct case when late_date-effective_date = approved_period and return_flag = 'false' then loan_id end) as new_dpd1,
# 		count(distinct case when late_date-effective_date = approved_period and return_flag = 'true' then loan_id end) as old_dpd1,
# 		count(distinct case when late_date-effective_date = approved_period then loan_id end) as total_dpd1,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'false' then loan_id end) as new_dpd3,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 and return_flag = 'true' then loan_id end) as old_dpd3,
# 		count(distinct case when DPD >= 3 and effective_date <= current_date-18 then loan_id end) as total_dpd3
# 	from
# 		(
# 		select
# 			t1.id loan_id,
# 			effective_date,
# 			return_flag,
# 			loan_status,
# 			approved_period,
# 			late_date,
# 			case
# 				when loan_status = 'COLLECTION' then current_date-late_date
# 				else round(late_fee /(approved_principal*0.025))::int end as DPD,
# 				t1.approved_principal
# 			from
# 				(
# 				select
# 					*
# 				from
# 					dw_gocash_go_cash_loan_gocash_core_loan
# 				where
# 					effective_date is not null
# 					and effective_date >= '2019-01-01'
#                     and approved_period=29
# 					and loan_status not in ('DENIED',
# 					'RESCIND',
# 					'APPROVING',
# 					'CREATED')) t1
# 			left join (
# 				select
# 					*
# 				from
# 					(
# 					select
# 						order_id,
# 						late_date,
# 						row_number() over(partition by order_id
# 					order by
# 						late_date asc) as num
# 					from
# 						dw_gocash_gocash_collection_col_case
# 					where
# 						(order_status = 'COLLECTION_PAIDOFF'
# 						or order_status = 'COLLECTION')
# 						and app_id not in ('Credits',
# 						'KASANDAAI'))t
# 				where
# 					num = 1) t2 on
# 				t1.id = t2.order_id
# 			where
# 				effective_date is not null
# 				and loan_status not in ('DENIED',
# 				'RESCIND',
# 				'APPROVING',
# 				'CREATED'))aa
# 	where
# 		effective_date >= current_date-38
# 		and effective_date <= current_date-32)u
# order by
# 	date
# """
#
# try:
#     rucui_fangkuan_df3 = get_df_from_pg(RUCUI_FANGKUAN_SQL)
#     rucui_fangkuan_df3.head()
#     rucui_fangkuan_df3.columns
#     rucui_fangkuan_df3.rename(columns={'date':'放款日期','new_cnt_num':'新客户','old_cnt_num':'老客户'\
#         ,'total_cnt_num':'整体','new_dpd1_rate':'新客户','old_dpd1_rate':'老客户','total_dpd1_rate':'整体'\
#         ,'new_dpd3_rate':'新客户','old_dpd3_rate':'老客户','total_dpd3_rate':'整体'},inplace=True)
#     the_col = [('','放款日期'),('放款件数','新客户'),('放款件数','老客户'),('放款件数','整体'),('首逾','新客户')\
#         ,('首逾','老客户'),('首逾','整体'),('DPD3','新客户'),('DPD3','老客户'),('DPD3','整体')]
#     rucui_fangkuan_df3.columns=pd.MultiIndex.from_tuples(the_col)
# except:
#     rucui_fangkuan_df3 = pd.DataFrame()


"""4.催收（案件）情况--------------------------------------------------------------------------"""
CHUCUI_ANJIAN_SQL="""
set enable_nestloop=off;

SELECT
to_char(u.late_date, 'YYYY-MM-DD') as date,
case_num,
concat(round(100.00 * late1 / case_num, 2), '%') as late1_case,
concat(round(100.00 * late2 / case_num, 2), '%') as late2_case,
concat(round(100.00 * late3 / case_num, 2), '%') as late3_case,
concat(round(100.00 * late4 / case_num, 2), '%') as late4_case,
concat(round(100.00 * late5 / case_num, 2), '%') as late5_case,
concat(round(100.00 * late6 / case_num, 2), '%') as late6_case,
concat(round(100.00 * late7 / case_num, 2), '%') as late7_case
from
(
select
t1.late_date late_date,
count(distinct t1.order_id)::int as case_num,
round(sum(approved_principal)/ 2000) as prin_sum
from
(
select
late_date,
id,
order_id,
approved_principal,
row_number() over(partition by order_id
order by
late_date desc) as num
from
dw_gocash_gocash_collection_col_case
where
order_status != 'ABNORMAL'
and order_status != 'PAIDOFF'
and app_id not in ('Credits','KASANDAAI'))t1
where
t1.late_date >= current_date-7
and t1.late_date<current_date + interval '0 day'
and num = 1
group by
late_date
order by
late_date asc)u
left join (
select
t1.late_date late_date,
count(case when t2.late_day = 1 then 1 else null end ) as late1,
count(case when t2.late_day<3 then 1 else null end ) as late2,
count(case when t2.late_day<4 then 1 else null end ) as late3,
count(case when t2.late_day<5 then 1 else null end ) as late4,
count(case when t2.late_day<6 then 1 else null end ) as late5,
count(case when t2.late_day<7 then 1 else null end ) as late6,
count(case when t2.late_day<8 then 1 else null end ) as late7,
count(*) as late,
sum(t2.refund_amount)/ 2000 as sum
from
(
select
late_date,
id,
order_id,
approved_principal,
row_number() over(partition by order_id
order by
late_date desc) as num
from
dw_gocash_gocash_collection_col_case
where
order_status != 'ABNORMAL'
and order_status != 'PAIDOFF'
and app_id not in ('Credits','KASANDAAI'))t1
inner join public.dw_gocash_gocash_collection_col_repayment t2 on
t1.id = t2.case_id
where
t1.late_date >= current_date-7
and t1.late_date < current_date + interval '0 day'
and num = 1
group by
t1.late_date
order by
late_date asc)v on
u.late_date = v.late_date
union
select
'合计' as date,
sum(case_num) as case_num,
concat(round(100.00 * sum(late1) / sum(case_num), 2), '%') as late1_case,
concat(round(100.00 * sum(late2) / sum(case_num), 2), '%') as late2_case,
concat(round(100.00 * sum(late3) / sum(case_num), 2), '%') as late3_case,
concat(round(100.00 * sum(late4) / sum(case_num), 2), '%') as late4_case,
concat(round(100.00 * sum(late5) / sum(case_num), 2), '%') as late5_case,
concat(round(100.00 * sum(late6) / sum(case_num), 2), '%') as late6_case,
concat(round(100.00 * sum(late7) / sum(case_num), 2), '%') as late7_case
from
(
select
t1.late_date late_date,
count(distinct t1.order_id)::int as case_num,
round(sum(approved_principal)/ 2000) as prin_sum
from
(
select
late_date,
id,
order_id,
approved_principal,
row_number() over(partition by order_id
order by
late_date desc) as num
from
dw_gocash_gocash_collection_col_case
where
order_status != 'ABNORMAL'
and order_status != 'PAIDOFF'
and app_id not in ('Credits','KASANDAAI'))t1
where
t1.late_date >= current_date-7
and t1.late_date<current_date + interval '0 day'
and num = 1
group by
late_date
order by
late_date asc)u
left join (
select
t1.late_date late_date,
count(case when t2.late_day = 1 then 1 else null end ) as late1,
count(case when t2.late_day<3 then 1 else null end ) as late2,
count(case when t2.late_day<4 then 1 else null end ) as late3,
count(case when t2.late_day<5 then 1 else null end ) as late4,
count(case when t2.late_day<6 then 1 else null end ) as late5,
count(case when t2.late_day<7 then 1 else null end ) as late6,
count(case when t2.late_day<8 then 1 else null end ) as late7,
count(*) as late,
sum(t2.refund_amount)/ 2000 as sum
from
(
select
late_date,
id,
order_id,
approved_principal,
row_number() over(partition by order_id
order by
late_date desc) as num
from
dw_gocash_gocash_collection_col_case
where
order_status != 'ABNORMAL'
and order_status != 'PAIDOFF'
and app_id not in ('Credits','KASANDAAI'))t1
inner join dw_gocash_gocash_collection_col_repayment t2 on
t1.id = t2.case_id
where
t1.late_date >= current_date-7
and t1.late_date < current_date + interval '0 day'
and num = 1
group by
t1.late_date
order by
late_date asc)v on
u.late_date = v.late_date
order by
date
"""

try:
    chucui_anjian_df = get_df_from_pg(CHUCUI_ANJIAN_SQL)
    chucui_anjian_df.head()
    chucui_anjian_df.rename(columns={'date':'入催日期','case_num':'案件总量','late1_case':'第一天回款'\
        ,'late2_case':'第二天回款','late3_case':'第三天回款','late4_case':'第四天回款','late5_case':'第五天回款'\
        ,'late6_case':'第六天回款','late7_case':'第七天回款'},inplace=True)
except:
    chucui_anjian_df = pd.DataFrame()
print('chucui_anjian_df')

"""5.催收（金额）情况--------------------------------------------------------------------------"""
CHUCUI_JINE_SQL = """
set enable_nestloop=off;

SELECT
to_char(u.late_date, 'YYYY-MM-DD') as date,
prin_sum,
concat(round((100 * sum1 / prin_sum)::decimal, 2), '%') as sum1_prin,
concat(round((100 * sum2 / prin_sum)::decimal, 2), '%') as sum2_prin,
concat(round((100 * sum3 / prin_sum)::decimal, 2), '%') as sum3_prin,
concat(round((100 * sum4 / prin_sum)::decimal, 2), '%') as sum4_prin,
concat(round((100 * sum5 / prin_sum)::decimal, 2), '%') as sum5_prin,
concat(round((100 * sum6 / prin_sum)::decimal, 2), '%') as sum6_prin,
concat(round((100 * sum7 / prin_sum)::decimal, 2), '%') as sum7_prin
from
(
select
t1.late_date late_date,
count(distinct t1.order_id) as case_num,
round(sum(approved_principal)/ 2000)::int as prin_sum
from
(
select
late_date,
id,
order_id,
approved_principal,
row_number() over(partition by order_id
order by
late_date desc) as num
from
dw_gocash_gocash_collection_col_case
where
order_status != 'ABNORMAL'
and order_status != 'PAIDOFF'
and app_id not in ('Credits','KASANDAAI'))t1
where
t1.late_date >= current_date-7
and t1.late_date<current_date + interval '0 day'
and num = 1
group by
late_date
order by
late_date asc)u
left join (
select
t1.late_date late_date,
count(*) as late,
sum(case when t2.late_day = 1 then t2.refund_amount else 0 end )/ 2000 as sum1,
sum(case when t2.late_day<3 then t2.refund_amount else 0 end )/ 2000 as sum2,
sum(case when t2.late_day<4 then t2.refund_amount else 0 end )/ 2000 as sum3,
sum(case when t2.late_day<5 then t2.refund_amount else 0 end )/ 2000 as sum4,
sum(case when t2.late_day<6 then t2.refund_amount else 0 end )/ 2000 as sum5,
sum(case when t2.late_day<7 then t2.refund_amount else 0 end )/ 2000 as sum6,
sum(case when t2.late_day<8 then t2.refund_amount else 0 end )/ 2000 as sum7,
sum(t2.refund_amount)/ 2000 as sum
from
(
select
late_date,
id,
order_id,
approved_principal,
row_number() over(partition by order_id
order by
late_date desc) as num
from
dw_gocash_gocash_collection_col_case
where
order_status != 'ABNORMAL'
and order_status != 'PAIDOFF'
and app_id not in ('Credits','KASANDAAI'))t1
inner join dw_gocash_gocash_collection_col_repayment t2 on
t1.id = t2.case_id
where
t1.late_date >= current_date-7
and t1.late_date < current_date + interval '0 day'
and num = 1
group by
t1.late_date
order by
late_date asc)v on
u.late_date = v.late_date
union
select
'合计' as date,
sum(prin_sum) as prin_sum,
concat(round((100 * sum(sum1) / sum(prin_sum))::decimal, 2), '%') as sum1_prin,
concat(round((100 * sum(sum2) / sum(prin_sum))::decimal, 2), '%') as sum2_prin,
concat(round((100 * sum(sum3) / sum(prin_sum))::decimal, 2), '%') as sum3_prin,
concat(round((100 * sum(sum4)/ sum(prin_sum))::decimal, 2), '%') as sum4_prin,
concat(round((100 * sum(sum5) / sum(prin_sum))::decimal, 2), '%') as sum5_prin,
concat(round((100 * sum(sum6) / sum(prin_sum))::decimal, 2), '%') as sum6_prin,
concat(round((100 * sum(sum7) / sum(prin_sum))::decimal, 2), '%') as sum7_prin
from
(
select
t1.late_date late_date,
count(distinct t1.order_id) as case_num,
round(sum(approved_principal)/ 2000)::int as prin_sum
from
(
select
late_date,
id,
order_id,
approved_principal,
row_number() over(partition by order_id
order by
late_date desc) as num
from
dw_gocash_gocash_collection_col_case
where
order_status != 'ABNORMAL'
and order_status != 'PAIDOFF'
and app_id not in ('Credits','KASANDAAI'))t1
where
t1.late_date >= current_date-7
and t1.late_date<current_date + interval '0 day'
and num = 1
group by
late_date
order by
late_date asc)u
left join (
select
t1.late_date late_date,
count(*) as late,
sum(case when t2.late_day = 1 then t2.refund_amount else 0 end )/ 2000 as sum1,
sum(case when t2.late_day<3 then t2.refund_amount else 0 end )/ 2000 as sum2,
sum(case when t2.late_day<4 then t2.refund_amount else 0 end )/ 2000 as sum3,
sum(case when t2.late_day<5 then t2.refund_amount else 0 end )/ 2000 as sum4,
sum(case when t2.late_day<6 then t2.refund_amount else 0 end )/ 2000 as sum5,
sum(case when t2.late_day<7 then t2.refund_amount else 0 end )/ 2000 as sum6,
sum(case when t2.late_day<8 then t2.refund_amount else 0 end )/ 2000 as sum7,
sum(t2.refund_amount)/ 2000 as sum
from
(
select
late_date,
id,
order_id,
approved_principal,
row_number() over(partition by order_id
order by
late_date desc) as num
from
dw_gocash_gocash_collection_col_case
where
order_status != 'ABNORMAL'
and order_status != 'PAIDOFF'
and app_id not in ('Credits','KASANDAAI'))t1
inner join dw_gocash_gocash_collection_col_repayment t2 on
t1.id = t2.case_id
where
t1.late_date >= current_date-7
and t1.late_date < current_date + interval '0 day'
and num = 1
group by
t1.late_date
order by
late_date asc)v on
u.late_date = v.late_date
order by
date
"""

try:
    chucui_jine_df = get_df_from_pg(CHUCUI_JINE_SQL)
    chucui_jine_df.head()
    chucui_jine_df.rename(columns={'date':'入催日期','prin_sum':'入催金额','sum1_prin':'第一天回款','sum2_prin':'第二天回款'\
        ,'sum3_prin':'第三天回款','sum4_prin':'第四天回款','sum5_prin':'第五天回款','sum6_prin':'第六天回款'\
        ,'sum7_prin':'第七天回款'},inplace=True)
except:
    chucui_jine_df = pd.DataFrame()
print('chucui_jine_df')


if __name__ =="__main__":
    try:
        fangkuan_html = se.format_df(fangkuan_df)
        fangkuan_body = se.format_body(fangkuan_html,form_name='一、放款情况')
    except:
        fangkuan_body = '一、放款情况（数据问题，暂时无法呈现）'

    try:
        rucui_html = se.format_df(rucui_df)
        rucui_body = se.format_body(rucui_html,form_name='三、入催（到期日）情况')
    except:
        rucui_body = '三、入催（到期日）情况（数据问题，暂时无法呈现）'

    try:
        rucui_fangkuan1_html = se.format_df(rucui_fangkuan_df1)
        rucui_fangkuan1_body = se.format_body(rucui_fangkuan1_html,form_name='二(1)、8天&15天DPD（放款日）情况')
    except:
        rucui_fangkuan1_body = '二(1)、8天&15天DPD（放款日）情况（数据问题，暂时无法呈现）'

    # try:
    #     rucui_fangkuan2_html = se.format_df(rucui_fangkuan_df2)
    #     rucui_fangkuan2_body = se.format_body(rucui_fangkuan2_html,form_name='二(2)、22天DPD（放款日）情况')
    # except:
    #     rucui_fangkuan2_body = '二(2)、22天DPD（放款日）情况（数据问题，暂时无法呈现）'
    #
    # try:
    #     rucui_fangkuan3_html = se.format_df(rucui_fangkuan_df3)
    #     rucui_fangkuan3_body = se.format_body(rucui_fangkuan3_html,form_name='二(3)、29天DPD（放款日）情况')
    # except:
    #     rucui_fangkuan3_body = '二(3)、29天DPD（放款日）情况（数据问题，暂时无法呈现）'

    try:
        chucui_anjian_html = se.format_df(chucui_anjian_df)
        chucui_anjian_body = se.format_body(chucui_anjian_html,form_name='四、出催（案件）情况')
    except:
        chucui_anjian_body = '四、出催（案件）情况（数据问题，暂时无法呈现）'

    try:
        chucui_jine_html = se.format_df(chucui_jine_df)
        chucui_jine_body = se.format_body(chucui_jine_html,form_name='五、出催（金额）情况')
    except:
        chucui_jine_body = '五、出催（金额）情况（数据问题，暂时无法呈现）'


    if send_email_elart == 1:
        mailtest(["riskcontrol-business@huojintech.com"], ["jiangshanjiao@mintechai.com"], \
        '【印尼风控数据每日监测——底层数据问题_%s】'%pd.to_datetime('today').strftime('%Y-%m-%d'), 'select * 检查\n\nloan表：%d条\ncol表：%d条\npayflow表：%d条\nrepayment表：%d条'%(loan_check.shape[0],col_check.shape[0],payflow_check.shape[0],repay_check.shape[0]))
    else:
        None

    html_msg= "<html>" + fangkuan_body + rucui_fangkuan1_body + rucui_body + chucui_anjian_body + chucui_jine_body + "</html>"
    # + rucui_fangkuan2_body + rucui_fangkuan3_body
    print('sending email')
    mailtest(["riskcontrol-business@huojintech.com"],["riskcontrol@huojintech.com"], \
    '【印尼风控数据每日监测_%s】'%pd.to_datetime('today').strftime('%Y-%m-%d'), "测试发送html", html_msg=html_msg)

