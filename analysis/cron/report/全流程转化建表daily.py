# coding: utf-8
"""
Python 3.6.0
全流程转化建表daily
"""
import sys
import os

#sys.path.append('C:/workspace/repos/genie')
sys.path.append('/home/ops/repos/newgenie/')

import os.path

import numpy as np
import pandas as pd

from functools import reduce
from jinja2 import Template


from utils3.data_io_utils import get_df_from_pg
from utils3.data_io_utils import upload_df_to_pg
from utils3.send_email import mailtest

QLCZHSQL = """
select
	to_char(reg_time::date,'YYYY-MM-DD') as reg_date ,
	date_trunc('week', reg_time)::date || '-' || (date_trunc('week', reg_time::timestamp)+ '6 days'::interval)::date as reg_week ,
	to_char(reg_time, 'YYYY-MM') as reg_month ,
	case when return_flag is null then 'none' else return_flag end as return_flag,
	count(distinct case when reg_time ::date> '2000-01-01' then customer_id end) as register ,
	count(distinct case when reg_time ::date> '2000-01-01' and status = '0' then customer_id end) as register_success ,
	count(distinct case when login_time ::date> '2000-01-01' then customer_id end) as login ,
	count(distinct case when profession_time ::date> '2000-01-01' and id_card_no is not null and id_card_name is not null and id_card_address is not null and marital_status is not null and religion is not null and education is not null then customer_id end) as basicandident_info ,
	count(distinct case when refe_time ::date> '2000-01-01' then customer_id end) as reference ,
	count(distinct case when account_time ::date> '2000-01-01' then customer_id end) as account ,
    count(distinct case when createtime ::date> '2000-01-01' then customer_id end) as RC1 ,
	count(distinct case when createtime ::date> '2000-01-01' and risklevelfirst in ('N', 'P') and rk = 1 then customer_id end) as RC1_pass ,
	count(distinct case when app_time::date> '2000-01-01' and app_time::date<(case when rk = 1 then createtime::date else '2000-01-01' end)+ interval '30 days' and photo_result is not null and photo_result <> '' and risklevelfirst in ('N', 'P') and rk = 1 then customer_id end) as photo,
	count(distinct case when app_time::date> '2000-01-01' and app_time::date<(case when rk = 1 then createtime::date else '2000-01-01' end)+ interval '30 days' and photo_result = 'ENABLE' and photo_result <> '' and risklevelfirst in ('N', 'P') and rk = 1 then customer_id end) as photo_pass,
	count(distinct case when app_time::date> '2000-01-01' and app_time::date<(case when rk = 1 then createtime::date else '2000-01-01' end)+ interval '30 days' and bank_card_result is not null and bank_card_result <> '' and risklevelfirst in ('N', 'P') and rk = 1 then customer_id end) as bank ,
	count(distinct case when app_time::date> '2000-01-01' and app_time::date<(case when rk = 1 then createtime::date else '2000-01-01' end)+ interval '30 days' and bank_card_result in('BIND', 'BINDING') and risklevelfirst in ('N', 'P') and bank_card_result <> '' and rk = 1 then customer_id end) as bank_pass ,
	count(distinct case when app_time::date> '2000-01-01' and app_time::date<(case when rk = 1 then createtime::date else '2000-01-01' end)+ interval '30 days' and bank_card_result in('BIND', 'BINDING') and photo_result = 'ENABLE' and risklevelfirst in ('N', 'P') and rk = 1 then customer_id end) as bankandphoto_pass ,
	count(distinct case when risklevelfirst in ('N', 'P') and rk = 1 and risklevelsecond is not null and risklevelsecond <> '' then customer_id end) as rc2 ,
	count(distinct case when risklevelfirst in ('N', 'P') and rk = 1 and risklevelsecond is not null and risklevelsecond <> '' and risklevelsecond in ('N', 'P') then customer_id end) as rc2pass ,
	count(distinct case when risklevelfirst in ('N', 'P') and rk = 1 and risklevelsecond in ('N', 'P') and human_trial = 'HUMAN_TRIAL' then customer_id end) as audit2,
	count(distinct case when risklevelfirst in ('N', 'P') and rk = 1 and risklevelsecond in ('N', 'P') and human_trial = 'HUMAN_TRIAL' and loan_status not in ('DENIED', 'RESCIND', 'APPROVING', 'CREATED') then customer_id end) as audit2pass,
	count(distinct case when effective_date> date('2000-01-01') and loan_status not in ('DENIED', 'RESCIND', 'APPROVING', 'CREATED') then customer_id else null end)as fangkuan
from
	(with reg as (
	select
		id as customer_id,
		device ,
		create_time as reg_time ,
		app_version,
		id_card_no,
		id_card_name,
		id_card_address,
		marital_status,
		religion,
		education,
		status
	from
		public.dw_gocash_go_cash_loan_gocash_core_customer
	where
		create_time is not null
		and create_time::date >= '2019-01-01') ,
	login as (
	select
		customer_id ,
		min(login_time) as login_time
	from
		public.dw_gocash_go_cash_loan_t_gocash_core_customer_login_flow
	group by
		1 ) ,
	profession as (
	select
		customer_id ,
		create_time as profession_time
	from
		public.dw_gocash_go_cash_loan_gocash_core_cusomer_profession ) ,
	refe as (
	select
		customer_id ,
		create_time as refe_time
	from
		public.dw_gocash_go_cash_loan_gocash_core_customer_refer ) ,
	account as (
	select
		customer_id ,
		create_time as account_time
	from
		public.dw_gocash_go_cash_loan_gocash_core_customer_account ) ,
	app as (
	select
		customer_id ,
		id ,
		apply_time as app_time ,
		loan_status ,
		risk_rank ,
		approved_time,
		effective_date,
		photo_result,
		bank_card_result,
		loan_no,
		channel,
		human_trial,
		return_flag
	from
		public.dw_gocash_go_cash_loan_gocash_core_loan ),
	riskresult as (
	select
		loanid,
		createtime,
		rc2updatetime,
		risklevelsecond,
		risklevelfirst,
		customerid,
		row_number() over(partition by customerid
	order by
		createtime) rk
	from
		risk_gocash_mongo_riskcontrolresult)
	select
		reg.* ,
		login.login_time ,
		profession.profession_time ,
		refe.refe_time ,
		account.account_time ,
		app.app_time ,
		app.loan_status ,
		app.risk_rank ,
		app.approved_time,
		app.id,
		app.photo_result,
		app.bank_card_result,
		app.loan_no,
		app.channel,
		app.effective_date,
		app.human_trial,
		app.return_flag,
		riskresult.*
	from
		reg
	left join login on
		reg.customer_id = login.customer_id
	left join profession on
		reg.customer_id = profession.customer_id
	left join refe on
		reg.customer_id = refe.customer_id
	left join account on
		reg.customer_id = account.customer_id
	left join app on
		reg.customer_id = app.customer_id
	left join riskresult on
		reg.customer_id::varchar = riskresult.customerid) tmp
group by
	1,
	2,
	3,
	4
"""

qlc_rc = get_df_from_pg(QLCZHSQL)
#qlc_rc.head()

#每天先删除原来数据库数据
upload_df_to_pg("TRUNCATE TABLE quanliucheng_data")

#建表
CREATESQL = """
CREATE TABLE IF NOT EXISTS quanliucheng_data(
     reg_date     varchar
     ,reg_week    varchar
     ,reg_month         varchar
     ,return_flag           varchar
     ,register           int
     ,register_success      int
     ,login    int
     ,basicandident_info           int
     ,reference      int
     ,account      int
     ,RC1      int
     ,RC1_pass      int
     ,photo      int
     ,photo_pass      int
     ,bank      int
     ,bank_pass      int
     ,bankandphoto_pass      int
     ,rc2      int
     ,rc2pass     int
     ,audit2     int
     ,audit2pass    int
     ,fangkuan   int
 )
 """
upload_df_to_pg(CREATESQL)

INSERTSQL = """
INSERT INTO quanliucheng_data
values
{% for i in var_list %}
{{ i }},
{% endfor %}
"""

def upload_to_risk_rule_data(df):
    var_list =[]
    for index, row in df.iterrows():
        var_list.append(tuple(row))
    num_lines = 5000
    upload_status = {}
    for line in range(0, len(var_list), num_lines):
        # print(line)
        insert_sql = Template(INSERTSQL).render(var_list=var_list[line:line+num_lines])[:-2]
        status = upload_df_to_pg(insert_sql)

upload_to_risk_rule_data(qlc_rc)
