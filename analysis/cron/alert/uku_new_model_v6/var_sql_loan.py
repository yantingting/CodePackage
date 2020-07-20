

VAR_SQL = """
set enable_nestloop=off;

with score as (
select id as loanid,orderno,customerid,createtime,ruleresult as score,datasources
from rt_risk_mongo_gocash_riskreport r
where ruleresultname ='newUserModelScoreV6'
and businessid='uku'
and createtime::date between '{{start_date}}'::date - interval '1 D' and '{{end_date[0:10]}}'
),
loan as (
select id::varchar as loan_id,effective_date,customer_id::text
from dw_gocash_go_cash_loan_gocash_core_loan
where effective_date between '{{start_date[0:10]}}' and '{{end_date[0:10]}}'
and (return_flag ='false' or return_flag is null)
and product_id=1
and device_approval='ANDROID'
)
select loanid, customerid, createtime, score, datasources
from 
(select loan.*,score.*
from loan
inner join score
on loan.loan_id = score.orderno)t
"""

