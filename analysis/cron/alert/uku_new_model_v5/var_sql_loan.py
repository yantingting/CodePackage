#encoding=utf-8
VAR_SQL = """
set enable_nestloop=off;

with score as (
SELECT orderno, customerid,createtime
,outputmap::json->'newUserModelScoreV5' as newusermodelscorev5
,outputmap::json->'newUserModelScoreV5InputModelParams' as newusermodelscorev5inputmodelparams
FROM rt_risk_mongo_gocash_riskreport r
WHERE ruleresultname in('newUserModelScoreV5')
and outputmap<>''
and businessid='uku'
and createtime::date between '{{start_date}}'::date - interval '1 D' and '{{end_date[0:10]}}'
),
loan as (
select id::varchar as loanid,effective_date,customer_id::text
from dw_gocash_go_cash_loan_gocash_core_loan
where effective_date between '{{start_date[0:10]}}' and '{{end_date[0:10]}}'
and (return_flag ='false' or return_flag is null)
and product_id=1
and device_approval='ANDROID'
)
select loanid, customerid, effective_date as createtime, newusermodelscorev5, newusermodelscorev5inputmodelparams
from 
(select loan.*,score.*
from loan
inner join score
on loan.loanid = score.orderno)t
"""

cash_SQL = """
set enable_nestloop=off;

with score as (
select id as loanid,orderno,customerid,createtime,ruleresult as score,datasources
from rt_risk_mongo_gocash_riskreport r
where ruleresultname ='newUserModelScoreV5'
and businessid='cashcash'
and createtime::date between '{{start_date}}'::date - interval '1 D' and '{{end_date[0:10]}}'
),
loan as (
select id::varchar as loan_id,effective_date,customer_id::text
from dw_gocash_go_cash_loan_gocash_core_loan
where effective_date between '{{start_date[0:10]}}' and '{{end_date[0:10]}}'
and (return_flag ='false' or return_flag is null)
and product_id=2
--and device_approval='API'
)
select loanid, customerid, createtime, score, datasources
from 
(select loan.*,score.*
from loan
inner join score
on loan.loan_id = score.orderno)t
"""




'''

VAR_SQL = """
with score as (
select id as loanid,orderno,customerid,createtime,ruleresult as score,datasources
from rt_risk_mongo_gocash_riskreport r
where ruleresultname ='newUserModelScoreV5'
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


VAR_SQL = """
with score as (
select loanid,customerid,createtime,newusermodelscorev5::float,newusermodelscorev5inputmodelparams
from(
SELECT a.loanid
    , a.customerid
    , case when a.createtime ~ '-' is false then to_timestamp(round(a.createtime::float/1000))
else a.createtime::timestamp end as createtime
    --, a.newusermodelscorev5
    , case when a.newusermodelscorev5 ~ '{' is true then a.newusermodelscorev5::json ->> 'newUserModelScoreV5' 
    else a.newusermodelscorev5 end as newusermodelscorev5
    , a.newusermodelscorev5inputmodelparams
FROM risk_mongo_gocash_installmentriskcontrolparams a
where a.newusermodelscorev5<>'' 
and a.businessid = 'uku'
--and a.pipelineid in('468','469','476','479','480')
)t
where createtime::date between '{{start_date}}'::date - interval '1 D' and '{{end_date[0:10]}}'
),
loan as (
select id::varchar as loan_id,effective_date
from dw_gocash_go_cash_loan_gocash_core_loan
where effective_date between '{{start_date[0:10]}}' and '{{end_date[0:10]}}'
and (return_flag ='false' or return_flag is null)
and product_id=1
and device_approval='ANDROID'
)
select loan.*,score.*
from loan
inner join score
on loan.loan_id = score.loanid
"""
'''



