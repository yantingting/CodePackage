#encoding=utf-8

VAR_SQL="""
set enable_nestloop=off;

SELECT loanid, customerid,createtime,ruleresult as score ,datasources
FROM (SELECT id as loan_id
    FROM rt_t_gocash_core_loan
    WHERE return_flag = 'true' and apply_time between '{{start_date}}' and '{{end_date}}'
    and product_id = 1) t1
INNER JOIN (SELECT id as loanid, customerid,createtime,ruleresult,datasources, orderno
           FROM rt_risk_mongo_gocash_riskreport r
           WHERE ruleresultname in('oldUserModelV3Score') and businessid='uku' 
           and createtime between '{{start_date}}' and '{{end_date}}') t2
ON t1.loan_id :: varchar = t2.orderno
"""


PER_SQL = """
set enable_nestloop=off;
WITH perf as (
SELECT id as loan_id
    , customer_id
    , apply_time
    , approved_period
    , loan_status
    , due_date
    , paid_off_time
    , extend_times
    , effective_date
    , late_days
    , return_flag
    , current_date
    , case when late_days >= 1 then 1 else 0 end as everx
    , case when late_days >= 3 then 1 else 0 end as ever3
    , case when late_days >= 7 then 1 else 0 end as ever7  
    , case when current_date::Date-effective_date>=approved_period+6 then 1 else 0 end as due_f7
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date!='1970-01-01' and effective_date between cast('{{ start_date }}':: timestamp as date)  and cast('{{ end_date }}':: timestamp as date) 
and return_flag = 'true'
),
score as (
SELECT orderno
     , customerid
     , risklevel
     , pipelineid
     , ruleresultname
     , ruleresult as score
FROM rt_risk_mongo_gocash_riskreport
WHERE ruleresultname in ('oldUserModelV3Score')
)
SELECT *
FROM perf
INNER JOIN score on perf.loan_id :: varchar = score.orderno
WHERE due_f7 = 1
"""

