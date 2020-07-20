#encoding=utf-8
VAR_SQL='''
set enable_nestloop=off;

SELECT orderno as loanid, customerid,createtime
,outputmap::json->'newUserModelScoreV5' as score
,outputmap::json->'newUserModelScoreV5InputModelParams' as newusermodelscorev5inputmodelparams
FROM rt_risk_mongo_gocash_riskreport r
WHERE ruleresultname in('newUserModelScoreV5')
and businessid='{{businessid}}'
and createtime between '{{start_date}}' and '{{end_date}}'
and outputmap<>''
'''


PER_SQL = """
set enable_nestloop=off;

WITH perf as (
SELECT 
a.id as loanid, customer_id::varchar as customer_id,apply_time, effective_date, current_date as curr_date, paid_off_time, due_date, loan_status,
case when loan_status in ('COLLECTION', 'COLLECTION_PAIDOFF') then 1 
else 0 end as flag,
case when current_date::Date-effective_date>=approved_period-1 then 1
else 0 end as due_f,
case when current_date::Date-effective_date>=approved_period+2 then 1
else 0 end as due_f3,
case when current_date::Date-effective_date>=approved_period+6 then 1
else 0 end as due_f7,
case when extend_times>3 then 0 
when paid_off_time::Date-due_date>3 then 1 
when loan_status='COLLECTION' and current_date::Date-due_date<=3 then -3 
when loan_status='COLLECTION' and current_date::Date-due_date>3 then 1
when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
else 0 end as flag3,
case when extend_times>3 then 0 
when paid_off_time::Date-due_date>7 then 1 
when loan_status='COLLECTION' and current_date::Date-due_date<=7 then -3 
when loan_status='COLLECTION' and current_date::Date-due_date>7 then 1
when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
else 0 end as flag7
FROM dw_gocash_go_cash_loan_gocash_core_loan a 
WHERE return_flag = 'false'
and product_id=1 
and effective_date between cast('{{ start_date }}':: timestamp as date)  and cast('{{ end_date }}':: timestamp as date)
),
score as (
SELECT orderno
     , customerid
     , risklevel
     , pipelineid
     , ruleresultname
     , ruleresult as score
FROM rt_risk_mongo_gocash_riskreport
WHERE ruleresultname = 'newUserModelScoreV5' 
and businessid='uku')
SELECT perf.*
    , score.*
FROM perf
INNER JOIN score on perf.loanid :: varchar = score.orderno
WHERE perf.flag7 in (0,1)
and due_f7=1
"""




