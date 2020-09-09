#encoding=utf-8

VAR_SQL='''
set enable_nestloop=off;

SELECT id as loanid,customerid,createtime,ruleresult as score,datasources
FROM rt_risk_mongo_gocash_riskreport r
WHERE ruleresultname in('linkAjaNewModelV1Score')
and businessid='{{businessid}}'
and createtime between '{{start_date}}' and '{{end_date}}'
'''

'''
SQL = """
WITH score as (
SELECT loanid
    , customerid
    , createtime
    , linkajanewmodelv1score
    , linkajanewmodelv1result
FROM rt_risk_mongo_gocash_installmentriskcontrolresult
WHERE linkajanewmodelv1result<>'' and businessid = '{{businessid}}' and createtime between '{{start_date}}' and '{{end_date}}' and device = 'IOS'
),
var as (
SELECT loanid as loanid2
    , maritalStatusCode
    , religionCode
    , educationCode
    , mailAddressCode
    , provinceCode
    , gender
    , age
    , iziPhoneAgeAge
    , iziInquiriesByType07d
    , iziInquiriesByType14d
    , iziInquiriesByType21d
    , iziInquiriesByType30d
    , iziInquiriesByType60d
    , iziInquiriesByType90d
    , iziInquiriesByTypeTotal
    , iziPhoneVerifyResult
    , referBroSis
    , referParents
    , referSpouse
    , jobCode
    , occupationOffice
    , occupationEntre
    , bankCodeBca
    , bankCodePermata
    , bankCodeBri
    , bankCodeMandiri
FROM risk_mongo_gocash_installmentriskcontrolparams
WHERE pipelineid in ('462','456')--and date(createtime) between cast('{{start_date}}':: timestamp + '-1 day' as date) and cast('{{end_date}}':: timestamp + '1 day' as date)
)
SELECT score.loanid
, score.customerid
, score.createtime
, score.linkajanewmodelv1score
, score.linkajanewmodelv1result
, var.*
FROM score
LEFT JOIN var on  score.loanid = var.loanid2
"""
'''

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
    ,case when extend_times>3 then 0 
     when paid_off_time::Date-due_date>0 then 1 
     when loan_status='COLLECTION' and current_date::Date-due_date<=0 then -3 
     when loan_status='COLLECTION' and current_date::Date-due_date>0 then 1
     when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
     when current_date-effective_date <= approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
     else 0 end as flag_1
     ,case when extend_times>3 then 0 
     when paid_off_time::Date-due_date > 3 then 1 
     when loan_status='COLLECTION' and current_date::Date-due_date<=3 then -3 
     when loan_status='COLLECTION' and current_date::Date-due_date>3 then 1
     when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
     when current_date-effective_date <= approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
     else 0 end as flag_3
    ,case when extend_times>3 then 0 
     when paid_off_time::Date-due_date > 7 then 1 
     when loan_status='COLLECTION' and current_date::Date-due_date<=7 then -3 
     when loan_status='COLLECTION' and current_date::Date-due_date>7 then 1
     when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
     when current_date-effective_date <= approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
     else 0 end as flag_7
    ,case when current_date - due_date >= 7 then 1 else 0 end as due7_flag
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date!='1970-01-01' and effective_date between cast('{{ start_date }}':: timestamp as date)  and cast('{{ end_date }}':: timestamp as date)
),
score as (
SELECT orderno
     , customerid
     , risklevel
     , pipelineid
     , ruleresultname
     , ruleresult as score
FROM rt_risk_mongo_gocash_riskreport
WHERE businessid='{{businessid}}' and ruleresultname = 'linkAjaNewModelV1Score' 
)
SELECT *
FROM score
INNER JOIN perf on perf.loan_id :: varchar = score.orderno
WHERE flag_7 in (0,1) and due7_flag = 1
"""

