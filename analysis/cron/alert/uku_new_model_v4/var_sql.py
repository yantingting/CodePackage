#encoding=utf-8
HOURLY_VAR_SQL = """
WITH score as (
SELECT loanid
    ,  customerid
    ,  createtime
    ,  newusermodelv4score
    ,  newusermodelv4result
FROM rt_risk_mongo_gocash_installmentriskcontrolresult
WHERE createtime between '{{start_date}}' and '{{end_date}}' and newusermodelv4score <> '' and businessid =  '{{businessid}}'
),
var as (
SELECT loanid as loanid2
    , riskno
    ,  pipelineid
    ,  gender
    ,  bankcode
    ,  city
    ,  education
    ,  industryinvolved
    ,  job
    ,  maritalstatus
    ,  occupationtype
    ,  province
FROM risk_mongo_gocash_installmentriskcontrolparams
--WHERE pipelineid in ('464', '455') --and date(createtime) between cast('{{start_date}}':: timestamp + '-1 day' as date) and cast('{{end_date}}':: timestamp + '1 day' as date)
)
SELECT score.loanid
     , score.customerid
     , score.createtime
     , score.newusermodelv4score
     , score.newusermodelv4result
     , var.*
FROM score
INNER JOIN var on score.loanid = var.loanid2
"""

VAR_SQL = """
WITH score as (
SELECT loanid
    ,  customerid
    ,  createtime
    ,  newusermodelv4score
    ,  newusermodelv4result
FROM rt_risk_mongo_gocash_installmentriskcontrolresult
WHERE createtime between '{{start_date}}' and '{{end_date}}' and newusermodelv4score <> ''
and businessid in  {{businessid}}  
),
var as (
SELECT loanid as loanid2
    ,  pipelineid
    ,  age
    ,  gender
    ,  iziphoneageage
    ,  iziinquiriesbytypetotal
    ,  bankcode
    ,  city
    ,  education
    ,  industryinvolved
    ,  job
    ,  maritalstatus
    ,  occupationtype
    ,  province
FROM risk_mongo_gocash_installmentriskcontrolparams
--WHERE pipelineid in ('464', '454') --and date(createtime) between cast('{{start_date}}':: timestamp + '-1 day' as date) and cast('{{end_date}}':: timestamp + '1 day' as date)
)
SELECT *
FROM (
    SELECT score.loanid
        , score.customerid
        , score.createtime
        , score.newusermodelv4score
        , score.newusermodelv4result
        , var.*
        , program.*
        , row_number() over(partition by score.customerid order by program.create_time desc) as rn
    FROM score
    LEFT JOIN var on score.loanid = var.loanid2
    LEFT JOIN (SELECT customer_id, create_time, packages
               FROM gocash_loan_risk_program_packages) program on score.customerid = program.customer_id
    WHERE score.createtime::timestamp > program.create_time :: timestamp
)t 
WHERE rn = 1
"""

