
--信用标签
select id as loan_id, effective_date, paid_off_time, due_date, loan_status, extend_times, approved_period,
cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date) as dt_date, date_add(effective_date, 72),
case when extend_times>3 then 0 
when datediff(cast(paid_off_time as date), due_date) > 7 then 1 
when loan_status='COLLECTION' and datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), due_date) <= 7 then -3 
when loan_status='COLLECTION' and datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), due_date) > 7 then 1
when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
when datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), effective_date) < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
else 0 end as flag7
from dw_gocash_go_cash_loan_gocash_core_loan
where date_add(effective_date, (approved_period+3)*4) = cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date) 
and return_flag = 'false' and extend_times > 0 and effective_date between '2019-06-27' and '2019-10-19'


--欺诈标签
select id as loan_id, effective_date, paid_off_time, due_date, loan_status, extend_times, approved_period, cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date) as dt_date, 
case when extend_times>3 then 0 
when datediff(cast(paid_off_time as date), due_date) > 7 then 1 
when loan_status='COLLECTION' and datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), due_date) <= 7 then -3 
when loan_status='COLLECTION' and datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), due_date) > 7 then 1
when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
when datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), effective_date) < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
else 0 end as flag7
from dw_gocash_go_cash_loan_gocash_core_loan
where date_add(date_add(effective_date, 8), approved_period) = cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date) 
and return_flag = 'false' and effective_date between '2019-09-17' and '2019-11-06'

