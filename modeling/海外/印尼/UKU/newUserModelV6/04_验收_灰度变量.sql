
-- izi top up
with a1 as (
select orderno, topup_0_30_min::int, topup_30_60_min::int, topup_360_720_min::int
from (
select a.*, b.*,
row_number() over(partition by orderno order by b.create_time desc) as rn 
from rt_risk_mongo_gocash_riskreportgray a 
inner join (
	select customer_id, create_time, 
	case when substring(message,1,1) = '{' then message::json #>>'{topup_0_30,min}' end as topup_0_30_min,
	case when substring(message,1,1) = '{' then message::json #>>'{topup_30_60,min}' end as topup_30_60_min,
	case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,min}' end as topup_360_720_min
	from gocash_oss_to_pup) b on a.customerid = b.customer_id::text and a.createtime::timestamp + '8 hour'  >= b.create_time::timestamp
where substring(a.createtime, 1, 13) = '2020-03-04 17' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
) t 
where rn = 1
),
a2 as (
select orderno, 
sum(case when varname='iziTopup0to30Min' then varvalue::int end) as iziTopup0to30Min,
sum(case when varname='iziTopup30to60Min' then varvalue::int end) as iziTopup30to60Min,
sum(case when varname='iziTopup360to720Min' then varvalue::int end) as iziTopup360to720Min
from (
	select orderno, ruleresultname, 
	json_array_elements(datasources::json) ->> 'dataSourceName' dataSourceName,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varName'  varname,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varValue'  varvalue
	from rt_risk_mongo_gocash_riskreportgray
	where substring(createtime, 1, 13) = '2020-03-04 17' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
	) tmp
group by orderno
)
select a1.*, a2.iziTopup0to30Min,a2.iziTopup30to60Min,a2.iziTopup360to720Min,
a1.topup_0_30_min = a2.iziTopup0to30Min, a1.topup_30_60_min=a2.iziTopup30to60Min, a1.topup_360_720_min = a2.iziTopup360to720Min
from a1 inner join a2 on a1.orderno = a2.orderno


-- izi inquire v4
with a1 as (
select orderno, --createtime, total, type, key, value, case when value='Within24hours' then null else value::date end ,
sum(case when type='B' and value='Within24hours' then 1 
	when type='B' and createtime::date - value::date <= 3 then 1 else 0 end ) as iziB3d,
sum(case when type='B' and value='Within24hours' then 1 
	when type='B' and createtime::date - value::date <= 7 then 1 else 0 end ) as iziB7d,
sum(case when type='B' and value='Within24hours' then 1 
	when type='B' and createtime::date - value::date <= 360 then 1 else 0 end ) as iziB360d,
sum(case when type='C' and value='Within24hours' then 1 
	when type='C' and createtime::date - value::date <= 30 then 1 else 0 end ) as iziC30d,
sum(case when type='C' and value='Within24hours' then 1 
	when type='C' and createtime::date - value::date <= 90 then 1 else 0 end ) as iziC90d,
sum(case when type='C' and value='Within24hours' then 1 
	when type='C' and createtime::date - value::date <= 360 then 1 else 0 end ) as iziC360d
from (
select a.*, b.*,
rank() over(partition by orderno order by b.create_time desc) as rn 
from rt_risk_mongo_gocash_riskreportgray a 
inner join (
	select customer_id, create_time, total, type, key, value
	from gocash_oss_inquiries_v4_result) b on a.customerid = b.customer_id::text and a.createtime::timestamp + '8 hour'  >= b.create_time::timestamp
where substring(a.createtime, 1, 13) = '2020-03-04 17' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
) t 
where rn = 1
group by orderno--, createtime, total, type, key, value, case when value='Within24hours' then null else value::date end 
),
a2 as (
select orderno, 
sum(case when varname='iziB3d' then varvalue::int end) as iziB3d,
sum(case when varname='iziB7d' then varvalue::int end) as iziB7d,
sum(case when varname='iziB360d' then varvalue::int end) as iziB360d,
sum(case when varname='iziC30d' then varvalue::int end) as iziC30d,
sum(case when varname='iziC90d' then varvalue::int end) as iziC90d,
sum(case when varname='iziC360d' then varvalue::int end) as iziC360d
from (
	select orderno, ruleresultname, 
	json_array_elements(datasources::json) ->> 'dataSourceName' dataSourceName,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varName'  varname,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varValue'  varvalue
	from rt_risk_mongo_gocash_riskreportgray
	where substring(createtime, 1, 13) = '2020-03-04 17' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
	) tmp
group by orderno
)
select a1.*, a2.iziB3d,a2.iziB7d,a2.iziB360d,a2.iziC30d,a2.iziC90d,a2.iziC360d,
a1.iziB3d = a2.iziB3d, a1.iziB7d=a2.iziB7d, a1.iziB360d = a2.iziB360d,a1.iziC30d = a2.iziC30d, a1.iziC90d=a2.iziC90d, a1.iziC360d = a2.iziC360d
from a1 inner join a2 on a1.orderno = a2.orderno

-- advanceAI
with a1 as (
select orderno,advMultiscore::float,
GDM41::float,
GDM57::float,
GDM72::float,
GDM89::float,
GDM105::float,
GDM106::float,
GDM107::float,
GDM164::float,
GDM177::float,
GDM210::float,
GDM227::float,
GDM237::float,
GDM261::float,
GDM337::float,
GDM348::float
from (
select a.*, b.*,
row_number() over(partition by orderno order by b.create_time desc) as rn 
from rt_risk_mongo_gocash_riskreportgray a 
inner join (
	select customer_id, create_time, data::json ->>'score' as advMultiscore, 
	data::json #>>'{features,GD_M_41}' as GDM41,
	data::json #>>'{features,GD_M_57}' as GDM57,
	data::json #>>'{features,GD_M_72}' as GDM72,
	data::json #>>'{features,GD_M_89}' as GDM89,
	data::json #>>'{features,GD_M_105}' as GDM105,
	data::json #>>'{features,GD_M_106}' as GDM106,
	data::json #>>'{features,GD_M_107}' as GDM107,
	data::json #>>'{features,GD_M_164}' as GDM164,
	data::json #>>'{features,GD_M_177}' as GDM177,
	data::json #>>'{features,GD_M_210}' as GDM210,
	data::json #>>'{features,GD_M_227}' as GDM227,
	data::json #>>'{features,GD_M_237}' as GDM237,
	data::json #>>'{features,GD_M_261}' as GDM261,
	data::json #>>'{features,GD_M_337}' as GDM337,
	data::json #>>'{features,GD_M_348}' as GDM348
	from gocash_oss_MULTI_PLATFORM_SCORE) b on a.customerid = b.customer_id::text and a.createtime::timestamp + '8 hour'  >= b.create_time::timestamp
where substring(a.createtime, 1, 13) = '2020-03-04 17' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
) t 
where rn = 1
),
a2 as (
select orderno, 
sum(case when varname='advMultiscore' then varvalue::float end) as advMultiscore,
sum(case when varname='GDM41' then varvalue::float end) as GDM41,
sum(case when varname='GDM57' then varvalue::float end) as GDM57,
sum(case when varname='GDM72' then varvalue::float end) as GDM72,
sum(case when varname='GDM89' then varvalue::float end) as GDM89,
sum(case when varname='GDM105' then varvalue::float end) as GDM105,
sum(case when varname='GDM106' then varvalue::float end) as GDM106,
sum(case when varname='GDM107' then varvalue::float end) as GDM107,
sum(case when varname='GDM164' then varvalue::float end) as GDM164,
sum(case when varname='GDM177' then varvalue::float end) as GDM177,
sum(case when varname='GDM210' then varvalue::float end) as GDM210,
sum(case when varname='GDM227' then varvalue::float end) as GDM227,
sum(case when varname='GDM237' then varvalue::float end) as GDM237,
sum(case when varname='GDM261' then varvalue::float end) as GDM261,
sum(case when varname='GDM337' then varvalue::float end) as GDM337,
sum(case when varname='GDM348' then varvalue::float end) as GDM348
from (
	select orderno, ruleresultname, 
	json_array_elements(datasources::json) ->> 'dataSourceName' dataSourceName,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varName'  varname,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varValue'  varvalue
	from rt_risk_mongo_gocash_riskreportgray
	where substring(createtime, 1, 13) = '2020-03-04 17' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
	) tmp
group by orderno
)
select a1.*, --a2.iziB3d,a2.iziB7d,a2.iziB360d,a2.iziC30d,a2.iziC90d,a2.iziC360d,
a1.advMultiscore = a2.advMultiscore, a1.GDM41=a2.GDM41, a1.GDM57 = a2.GDM57,a1.GDM72 = a2.GDM72, a1.GDM89=a2.GDM89, a1.GDM105 = a2.GDM105,
a1.GDM106 = a2.GDM106, a1.GDM107=a2.GDM107, a1.GDM164 = a2.GDM164,a1.GDM177 = a2.GDM177, a1.GDM210=a2.GDM210, a1.GDM227 = a2.GDM227,
a1.GDM237 = a2.GDM237, a1.GDM261=a2.GDM261, a1.GDM337 = a2.GDM337,a1.GDM348 = a2.GDM348
from a1 inner join a2 on a1.orderno = a2.orderno


--altitude
with a1 as (
select orderno,case when substring(gps,1,1) = '{' then gps::json #>> '{altitude}' end as altitude
from (
select a.*, b.*,
row_number() over(partition by orderno order by b.create_time desc) as rn 
from rt_risk_mongo_gocash_riskreportgray a 
inner join (
	select customer_id, create_time, 
    case when jailbreak_status in ('True','true') then 1 when jailbreak_status in ('False','false') then 0 else -1 end as jailbreak_status, 
    case when simulator_status in ('True','true') then 1 when simulator_status in ('False','false') then 0 else -1 end as simulator_status, gps
	from gocash_loan_risk_program_baseinfo) b on a.customerid = b.customer_id::text and a.createtime::timestamp + '8 hour'  >= b.create_time::timestamp
where substring(a.createtime, 1, 13) = '2020-03-04 17' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
) t 
where rn = 1
),
a2 as (
select orderno, 
sum(case when varname='altitude' then varvalue::float end) as altitude
from (
	select orderno, ruleresultname, 
	json_array_elements(datasources::json) ->> 'dataSourceName' dataSourceName,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varName'  varname,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varValue'  varvalue
	from rt_risk_mongo_gocash_riskreportgray
	where substring(createtime, 1, 13) = '2020-03-04 17' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
	) tmp
group by orderno
)
select a1.*, a2.altitude,
a1.altitude::float = a2.altitude::float, round(a1.altitude::float) = round(a2.altitude::float)
from a1 inner join a2 on a1.orderno = a2.orderno