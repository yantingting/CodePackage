# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import os
import time
import pandas as pd
import numpy as np
import psycopg2
import pickle as pickle

def load_data_from_pickle(file_path, file_name):
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'rb') as infile:
        result = pickle.load(infile)
    return result

path_rawdata = 'D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212/'
path = 'D:/Model Development/202001 IDN new v6/01 Data/'

import psycopg2
def get_df_from_pg(SQL):
    usename = "postgres"
    password = "Mintq2019"
    db = "risk_dm"
    host = "192.168.2.19"
    port = "5432"
    try:
        conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)
        print("Opened database successfully")
    except Exception as e:
        print(e)
    cur = conn.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    df = pd.DataFrame(rows,columns=[i.name for i in cur.description])
    df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
    return df

# 分流占比检查
sql = '''
select substring(createtime, 1, 13),  count(*), 
count(case when pipelineid in ('479','480') then pipelineid end) v5, count(case when pipelineid in ('492','493') then pipelineid end) v6,
1.0*count(case when pipelineid in ('479','480') then pipelineid end)/count(case when pipelineid in ('492','493') then pipelineid end) rt, 3.0/7 as expect
from rt_risk_mongo_gocash_riskreportgray
where createtime::date between '2020-03-10' and '2020-03-11' and ((pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6')
or (pipelineid in ('479','480') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV5'))
group by 1
order by 1
'''
df = get_df_from_pg(sql)
print(df)

'''****************************** 提额检查 *****************************'''
# 模型上线提额
sql = '''
select a.orderno, a.customerid, ruleresult, b.upamount, a.risklevel
from rt_risk_mongo_gocash_riskreportgray a 
left join rt_risk_mongo_gocash_installmentriskcontrolresultgray b on a.orderno = b.orderno
where a.createtime::date between '2020-03-10' and '2020-03-11' and a.pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
order by substring(ruleresult, 1,3)::int desc
'''
df = get_df_from_pg(sql)
print(df)

# 3/13提额
sql = '''
select a.orderno, a.customerid, ruleresult, b.initial_principal, b.upamount, a.risklevel, case when c.grouping = 'Test6' then 'Test6' when c.grouping in ('Test5', 'Test7','Test8') then 'Test578' end
from rt_risk_mongo_gocash_riskreport a 
left join rt_risk_mongo_gocash_installmentriskcontrolresult b on a.orderno = b.orderno
left join rt_t_gocash_core_loan c on a.orderno = c.id::text 
where a.createtime::date between '2020-03-14' and '2020-03-16' and a.pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
order by case when c.grouping = 'Test6' then 'Test6' when c.grouping in ('Test5', 'Test7','Test8') then 'Test578' end desc,
substring(ruleresult, 1,3)::int desc
'''

# 3/17提额降额
sql = '''
select a.orderno, a.customerid, ruleresult, b.initial_principal, b.upamount, a.risklevel
from rt_risk_mongo_gocash_riskreport a 
left join rt_risk_mongo_gocash_installmentriskcontrolresult b on a.orderno = b.orderno
left join rt_t_gocash_core_loan c on a.orderno = c.id::text 
where a.createtime::date between '2020-03-18' and '2020-03-18' and a.pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
order by 
substring(ruleresult, 1,3)::int desc

select a.orderno, a.customerid, ruleresult, b.initial_principal, b.upamount, a.risklevel
from rt_risk_mongo_gocash_riskreport a 
left join rt_risk_mongo_gocash_installmentriskcontrolresult b on a.orderno = b.orderno
left join rt_t_gocash_core_loan c on a.orderno = c.id::text 
where a.createtime::date between '2020-03-18' and '2020-03-18' and a.pipelineid in ('479','480') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV5'
order by 
substring(ruleresult, 1,3)::int desc
'''


'''****************************** 提额检查 *****************************'''
sql = '''
select orderno as loan_id, substring(ruleresult,1,3)::int as scorev6
from rt_risk_mongo_gocash_riskreport
where createtime::date between '2020-03-11' and '2020-03-16' and pipelineid in ('492','493') 
and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
'''
df = get_df_from_pg(sql)
print(df.shape)
print(df.loan_id.nunique())

# data_scored = pd.read_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output 20200219\\data_scored_0221_0222 1.csv')
# print(data_scored.dtypes)
# set(data_scored.score_bin_10)

score_bin_10 = [-np.inf,
                577,
586,
594,
602,
611,
620,
630,
642,
658,
np.inf]

df['score_bin_10'] = pd.cut(df.scorev6, score_bin_10, precision=0).astype(str)

psi = df.groupby('score_bin_10').size().to_frame().reset_index()
psi.columns = ['score_bin_10','ct']
psi.ct = psi.ct.astype(int)
total = psi.ct.sum()
psi['percentage'] = psi.ct.apply(lambda x: 100*x/total)

print(psi)


# 21岁以上线上模型分分布
sql = '''
select t1.*, over21
from (
select orderno as loan_id, substring(ruleresult,1,3)::int as scorev6
from rt_risk_mongo_gocash_riskreport
where createtime::date between '2020-03-11' and '2020-03-13' and pipelineid in ('492','493') 
and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6' and orderno <> '444967429977243648' ) t1
left join (
select orderno as loan_id, 
age(cast(json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varValue' as date)) >= '21 years' as over21
from rt_risk_mongo_gocash_riskreport
where createtime::date between '2020-03-11' and '2020-03-13' and pipelineid in ('492','493') 
and ruleresultname = 'invalidAge' and orderno <> '444967429977243648' ) t2 on t1.loan_id = t2.loan_id  --年龄脏数据 '444967429977243648' 

'''
df = get_df_from_pg(sql)
print(df.shape)
print(df.loan_id.nunique())
print(df.over21.value_counts(dropna = False))

df_bk = df.copy()

df_use = df.copy()
# df_use = df[df.over21 == True]

score_bin_10 = [-np.inf,
                577,
586,
594,
602,
611,
620,
630,
642,
658,
np.inf]

df_use['score_bin_10'] = pd.cut(df_use.scorev6, score_bin_10, precision=0).astype(str)

psi = df_use.groupby('score_bin_10').size().to_frame().reset_index()
psi.columns = ['score_bin_10','ct']
psi.ct = psi.ct.astype(int)
total = psi.ct.sum()
psi['percentage'] = psi.ct.apply(lambda x: 100*x/total)

print(psi)


'''****************************** 20200316模型阈值调整 *****************************'''
sql = '''
select orderno as loan_id, substring(ruleresult,1,3)::int as scorev6
from rt_risk_mongo_gocash_riskreport
where createtime::date between '2020-03-11' and '2020-03-16' and pipelineid in ('492','493') 
and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
'''
df = get_df_from_pg(sql)
print(df.shape)
print(df.loan_id.nunique())

# 训练20等分
score_bin_10 = [-np.inf, 589, 600,609,615,621,625,630,634,637,641,644,648,652,655,658,662,666,670,676,np.inf]

df['score_bin_10'] = pd.cut(df.scorev6, score_bin_10, precision=0).astype(str)

psi = df.groupby('score_bin_10').size().to_frame().reset_index()
psi.columns = ['score_bin_10','ct']
psi.ct = psi.ct.astype(int)
total = psi.ct.sum()
psi['percentage'] = psi.ct.apply(lambda x: 100*x/total)

psi.reset_index()
print(psi)



# V5 放款
sql = '''
select distinct
    loan_id, 
    customer_id,
    effective_date,
    newusermodelscorev5,
    case when dpd>=7 then 1 else 0 end as dpd7_flag
from
	(
	select
		t1.id loan_id,
		customer_id,
		effective_date,
		newusermodelscorev5,
		row_number() over(partition by t1.id order by t4.createtime desc) as rn,
		case
			when loan_status = 'COLLECTION' then current_date-late_date
			else round(late_fee /(approved_principal*0.025))::int
	end as DPD
	from
		(
		select
			*
		from
			rt_t_gocash_core_loan
		where
			return_flag = 'false'
			and effective_date between '2020-02-01' and '2020-02-13'
            and product_id='1'
            and device_approval='ANDROID'
			and loan_status not in ('DENIED',
			'RESCIND',
			'APPROVING',
			'CREATED')
			and approved_period in (8,15,22) )t1
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
    left join (
		select
			loanid,
			customerid,
			newusermodelscorev5,
			createtime
		from
			rt_risk_mongo_gocash_installmentriskcontrolresult where newusermodelscorev5 is not null) t4 on
		t1.customer_id::varchar = t4.customerid
    )t
where rn = 1
'''
df = get_df_from_pg(sql)
print(df.shape)
print(df.loan_id.nunique())

df.newusermodelscorev5 = df.newusermodelscorev5.astype(float)
df.loan_id = df.loan_id.astype(str)

decile_score = pd.qcut(df.newusermodelscorev5, q=20, duplicates='drop', precision=0).astype(str)

data_scored_loan = pd.DataFrame([df['loan_id'].values, df['newusermodelscorev5'].values, df['dpd7_flag'].values, decile_score]).T
data_scored_loan.columns= ['loan_id', 'scorev5', 'dpd7_flag', 'decile_score20']

# V5 申请
sql = '''
select orderno as loan_id, substring(ruleresult,1,3)::int as scorev5
from rt_risk_mongo_gocash_riskreport
where createtime::date between '2020-03-11' and '2020-03-17' and pipelineid in ('479','480') 
and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV5'
'''
df = get_df_from_pg(sql)
print(df.shape)
print(df.loan_id.nunique())

set(decile_score)

score_bin_20 = [-np.inf, 619,
620,
622,
623,
625,
626,
628,
629,
631,
633,
634,
636,
638,
641,
643,
646,
649,
653,
659
,np.inf]

df['score_bin_20'] = pd.cut(df.scorev5, score_bin_20, precision=0).astype(str)



writer = pd.ExcelWriter('D:\\Model Development\\202001 IDN new v6\\04 Document\\V5阈值调整 20200317.xlsx')
data_scored_loan.to_excel(writer, '放款0221-0213')
df.to_excel(writer, '申请0311-0317')
writer.save()
