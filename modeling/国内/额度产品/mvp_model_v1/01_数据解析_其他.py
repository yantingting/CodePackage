
import os
import time
import pandas as pd
import psycopg2

filepath_out = 'D:/Model Development/201910 MVP/01 Data/raw data 20191108/'

usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)

#'''FLAG'''
#sql_flag_0920 = '''
#select order_no, effective_date, business_id, case when max_late_day >= 7 then 1 else 0 end as mob2_ever7
#from t_loan_performance
#where dt = '20191027' and business_id in ('tb', 'xjbk','rong360') and effective_date between '2019-07-04' and '2019-10-22'
#union
#SELECT t1.order_no, effective_date, business_id, case when sum(flag)=0 then 0 when sum(flag) >0 then 1 else null end as mob2_ever7
#from (
#	SELECT t1.order_no, t1.effective_date, t1.business_id, t2.total_terms ,t2.current_term,
#		case when  t2.repay_status='COLLECTION' or 
#		(t2.repay_status='COLLECTION_GATEWAY_REPAID' or t2.repay_status='COLLECTION_REPAID' or t2.repay_status='THIRDPART_COLLECTION_REPAID')
#		and date(t3.paid_time) - t2.due_date >= 7 then 1 ELSE 0 end as flag
#-- 		, t2.repay_status, to_date(t3.paid_time), t2.due_date
#	FROM temp_credit_modelscore_v6yl t1
#	INNER JOIN dw_dc_compensatory_cp_core_schedule t2 on t1.order_no=t2.order_no and t2.current_term<=2
#	LEFT JOIN dw_dc_compensatory_cp_core_total_pay_flow t3 on t2.id=t3.schedule_id and t3.paychannel_flow_status= 'PAID'
#	where t1.sample_set in ('train', 'test')) t1
#group by order_no, effective_date, business_id
#'''
#conn.rollback()
#cur = conn.cursor()
#cur.execute(sql_flag_0920)
#raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
#r_flag_0920 = raw.copy()
#
#print(r_flag_0920.shape)
#print(r_flag_0920.order_no.nunique()) 
#
#r_flag_0920_f = r_flag_0920.copy()
#
#r_flag_0920_f['sample_set'] = pd.to_datetime(r_flag_0920_f['effective_date']).apply(lambda x: \
#    'test' if x >= pd.to_datetime('2019-08-15')  else (
#    'train' if x >= pd.to_datetime('2019-07-04') else '' ))
#r_flag_0920_f.groupby('sample_set').size()
#r_flag_0920_f.groupby('sample_set').mean()
#sum(r_flag_0920_f[r_flag_0920_f['sample_set']=='test'].mob2_ever7)
#
#r_flag_0920_f.to_excel(filepath_out + "r_flag_0920_f.xlsx")

''' 所有样本 '''
#r_x_with_y_v6 = pd.read_excel('D:/Model Development/201908 BK Model/01 Data/raw data/x_with_y_v6.xlsx')
#r_base0 = r_x_with_y_v6[(r_x_with_y_v6['sample_set']=='train') | (r_x_with_y_v6['sample_set']=='test')]

sql_base = '''
select a.order_no, a.effective_date, a.business_id
from t_loan_performance a 
where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'  
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_base)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_base = raw.copy()

print(r_base.shape)
print(r_base.order_no.nunique())  # 11947

r_base.to_excel(filepath_out + "r_base_f.xlsx")

''' 基本信息 '''
sql_cust = '''
SELECT t0.order_no
,coalesce(rong.education, dc.education, bk.education) as education
,coalesce(rong.maritalstatus, dc.maritalstatus, bk.maritalstatus) as maritalstatus
,case when coalesce(rong.idcardgender, dc.idcardgender, bk.idcardgender) = '男' then 1
	  when coalesce(rong.idcardgender, dc.idcardgender, bk.idcardgender) = '女' then 0
	  else -1 end as idcardgender
,coalesce(rong.age, dc.age, bk.age) as age
,coalesce(rong.industry, dc.industry, bk.industry) as industry 
,coalesce(rong.monthlyincome, dc.monthlyincome, bk.monthlyincome) as monthlyincome 
,coalesce(rong.positiontype, dc.positiontype, bk.positiontype) as positiontype 
,case coalesce(rong.id_city_level, dc.id_city_level, bk.id_city_level) 
	when '一线城市' then 0
	when '新一线城市' then 1
	when '二线城市' then 2
	when '三线城市' then 3
	when '四线城市' then 4
	when '五线城市' then 5 
	else -1 end as id_city_level	
,case when coalesce(rong.age, dc.age, bk.age) = '' then '-9999'
      when cast(coalesce(rong.age, dc.age, bk.age) as integer)  >30 and (coalesce(rong.maritalstatus, dc.maritalstatus, bk.maritalstatus) = '1' or 
      coalesce(rong.maritalstatus, dc.maritalstatus, bk.maritalstatus) ='2' or coalesce(rong.maritalstatus, dc.maritalstatus, bk.maritalstatus) ='4') then 1
	  when cast(coalesce(rong.age, dc.age, bk.age) as integer)  <=30 and coalesce(rong.maritalstatus, dc.maritalstatus, bk.maritalstatus) ='3' then 2
	  else 3 end as age_marital
from (
	select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
	--union 
	--select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
) t0 
left join rong360_customer_history_result rong on t0.order_no = rong.order_no
left join dc_customer_history_result dc on t0.order_no = dc.order_no
left join bk_customer_history_result bk on t0.order_no = bk.order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_cust)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_cust = raw.copy()  
print(r_cust.shape)
print(r_cust.order_no.nunique())

r_cust_f = r_cust.copy()

var_to_float = list(r_cust_f.iloc[:,1:].columns)
for x in var_to_float:
    r_cust_f[x] = r_cust_f[x].astype(float)
print(r_cust_f.dtypes)

''' 删除常数值变量 '''
var = list(r_cust_f.iloc[:,1:].columns)
for x in var:
    print(r_cust_f[x].std(), x) 

r_cust_f.to_excel(filepath_out + "r_cust_f.xlsx")



''' GPS '''
sql_gps = '''
select order_no
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '31_121'then 1 else 0 end as gps_31_121
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '22_113'then 1 else 0 end as gps_22_113
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '23_113'then 1 else 0 end as gps_23_113
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '31_120'then 1 else 0 end as gps_31_120
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '39_116'then 1 else 0 end as gps_39_116
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '22_114'then 1 else 0 end as gps_22_114
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '30_120'then 1 else 0 end as gps_30_120
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '30_114'then 1 else 0 end as gps_30_114
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '30_104'then 1 else 0 end as gps_30_104
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '41_123'then 1 else 0 end as gps_41_123
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '24_118'then 1 else 0 end as gps_24_118
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '39_117'then 1 else 0 end as gps_39_117
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '34_113'then 1 else 0 end as gps_34_113
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '32_119'then 1 else 0 end as gps_32_119
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '29_106'then 1 else 0 end as gps_29_106
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '34_108'then 1 else 0 end as gps_34_108
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '31_119'then 1 else 0 end as gps_31_119
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '31_117'then 1 else 0 end as gps_31_117
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '32_118'then 1 else 0 end as gps_32_118
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '36_120'then 1 else 0 end as gps_36_120
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '29_121'then 1 else 0 end as gps_29_121
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '23_116'then 1 else 0 end as gps_23_116
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '45_126'then 1 else 0 end as gps_45_126
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '36_117'then 1 else 0 end as gps_36_117
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '31_118'then 1 else 0 end as gps_31_118
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '28_112'then 1 else 0 end as gps_28_112
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '40_116'then 1 else 0 end as gps_40_116
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '26_119'then 1 else 0 end as gps_26_119
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '43_125'then 1 else 0 end as gps_43_125
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '27_120'then 1 else 0 end as gps_27_120
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '22_108'then 1 else 0 end as gps_22_108
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '28_113'then 1 else 0 end as gps_28_113
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '30_103'then 1 else 0 end as gps_30_103
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '26_106'then 1 else 0 end as gps_26_106
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '25_102'then 1 else 0 end as gps_25_102
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '37_112'then 1 else 0 end as gps_37_112
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '38_121'then 1 else 0 end as gps_38_121
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '29_120'then 1 else 0 end as gps_29_120
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '38_114'then 1 else 0 end as gps_38_114
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '28_121'then 1 else 0 end as gps_28_121
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '28_115'then 1 else 0 end as gps_28_115
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '32_120'then 1 else 0 end as gps_32_120
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '28_120'then 1 else 0 end as gps_28_120
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '23_114'then 1 else 0 end as gps_23_114
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '30_121'then 1 else 0 end as gps_30_121
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '36_116'then 1 else 0 end as gps_36_116
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '25_119'then 1 else 0 end as gps_25_119
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '34_112'then 1 else 0 end as gps_34_112
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '36_103'then 1 else 0 end as gps_36_103
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '29_119'then 1 else 0 end as gps_29_119
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '34_117'then 1 else 0 end as gps_34_117
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '36_119'then 1 else 0 end as gps_36_119
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '39_121'then 1 else 0 end as gps_39_121
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '30_119'then 1 else 0 end as gps_30_119
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '21_110'then 1 else 0 end as gps_21_110
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '36_118'then 1 else 0 end as gps_36_118
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '23_112'then 1 else 0 end as gps_23_112
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '27_113'then 1 else 0 end as gps_27_113
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '36_114'then 1 else 0 end as gps_36_114
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '38_106'then 1 else 0 end as gps_38_106
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '31_104'then 1 else 0 end as gps_31_104
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '39_118'then 1 else 0 end as gps_39_118
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '34_109'then 1 else 0 end as gps_34_109
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '24_117'then 1 else 0 end as gps_24_117
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '33_119'then 1 else 0 end as gps_33_119
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '35_118'then 1 else 0 end as gps_35_118
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '40_111'then 1 else 0 end as gps_40_111
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '24_102'then 1 else 0 end as gps_24_102
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '37_121'then 1 else 0 end as gps_37_121
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '38_115'then 1 else 0 end as gps_38_115
,case when split_part(latitude, '.', 1)||'_'||split_part(longitude, '.', 1) = '32_117'then 1 else 0 end as gps_32_117
from (
select distinct t0.order_no, coalesce(bk.latitude, dc.latitude) as latitude, coalesce(bk.longitude, dc.longitude) as longitude
from (
	select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk') and effective_date between '2019-07-04' and '2019-10-22'
	--union 
	--select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
) t0
left join t_loan_performance t1 on t0.order_no = t1.order_no
left join bk_gps_device_result bk on t0.order_no = bk.order_no
left join dc_gps_device_result dc on t0.order_no = dc.order_no
union
select t0.order_no
,cast(oss ::json #>> '{gps}' as json) ::json ->>'latitude' as latitude, 
cast(oss ::json #>> '{gps}' as json) ::json ->>'longitude' as longitude
from t_loan_performance t0
left join risk_mongo_installmentmessagerelated m on t0.order_no = m.orderno and topicname in ('risk_program', 'program')
left join risk_oss_program p on m.taskid = p.taskid and p.businessid='rong360'
where t0.dt='20191108'  and business_id in ('rong360') and effective_date between '2019-07-04' and '2019-10-22' ) t
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_gps)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_gps = raw.copy()  
print(r_gps.shape)
print(r_gps.order_no.nunique())

r_gps_f = r_gps.copy()

var_to_float = list(r_gps_f.iloc[:,1:].columns)
for x in var_to_float:
    r_gps_f[x] = r_gps_f[x].astype(float)
print(r_gps_f.dtypes)

''' 删除常数值变量 '''
var = list(r_gps_f.iloc[:,1:].columns)
for x in var:
    print(r_gps_f[x].std(), x) 


r_gps_f.to_excel(filepath_out + "r_gps_f.xlsx")



'''同盾'''
sql_td_var = '''
select order_no
,sum(case when rule_id = '47580344' and dimension='借款人身份证' then count else null end) as r47580344_id_platform_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='大数据金融' then count else null end) as r47580344_id_industry_1_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='大型消费金融公司' then count else null end) as r47580344_id_industry_6_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='P2P网贷' then count else null end) as r47580344_id_industry_9_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='互联网金融门户' then count else null end) as r47580344_id_industry_13_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='一般消费分期平台' then count else null end) as r47580344_id_industry_15_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='厂商汽车金融' then count else null end) as r47580344_id_industry_16_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='小额贷款公司' then count else null end) as r47580344_id_industry_18_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='房地产金融' then count else null end) as r47580344_id_industry_20_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='融资租赁' then count else null end) as r47580344_id_industry_21_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='理财机构' then count else null end) as r47580344_id_industry_22_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='第三方支付' then count else null end) as r47580344_id_industry_24_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' then count else null end) as r47580344_m_platform_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='大数据金融' then count else null end) as r47580344_m_industry_1_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='大型消费金融公司' then count else null end) as r47580344_m_industry_6_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='P2P网贷' then count else null end) as r47580344_m_industry_9_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='互联网金融门户' then count else null end) as r47580344_m_industry_13_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='一般消费分期平台' then count else null end) as r47580344_m_industry_15_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='厂商汽车金融' then count else null end) as r47580344_m_industry_16_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='小额贷款公司' then count else null end) as r47580344_m_industry_18_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='房地产金融' then count else null end) as r47580344_m_industry_20_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='融资租赁' then count else null end) as r47580344_m_industry_21_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='理财机构' then count else null end) as r47580344_m_industry_22_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='第三方支付' then count else null end) as r47580344_m_industry_24_ct

,sum(case when rule_id = '47580334' and dimension='借款人身份证' then count else null end) as r47580334_id_platform_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='大数据金融' then count else null end) as r47580334_id_industry_1_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='大型消费金融公司' then count else null end) as r47580334_id_industry_6_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='P2P网贷' then count else null end) as r47580334_id_industry_9_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='互联网金融门户' then count else null end) as r47580334_id_industry_13_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='一般消费分期平台' then count else null end) as r47580334_id_industry_15_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='厂商汽车金融' then count else null end) as r47580334_id_industry_16_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='小额贷款公司' then count else null end) as r47580334_id_industry_18_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='房地产金融' then count else null end) as r47580334_id_industry_20_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='融资租赁' then count else null end) as r47580334_id_industry_21_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='理财机构' then count else null end) as r47580334_id_industry_22_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='第三方支付' then count else null end) as r47580334_id_industry_24_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' then count else null end) as r47580334_m_platform_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='大数据金融' then count else null end) as r47580334_m_industry_1_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='大型消费金融公司' then count else null end) as r47580334_m_industry_6_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='P2P网贷' then count else null end) as r47580334_m_industry_9_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='互联网金融门户' then count else null end) as r47580334_m_industry_13_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='一般消费分期平台' then count else null end) as r47580334_m_industry_15_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='厂商汽车金融' then count else null end) as r47580334_m_industry_16_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='小额贷款公司' then count else null end) as r47580334_m_industry_18_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='房地产金融' then count else null end) as r47580334_m_industry_20_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='融资租赁' then count else null end) as r47580334_m_industry_21_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='理财机构' then count else null end) as r47580334_m_industry_22_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='第三方支付' then count else null end) as r47580334_m_industry_24_ct
from (
	select t0.order_no, rule_id
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'dimension'  as dimension
	,cast(json_array_elements(cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'detail' as json)) ::json ->> 'count' as int) as count
	,json_array_elements(cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'detail' as json)) ::json ->> 'industry_display_name' as industry_display_name
	from  t_loan_performance t0
	left join risk_mongo_installmentmessagerelated r on t0.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580344', '47580334')
	where t0.dt='20191108' and t0.business_id in ('tb', 'xjbk', 'rong360') and t0.effective_date between '2019-07-04' and '2019-10-22'
) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_var)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_var = raw.copy()   
print(r_td_var.shape)
print(r_td_var.order_no.nunique())

#r_td_var_f = r_td_var.append(r_base0, ignore_index=True)
#r_td_var_f = r_td_var_f[list(r_td_var.columns)]

r_td_var_f = r_td_var.copy()

var_to_float = list(r_td_var_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_var_f[x] = r_td_var_f[x].astype(float)
print(r_td_var_f.dtypes)

''' 删除常数值变量 '''
var = list(r_td_var_f.iloc[:,1:].columns)
for x in var:
    if r_td_var_f[x].std() <=0.001:
        print(r_td_var_f[x].std(), x) 
    if pd.isnull(r_td_var_f[x].std()):
        print(r_td_var_f[x].std(), x)

r_td_var_f =  r_td_var_f.drop(['r47580344_id_industry_20_ct'
,'r47580344_id_industry_24_ct'
,'r47580344_m_industry_20_ct'
,'r47580344_m_industry_24_ct'
,'r47580334_id_industry_16_ct'
,'r47580334_id_industry_20_ct'
,'r47580334_id_industry_24_ct'
,'r47580334_m_industry_20_ct'
,'r47580334_m_industry_24_ct'], axis=1)

r_td_var_f.to_excel(filepath_out + "r_td_var_f.xlsx")


sql_td_duotou = '''
select order_no
,max(case when rule_id = '47580344' then platform_count else -1 end) as r47580344_platform_ct
,max(case when rule_id = '47580344' and industry_display_name='大数据金融' then count else -1 end) as r47580344_industry_1_ct
,max(case when rule_id = '47580344' and industry_display_name='银行小微贷款' then count else -1 end) as r47580344_industry_2_ct
,max(case when rule_id = '47580344' and industry_display_name='直销银行' then count else -1 end) as r47580344_industry_3_ct
,max(case when rule_id = '47580344' and industry_display_name='网上银行' then count else -1 end) as r47580344_industry_4_ct
,max(case when rule_id = '47580344' and industry_display_name='银行消费金融公司' then count else -1 end) as r47580344_industry_5_ct
,max(case when rule_id = '47580344' and industry_display_name='大型消费金融公司' then count else -1 end) as r47580344_industry_6_ct
,max(case when rule_id = '47580344' and industry_display_name='综合类电商平台' then count else -1 end) as r47580344_industry_7_ct
,max(case when rule_id = '47580344' and industry_display_name='汽车租赁' then count else -1 end) as r47580344_industry_8_ct
,max(case when rule_id = '47580344' and industry_display_name='P2P网贷' then count else -1 end) as r47580344_industry_9_ct
,max(case when rule_id = '47580344' and industry_display_name='信息中介' then count else -1 end) as r47580344_industry_10_ct
,max(case when rule_id = '47580344' and industry_display_name='设备租赁' then count else -1 end) as r47580344_industry_11_ct
,max(case when rule_id = '47580344' and industry_display_name='财产保险' then count else -1 end) as r47580344_industry_12_ct
,max(case when rule_id = '47580344' and industry_display_name='互联网金融门户' then count else -1 end) as r47580344_industry_13_ct
,max(case when rule_id = '47580344' and industry_display_name='第三方服务商' then count else -1 end) as r47580344_industry_14_ct
,max(case when rule_id = '47580344' and industry_display_name='一般消费分期平台' then count else -1 end) as r47580344_industry_15_ct
,max(case when rule_id = '47580344' and industry_display_name='厂商汽车金融' then count else -1 end) as r47580344_industry_16_ct
,max(case when rule_id = '47580344' and industry_display_name='银行个人业务' then count else -1 end) as r47580344_industry_17_ct
,max(case when rule_id = '47580344' and industry_display_name='小额贷款公司' then count else -1 end) as r47580344_industry_18_ct
,max(case when rule_id = '47580344' and industry_display_name='O2O' then count else -1 end) as r47580344_industry_19_ct
,max(case when rule_id = '47580344' and industry_display_name='房地产金融' then count else -1 end) as r47580344_industry_20_ct
,max(case when rule_id = '47580344' and industry_display_name='融资租赁' then count else -1 end) as r47580344_industry_21_ct
,max(case when rule_id = '47580344' and industry_display_name='理财机构' then count else -1 end) as r47580344_industry_22_ct
,max(case when rule_id = '47580344' and industry_display_name='信用卡中心' then count else -1 end) as r47580344_industry_23_ct
,max(case when rule_id = '47580344' and industry_display_name='第三方支付' then count else -1 end) as r47580344_industry_24_ct
,max(case when rule_id = '47580334' then platform_count else -1 end) as r47580334_platform_ct
,max(case when rule_id = '47580334' and industry_display_name='大数据金融' then count else -1 end) as r47580334_industry_1_ct
,max(case when rule_id = '47580334' and industry_display_name='银行小微贷款' then count else -1 end) as r47580334_industry_2_ct
,max(case when rule_id = '47580334' and industry_display_name='直销银行' then count else -1 end) as r47580334_industry_3_ct
,max(case when rule_id = '47580334' and industry_display_name='网上银行' then count else -1 end) as r47580334_industry_4_ct
,max(case when rule_id = '47580334' and industry_display_name='银行消费金融公司' then count else -1 end) as r47580334_industry_5_ct
,max(case when rule_id = '47580334' and industry_display_name='大型消费金融公司' then count else -1 end) as r47580334_industry_6_ct
,max(case when rule_id = '47580334' and industry_display_name='综合类电商平台' then count else -1 end) as r47580334_industry_7_ct
,max(case when rule_id = '47580334' and industry_display_name='汽车租赁' then count else -1 end) as r47580334_industry_8_ct
,max(case when rule_id = '47580334' and industry_display_name='P2P网贷' then count else -1 end) as r47580334_industry_9_ct
,max(case when rule_id = '47580334' and industry_display_name='信息中介' then count else -1 end) as r47580334_industry_10_ct
,max(case when rule_id = '47580334' and industry_display_name='设备租赁' then count else -1 end) as r47580334_industry_11_ct
,max(case when rule_id = '47580334' and industry_display_name='财产保险' then count else -1 end) as r47580334_industry_12_ct
,max(case when rule_id = '47580334' and industry_display_name='互联网金融门户' then count else -1 end) as r47580334_industry_13_ct
,max(case when rule_id = '47580334' and industry_display_name='第三方服务商' then count else -1 end) as r47580334_industry_14_ct
,max(case when rule_id = '47580334' and industry_display_name='一般消费分期平台' then count else -1 end) as r47580334_industry_15_ct
,max(case when rule_id = '47580334' and industry_display_name='厂商汽车金融' then count else -1 end) as r47580334_industry_16_ct
,max(case when rule_id = '47580334' and industry_display_name='银行个人业务' then count else -1 end) as r47580334_industry_17_ct
,max(case when rule_id = '47580334' and industry_display_name='小额贷款公司' then count else -1 end) as r47580334_industry_18_ct
,max(case when rule_id = '47580334' and industry_display_name='O2O' then count else -1 end) as r47580334_industry_19_ct
,max(case when rule_id = '47580334' and industry_display_name='房地产金融' then count else -1 end) as r47580334_industry_20_ct
,max(case when rule_id = '47580334' and industry_display_name='融资租赁' then count else -1 end) as r47580334_industry_21_ct
,max(case when rule_id = '47580334' and industry_display_name='理财机构' then count else -1 end) as r47580334_industry_22_ct
,max(case when rule_id = '47580334' and industry_display_name='信用卡中心' then count else -1 end) as r47580334_industry_23_ct
,max(case when rule_id = '47580334' and industry_display_name='第三方支付' then count else -1 end) as r47580334_industry_24_ct
,max(case when rule_id = '47580324' then platform_count else -1 end) as r47580324_platform_ct
,max(case when rule_id = '47580324' and industry_display_name='大数据金融' then count else -1 end) as r47580324_industry_1_ct
,max(case when rule_id = '47580324' and industry_display_name='银行小微贷款' then count else -1 end) as r47580324_industry_2_ct
,max(case when rule_id = '47580324' and industry_display_name='直销银行' then count else -1 end) as r47580324_industry_3_ct
,max(case when rule_id = '47580324' and industry_display_name='网上银行' then count else -1 end) as r47580324_industry_4_ct
,max(case when rule_id = '47580324' and industry_display_name='银行消费金融公司' then count else -1 end) as r47580324_industry_5_ct
,max(case when rule_id = '47580324' and industry_display_name='大型消费金融公司' then count else -1 end) as r47580324_industry_6_ct
,max(case when rule_id = '47580324' and industry_display_name='综合类电商平台' then count else -1 end) as r47580324_industry_7_ct
,max(case when rule_id = '47580324' and industry_display_name='汽车租赁' then count else -1 end) as r47580324_industry_8_ct
,max(case when rule_id = '47580324' and industry_display_name='P2P网贷' then count else -1 end) as r47580324_industry_9_ct
,max(case when rule_id = '47580324' and industry_display_name='信息中介' then count else -1 end) as r47580324_industry_10_ct
,max(case when rule_id = '47580324' and industry_display_name='设备租赁' then count else -1 end) as r47580324_industry_11_ct
,max(case when rule_id = '47580324' and industry_display_name='财产保险' then count else -1 end) as r47580324_industry_12_ct
,max(case when rule_id = '47580324' and industry_display_name='互联网金融门户' then count else -1 end) as r47580324_industry_13_ct
,max(case when rule_id = '47580324' and industry_display_name='第三方服务商' then count else -1 end) as r47580324_industry_14_ct
,max(case when rule_id = '47580324' and industry_display_name='一般消费分期平台' then count else -1 end) as r47580324_industry_15_ct
,max(case when rule_id = '47580324' and industry_display_name='厂商汽车金融' then count else -1 end) as r47580324_industry_16_ct
,max(case when rule_id = '47580324' and industry_display_name='银行个人业务' then count else -1 end) as r47580324_industry_17_ct
,max(case when rule_id = '47580324' and industry_display_name='小额贷款公司' then count else -1 end) as r47580324_industry_18_ct
,max(case when rule_id = '47580324' and industry_display_name='O2O' then count else -1 end) as r47580324_industry_19_ct
,max(case when rule_id = '47580324' and industry_display_name='房地产金融' then count else -1 end) as r47580324_industry_20_ct
,max(case when rule_id = '47580324' and industry_display_name='融资租赁' then count else -1 end) as r47580324_industry_21_ct
,max(case when rule_id = '47580324' and industry_display_name='理财机构' then count else -1 end) as r47580324_industry_22_ct
,max(case when rule_id = '47580324' and industry_display_name='信用卡中心' then count else -1 end) as r47580324_industry_23_ct
,max(case when rule_id = '47580324' and industry_display_name='第三方支付' then count else -1 end) as r47580324_industry_24_ct
,max(case when rule_id = '47580354' then platform_count else -1 end) as r47580354_platform_ct
,max(case when rule_id = '47580354' and industry_display_name='大数据金融' then count else -1 end) as r47580354_industry_1_ct
,max(case when rule_id = '47580354' and industry_display_name='银行小微贷款' then count else -1 end) as r47580354_industry_2_ct
,max(case when rule_id = '47580354' and industry_display_name='直销银行' then count else -1 end) as r47580354_industry_3_ct
,max(case when rule_id = '47580354' and industry_display_name='网上银行' then count else -1 end) as r47580354_industry_4_ct
,max(case when rule_id = '47580354' and industry_display_name='银行消费金融公司' then count else -1 end) as r47580354_industry_5_ct
,max(case when rule_id = '47580354' and industry_display_name='大型消费金融公司' then count else -1 end) as r47580354_industry_6_ct
,max(case when rule_id = '47580354' and industry_display_name='综合类电商平台' then count else -1 end) as r47580354_industry_7_ct
,max(case when rule_id = '47580354' and industry_display_name='汽车租赁' then count else -1 end) as r47580354_industry_8_ct
,max(case when rule_id = '47580354' and industry_display_name='P2P网贷' then count else -1 end) as r47580354_industry_9_ct
,max(case when rule_id = '47580354' and industry_display_name='信息中介' then count else -1 end) as r47580354_industry_10_ct
,max(case when rule_id = '47580354' and industry_display_name='设备租赁' then count else -1 end) as r47580354_industry_11_ct
,max(case when rule_id = '47580354' and industry_display_name='财产保险' then count else -1 end) as r47580354_industry_12_ct
,max(case when rule_id = '47580354' and industry_display_name='互联网金融门户' then count else -1 end) as r47580354_industry_13_ct
,max(case when rule_id = '47580354' and industry_display_name='第三方服务商' then count else -1 end) as r47580354_industry_14_ct
,max(case when rule_id = '47580354' and industry_display_name='一般消费分期平台' then count else -1 end) as r47580354_industry_15_ct
,max(case when rule_id = '47580354' and industry_display_name='厂商汽车金融' then count else -1 end) as r47580354_industry_16_ct
,max(case when rule_id = '47580354' and industry_display_name='银行个人业务' then count else -1 end) as r47580354_industry_17_ct
,max(case when rule_id = '47580354' and industry_display_name='小额贷款公司' then count else -1 end) as r47580354_industry_18_ct
,max(case when rule_id = '47580354' and industry_display_name='O2O' then count else -1 end) as r47580354_industry_19_ct
,max(case when rule_id = '47580354' and industry_display_name='房地产金融' then count else -1 end) as r47580354_industry_20_ct
,max(case when rule_id = '47580354' and industry_display_name='融资租赁' then count else -1 end) as r47580354_industry_21_ct
,max(case when rule_id = '47580354' and industry_display_name='理财机构' then count else -1 end) as r47580354_industry_22_ct
,max(case when rule_id = '47580354' and industry_display_name='信用卡中心' then count else -1 end) as r47580354_industry_23_ct
,max(case when rule_id = '47580354' and industry_display_name='第三方支付' then count else -1 end) as r47580354_industry_24_ct
from (
	select t1.order_no, rule_id
	,cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_count' as int) as platform_count
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail' as json)) ::json ->> 'industry_display_name' as industry_display_name
	,cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail' as json)) ::json ->> 'count' as int) as count
	from (select * from t_loan_performance 
		  where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22' ) t1 
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580344', '47580334', '47580324', '47580354')
) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_duotou)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_duotou = raw.copy()
print(r_td_duotou.shape)
print(r_td_duotou.order_no.nunique())

#r_td_duotou_f = r_td_duotou.append(r_base0, ignore_index=True)
#r_td_duotou_f = r_td_duotou_f[list(r_td_duotou.columns)]

r_td_duotou_f = r_td_duotou.copy()

var_to_float = list(r_td_duotou_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_duotou_f[x] = r_td_duotou_f[x].astype(float)
print(r_td_duotou_f.dtypes)

''' 删除常数值变量 '''
var = list(r_td_duotou_f.iloc[:,1:].columns)
for x in var:
    if r_td_duotou_f[x].std() <=0.001:
        print(r_td_duotou_f[x].std(), x) 
    if pd.isnull(r_td_duotou_f[x].std()):
        print(r_td_duotou_f[x].std(), x)

r_td_duotou_f =  r_td_duotou_f.drop(['r47580324_industry_8_ct'
,'r47580324_industry_11_ct'
,'r47580324_industry_20_ct'
,'r47580354_industry_1_ct'
,'r47580354_industry_3_ct'
,'r47580354_industry_4_ct'
,'r47580354_industry_6_ct'
,'r47580354_industry_7_ct'
,'r47580354_industry_10_ct'
,'r47580354_industry_12_ct'
,'r47580354_industry_16_ct'
,'r47580354_industry_19_ct'
,'r47580354_industry_20_ct'
,'r47580354_industry_22_ct'
,'r47580354_industry_23_ct'
,'r47580354_industry_24_ct'], axis=1)

r_td_duotou_f.to_excel(filepath_out + "r_td_duotou_f.xlsx")


sql_td_blacklist = '''
select order_no
,max(case when rule_id='47579484' then 1 else -1 end) as r47579484_flag
,max(case when rule_id='47579284' then 1 else -1 end) as r47579284_flag
,max(case when rule_id='47579464' then 1 else -1 end) as r47579464_flag
,max(case when rule_id='47579544' then 1 else -1 end) as r47579544_flag
,max(case when rule_id='47580474' and fraud_type='agency' then 1 else -1 end) as r47580474_agency
,max(case when rule_id='47580474' and fraud_type='consumercreditRisk' then 1 else -1 end) as r47580474_consumercreditRisk
,max(case when rule_id='47580474' and fraud_type='creditSuspicious' then 1 else -1 end) as r47580474_creditSuspicious
,max(case when rule_id='47580474' and fraud_type='suspiciousLoan' then 1 else -1 end) as r47580474_suspiciousLoan
,max(case when rule_id='47580484' and fraud_type='agency' then 1 else -1 end) as r47580484_agency
,max(case when rule_id='47580484' and fraud_type='consumercreditRisk' then 1 else -1 end) as r47580484_consumercreditRisk
,max(case when rule_id='47580484' and fraud_type='creditSuspicious' then 1 else -1 end) as r47580484_creditSuspicious
,max(case when rule_id='47580484' and fraud_type='suspiciousLoan' then 1 else -1 end) as r47580484_suspiciousLoan
from (
	select t1.order_no, rule_id, risk_name
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'grey_list_details' as json)) ::json ->> 'fraud_type' as fraud_type
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'grey_list_details' as json)) ::json ->> 'risk_level' as risk_level
	,json_array_elements(cast(risk_detail as json)) ::json ->> 'fraud_type_display_name' as fraud_type_display_name
	from (select * from t_loan_performance 
		  where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22')t1
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580474','47580484','47579484','47579284','47579464','47579544')
) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_blacklist)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_blacklist = raw.copy()
print(r_td_blacklist.shape)
print(r_td_blacklist.order_no.nunique())

#r_td_blacklist_f = r_td_blacklist.append(r_base0, ignore_index=True)
#r_td_blacklist_f = r_td_blacklist_f[list(r_td_blacklist.columns)]

r_td_blacklist_f = r_td_blacklist.copy()

var_to_float = list(r_td_blacklist_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_blacklist_f[x] = r_td_blacklist_f[x].astype(float)
print(r_td_blacklist_f.dtypes)

''' 删除常数值变量 '''
var = list(r_td_blacklist_f.iloc[:,1:].columns)
for x in var:
    if (r_td_blacklist_f[x].std() <=0.001) | (pd.isnull(r_td_blacklist_f[x].std())):
        print(r_td_blacklist_f[x].std(), x) 

r_td_blacklist_f =  r_td_blacklist_f.drop(['r47579284_flag','r47579464_flag'], axis=1)

r_td_blacklist_f.to_excel(filepath_out + "r_td_blacklist_f.xlsx")


'''-- 关联亲属---'''
sql_td_relation = '''
select order_no
 , max(case when rule_id = '47580234' then result else -1 end) as r47580234_result
 , max(case when rule_id = '47580254' then result else -1 end) as r47580254_result
 , max(case when rule_id = '47580274' then result else -1 end) as r47580274_result
 from (
 select t1.order_no, rule_id, risk_detail
 ,cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'result' as int) as result
 from (select * from t_loan_performance 
		  where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22')t1
 inner join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
 inner join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580234','47580254','47580274')
   ) tmp
 group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_relation)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_relation = raw.copy()
print(r_td_relation.shape)
print(r_td_relation.order_no.nunique())

#r_td_relation_f = r_td_relation.append(r_base0, ignore_index=True)
#r_td_relation_f = r_td_relation_f[list(r_td_relation.columns)]

r_td_relation_f = r_td_relation.copy()

var_to_float = list(r_td_relation_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_relation_f[x] = r_td_relation_f[x].astype(float)
print(r_td_relation_f.dtypes)

''' 删除常数值变量 '''
var = list(r_td_relation_f.iloc[:,1:].columns)
for x in var:
    if (r_td_relation_f[x].std() <=0.001) | (pd.isnull(r_td_relation_f[x].std())):
        print(r_td_relation_f[x].std(), x) 

#r_td_relation_f =  r_td_relation_f.drop(['r47580254_result'], axis=1)

r_td_relation_f.to_excel(filepath_out + "r_td_relation_f.xlsx")


'''-- 模糊名单---4var'''
sql_td_list = '''
select order_no
,max(case when rule_id = '47579864' then 1 else -1 end) as r47579864_flag
,max(case when rule_id = '47579884' then 1 else -1 end) as r47579884_flag
,max(case when rule_id = '47579834' then 1 else -1 end) as r47579834_flag
,max(case when rule_id = '47579804' then 1 else -1 end) as r47579804_flag
from (
	select t1.order_no, rule_id, risk_detail
	from (select * from t_loan_performance 
		  where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22') t1
	inner join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	inner join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47579864','47579884','47579834','47579804')
) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_list)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_list = raw.copy()
print(r_td_list.shape)
print(r_td_list.order_no.nunique())

#r_td_list_f = r_td_list.append(r_base0, ignore_index=True)
#r_td_list_f = r_td_list_f[list(r_td_list.columns)]

r_td_list_f = r_td_list.copy()

var_to_float = list(r_td_list_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_list_f[x] = r_td_list_f[x].astype(float)
print(r_td_list_f.dtypes)

''' 删除常数值变量 '''
var = list(r_td_list_f.iloc[:,1:].columns)
for x in var:
    if (r_td_list_f[x].std() <=0.001) | (pd.isnull(r_td_list_f[x].std())):
        print(r_td_list_f[x].std(), x) 

r_td_list_f =  r_td_list_f.drop(['r47579804_flag'], axis=1)

r_td_list_f.to_excel(filepath_out + "r_td_list_f.xlsx")


'''----多个关联---6 var'''
sql_td_connect = '''
select order_no
,max(case when rule_id = '47580034' and sub_dim_type='accountMobile' then count else -1 end) as r47580034_ct
,max(case when rule_id = '47580054' and sub_dim_type='idNumber' then count else -1 end) as r47580054_ct
,max(case when rule_id = '47580154' then count else '-1' end) as r47580154_ct
,max(case when rule_id = '47580194' and note='1天内身份证关联设备数' then count else -1 end) as r47580194_ct
,max(case when rule_id = '47580204' and note='7天内身份证关联设备数' then count else -1 end) as r47580204_ct
,max(case when rule_id = '47580214' and note='1个月内身份证关联设备数' then count else -1 end) as r47580214_ct
from (
	select t1.order_no, rule_id, risk_detail
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'frequency_detail_list' as json)) ::json ->> 'note' as note
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'frequency_detail_list' as json)) ::json ->> 'dim_type' as dim_type
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'frequency_detail_list' as json)) ::json ->> 'sub_dim_type' as sub_dim_type
	,cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'frequency_detail_list' as json)) ::json ->> 'count' as int) as count
	from (select * from t_loan_performance 
		  where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22') t1
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580194','47580204','47580214','47580054','47580034','47580154')
) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_connect)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_connect = raw.copy()
print(r_td_connect.shape)
print(r_td_connect.order_no.nunique())

#r_td_connect_f = r_td_connect.append(r_base0, ignore_index=True)
#r_td_connect_f = r_td_connect_f[list(r_td_connect.columns)]

r_td_connect_f = r_td_connect.copy()

var_to_float = list(r_td_connect_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_connect_f[x] = r_td_connect_f[x].astype(float)
print(r_td_connect_f.dtypes)

''' 删除常数值变量 '''
var = list(r_td_connect_f.iloc[:,1:].columns)
for x in var:
    if (r_td_connect_f[x].std() <=0.001) | (pd.isnull(r_td_connect_f[x].std())):
        print(r_td_connect_f[x].std(), x) 

r_td_connect_f =  r_td_connect_f.drop(['r47580154_ct','r47580204_ct','r47580214_ct'], axis=1)

r_td_connect_f.to_excel(filepath_out + "r_td_connect_f.xlsx")


'''---高风险地区---1'''
sql_td_highriskarea = '''
select order_no
,max(case when rule_id is not null then 1 else -1 end) as r47579154_flag
from (
	select t1.order_no, rule_id, risk_detail
	from (select * from t_loan_performance 
		  where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22') t1 
	inner join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	inner join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47579154')
) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_highriskarea)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_highriskarea = raw.copy()
print(r_td_highriskarea.shape)
print(r_td_highriskarea.order_no.nunique())

#r_td_highriskarea_f = r_td_highriskarea.append(r_base0, ignore_index=True)
#r_td_highriskarea_f = r_td_highriskarea_f[list(r_td_highriskarea.columns)]

r_td_highriskarea_f = r_td_highriskarea.copy()

var_to_float = list(r_td_highriskarea_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_highriskarea_f[x] = r_td_highriskarea_f[x].astype(float)
print(r_td_highriskarea_f.dtypes)

''' 删除常数值变量 '''
var = list(r_td_highriskarea_f.iloc[:,1:].columns)
for x in var:
    if (r_td_highriskarea_f[x].std() <=0.001) | (pd.isnull(r_td_highriskarea_f[x].std())):
        print(r_td_highriskarea_f[x].std(), x) 

r_td_highriskarea_f =  r_td_highriskarea_f.drop(['r47579154_flag'], axis=1)

r_td_highriskarea_f.to_excel(filepath_out + "r_td_highriskarea_f.xlsx")



'''---一二阶联系人---'''
sql_td_contact = '''
select order_no
,max(case when rule_id='47580084' and note='3个月内申请人身份证作为第一联系人身份证出现的次数' then count else -1 end) as r47580084_1
,max(case when rule_id='47580084' and note='3个月内申请人身份证作为第二联系人身份证出现的次数' then count else -1 end) as r47580084_2
,max(case when rule_id='47580094' and note='3个月内申请人手机号作为第一联系人手机号出现的次数' then count else -1 end) as r47580094_1
,max(case when rule_id='47580094' and note='3个月内申请人手机号作为第二联系人手机号出现的次数' then count else -1 end) as r47580094_2
from (
	select t1.order_no, rule_id, risk_detail
    ,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'cross_frequency_detail_list' as json)) ::json ->>'note' as note
	,cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'cross_frequency_detail_list' as json)) ::json ->>'count' as int) as count
	from (select * from t_loan_performance 
		  where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22') t1 
	inner join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	inner join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580094','47580084')
) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_contact)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_contact = raw.copy()
print(r_td_contact.shape)
print(r_td_contact.order_no.nunique())

#r_td_contact_f = r_td_contact.append(r_base0, ignore_index=True)
#r_td_contact_f = r_td_contact_f[list(r_td_contact.columns)]

r_td_contact_f = r_td_contact.copy()

var_to_float = list(r_td_contact_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_contact_f[x] = r_td_contact_f[x].astype(float)
print(r_td_contact_f.dtypes)

''' 删除常数值变量 '''
var = list(r_td_contact_f.iloc[:,1:].columns)
for x in var:
    if (r_td_contact_f[x].std() <=0.001) | (pd.isnull(r_td_contact_f[x].std())):
        print(r_td_contact_f[x].std(), x) 

r_td_contact_f =  r_td_contact_f.drop(['r47580084_2'], axis=1)

r_td_contact_f.to_excel(filepath_out + "r_td_contact_f.xlsx")
    

'''---分数---'''
sql_td_score = '''
select t1.order_no
,max(final_score) final_score
,max(case when rule_id = '47580344' then score else '-1' end) r47580344_score
,max(case when rule_id = '47580334' then score else '-1' end) r47580334_score
,max(case when rule_id = '47580324' then score else '-1' end) r47580324_score
,max(case when rule_id = '47580474' then score else '-1' end) r47580474_score
,max(case when rule_id = '47580354' then score else '-1' end) r47580354_score
,max(case when rule_id = '47580484' then score else '-1' end) r47580484_score
,max(case when rule_id = '47580034' then score else '-1' end) r47580034_score
,max(case when rule_id = '47580094' then score else '-1' end) r47580094_score
,max(case when rule_id = '47580044' then score else '-1' end) r47580044_score
,max(case when rule_id = '47580234' then score else '-1' end) r47580234_score
,max(case when rule_id = '47580054' then score else '-1' end) r47580054_score
,max(case when rule_id = '47580254' then score else '-1' end) r47580254_score
,max(case when rule_id = '47579154' then score else '-1' end) r47579154_score
,max(case when rule_id = '47579484' then score else '-1' end) r47579484_score
,max(case when rule_id = '47580194' then score else '-1' end) r47580194_score
,max(case when rule_id = '47579864' then score else '-1' end) r47579864_score
,max(case when rule_id = '47579734' then score else '-1' end) r47579734_score
,max(case when rule_id = '47579524' then score else '-1' end) r47579524_score
,max(case when rule_id = '47579544' then score else '-1' end) r47579544_score
,max(case when rule_id = '47580204' then score else '-1' end) r47580204_score
,max(case when rule_id = '47580274' then score else '-1' end) r47580274_score
,max(case when rule_id = '47579464' then score else '-1' end) r47579464_score
,max(case when rule_id = '47579564' then score else '-1' end) r47579564_score
,max(case when rule_id = '47579884' then score else '-1' end) r47579884_score
,max(case when rule_id = '47580214' then score else '-1' end) r47580214_score
,max(case when rule_id = '47579284' then score else '-1' end) r47579284_score
,max(case when rule_id = '47580014' then score else '-1' end) r47580014_score
,max(case when rule_id = '47579834' then score else '-1' end) r47579834_score
,max(case when rule_id = '47580084' then score else '-1' end) r47580084_score
,max(case when rule_id = '47579804' then score else '-1' end) r47579804_score
,max(case when rule_id = '47579994' then score else '-1' end) r47579994_score
,max(case when rule_id = '47580224' then score else '-1' end) r47580224_score
,max(case when rule_id = '47580024' then score else '-1' end) r47580024_score
,max(case when rule_id = '47579764' then score else '-1' end) r47579764_score
,max(case when rule_id = '47580154' then score else '-1' end) r47580154_score
,max(case when rule_id = '47579714' then score else '-1' end) r47579714_score
,max(case when rule_id = '47580344' then policy_score else '-1' end) r47580344_policy_score
,max(case when rule_id = '47580334' then policy_score else '-1' end) r47580334_policy_score
,max(case when rule_id = '47580324' then policy_score else '-1' end) r47580324_policy_score
,max(case when rule_id = '47580474' then policy_score else '-1' end) r47580474_policy_score
,max(case when rule_id = '47580354' then policy_score else '-1' end) r47580354_policy_score
,max(case when rule_id = '47580484' then policy_score else '-1' end) r47580484_policy_score
,max(case when rule_id = '47580034' then policy_score else '-1' end) r47580034_policy_score
,max(case when rule_id = '47580094' then policy_score else '-1' end) r47580094_policy_score
,max(case when rule_id = '47580044' then policy_score else '-1' end) r47580044_policy_score
,max(case when rule_id = '47580234' then policy_score else '-1' end) r47580234_policy_score
,max(case when rule_id = '47580054' then policy_score else '-1' end) r47580054_policy_score
,max(case when rule_id = '47580254' then policy_score else '-1' end) r47580254_policy_score
,max(case when rule_id = '47579154' then policy_score else '-1' end) r47579154_policy_score
,max(case when rule_id = '47579484' then policy_score else '-1' end) r47579484_policy_score
,max(case when rule_id = '47580194' then policy_score else '-1' end) r47580194_policy_score
,max(case when rule_id = '47579864' then policy_score else '-1' end) r47579864_policy_score
,max(case when rule_id = '47579734' then policy_score else '-1' end) r47579734_policy_score
,max(case when rule_id = '47579524' then policy_score else '-1' end) r47579524_policy_score
,max(case when rule_id = '47579544' then policy_score else '-1' end) r47579544_policy_score
,max(case when rule_id = '47580204' then policy_score else '-1' end) r47580204_policy_score
,max(case when rule_id = '47580274' then policy_score else '-1' end) r47580274_policy_score
,max(case when rule_id = '47579464' then policy_score else '-1' end) r47579464_policy_score
,max(case when rule_id = '47579564' then policy_score else '-1' end) r47579564_policy_score
,max(case when rule_id = '47579884' then policy_score else '-1' end) r47579884_policy_score
,max(case when rule_id = '47580214' then policy_score else '-1' end) r47580214_policy_score
,max(case when rule_id = '47579284' then policy_score else '-1' end) r47579284_policy_score
,max(case when rule_id = '47580014' then policy_score else '-1' end) r47580014_policy_score
,max(case when rule_id = '47579834' then policy_score else '-1' end) r47579834_policy_score
,max(case when rule_id = '47580084' then policy_score else '-1' end) r47580084_policy_score
,max(case when rule_id = '47579804' then policy_score else '-1' end) r47579804_policy_score
,max(case when rule_id = '47579994' then policy_score else '-1' end) r47579994_policy_score
,max(case when rule_id = '47580224' then policy_score else '-1' end) r47580224_policy_score
,max(case when rule_id = '47580024' then policy_score else '-1' end) r47580024_policy_score
,max(case when rule_id = '47579764' then policy_score else '-1' end) r47579764_policy_score
,max(case when rule_id = '47580154' then policy_score else '-1' end) r47580154_policy_score
,max(case when rule_id = '47579714' then policy_score else '-1' end) r47579714_policy_score
from (select * from t_loan_performance 
	  where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22') t1
left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid
group by t1.order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_score)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_score = raw.copy()
print(r_td_score.shape)
print(r_td_score.order_no.nunique())

#r_td_score_f = r_td_score.append(r_base0, ignore_index=True)
#r_td_score_f = r_td_score_f[list(r_td_score.columns)]

r_td_score_f = r_td_score.copy()

var_to_float = list(r_td_score_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_score_f[x] = r_td_score_f[x].astype(float)
print(r_td_score_f.dtypes)

#''' 删除常数值变量 '''
var = list(r_td_score_f.iloc[:,1:].columns)
for x in var:
     if (r_td_score_f[x].std() <=0.001) | (pd.isnull(r_td_score_f[x].std())):
        print(r_td_score_f[x].std(), x) 

r_td_score_f =  r_td_score_f.drop(['r47580204_score'
,'r47580214_score'
,'r47579804_score'
,'r47579994_score'
,'r47580224_score'
,'r47580024_score'
,'r47579764_score'
,'r47580154_score'
,'r47579714_score'
,'r47580204_policy_score'
,'r47580214_policy_score'
,'r47579804_policy_score'
,'r47579994_policy_score'
,'r47580224_policy_score'
,'r47580024_policy_score'
,'r47579764_policy_score'
,'r47580154_policy_score'
,'r47579714_policy_score'], axis=1)

r_td_score_f.to_excel(filepath_out + "r_td_score_f.xlsx")


'''同盾贷前审核 合并'''
r_td_loanreview_f = pd.DataFrame(r_base['order_no'])
r_td_loanreview_f = r_td_loanreview_f.merge(r_td_duotou_f, how='left', on='order_no')
r_td_loanreview_f = r_td_loanreview_f.merge(r_td_blacklist_f, how='left', on='order_no')
r_td_loanreview_f = r_td_loanreview_f.merge(r_td_connect_f, how='left', on='order_no')
r_td_loanreview_f = r_td_loanreview_f.merge(r_td_contact_f, how='left', on='order_no')
r_td_loanreview_f = r_td_loanreview_f.merge(r_td_highriskarea_f, how='left', on='order_no')
r_td_loanreview_f = r_td_loanreview_f.merge(r_td_list_f, how='left', on='order_no')
r_td_loanreview_f = r_td_loanreview_f.merge(r_td_relation_f, how='left', on='order_no')
r_td_loanreview_f = r_td_loanreview_f.merge(r_td_score_f, how='left', on='order_no')

r_td_loanreview_f.to_excel(filepath_out + "r_td_loanreview_f.xlsx")

'''--同盾小额现金贷分---'''
sql_td_microscore = '''
select t0.order_no, micro_cash_score
from t_loan_performance t0
left join risk_mongo_installmentmessagerelated t1 on t0.order_no = t1.orderno and topicname in ('Application_thirdPart','Application_thirdPart_tongduncash') and databasename = 'installmentTongdunCash' 
left join bakrt_tongdun_cashscore_result t2 on t1.messageno = t2.taskid
where t0.dt='20191108' and t0.business_id in ('tb', 'xjbk', 'rong360') and t0.effective_date between '2019-07-04' and '2019-10-22'
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td_microscore)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_td_microscore = raw.copy()
print(r_td_microscore.shape)
print(r_td_microscore.order_no.nunique())

#r_td_microscore_f = r_td_microscore.append(r_base0, ignore_index=True)
#r_td_microscore_f = r_td_microscore_f[list(r_td_microscore.columns)]

r_td_microscore_f = r_td_microscore.copy()

var_to_float = list(r_td_microscore_f.iloc[:,1:].columns)
for x in var_to_float:
    r_td_microscore_f[x] = r_td_microscore_f[x].astype(float)
print(r_td_microscore_f.dtypes)

#''' 删除常数值变量 '''
var = list(r_td_microscore_f.iloc[:,1:].columns)
for x in var:
     if (r_td_microscore_f[x].std() <=0.001) | (pd.isnull(r_td_microscore_f[x].std())):
        print(r_td_microscore_f[x].std(), x) 

r_td_microscore_f.to_excel(filepath_out + "r_td_microscore_f.xlsx")



'''信德'''   
sql_xd = '''
with xd as (
select order_no, createtime
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'firstLoanTime' as firstLoanTime
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'totalLoanCount' as totalLoanCount
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'totalOverDueCount' as totalOverDueCount
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'longestOverDuePaid' as longestOverDuePaid
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'currentOverDueCount' as currentOverDueCount
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'currentOverDueAmount' as currentOverDueAmount
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'longestOverDueUnpaid' as longestOverDueUnpaid
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'isLastLoanRefused' as isLastLoanRefused
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'lastLoanRefuseTime' as lastLoanRefuseTime
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{advanceResult}' as json)::json ->>0 as json)::json ->> 'overDue90ContactsCount' as overDue90ContactsCount
	from bk_xindeblacklist_application where oss<>''
union 
select order_no , createtime
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'firstLoanTime' as firstLoanTime
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'totalLoanCount' as totalLoanCount
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'totalOverDueCount' as totalOverDueCount
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'longestOverDuePaid' as longestOverDuePaid
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'currentOverDueCount' as currentOverDueCount
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'currentOverDueAmount' as currentOverDueAmount
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'longestOverDueUnpaid' as longestOverDueUnpaid
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'isLastLoanRefused' as isLastLoanRefused
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json)::json ->>0 as json)::json ->> 'lastLoanRefuseTime' as lastLoanRefuseTime
, cast(cast(cast(oss::json #>>'{result}' as json):: json #>>'{advanceResult}' as json)::json ->>0 as json)::json ->> 'overDue90ContactsCount' as overDue90ContactsCount
from dc_xindeblacklist_application where oss<>'' 
)
select t0.order_no
,overDue90ContactsCount
,case when firstLoanTime = '' then -9999
	  else date(createtime) - date(firstLoanTime) 
	  end as firstLoanTimediff
,totalLoanCount
,totalOverDueCount
,case longestOverDuePaid when 'M1' then 1 when 'M2' then 2 when 'M3' then 3 when 'M4' then 4 else -1 end as longestOverDuePaid
,currentOverDueCount
,currentOverDueAmount
,case longestOverDueUnpaid when 'M1' then 1 when 'M2' then 2 when 'M3' then 3 when 'M4' then 4 else -1 end as longestOverDueUnpaid
,case when isLastLoanRefused is null then null
	  when isLastLoanRefused = 'true' then 1
	  else 0
	  end as isLastLoanRefused_flag
,case when lastLoanRefuseTime = '' or lastLoanRefuseTime is null then -9999
      else round((date(createtime) - date(concat(substring(lastLoanRefuseTime,1,4),'-',substring(lastLoanRefuseTime,5,7),'-','01')))/30,1)
      end as lastLoanRefuseTimediff 
from t_loan_performance t0
left join xd on t0.order_no = xd.order_no 
where t0.dt='20191108' and t0.business_id in ('tb', 'xjbk', 'rong360') and t0.effective_date between '2019-07-04' and '2019-10-22'  
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_xd)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_xd = raw.copy()
print(r_xd.shape)
print(r_xd.order_no.nunique())   

r_xd_f = r_xd.append(r_base0, ignore_index=True)
r_xd_f = r_xd_f[list(r_xd.columns)]

r_xd_f = r_xd.copy()

var_to_float = list(r_xd_f.iloc[:,1:].columns)
for x in var_to_float:
    r_xd_f[x] = r_xd_f[x].astype(float)
print(r_xd_f.dtypes) 

''' 删除常数值变量 '''
var = list(r_xd_f.iloc[:,1:].columns)
for x in var:
     if (r_xd_f[x].std() <=0.001) | (pd.isnull(r_xd_f[x].std())):
        print(r_xd_f[x].std(), x) 

r_xd_f =  r_xd_f.drop(['longestoverdueunpaid'], axis=1)

r_xd_f.to_excel(filepath_out + "r_xd_f.xlsx")



'''----------新颜-----------'''
sql_xy = """
with xy as (
select order_no, createtime
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_count}', oss::json #>> '{data,report_detail,loans_count}') as loans_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_long_time}', oss::json #>> '{data,report_detail,loans_long_time}') as loans_long_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,consfin_org_count}', oss::json #>> '{data,report_detail,consfin_org_count}') as consfin_org_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_cash_count}', oss::json #>> '{data,report_detail,loans_cash_count}') as loans_cash_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_six_month}', oss::json #>> '{data,report_detail,latest_six_month}') as latest_six_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,history_fail_fee}', oss::json #>> '{data,report_detail,history_fail_fee}') as history_fail_fee
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_three_month}', oss::json #>> '{data,report_detail,latest_three_month}') as latest_three_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month_fail}', oss::json #>> '{data,report_detail,latest_one_month_fail}') as latest_one_month_fail
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month}', oss::json #>> '{data,report_detail,latest_one_month}') as latest_one_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month_suc}', oss::json #>> '{data,report_detail,latest_one_month_suc}') as latest_one_month_suc
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_latest_time}', oss::json #>> '{data,report_detail,loans_latest_time}') as loans_latest_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_org_count}', oss::json #>> '{data,report_detail,loans_org_count}') as loans_org_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,history_suc_fee}', oss::json #>> '{data,report_detail,history_suc_fee}') as history_suc_fee
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_credibility}', oss::json #>> '{data,report_detail,loans_credibility}') as loans_credibility
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_score}', oss::json #>> '{data,report_detail,loans_score}') as loans_score
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_settle_count}', oss::json #>> '{data,report_detail,loans_settle_count}') as loans_settle_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_overdue_count}', oss::json #>> '{data,report_detail,loans_overdue_count}') as loans_overdue_count
from dc_xinyan_application  where oss <>'' 
union 
select order_no, createtime
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_count}', oss::json #>> '{data,report_detail,loans_count}') as loans_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_long_time}', oss::json #>> '{data,report_detail,loans_long_time}') as loans_long_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,consfin_org_count}', oss::json #>> '{data,report_detail,consfin_org_count}') as consfin_org_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_cash_count}', oss::json #>> '{data,report_detail,loans_cash_count}') as loans_cash_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_six_month}', oss::json #>> '{data,report_detail,latest_six_month}') as latest_six_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,history_fail_fee}', oss::json #>> '{data,report_detail,history_fail_fee}') as history_fail_fee
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_three_month}', oss::json #>> '{data,report_detail,latest_three_month}') as latest_three_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month_fail}', oss::json #>> '{data,report_detail,latest_one_month_fail}') as latest_one_month_fail
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month}', oss::json #>> '{data,report_detail,latest_one_month}') as latest_one_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month_suc}', oss::json #>> '{data,report_detail,latest_one_month_suc}') as latest_one_month_suc
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_latest_time}', oss::json #>> '{data,report_detail,loans_latest_time}') as loans_latest_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_org_count}', oss::json #>> '{data,report_detail,loans_org_count}') as loans_org_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,history_suc_fee}', oss::json #>> '{data,report_detail,history_suc_fee}') as history_suc_fee
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_credibility}', oss::json #>> '{data,report_detail,loans_credibility}') as loans_credibility
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_score}', oss::json #>> '{data,report_detail,loans_score}') as loans_score
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_settle_count}', oss::json #>> '{data,report_detail,loans_settle_count}') as loans_settle_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_overdue_count}', oss::json #>> '{data,report_detail,loans_overdue_count}') as loans_overdue_count
from bk_xinyan_application  where oss <>'' 
)
select t0.order_no
, loans_count
, loans_long_time
, consfin_org_count
, loans_cash_count
, latest_six_month
, history_fail_fee
, latest_three_month
, latest_one_month_fail
, latest_one_month
, latest_one_month_suc
, loans_org_count
, history_suc_fee
, loans_credibility
, loans_score
, loans_settle_count
, loans_overdue_count 
, case when loans_latest_time in ('-9999','-9998') then -9999
	  else date(createtime) - date(loans_latest_time) 
	  end as loans_latest_timediff
from (
	select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
	--union 
	--select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
) t0
left join xy t2 on t0.order_no = t2.order_no
"""
conn.rollback()
cur = conn.cursor()
cur.execute(sql_xy)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_xy = raw.copy()
print(r_xy.shape)
print(r_xy.order_no.nunique())

r_xy_f = r_xy.copy()

var_to_float = list(r_xy_f.iloc[:,1:].columns)
for x in var_to_float:
    r_xy_f[x] = r_xy_f[x].astype(float)
print(r_xy_f.dtypes)

''' 删除常数值变量 '''
var = list(r_xy_f.iloc[:,1:].columns)
for x in var:
     if (r_xy_f[x].std() <=0.001) | (pd.isnull(r_xy_f[x].std())):
        print(r_xy_f[x].std(), x) 

r_xy_f.to_excel(filepath_out + "r_xy_f.xlsx")


'''----------聚信立-----------'''
sql_jxl1 = """
select t0.order_no
,coalesce(bk.phone_gray_score , dc.phone_gray_score ) phone_gray_score
,coalesce(bk.social_liveness , dc.social_liveness ) social_liveness
,case when coalesce(bk.recent_active_time, dc.recent_active_time)='-9999' then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.recent_active_time, dc.recent_active_time)) end as recent_active_time_diff
,coalesce(bk.social_influence, dc.social_influence) social_influence
,coalesce(bk.most_familiar_to_all, dc.most_familiar_to_all) most_familiar_to_all
,coalesce(bk.most_familiar_be_all , dc.most_familiar_be_all ) most_familiar_be_all
,coalesce(bk.most_familiar_all, dc.most_familiar_all) most_familiar_all
,coalesce(bk.most_familiar_to_applied , dc.most_familiar_to_applied ) most_familiar_to_applied
,coalesce(bk.most_familiar_be_applied , dc.most_familiar_be_applied ) most_familiar_be_applied
,coalesce(bk.most_familiar_applied , dc.most_familiar_applied ) most_familiar_applied
,coalesce(bk.to_max, dc.to_max) to_max
,coalesce(bk.to_mean, dc.to_mean) to_mean
,coalesce(bk.to_min, dc.to_min) to_min
,coalesce(bk.be_max, dc.be_max) be_max
,coalesce(bk.be_mean, dc.be_mean) be_mean
,coalesce(bk.be_min, dc.be_min) be_min
,coalesce(bk.max, dc.max) max
,coalesce(bk.mean, dc.mean) mean
,coalesce(bk.min, dc.min) min
,coalesce(bk.to_is_familiar, dc.to_is_familiar) to_is_familiar
,coalesce(bk.to_median_familiar, dc.to_median_familiar) to_median_familiar
,coalesce(bk.to_not_familiar, dc.to_not_familiar) to_not_familiar
,coalesce(bk.be_is_familiar, dc.be_is_familiar) be_is_familiar
,coalesce(bk.be_median_familiar, dc.be_median_familiar) be_median_familiar
,coalesce(bk.be_not_familiar, dc.be_not_familiar) be_not_familiar
,coalesce(bk.is_familiar, dc.is_familiar) is_familiar
,coalesce(bk.median_familiar, dc.median_familiar) median_familiar
,coalesce(bk.not_familiar, dc.not_familiar) not_familiar
,case when coalesce(bk.recent_time_to_all, dc.recent_time_to_all)='-9999' then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.recent_time_to_all, dc.recent_time_to_all)) end as recent_time_to_all_diff
,case when coalesce(bk.recent_time_be_all, dc.recent_time_be_all) ='-9999' then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.recent_time_be_all, dc.recent_time_be_all)) end as recent_time_be_all_diff
,case when coalesce(bk.recent_time_to_black, dc.recent_time_to_black)='-9999' then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.recent_time_to_black, dc.recent_time_to_black)) end as recent_time_to_black_diff
,case when coalesce(bk.recent_time_be_black, dc.recent_time_be_black) ='-9999' then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.recent_time_be_black, dc.recent_time_be_black)) end as recent_time_be_black_diff
,case when coalesce(bk.recent_time_to_applied, dc.recent_time_to_applied) ='-9999' then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.recent_time_to_applied, dc.recent_time_to_applied)) end as recent_time_to_applied_diff 
,case when coalesce(bk.recent_time_be_applied, dc.recent_time_be_applied) ='-9999' then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.recent_time_be_applied, dc.recent_time_be_applied)) end as recent_time_be_applied_diff 
,coalesce(bk.call_cnt_to_all  , dc.call_cnt_to_all  ) call_cnt_to_all
,coalesce(bk.call_cnt_be_all  , dc.call_cnt_be_all  ) call_cnt_be_all
,coalesce(bk.call_cnt_to_black  , dc.call_cnt_to_black  ) call_cnt_to_black
,coalesce(bk.call_cnt_be_black , dc.call_cnt_be_black ) call_cnt_be_black
,coalesce(bk.call_cnt_to_applied, dc.call_cnt_to_applied) call_cnt_to_applied
,coalesce(bk.call_cnt_be_applied, dc.call_cnt_be_applied) call_cnt_be_applied
,coalesce(bk.time_spent_to_all, dc.time_spent_to_all) time_spent_to_all
,coalesce(bk.time_spent_be_all, dc.time_spent_be_all) time_spent_be_all
,coalesce(bk.time_spent_to_black , dc.time_spent_to_black ) time_spent_to_black
,coalesce(bk.time_spent_be_black, dc.time_spent_be_black) time_spent_be_black
,coalesce(bk.time_spent_to_applied , dc.time_spent_to_applied ) time_spent_to_applied
,coalesce(bk.time_spent_be_applied , dc.time_spent_be_applied ) time_spent_be_applied
,coalesce(bk.cnt_to_all, dc.cnt_to_all) cnt_to_all
,coalesce(bk.cnt_be_all, dc.cnt_be_all) cnt_be_all
,coalesce(bk.cnt_all , dc.cnt_all ) cnt_all 
,coalesce(bk.cnt_router, dc.cnt_router) cnt_router
,coalesce(bk.router_ratio, dc.router_ratio) router_ratio
,coalesce(bk.cnt_to_black, dc.cnt_to_black) cnt_to_black
,coalesce(bk.cnt_be_black, dc.cnt_be_black) cnt_be_black
,coalesce(bk.cnt_black, dc.cnt_black) cnt_black
,coalesce(bk.black_ratio, dc.black_ratio) black_ratio
,coalesce(bk.cnt_black2, dc.cnt_black2) cnt_black2
,coalesce(bk.cnt_to_applied, dc.cnt_to_applied) cnt_to_applied
,coalesce(bk.cnt_be_applied , dc.cnt_be_applied ) cnt_be_applied 
,coalesce(bk.cnt_applied , dc.cnt_applied ) cnt_applied 
,coalesce(bk.pct_cnt_to_all , dc.pct_cnt_to_all ) pct_cnt_to_all 
,coalesce(bk.pct_cnt_be_all, dc.pct_cnt_be_all) pct_cnt_be_all
,coalesce(bk.pct_cnt_all , dc.pct_cnt_all ) pct_cnt_all 
,coalesce(bk.pct_cnt_router, dc.pct_cnt_router) pct_cnt_router
,coalesce(bk.pct_router_ratio, dc.pct_router_ratio) pct_router_ratio
,coalesce(bk.pct_cnt_to_black , dc.pct_cnt_to_black ) pct_cnt_to_black 
,coalesce(bk.pct_cnt_be_black, dc.pct_cnt_be_black) pct_cnt_be_black
,coalesce(bk.pct_cnt_black, dc.pct_cnt_black) pct_cnt_black
,coalesce(bk.pct_black_ratio, dc.pct_black_ratio) pct_black_ratio
,coalesce(bk.pct_cnt_black2, dc.pct_cnt_black2) pct_cnt_black2
,coalesce(bk.pct_cnt_to_applied , dc.pct_cnt_to_applied ) pct_cnt_to_applied 
,coalesce(bk.pct_cnt_be_applied, dc.pct_cnt_be_applied) pct_cnt_be_applied
,coalesce(bk.pct_cnt_applied, dc.pct_cnt_applied) pct_cnt_applied
,case when coalesce(bk.to_recent_query_time, dc.to_recent_query_time) ='-9999' then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.to_recent_query_time, dc.to_recent_query_time)) end as to_recent_query_time_diff
,case when coalesce(bk.be_recent_query_time, dc.be_recent_query_time) ='-9999' then -9999
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.be_recent_query_time, dc.be_recent_query_time)) end as be_recent_query_time_diff
,coalesce(bk.to_query_cnt_05 , dc.to_query_cnt_05 ) to_query_cnt_05 
,coalesce(bk.be_query_cnt_05 , dc.be_query_cnt_05 ) be_query_cnt_05 
,coalesce(bk.query_cnt_05 , dc.query_cnt_05 ) query_cnt_05 
,coalesce(bk.to_query_cnt_1 , dc.to_query_cnt_1 ) to_query_cnt_1 
,coalesce(bk.be_query_cnt_1 , dc.be_query_cnt_1 ) be_query_cnt_1 
,coalesce(bk.query_cnt_1, dc.query_cnt_1) query_cnt_1
,coalesce(bk.to_query_cnt_2, dc.to_query_cnt_2) to_query_cnt_2
,coalesce(bk.be_query_cnt_2, dc.be_query_cnt_2) be_query_cnt_2
,coalesce(bk.query_cnt_2, dc.query_cnt_2) query_cnt_2
,coalesce(bk.to_query_cnt_3, dc.to_query_cnt_3) to_query_cnt_3
,coalesce(bk.be_query_cnt_3, dc.be_query_cnt_3) be_query_cnt_3
,coalesce(bk.query_cnt_3, dc.query_cnt_3) query_cnt_3
,coalesce(bk.to_query_cnt_6 , dc.to_query_cnt_6 ) to_query_cnt_6 
,coalesce(bk.be_query_cnt_6, dc.be_query_cnt_6) be_query_cnt_6
,coalesce(bk.query_cnt_6 , dc.query_cnt_6 ) query_cnt_6 
,coalesce(bk.to_query_cnt_9, dc.to_query_cnt_9) to_query_cnt_9
,coalesce(bk.be_query_cnt_9, dc.be_query_cnt_9) be_query_cnt_9
,coalesce(bk.query_cnt_9, dc.query_cnt_9) query_cnt_9
,coalesce(bk.to_query_cnt_12, dc.to_query_cnt_12) to_query_cnt_12
,coalesce(bk.be_query_cnt_12, dc.be_query_cnt_12) be_query_cnt_12
,coalesce(bk.query_cnt_12, dc.query_cnt_12) query_cnt_12
,coalesce(bk.to_org_cnt_05, dc.to_org_cnt_05) to_org_cnt_05
,coalesce(bk.be_org_cnt_05, dc.be_org_cnt_05) be_org_cnt_05
,coalesce(bk.org_cnt_05 , dc.org_cnt_05 ) org_cnt_05 
,coalesce(bk.to_org_cnt_1, dc.to_org_cnt_1) to_org_cnt_1
,coalesce(bk.be_org_cnt_1 , dc.be_org_cnt_1 ) be_org_cnt_1 
,coalesce(bk.org_cnt_1, dc.org_cnt_1) org_cnt_1
,coalesce(bk.to_org_cnt_2, dc.to_org_cnt_2) to_org_cnt_2
,coalesce(bk.be_org_cnt_2 , dc.be_org_cnt_2 ) be_org_cnt_2 
,coalesce(bk.org_cnt_2 , dc.org_cnt_2 ) org_cnt_2 
,coalesce(bk.to_org_cnt_3 , dc.to_org_cnt_3 ) to_org_cnt_3 
,coalesce(bk.be_org_cnt_3 , dc.be_org_cnt_3 ) be_org_cnt_3 
,coalesce(bk.org_cnt_3, dc.org_cnt_3) org_cnt_3
,coalesce(bk.to_org_cnt_6 , dc.to_org_cnt_6 ) to_org_cnt_6 
,coalesce(bk.be_org_cnt_6 , dc.be_org_cnt_6 ) be_org_cnt_6 
,coalesce(bk.org_cnt_6 , dc.org_cnt_6 ) org_cnt_6 
,coalesce(bk.to_org_cnt_9 , dc.to_org_cnt_9 ) to_org_cnt_9 
,coalesce(bk.be_org_cnt_9 , dc.be_org_cnt_9 ) be_org_cnt_9 
,coalesce(bk.org_cnt_9 , dc.org_cnt_9 ) org_cnt_9 
,coalesce(bk.to_org_cnt_12, dc.to_org_cnt_12) to_org_cnt_12
,coalesce(bk.be_org_cnt_12, dc.be_org_cnt_12) be_org_cnt_12
,coalesce(bk.org_cnt_12, dc.org_cnt_12) org_cnt_12
,coalesce(bk.weight_to_all, dc.weight_to_all) weight_to_all
,coalesce(bk.weight_be_all, dc.weight_be_all) weight_be_all
,coalesce(bk.weight_all, dc.weight_all) weight_all
,coalesce(bk.weight_to_black , dc.weight_to_black ) weight_to_black 
,coalesce(bk.weight_be_black , dc.weight_be_black ) weight_be_black 
,coalesce(bk.weight_black, dc.weight_black) weight_black
,coalesce(bk.weight_to_applied , dc.weight_to_applied ) weight_to_applied 
,coalesce(bk.weight_be_applied , dc.weight_be_applied ) weight_be_applied 
,coalesce(bk.weight_applied, dc.weight_applied) weight_applied
,coalesce(bk.searched_org_cnt , dc.searched_org_cnt ) searched_org_cnt 
,coalesce(bk.register_cnt, dc.register_cnt) register_cnt
,case when trim(coalesce(bk.blacklist_update_time_name_idcard, dc.blacklist_update_time_name_idcard)) in ('','-9999') then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.blacklist_update_time_name_idcard, dc.blacklist_update_time_name_idcard)) end as blacklist_update_time_name_idcard_diff
,case when coalesce(bk.blacklist_name_with_idcard , dc.blacklist_name_with_idcard) ='true' then 1 when coalesce(bk.blacklist_name_with_idcard , dc.blacklist_name_with_idcard)='false' then 0 else null end as blacklist_name_with_idcard
,case when trim(coalesce(bk.blacklist_update_time_name_phone, dc.blacklist_update_time_name_phone)) in ('','-9999') then -9999 
	else date(coalesce(bka.createtime, dca.createtime)) - date(coalesce(bk.blacklist_update_time_name_phone, dc.blacklist_update_time_name_phone)) end as blacklist_update_time_name_phone_diff
,case when coalesce(bk.blacklist_name_with_phone , dc.blacklist_name_with_phone) ='true' then 1 when coalesce(bk.blacklist_name_with_phone , dc.blacklist_name_with_phone)='false' then 0 else null end as blacklist_name_with_phone
,coalesce(bk.d30_iou_platform_cnt , dc.d30_iou_platform_cnt ) d30_iou_platform_cnt 
,coalesce(bk.total_loan_amount, dc.total_loan_amount) total_loan_amount
,coalesce(bk.overdue_amount, dc.overdue_amount) overdue_amount
,coalesce(bk.d360_iou_query_times  , dc.d360_iou_query_times  ) d360_iou_query_times  
,coalesce(bk.in_repayment_interest  , dc.in_repayment_interest  ) in_repayment_interest  
,coalesce(bk.in_repayment_amount, dc.in_repayment_amount) in_repayment_amount
,coalesce(bk.overdue_payment_times, dc.overdue_payment_times) overdue_payment_times
,coalesce(bk.overdue_times, dc.overdue_times) overdue_times
,coalesce(bk.overdue_interest, dc.overdue_interest) overdue_interest
,coalesce(bk.in_repayment_times, dc.in_repayment_times) in_repayment_times
,coalesce(bk.d360_iou_platform_cnt, dc.d360_iou_platform_cnt) d360_iou_platform_cnt
,coalesce(bk.overdue_payment_interest, dc.overdue_payment_interest) overdue_payment_interest
,coalesce(bk.overdue_payment_amount, dc.overdue_payment_amount) overdue_payment_amount
,coalesce(bk.d90_iou_platform_cnt, dc.d90_iou_platform_cnt) d90_iou_platform_cnt
,coalesce(bk.total_loan_times , dc.total_loan_times ) total_loan_times 
,coalesce(bk.d90_iou_query_times, dc.d90_iou_query_times) d90_iou_query_times
,coalesce(bk.d30_iou_query_times , dc.d30_iou_query_times ) d30_iou_query_times 
,case when coalesce(bk.phone_with_other_idcards, dc.phone_with_other_idcards)='[]' then 0 when coalesce(bk.phone_with_other_idcards, dc.phone_with_other_idcards)='-9999' then -9999 
	else json_array_length(cast(coalesce(bk.phone_with_other_idcards, dc.phone_with_other_idcards) as json)) end as phone_with_other_idcards_ct 
,case when coalesce(bk.phone_with_other_names, dc.phone_with_other_names)='[]' then 0 when coalesce(bk.phone_with_other_names, dc.phone_with_other_names)='-9999' then -9999 
	else json_array_length(cast(coalesce(bk.phone_with_other_names, dc.phone_with_other_names) as json)) end as phone_with_other_names_ct 
from (
	select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
	union 
	select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
) t0
left join bk_juxinli_result bk on t0.order_no = bk.order_no 
left join dc_juxinli_result dc on t0.order_no = dc.order_no
left join bk_juxinli_application bka on t0.order_no = bka.order_no 
left join dc_juxinli_application dca on t0.order_no = dca.order_no
"""
conn.rollback()
cur = conn.cursor()
cur.execute(sql_jxl1)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_jxl1 = raw.copy()
print(r_jxl1.shape)
print(r_jxl1.order_no.nunique())

r_jxl1_f = r_jxl1.copy()

var_to_float = list(r_jxl1_f.iloc[:,1:].columns)
for x in var_to_float:
    r_jxl1_f[x] = r_jxl1_f[x].astype(float)
print(r_jxl1_f.dtypes)

''' 删除常数值变量 '''
var = list(r_jxl1_f.iloc[:,1:].columns)
for x in var:
    if r_jxl1_f[x].std()==0:
        print(r_jxl1_f[x].std(), x) 


# 1 vars
''' idcard_with_other_phones_times'''
sql_jxl2 = '''
select order_no, coalesce(sum(times),0) as idcard_with_other_phones_times
from (
	select t0.order_no, cast(json_array_elements(cast(coalesce(bk.idcard_with_other_phones, dc.idcard_with_other_phones) as json) ) ::json ->> 'times'  as int) as times
	from (
		select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
		union 
		select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')) t0
	left join bk_juxinli_result  bk on t0.order_no = bk.order_no and bk.idcard_with_other_phones<>'-9999'
	left join dc_juxinli_result dc on t0.order_no = dc.order_no and dc.idcard_with_other_phones<>'-9999'
	) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_jxl2)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_jxl2 = raw.copy()
print(r_jxl2.shape)
print(r_jxl2.order_no.nunique())

r_jxl2_f = r_jxl2.copy()

var_to_float = list(r_jxl2_f.iloc[:,1:].columns)
for x in var_to_float:
    r_jxl2_f[x] = r_jxl2_f[x].astype(float)
print(r_jxl2_f.dtypes)

''' 删除常数值变量 '''
var = list(r_jxl2_f.iloc[:,1:].columns)
for x in var:
    print(r_jxl2_f[x].std(), x) 


# 3 vars
''' register_orgs_count'''
sql_jxl3 = '''
select order_no, 
sum(case when register_orgs_label='贷款' then register_orgs_count else 0 end) as register_orgs_loan_ct,
sum(case when register_orgs_label='理财/贷款' then register_orgs_count else 0 end) as register_orgs_loan_finance_ct,
sum(case when register_orgs_label='理财' then register_orgs_count else 0 end) as register_orgs_finance_ct 
from (
	select t0.order_no, 
	cast(json_array_elements(cast(coalesce(bk.register_orgs_statistics, dc.register_orgs_statistics) as json)) ::json ->> 'count'  as int) as register_orgs_count,
	json_array_elements(cast(coalesce(bk.register_orgs_statistics, dc.register_orgs_statistics) as json)) ::json ->> 'label'   as register_orgs_label
	from  (
		select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
		union 
		select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
	) t0
	left join dc_juxinli_result dc on t0.order_no = dc.order_no and dc.register_orgs_statistics<>'-9999'
	left join bk_juxinli_result bk on t0.order_no = bk.order_no and bk.register_orgs_statistics<>'-9999'
	) tmp
group by order_no 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_jxl3)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_jxl3 = raw.copy()
print(r_jxl3.shape)
print(r_jxl3.order_no.nunique())

r_jxl3_f = r_jxl3.copy()

var_to_float = list(r_jxl3_f.iloc[:,1:].columns)
for x in var_to_float:
    r_jxl3_f[x] = r_jxl3_f[x].astype(float)
print(r_jxl3_f.dtypes)

''' 删除常数值变量 '''
var = list(r_jxl3_f.iloc[:,1:].columns)
for x in var:
    print(r_jxl3_f[x].std(), x) 


#18 vars
''' user_searched_history_by_orgs'''  
sql_jxl4 = '''
select  order_no
,sum(case when searched_org like '线上%' then 1 else 0 end) as searched_org_online_ct
,sum(case when searched_org like '线下%' then 1 else 0 end) as searched_org_offline_ct
,sum(case when searched_org='其他' then 1 else 0 end) as searched_org_1_ct
,sum(case when searched_org='小额信用贷' then 1 else 0 end) as searched_org_2_ct
,sum(case when searched_org='线上信用卡代还' then 1 else 0 end) as searched_org_3_ct
,sum(case when searched_org='线上信用现金贷' then 1 else 0 end) as searched_org_4_ct
,sum(case when searched_org='线上微额快速贷' then 1 else 0 end) as searched_org_5_ct
,sum(case when searched_org='线上抵押贷款' then 1 else 0 end) as searched_org_6_ct
,sum(case when searched_org='线上消费分期' then 1 else 0 end) as searched_org_7_ct
,sum(case when searched_org='线上租房分期' then 1 else 0 end) as searched_org_8_ct
,sum(case when searched_org='线上经营贷' then 1 else 0 end) as searched_org_9_ct
,sum(case when searched_org='线下供应链贷款' then 1 else 0 end) as searched_org_10_ct
,sum(case when searched_org='线下信用现金贷' then 1 else 0 end) as searched_org_11_ct
,sum(case when searched_org='线下微额快速贷' then 1 else 0 end) as searched_org_12_ct
,sum(case when searched_org='线下抵押贷款' then 1 else 0 end) as searched_org_13_ct
,sum(case when searched_org='线下消费分期' then 1 else 0 end) as searched_org_14_ct
,sum(case when searched_org='线下经营贷' then 1 else 0 end) as searched_org_15_ct
,sum(case when searched_org='贷款中介' then 1 else 0 end) as searched_org_16_ct
from (
	select t0.order_no, json_array_elements(cast(coalesce(dc.user_searched_history_by_orgs, bk.user_searched_history_by_orgs) as json)) ::json ->>'searched_org' searched_org
	from  (
		select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
		union 
		select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
	) t0
    left join dc_juxinli_result dc on t0.order_no = dc.order_no and dc.user_searched_history_by_orgs<>'-9999'
    left join bk_juxinli_result bk on t0.order_no = bk.order_no and bk.user_searched_history_by_orgs<>'-9999'
	) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_jxl4)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_jxl4 = raw.copy()
print(r_jxl4.shape)
print(r_jxl4.order_no.nunique())

r_jxl4_f = r_jxl4.copy()

var_to_float = list(r_jxl4_f.iloc[:,1:].columns)
for x in var_to_float:
    r_jxl4_f[x] = r_jxl4_f[x].astype(float)
print(r_jxl4_f.dtypes)

''' 删除常数值变量 '''
var = list(r_jxl4_f.iloc[:,1:].columns)
for x in var:
    print(r_jxl4_f[x].std(), x) 

# 18 vars
sql_jxl5 = '''
select  order_no
,count(case when susp_org_type like '线上%' then susp_org_type end) as phone_applied_in_orgs_online_ct
,count(case when susp_org_type like '线下%' then susp_org_type end) as phone_applied_in_orgs_offline_ct
,count(case when susp_org_type in ('其他' , '其它') then susp_org_type end) as phone_applied_in_orgs_1_ct
,count(case when susp_org_type='小额信用贷' then susp_org_type end) as phone_applied_in_orgs_2_ct
,count(case when susp_org_type='线上信用卡代还' then susp_org_type end) as phone_applied_in_orgs_3_ct
,count(case when susp_org_type='线上信用现金贷' then susp_org_type end) as phone_applied_in_orgs_4_ct
,count(case when susp_org_type='线上微额快速贷' then susp_org_type end) as phone_applied_in_orgs_5_ct
,count(case when susp_org_type='线上抵押贷款' then susp_org_type end) as phone_applied_in_orgs_6_ct
,count(case when susp_org_type='线上消费分期' then susp_org_type end) as phone_applied_in_orgs_7_ct
,count(case when susp_org_type='线上租房分期' then susp_org_type end) as phone_applied_in_orgs_8_ct
,count(case when susp_org_type='线上经营贷' then susp_org_type end) as phone_applied_in_orgs_9_ct
,count(case when susp_org_type='线下供应链贷款' then susp_org_type end) as phone_applied_in_orgs_10_ct
,count(case when susp_org_type='线下信用现金贷' then susp_org_type end) as phone_applied_in_orgs_11_ct
,count(case when susp_org_type='线下微额快速贷' then susp_org_type end) as phone_applied_in_orgs_12_ct
,count(case when susp_org_type='线下抵押贷款' then susp_org_type end) as phone_applied_in_orgs_13_ct
,count(case when susp_org_type='线下消费分期' then susp_org_type end) as phone_applied_in_orgs_14_ct
,count(case when susp_org_type='线下经营贷' then susp_org_type end) as phone_applied_in_orgs_15_ct
,count(case when susp_org_type='贷款中介' then susp_org_type end) as phone_applied_in_orgs_16_ct
from (
	select t0.order_no, json_array_elements(cast(coalesce(dc.phone_applied_in_orgs, bk.phone_applied_in_orgs) as json)) ::json ->> 'susp_org_type'  as susp_org_type
	from (
		select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
		union 
		select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
	) t0
    left join dc_juxinli_result dc on t0.order_no = dc.order_no and dc.phone_applied_in_orgs<>'-9999'
    left join bk_juxinli_result bk on t0.order_no = bk.order_no and bk.phone_applied_in_orgs<>'-9999'
	) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_jxl5)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_jxl5 = raw.copy()
print(r_jxl5.shape)
print(r_jxl5.order_no.nunique())

r_jxl5_f = r_jxl5.copy()

var_to_float = list(r_jxl5_f.iloc[:,1:].columns)
for x in var_to_float:
    r_jxl5_f[x] = r_jxl5_f[x].astype(float)
print(r_jxl5_f.dtypes)

''' 删除常数值变量 '''
var = list(r_jxl5_f.iloc[:,1:].columns)
for x in var:
    print(r_jxl5_f[x].std(), x) 

# 18 vars
sql_jxl6 = '''
select  order_no
,count(case when susp_org_type like '线上%' then susp_org_type end) as idcard_applied_in_orgs_online_ct
,count(case when susp_org_type like '线下%' then susp_org_type end) as idcard_applied_in_orgs_offline_ct
,count(case when susp_org_type in ('其他' , '其它') then susp_org_type end) as idcard_applied_in_orgs_1_ct
,count(case when susp_org_type='小额信用贷' then susp_org_type end) as idcard_applied_in_orgs_2_ct
,count(case when susp_org_type='线上信用卡代还' then susp_org_type end) as idcard_applied_in_orgs_3_ct
,count(case when susp_org_type='线上信用现金贷' then susp_org_type end) as idcard_applied_in_orgs_4_ct
,count(case when susp_org_type='线上微额快速贷' then susp_org_type end) as idcard_applied_in_orgs_5_ct
,count(case when susp_org_type='线上抵押贷款' then susp_org_type end) as idcard_applied_in_orgs_6_ct
,count(case when susp_org_type='线上消费分期' then susp_org_type end) as idcard_applied_in_orgs_7_ct
,count(case when susp_org_type='线上租房分期' then susp_org_type end) as idcard_applied_in_orgs_8_ct
,count(case when susp_org_type='线上经营贷' then susp_org_type end) as idcard_applied_in_orgs_9_ct
,count(case when susp_org_type='线下供应链贷款' then susp_org_type end) as idcard_applied_in_orgs_10_ct
,count(case when susp_org_type='线下信用现金贷' then susp_org_type end) as idcard_applied_in_orgs_11_ct
,count(case when susp_org_type='线下微额快速贷' then susp_org_type end) as idcard_applied_in_orgs_12_ct
,count(case when susp_org_type='线下抵押贷款' then susp_org_type end) as idcard_applied_in_orgs_13_ct
,count(case when susp_org_type='线下消费分期' then susp_org_type end) as idcard_applied_in_orgs_14_ct
,count(case when susp_org_type='线下经营贷' then susp_org_type end) as idcard_applied_in_orgs_15_ct
,count(case when susp_org_type='贷款中介' then susp_org_type end) as idcard_applied_in_orgs_16_ct
from (
	select t0.order_no, json_array_elements(cast(coalesce(dc.idcard_applied_in_orgs, bk.idcard_applied_in_orgs) as json)) ::json ->> 'susp_org_type'  as susp_org_type
	from (
		select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
		union 
		select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
	) t0
    left join dc_juxinli_result dc on t0.order_no = dc.order_no and dc.idcard_applied_in_orgs<>'-9999'
    left join bk_juxinli_result bk on t0.order_no = bk.order_no and bk.idcard_applied_in_orgs<>'-9999'
	) tmp
group by order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_jxl6)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_jxl6 = raw.copy()
print(r_jxl6.shape)
print(r_jxl6.order_no.nunique())

r_jxl6_f = r_jxl6.copy()

var_to_float = list(r_jxl6_f.iloc[:,1:].columns)
for x in var_to_float:
    r_jxl6_f[x] = r_jxl6_f[x].astype(float)
print(r_jxl6_f.dtypes)

''' 删除常数值变量 '''
var = list(r_jxl6_f.iloc[:,1:].columns)
for x in var:
    print(r_jxl6_f[x].std(), x) 


sql_jxl7 = '''
select t0.order_no
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'pct_cnt_org_cash' as d_7pct_cnt_org_cash
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'cnt_cc' as d_7cnt_cc
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'cnt_org' as d_7cnt_org
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'pct_cnt_org_all' as d_7pct_cnt_org_all
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'cnt' as d_7cnt
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'pct_cnt_org_cf' as d_7pct_cnt_org_cf
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'cnt_cf' as d_7cnt_cf
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'pct_cnt_cc' as d_7pct_cnt_cc
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'cnt_org_cf' as d_7cnt_org_cf
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'pct_cnt_cf' as d_7pct_cnt_cf
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'cnt_cash' as d_7cnt_cash
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'pct_cnt_cash' as d_7pct_cnt_cash
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'cnt_org_cc' as d_7cnt_org_cc
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'pct_cnt_org_cc' as d_7pct_cnt_org_cc
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'pct_cnt_all' as d_7pct_cnt_all
,cast(coalesce(bk.d_7, dc.d_7) as json) ::json ->> 'cnt_org_cash' as d_7cnt_org_cash
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'pct_cnt_org_cash' as d_15pct_cnt_org_cash
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'cnt_cc' as d_15cnt_cc
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'cnt_org' as d_15cnt_org
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'pct_cnt_org_all' as d_15pct_cnt_org_all
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'cnt' as d_15cnt
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'pct_cnt_org_cf' as d_15pct_cnt_org_cf
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'cnt_cf' as d_15cnt_cf
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'pct_cnt_cc' as d_15pct_cnt_cc
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'cnt_org_cf' as d_15cnt_org_cf
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'pct_cnt_cf' as d_15pct_cnt_cf
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'cnt_cash' as d_15cnt_cash
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'pct_cnt_cash' as d_15pct_cnt_cash
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'cnt_org_cc' as d_15cnt_org_cc
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'pct_cnt_org_cc' as d_15pct_cnt_org_cc
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'pct_cnt_all' as d_15pct_cnt_all
,cast(coalesce(bk.d_15, dc.d_15) as json) ::json ->> 'cnt_org_cash' as d_15cnt_org_cash
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'pct_cnt_org_cash' as d_30pct_cnt_org_cash
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'cnt_cc' as d_30cnt_cc
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'cnt_org' as d_30cnt_org
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'pct_cnt_org_all' as d_30pct_cnt_org_all
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'cnt' as d_30cnt
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'pct_cnt_org_cf' as d_30pct_cnt_org_cf
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'cnt_cf' as d_30cnt_cf
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'pct_cnt_cc' as d_30pct_cnt_cc
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'cnt_org_cf' as d_30cnt_org_cf
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'pct_cnt_cf' as d_30pct_cnt_cf
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'cnt_cash' as d_30cnt_cash
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'pct_cnt_cash' as d_30pct_cnt_cash
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'cnt_org_cc' as d_30cnt_org_cc
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'pct_cnt_org_cc' as d_30pct_cnt_org_cc
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'pct_cnt_all' as d_30pct_cnt_all
,cast(coalesce(bk.d_30, dc.d_30) as json) ::json ->> 'cnt_org_cash' as d_30cnt_org_cash
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'pct_cnt_org_cash' as d_60pct_cnt_org_cash
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'cnt_cc' as d_60cnt_cc
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'cnt_org' as d_60cnt_org
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'pct_cnt_org_all' as d_60pct_cnt_org_all
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'cnt' as d_60cnt
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'pct_cnt_org_cf' as d_60pct_cnt_org_cf
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'cnt_cf' as d_60cnt_cf
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'pct_cnt_cc' as d_60pct_cnt_cc
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'cnt_org_cf' as d_60cnt_org_cf
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'pct_cnt_cf' as d_60pct_cnt_cf
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'cnt_cash' as d_60cnt_cash
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'pct_cnt_cash' as d_60pct_cnt_cash
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'cnt_org_cc' as d_60cnt_org_cc
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'pct_cnt_org_cc' as d_60pct_cnt_org_cc
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'pct_cnt_all' as d_60pct_cnt_all
,cast(coalesce(bk.d_60, dc.d_60) as json) ::json ->> 'cnt_org_cash' as d_60cnt_org_cash
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'pct_cnt_org_cash' as d_90pct_cnt_org_cash
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'cnt_cc' as d_90cnt_cc
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'cnt_org' as d_90cnt_org
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'pct_cnt_org_all' as d_90pct_cnt_org_all
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'cnt' as d_90cnt
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'pct_cnt_org_cf' as d_90pct_cnt_org_cf
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'cnt_cf' as d_90cnt_cf
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'pct_cnt_cc' as d_90pct_cnt_cc
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'cnt_org_cf' as d_90cnt_org_cf
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'pct_cnt_cf' as d_90pct_cnt_cf
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'cnt_cash' as d_90cnt_cash
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'pct_cnt_cash' as d_90pct_cnt_cash
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'cnt_org_cc' as d_90cnt_org_cc
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'pct_cnt_org_cc' as d_90pct_cnt_org_cc
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'pct_cnt_all' as d_90pct_cnt_all
,cast(coalesce(bk.d_90, dc.d_90) as json) ::json ->> 'cnt_org_cash' as d_90cnt_org_cash
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'pct_cnt_org_cash' as m_4pct_cnt_org_cash
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'cnt_cc' as m_4cnt_cc
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'cnt_org' as m_4cnt_org
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'pct_cnt_org_all' as m_4pct_cnt_org_all
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'cnt' as m_4cnt
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'pct_cnt_org_cf' as m_4pct_cnt_org_cf
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'cnt_cf' as m_4cnt_cf
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'pct_cnt_cc' as m_4pct_cnt_cc
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'cnt_org_cf' as m_4cnt_org_cf
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'pct_cnt_cf' as m_4pct_cnt_cf
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'cnt_cash' as m_4cnt_cash
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'pct_cnt_cash' as m_4pct_cnt_cash
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'cnt_org_cc' as m_4cnt_org_cc
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'pct_cnt_org_cc' as m_4pct_cnt_org_cc
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'pct_cnt_all' as m_4pct_cnt_all
,cast(coalesce(bk.m_4, dc.m_4) as json) ::json ->> 'cnt_org_cash' as m_4cnt_org_cash
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'pct_cnt_org_cash' as m_5pct_cnt_org_cash
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'cnt_cc' as m_5cnt_cc
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'cnt_org' as m_5cnt_org
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'pct_cnt_org_all' as m_5pct_cnt_org_all
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'cnt' as m_5cnt
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'pct_cnt_org_cf' as m_5pct_cnt_org_cf
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'cnt_cf' as m_5cnt_cf
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'pct_cnt_cc' as m_5pct_cnt_cc
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'cnt_org_cf' as m_5cnt_org_cf
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'pct_cnt_cf' as m_5pct_cnt_cf
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'cnt_cash' as m_5cnt_cash
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'pct_cnt_cash' as m_5pct_cnt_cash
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'cnt_org_cc' as m_5cnt_org_cc
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'pct_cnt_org_cc' as m_5pct_cnt_org_cc
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'pct_cnt_all' as m_5pct_cnt_all
,cast(coalesce(bk.m_5, dc.m_5) as json) ::json ->> 'cnt_org_cash' as m_5cnt_org_cash
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'pct_cnt_org_cash' as m_6pct_cnt_org_cash
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'cnt_cc' as m_6cnt_cc
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'cnt_org' as m_6cnt_org
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'pct_cnt_org_all' as m_6pct_cnt_org_all
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'cnt' as m_6cnt
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'pct_cnt_org_cf' as m_6pct_cnt_org_cf
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'cnt_cf' as m_6cnt_cf
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'pct_cnt_cc' as m_6pct_cnt_cc
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'cnt_org_cf' as m_6cnt_org_cf
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'pct_cnt_cf' as m_6pct_cnt_cf
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'cnt_cash' as m_6cnt_cash
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'pct_cnt_cash' as m_6pct_cnt_cash
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'cnt_org_cc' as m_6cnt_org_cc
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'pct_cnt_org_cc' as m_6pct_cnt_org_cc
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'pct_cnt_all' as m_6pct_cnt_all
,cast(coalesce(bk.m_6, dc.m_6) as json) ::json ->> 'cnt_org_cash' as m_6cnt_org_cash
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'pct_cnt_org_cash' as m_9pct_cnt_org_cash
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'cnt_cc' as m_9cnt_cc
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'cnt_org' as m_9cnt_org
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'pct_cnt_org_all' as m_9pct_cnt_org_all
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'cnt' as m_9cnt
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'pct_cnt_org_cf' as m_9pct_cnt_org_cf
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'cnt_cf' as m_9cnt_cf
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'pct_cnt_cc' as m_9pct_cnt_cc
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'cnt_org_cf' as m_9cnt_org_cf
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'pct_cnt_cf' as m_9pct_cnt_cf
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'cnt_cash' as m_9cnt_cash
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'pct_cnt_cash' as m_9pct_cnt_cash
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'cnt_org_cc' as m_9cnt_org_cc
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'pct_cnt_org_cc' as m_9pct_cnt_org_cc
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'pct_cnt_all' as m_9pct_cnt_all
,cast(coalesce(bk.m_9, dc.m_9) as json) ::json ->> 'cnt_org_cash' as m_9cnt_org_cash
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'pct_cnt_org_cash' as m_12pct_cnt_org_cash
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'cnt_cc' as m_12cnt_cc
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'cnt_org' as m_12cnt_org
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'pct_cnt_org_all' as m_12pct_cnt_org_all
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'cnt' as m_12cnt
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'pct_cnt_org_cf' as m_12pct_cnt_org_cf
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'cnt_cf' as m_12cnt_cf
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'pct_cnt_cc' as m_12pct_cnt_cc
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'cnt_org_cf' as m_12cnt_org_cf
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'pct_cnt_cf' as m_12pct_cnt_cf
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'cnt_cash' as m_12cnt_cash
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'pct_cnt_cash' as m_12pct_cnt_cash
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'cnt_org_cc' as m_12cnt_org_cc
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'pct_cnt_org_cc' as m_12pct_cnt_org_cc
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'pct_cnt_all' as m_12pct_cnt_all
,cast(coalesce(bk.m_12, dc.m_12) as json) ::json ->> 'cnt_org_cash' as m_12cnt_org_cash
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'pct_cnt_org_cash' as m_18pct_cnt_org_cash
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'cnt_cc' as m_18cnt_cc
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'cnt_org' as m_18cnt_org
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'pct_cnt_org_all' as m_18pct_cnt_org_all
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'cnt' as m_18cnt
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'pct_cnt_org_cf' as m_18pct_cnt_org_cf
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'cnt_cf' as m_18cnt_cf
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'pct_cnt_cc' as m_18pct_cnt_cc
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'cnt_org_cf' as m_18cnt_org_cf
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'pct_cnt_cf' as m_18pct_cnt_cf
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'cnt_cash' as m_18cnt_cash
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'pct_cnt_cash' as m_18pct_cnt_cash
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'cnt_org_cc' as m_18cnt_org_cc
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'pct_cnt_org_cc' as m_18pct_cnt_org_cc
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'pct_cnt_all' as m_18pct_cnt_all
,cast(coalesce(bk.m_18, dc.m_18) as json) ::json ->> 'cnt_org_cash' as m_18cnt_org_cash
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'pct_cnt_org_cash' as m_24pct_cnt_org_cash
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'cnt_cc' as m_24cnt_cc
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'cnt_org' as m_24cnt_org
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'pct_cnt_org_all' as m_24pct_cnt_org_all
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'cnt' as m_24cnt
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'pct_cnt_org_cf' as m_24pct_cnt_org_cf
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'cnt_cf' as m_24cnt_cf
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'pct_cnt_cc' as m_24pct_cnt_cc
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'cnt_org_cf' as m_24cnt_org_cf
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'pct_cnt_cf' as m_24pct_cnt_cf
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'cnt_cash' as m_24cnt_cash
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'pct_cnt_cash' as m_24pct_cnt_cash
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'cnt_org_cc' as m_24cnt_org_cc
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'pct_cnt_org_cc' as m_24pct_cnt_org_cc
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'pct_cnt_all' as m_24pct_cnt_all
,cast(coalesce(bk.m_24, dc.m_24) as json) ::json ->> 'cnt_org_cash' as m_24cnt_org_cash
from (
	select order_no from t_loan_performance where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
	union 
	select order_no from temp_credit_modelscore_v6yl where sample_set in ('train', 'test')
) t0
left join dc_juxinli_result dc on t0.order_no = dc.order_no and dc.d_7<>'-9999'
left join bk_juxinli_result bk on t0.order_no = bk.order_no and bk.d_7<>'-9999'
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_jxl7)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_jxl7 = raw.copy()
print(r_jxl7.shape)
print(r_jxl7.order_no.nunique())

r_jxl7_f = r_jxl7.copy()

var_to_float = list(r_jxl7_f.iloc[:,1:].columns)
for x in var_to_float:
    r_jxl7_f[x] = r_jxl7_f[x].astype(float)
print(r_jxl7_f.dtypes)

''' 删除常数值变量 '''
var = list(r_jxl7_f.iloc[:,1:].columns)
for x in var:
    print(r_jxl7_f[x].std(), x) 
    
r_jxl_f = r_jxl1_f.merge(r_jxl2_f, how='left', on='order_no')
r_jxl_f = r_jxl_f.merge(r_jxl3_f, how='left', on='order_no')
r_jxl_f = r_jxl_f.merge(r_jxl4_f, how='left', on='order_no')
r_jxl_f = r_jxl_f.merge(r_jxl5_f, how='left', on='order_no')
r_jxl_f = r_jxl_f.merge(r_jxl6_f, how='left', on='order_no')
r_jxl_f = r_jxl_f.merge(r_jxl7_f, how='left', on='order_no')

r_jxl_f.to_excel(filepath_out + "r_jxl_f.xlsx")



'''天御'''
sql_ty = '''
with ty_raw as (
select order_no
, oss :: json #>> '{riskScore}' as riskscore
, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCode' as riskcode
, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCodeValue' as riskcodevalue
from  bk_tianyu_application where oss <> ''
union 
select order_no, coalesce(riskscore_kfk, riskscore_oss) as riskscore, 
coalesce(riskcode_kfk, riskcode_oss) as riskcode, coalesce(riskcodevalue_kfk, riskcodevalue_oss) as riskcodevalue
from (
	select order_no
	, oss :: json #>> '{riskscore}' as riskscore_kfk
	, json_array_elements(case when oss :: json #>> '{riskinfo}'= '[]' then '[null]' 
							   else cast(oss :: json #>> '{riskinfo}' as json) end)::json ->> 'riskcode' as riskcode_kfk
	, json_array_elements(case when oss :: json #>> '{riskinfo}'= '[]' then '[null]' 
							   else cast(oss :: json #>> '{riskinfo}' as json) end)::json ->> 'riskcodevalue' as riskcodevalue_kfk
	, oss :: json #>> '{riskScore}' as riskscore_oss
	, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' 
							   else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCode' as riskcode_oss
	, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' 
							   else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCodeValue' as riskcodevalue_oss
	from dc_tianyu_application 
	where oss <>'') tmp
)
select distinct t0.order_no
, max(riskscore) as ty_riskscore
, max(case when riskcode = '1' then riskcodevalue else null end) as ty_riskcode_1
, max(case when riskcode = '2' then riskcodevalue else null end) as ty_riskcode_2
, max(case when riskcode = '3' then riskcodevalue else null end) as ty_riskcode_3
, max(case when riskcode = '4' then riskcodevalue else null end) as ty_riskcode_4
, max(case when riskcode = '5' then riskcodevalue else null end) as ty_riskcode_5
, max(case when riskcode = '6' then riskcodevalue else null end) as ty_riskcode_6
, max(case when riskcode = '7' then riskcodevalue else null end) as ty_riskcode_7
, max(case when riskcode = '8' then riskcodevalue else null end) as ty_riskcode_8
, max(case when riskcode = '301' then riskcodevalue else null end) as ty_riskcode_301
, max(case when riskcode = '503' then riskcodevalue else null end) as ty_riskcode_503
from t_loan_performance t0
left join ty_raw t1  on t0.order_no = t1.order_no
where t0.dt='20191108' and t0.business_id in ('tb', 'xjbk', 'rong360') and t0.effective_date between '2019-07-04' and '2019-10-22'
group by t0.order_no
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_ty)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_ty = raw.copy()
print(r_ty.shape)
print(r_ty.order_no.nunique())

#r_ty_f = r_ty.append(r_base0, ignore_index=True)
#r_ty_f = r_ty_f[list(r_ty.columns)]

r_ty_f = r_ty.copy()

var_to_float = list(r_ty_f.iloc[:,1:].columns)
for x in var_to_float:
    r_ty_f[x] = r_ty_f[x].astype(float)
print(r_ty_f.dtypes)

''' 删除常数值变量 '''
var = list(r_ty_f.iloc[:,1:].columns)
for x in var:
    if (r_ty_f[x].std() <=0.001) | (pd.isnull(r_ty_f[x].std())):
        print(r_ty_f[x].std(), x) 

r_ty_f =  r_ty_f.drop(['ty_riskcode_3', 'ty_riskcode_5', 'ty_riskcode_7', 'ty_riskcode_301'], axis=1)

r_ty_f.to_excel(filepath_out + "r_ty_f.xlsx")



''' 读取数据'''
r_cust_f = pd.read_excel(filepath_out + 'r_cust_f.xlsx')
r_xy_f = pd.read_excel(filepath_out + 'r_xy_f.xlsx')
r_jxl_f = pd.read_excel(filepath_out + 'r_jxl_f.xlsx')

r_td_var = pd.read_excel(filepath_out + 'r_td_var.xlsx')
r_td_loanreview_f = pd.read_excel(filepath_out + 'r_td_loanreview_f.xlsx')
r_td_microscore = pd.read_excel(filepath_out + 'r_td_microscore.xlsx')
r_ty_f = pd.read_excel(filepath_out + 'r_ty_f.xlsx')
r_xd_f = pd.read_excel(filepath_out + 'r_xd_f.xlsx')








