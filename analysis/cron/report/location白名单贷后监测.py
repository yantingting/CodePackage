# coding: utf-8
"""
Python 3.6.0
uku周报
"""
# import sys
# import os
#
# sys.path.append("C:\\Users\\Mint\\Documents\\repos\\genie")

import datetime
import os
import pandas as pd
import numpy as np
import sys

sys.path.append('C:/workspace/repos/genie')

from utils3.data_io_utils import *

"""通过率---------------------------------------------------------------------"""
yd_sql="""
select
	a.id as loan_id,
    apply_time::date as app_date,
	case when b.company_area is null or b.company_area='' then 'no,data,no' else b.company_area end as work_city,
    case when loan_status not in ('DENIED','RESCIND','APPROVING','CREATED') then 1 else 0 end as if_funded
from
	(
	select
		id,
		customer_id,
        apply_time,
        loan_status
	from
		public.dw_gocash_go_cash_loan_gocash_core_loan
	where
		apply_time::date >= '2019-12-10'
		and apply_time::date <= '2020-01-01'
        and return_flag='false')a
left join (
	select
		customer_id,
		company_area
	from
		dw_gocash_go_cash_loan_gocash_core_cusomer_profession)b on
	a.customer_id = b.customer_id
"""
yd_data=get_df_from_pg(yd_sql)

#公司地址拆分，提取城市
list_temp = []
for i,j in enumerate(yd_data['work_city']):
    list_temp = j.split(',')
    yd_data.loc[i, 'company_city'] = list_temp[1]

#加载白名单
white_list_data=pd.read_excel('C:/Users/Mint/Desktop/GPS_WWHITELIST.xlsx')
white_list=list(white_list_data['city'])

#判断城市是否在白名单中
for i,j in enumerate(yd_data['company_city']):
    if str(j) in white_list:
        yd_data.loc[i, 'if_in_whitelist'] = 1
    else:
        yd_data.loc[i, 'if_in_whitelist'] = 0

#计算申请量
def countall(a):
    if a>-1:
        return 1
    else:
        return 0

yd_data['all_count']=yd_data.apply(lambda x: countall(x.if_funded), axis = 1)

#计算未在白名单的城市放款
def rejectf(a, b):
    if a==0 and b == 1:
        return 1
    else:
        return 0

yd_data['if_reject_funded'] = yd_data.apply(lambda x: rejectf(x.if_in_whitelist, x.if_funded), axis = 1)

#求和计算
yd_sum=yd_data.groupby('app_date')['all_count','if_reject_funded'].sum()
yd_sum=yd_sum.reset_index()
yd_sum['pass_rate']=yd_sum['if_reject_funded']/yd_sum['all_count']

#输出通过率
yd_sum.to_excel('C:/Users/Mint/Desktop/pass_data0102.xlsx')

"""dpd--------------------------------------------------------------------------"""

dpd_sql="""
select distinct
	effective_date,
    loan_id,
    case when company_area is null or company_area='' then 'no,data,no' else company_area end as work_city,
	case when late_date-effective_date = approved_period then 1 else 0 end as dpd1,
	case when DPD >= 3 then 1 else 0 end as dpd3
from
	(
	select
		t1.id loan_id,
		effective_date,
		return_flag,
		loan_status,
		approved_period,
		late_date,
		approved_principal,
        company_area,
		case
			when loan_status = 'COLLECTION' then current_date-late_date
			else round(late_fee /(approved_principal*0.025))::int end as DPD
		from
			(
			select
				*
			from
				dw_gocash_go_cash_loan_gocash_core_loan
			where
				effective_date is not null
				and effective_date >= '2019-12-10'
                and effective_date <= '2019-12-18'
				and loan_status not in ('DENIED',
				'RESCIND',
				'APPROVING',
				'CREATED')
                and return_flag='false') t1
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
        left join
             (select
		            customer_id,
		            company_area
	           from
		            dw_gocash_go_cash_loan_gocash_core_cusomer_profession)t3
          on t1.customer_id=t3.customer_id)t
"""
dpd_data=get_df_from_pg(dpd_sql)

list_temp = []
for i,j in enumerate(dpd_data['work_city']):
    list_temp = j.split(',')
    dpd_data.loc[i, 'company_city'] = list_temp[1]

for i,j in enumerate(dpd_data['company_city']):
    if str(j) in white_list:
        dpd_data.loc[i, 'if_in_whitelist'] = 'pass'
    else:
        dpd_data.loc[i, 'if_in_whitelist'] = 'reject'

def passc(a, b):
    if a=='pass' and b == 1:
        return 1
    else:
        return 0

def rejectc(a, b):
    if a=='reject' and b == 1:
        return 1
    else:
        return 0

#按是否在白名单名分别计算dpd
dpd_data['dpd1_pass'] = dpd_data.apply(lambda x: passc(x.if_in_whitelist, x.dpd1), axis = 1)
dpd_data['dpd1_reject'] = dpd_data.apply(lambda x: rejectc(x.if_in_whitelist, x.dpd1), axis = 1)
dpd_data['dpd3_pass'] = dpd_data.apply(lambda x: passc(x.if_in_whitelist, x.dpd3), axis = 1)
dpd_data['dpd3_reject'] = dpd_data.apply(lambda x: rejectc(x.if_in_whitelist, x.dpd3), axis = 1)

def countp(a):
    if a=='pass':
        return 1
    else:
        return 0

def countr(a):
    if a=='reject':
        return 1
    else:
        return 0

def countall(a):
    if a=='pass' or a=='reject':
        return 1
    else:
        return 0

# 按是否在白名单名分别计算单量
dpd_data['pass_count']=dpd_data.apply(lambda x: countp(x.if_in_whitelist), axis = 1)
dpd_data['reject_count']=dpd_data.apply(lambda x: countr(x.if_in_whitelist), axis = 1)
dpd_data['all_count']=dpd_data.apply(lambda x: countall(x.if_in_whitelist), axis = 1)


dpd_sum_data=dpd_data.groupby('effective_date')['dpd1_pass','dpd1_reject','dpd3_pass','dpd3_reject','pass_count','reject_count'].sum()
dpd_sum_data=dpd_sum_data.reset_index()

dpd_sum_all=pd.Series({'effective_date':'all','dpd1_pass':dpd_sum_data['dpd1_pass'].sum(),'dpd1_reject':dpd_sum_data['dpd1_reject'].sum(),'dpd3_pass':dpd_sum_data['dpd3_pass'].sum(),'dpd3_reject':dpd_sum_data['dpd3_reject'].sum(),'pass_count':dpd_sum_data['pass_count'].sum(),'reject_count':dpd_sum_data['reject_count'].sum()})
dpd_sum_data=dpd_sum_data.append(dpd_sum_all,ignore_index=True)

dpd_sum_data['dpd1_pass_rate']=dpd_sum_data['dpd1_pass']/dpd_sum_data['pass_count']
dpd_sum_data['dpd1_reject_rate']=dpd_sum_data['dpd1_reject']/dpd_sum_data['reject_count']
dpd_sum_data['dpd3_pass_rate']=dpd_sum_data['dpd3_pass']/dpd_sum_data['pass_count']
dpd_sum_data['dpd3_reject_rate']=dpd_sum_data['dpd3_reject']/dpd_sum_data['reject_count']

dpd_sum_data['dpd1_pass_rate']=dpd_sum_data['dpd1_pass_rate'].apply(lambda x: '%.2f%%' % (x*100))
dpd_sum_data['dpd1_reject_rate']=dpd_sum_data['dpd1_reject_rate'].apply(lambda x: '%.2f%%' % (x*100))
dpd_sum_data['dpd3_pass_rate']=dpd_sum_data['dpd3_pass_rate'].apply(lambda x: '%.2f%%' % (x*100))
dpd_sum_data['dpd3_reject_rate']=dpd_sum_data['dpd3_reject_rate'].apply(lambda x: '%.2f%%' % (x*100))

dpd_sum_data.to_excel('C:/Users/Mint/Desktop/dpd_sum_data0102.xlsx')

#城市维度
import datetime
#dpd3与dpd1时间维度不同
dpd3_data=dpd_data[dpd_data['effective_date']<=datetime.date(2019,12,15)]

dpd1_city_data=dpd_data.groupby('company_city')['dpd1','all_count'].sum()
dpd1_city_data=dpd1_city_data.reset_index()

#计算首逾
dpd1_city_data['dpd1_rate']=dpd1_city_data['dpd1']/dpd1_city_data['all_count']
dpd1_city_data.sort_values(by='dpd1_rate',inplace=True,ascending=False)
dpd1_city_data['dpd1_rate']=dpd1_city_data['dpd1_rate'].apply(lambda x: '%.2f%%' % (x*100))

#获取白名单省份
dpd1_city_data=dpd1_city_data.merge(white_list_data,how='left',left_on='company_city',right_on='city')

#dpd1_city_data.to_excel('C:/Users/Mint/Desktop/dpd1_city_data0102.xlsx')

dpd3_city_data=dpd3_data.groupby('company_city')['dpd3','all_count'].sum()
dpd3_city_data=dpd3_city_data.reset_index()

dpd3_city_data['dpd3_rate']=dpd3_city_data['dpd3']/dpd3_city_data['all_count']
dpd3_city_data.sort_values(by='dpd3_rate',inplace=True,ascending=False)
dpd3_city_data['dpd3_rate']=dpd3_city_data['dpd3_rate'].apply(lambda x: '%.2f%%' % (x*100))

dpd3_city_data=dpd3_city_data.merge(white_list_data,how='left',left_on='company_city',right_on='city')

#dpd3_city_data.to_excel('C:/Users/Mint/Desktop/dpd3_city_data.xlsx')

#dpd1和dpd3合并
dpd_city_data=dpd1_city_data.merge(dpd3_city_data,how='outer',on='company_city')
dpd_city_data.drop(['country_y','province_y','city_y'],axis=1)
dpd_city_data.to_excel('C:/Users/Mint/Desktop/dpd_city_data.xlsx')
