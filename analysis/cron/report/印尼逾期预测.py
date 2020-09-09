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
from itertools import islice
import datetime

sys.path.append('C:/Users/wangj/Documents/GitHub/genie')

from utils3.data_io_utils import *

date = datetime.date.today().strftime('%m-%d')
file_path= os.path.join('D:/riskcontrol/Seafile/风控/策略&产品/策略/02）印尼PD/印尼数据/每周逾期预测/', date)
if not os.path.exists(file_path):
    os.mkdir(file_path)

"""加载数据---------------------------------------------------------------------"""
rucui_sql="""
select
	due_date::date,
	return_flag,
	count(distinct ab.id) as total_loan,
	count(distinct case when due_date-effective_date+1=approved_period then ab.id else null end) as shoucidaoqi_loan,
	count(distinct case when due_date-effective_date+1=approved_period and ab.late_day>0 and order_status is not null and order_status <> 'PAIDOFF' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as shouyu_loan,
	count(distinct case when due_date-effective_date+1>approved_period then ab.id else null end) as zhanqidaoqi_loan,
	count(distinct case when due_date-effective_date+1>approved_period and late_day>0 and ab.order_status is not null and ab.order_status <> 'PAIDOFF' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as zhanqiyuqi_loan,
	count(distinct case when ab.order_status is not null and ab.order_status <> 'PAIDOFF' and order_status != 'ABNORMAL' and ab.late_day>0 and late_date-due_date = 1 then ab.id else null end) as rucui_loan
from
	(
	select
		a.id,
		approved_principal,
		approved_period,
		effective_date,
		return_flag,
		due_date,
		order_status,
		late_day,
		late_date,
		rk
	from
		(
		select
			id,
			approved_principal,
			approved_period,
			effective_date,
			return_flag,
			due_date
		from
			public.dw_gocash_go_cash_loan_gocash_core_loan
		where
			loan_status not in ('DENIED',
			'RESCIND',
			'APPROVING',
			'CREATED') )a
	left join (
		select
			id,
			order_id,
			order_status,
			late_day,
			late_date,
			row_number() over(partition by order_id
		order by
			create_time) as rk
		from
			public.dw_gocash_gocash_collection_col_case
		where
			app_id not in ('Credits',
			'KASANDAAI'))b on
		a.id = b.order_id
union
	select
		a1.loan_id as id,
		a2.approved_principal,
		a2.approved_period,
		a2.effective_date,
		a2.return_flag,
		a1.his_due_date as due_date,
		order_status,
		late_day,
		late_date,
		rk
	from
		(
		select
			loan_id,
			his_due_date
		from
			dw_gocash_go_cash_loan_gocash_core_loan_pay_flow
		where
			status = 'SUCCESS'
			and extend_fee>0) a1
	left join (
		select
			id,
			approved_principal,
			approved_period,
			effective_date,
			return_flag
		from
			dw_gocash_go_cash_loan_gocash_core_loan
		where
			loan_status not in ('DENIED',
			'RESCIND',
			'APPROVING',
			'CREATED'))a2 on
		a1.loan_id = a2.id
	left join (
		select
			id,
			order_id,
			order_status,
			late_day,
			late_date,
			row_number() over(partition by order_id
		order by
			create_time) as rk
		from
			public.dw_gocash_gocash_collection_col_case
		where
			app_id not in ('Credits',
			'KASANDAAI'))b on
		a1.loan_id = b.order_id)ab
left join (
	select
		loan_id,
		sum(extend_fee) as extend_fee_sum
	from
		dw_gocash_go_cash_loan_gocash_core_loan_pay_flow
	where
		status = 'SUCCESS'
	group by
		1)d on
	ab.id = d.loan_id
left join (
	select
		order_id,
		id
	from
		public.dw_gocash_gocash_collection_col_case)e on
	ab.id = e.order_id
where
	due_date::date>current_date-31
	and due_date::date <= current_date+14
group by
	due_date::date,
	return_flag
"""
rucui_data=get_df_from_pg(rucui_sql)

rucui_data['shouyu_rate']=rucui_data['shouyu_loan']/rucui_data['shoucidaoqi_loan']
rucui_data['zhanqiyuqi_rate']=rucui_data['zhanqiyuqi_loan']/rucui_data['zhanqidaoqi_loan']

rucui_data_new=rucui_data[rucui_data['return_flag']=='false']
rucui_data_old=rucui_data[rucui_data['return_flag']=='true']

"""新客户---------------------------------------------------------------------"""

shouyu_list = list(rucui_data_new['shouyu_rate'])
zhanqirucui_list = list(rucui_data_new['zhanqiyuqi_rate'])
for i in range(1,16):
    rucui_data_new.loc[2*(i+29),'shouyu_rate'] = 1.55*np.mean(pd.Series(shouyu_list[i-1:i+6]))
    rucui_data_new.loc[2*(i+29),'zhanqiyuqi_rate'] = 1.45*np.mean(pd.Series(zhanqirucui_list[i-1:i+6]))
    rucui_data_new.loc[2*(i+29),'shouyu_loan'] = rucui_data_new.loc[2*(i+29),'shoucidaoqi_loan']*rucui_data_new.loc[2*(i+29),'shouyu_rate']
    rucui_data_new.loc[2*(i+29),'zhanqiyuqi_loan'] = rucui_data_new.loc[2*(i+29),'zhanqidaoqi_loan']*rucui_data_new.loc[2*(i+29),'zhanqiyuqi_rate']
    rucui_data_new.loc[2*(i+29),'rucui_loan'] = rucui_data_new.loc[2*(i+29),'shouyu_loan']+rucui_data_new.loc[2*(i+29),'zhanqiyuqi_loan']

rucui_data_new['shouyu_loan']=rucui_data_new['shouyu_loan'].astype(int)
rucui_data_new['zhanqiyuqi_loan']=rucui_data_new['zhanqiyuqi_loan'].astype(int)
rucui_data_new['rucui_loan']=rucui_data_new['rucui_loan'].astype(int)

"""老客户---------------------------------------------------------------------"""
shouyu_list = list(rucui_data_old['shouyu_rate'])
zhanqirucui_list = list(rucui_data_old['zhanqiyuqi_rate'])
for i in range(1,16):
    rucui_data_old.loc[2*(i+29)+1,'shouyu_rate'] = 1.6*np.mean(pd.Series(shouyu_list[i-1:i+6]))
    rucui_data_old.loc[2*(i+29)+1,'zhanqiyuqi_rate'] = 1.5*np.mean(pd.Series(zhanqirucui_list[i-1:i+6]))
    rucui_data_old.loc[2*(i+29)+1,'shouyu_loan'] = rucui_data_old.loc[2*(i+29)+1,'shoucidaoqi_loan']*rucui_data_old.loc[2*(i+29)+1,'shouyu_rate']
    rucui_data_old.loc[2*(i+29)+1,'zhanqiyuqi_loan'] = rucui_data_old.loc[2*(i+29)+1,'zhanqidaoqi_loan']*rucui_data_old.loc[2*(i+29)+1,'zhanqiyuqi_rate']
    rucui_data_old.loc[2*(i+29)+1,'rucui_loan'] = rucui_data_old.loc[2*(i+29)+1,'shouyu_loan']+rucui_data_old.loc[2*(i+29)+1,'zhanqiyuqi_loan']

rucui_data_old['shouyu_loan']=rucui_data_old['shouyu_loan'].astype(int)
rucui_data_old['zhanqiyuqi_loan']=rucui_data_old['zhanqiyuqi_loan'].astype(int)
rucui_data_old['rucui_loan']=rucui_data_old['rucui_loan'].astype(int)

"""合并---------------------------------------------------------------------"""
rucui_data_predict=rucui_data_new.merge(rucui_data_old,how='left',on='due_date')
rucui_data_predict['rucui_all']=rucui_data_predict['rucui_loan_x']+rucui_data_predict['rucui_loan_y']
rucui_data_predict_brief=rucui_data_predict.drop(["total_loan_x","shoucidaoqi_loan_x",'shouyu_loan_x','zhanqidaoqi_loan_x','zhanqiyuqi_loan_x','shouyu_rate_x','zhanqiyuqi_rate_x','total_loan_y','shoucidaoqi_loan_y','shouyu_loan_y','zhanqidaoqi_loan_y','zhanqiyuqi_loan_y','shouyu_rate_y','zhanqiyuqi_rate_y','return_flag_x','return_flag_y'],axis=1)
rucui_data_predict_brief.rename(columns={'due_date':'到期日','rucui_loan_x':'新客户入催量','rucui_loan_y':'老客户入催量','rucui_all':'总入催量'},inplace=True)
rucui_data_predict.to_excel(os.path.join(file_path,'rucui_data_predict_detail%s.xlsx')%pd.to_datetime('today').strftime('%Y%m%d'),index=False)
rucui_data_predict_brief.to_excel(os.path.join(file_path,'印尼逾期预测%s.xlsx')%pd.to_datetime('today').strftime('%Y%m%d'),index=False)
