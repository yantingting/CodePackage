#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 0603_召回分析.py
@Time    : 2020-06-09 09:51
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import os
import sys
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.2f' % x)
import datetime
import numpy as np
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)
sys.path.append('/Users/yantingting/Documents/MintechModel/newgenie/utils3/')
from data_io_utils import *



today_date = datetime.date.today().strftime('%m%d')
today_date
file_path = '/Users/yantingting/Seafile/风控/模型/10 印尼/召回项目/0603_印尼召回'
if not os.path.exists(file_path):
    os.makedirs(file_path)



# ''' ---------------------------- 是否定价以及每组的转化分析 ----------------------------'''
query0 = '''

with loan as (select * from rt_t_gocash_core_loan where apply_time>='2020-06-05 11:00:00'),
zh as (select  customer_id,type from zh_0603),
repay as (select customer_id,prize_type,amount,create_time,update_time,view_time,prize_status
from rt_t_gocash_core_prize_grant_flow
where prize_id in (169, 168)),
quota as (select customer_id,prize_type,amount,create_time,update_time,view_time,prize_status
from rt_t_gocash_core_prize_grant_flow
where prize_id in (170)),
price as (select * from rt_t_gocash_risk_control_pricing),
risk as (select *
from (
with rc as (select distinct orderno,createtime,customerid,ruleresultname,ruleresult
from rt_risk_mongo_gocash_riskreport
where typeid !='dj' and createtime>='2020-06-05 12:00:00' and ruleresultname like '%Model%Score%' and ruleid not in ('222','246')),
dj as(select customerid,orderno,ruleresultname,ruleresult,createtime,risklevel as djlevel
from rt_risk_mongo_gocash_riskreport 
where typeid = 'dj' and createtime>='2020-06-05 12:00:00')
select dj.customerid,dj.orderno as djorder,dj.createtime as djtime,dj.ruleresultname as dj_ruleresultname,dj.ruleresult as dj_ruleresult,dj.djlevel,
rc.orderno as rcorder,rc.createtime as rctime,rc.ruleresultname,rc.ruleresult as score,
row_number()over(partition by dj.customerid order by dj.createtime desc) as rank1
from dj
left join rc 
on rc.customerid = dj.customerid
where rc.createtime>dj.createtime or rc.createtime is null) t 
where rank1 = 1),
customer as (select id as customer_id,cell_phone from rt_t_gocash_core_customer)
select zh.customer_id as customer_id,zh.type as ordertype,
repay.amount as repaymnet,repay.create_time as repay_create_time,repay.update_time as repay_updatetime,
repay.view_time as repay_view_time,repay.prize_status as repay_prize_status,
quota.amount as quota_amount,quota.create_time as quota_create_time,quota.update_time as quota_updatetime,
quota.view_time as quota_view_time,quota.prize_status as quota_prize_status,
risk.*,
price.risk_rating,price.loan_quota,price.period,
loan.apply_time,loan.approved_principal,loan.effective_date,loan.return_flag,
customer.cell_phone
from zh 
left join repay on zh.customer_id = repay.customer_id
left join quota on zh.customer_id = quota.customer_id
left join risk on zh.customer_id::varchar = risk.customerid
left join price on risk.djorder = price.trace_id
left join loan on risk.rcorder = loan.id::varchar
left join customer on zh.customer_id = customer.customer_id;
'''

df0 = DataBase().get_df_from_pg(query0)
save_data_to_pickle(df0, file_path,'df0.pkl')
df0.info()



# ''' --------- 6/12 对未响应的样本做了第二次召回，发放了按比例减免的还款抵扣券 ---------'''

# # df0[df0['djorder'].isna()][['customer_id', 'cell_phone','ordertype']].to_excel(os.path.join(file_path,'样本0612.xlsx'))
# df_temp = pd.read_excel(os.path.join(file_path,'样本0612.xlsx'))
# df_temp.head()
# df_temp['customer_id'] = df_temp['customer_id'].astype(str)
# df_temp['cell_phone'] = df_temp['cell_phone'].astype(str)
# df_temp[df_temp['ordertype'] == 'new_reject'].head(10000).to_excel(os.path.join(file_path,'发抵扣券样本.xlsx'),index = False)



# 1) 召回的转化，发短信之后多长时间响应
# count   11017.00
# mean      534.26
# std       468.98
# min         0.00
# 25%       146.00
# 50%       344.00
# 75%       897.00
# max      1440.00

#           customer_id  djorder  rcorder  fund   dj   rc  fangkuan
# ordertype
# new_denied          13569     1779     1397   581 0.13 0.79      0.42
# new_notapply         7779       63       50    21 0.01 0.79      0.42
# new_reject         151738     5771     1775   367 0.04 0.31      0.21
# new_rescind          2116      112       88    58 0.05 0.79      0.66
# old_denied          26129     2831     2225  1166 0.11 0.79      0.52
# old_notapply        13149      109       84    46 0.01 0.77      0.55
# old_rescind          4157      352      291   232 0.08 0.83      0.80
# 2）使用提额券，未使用提额券的人数占比。的模型分分布；
# 4）

df0['message_time'] = pd.to_datetime('2020-06-05 12:00:00')
df0['djtime'] = df0['djtime'].apply(lambda x: pd.to_datetime(x))

df0['message_dj'] =df0.apply(lambda x: round((x['djtime']-x['message_time']).seconds/60,0) , axis = 1)
df0['message_dj'].describe()
df0['effective_date'] = df0['effective_date'].apply(lambda x: pd.to_datetime(x))
df0['fund'] = df0['effective_date'].apply(lambda x: 1 if x >= pd.to_datetime('2020-06-05')  else 0)
df_zhuanhua = df0.groupby(['ordertype']).agg({'customer_id': 'count',
                                'djorder': 'count',
                                'rcorder': 'count',
                                'fund' : 'sum'})
df_zhuanhua['dj'] = round(df_zhuanhua['djorder']/df_zhuanhua['customer_id'],2)
df_zhuanhua['rc'] = round(df_zhuanhua['rcorder']/df_zhuanhua['djorder'],2)
df_zhuanhua['fangkuan'] = round(df_zhuanhua['fund']/df_zhuanhua['rcorder'],2)


df0['if_quota'] = df0['quota_amount'].apply(lambda x: 1 if x>10 else 0 )

# df0[~df0['approved_principal'].isna()][['loan_quota','approved_principal','quota_prize_status']].head(30)
df0.groupby(['ordertype','if_quota','quota_prize_status']).agg({'customer_id': 'count',
                                'djorder': 'count',
                                'rcorder': 'count',
                                'fund' : 'sum'})
df0['quota_prize_status'].value_counts()

df0['customer_id'] = df0['customer_id'].astype(str)
df0.to_excel(os.path.join(file_path,'df0.xlsx'), index = False)
df0.info()


# ''' ---------------------------- 贷后表现 ----------------------------'''