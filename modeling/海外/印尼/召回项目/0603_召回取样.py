#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 0603_召回取样.py
@Time    : 2020-06-03 14:05
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

'''
背景：在月放款3000w的压力之下，对部分满足需求的客户提供提额券和还款抵扣券，激励沉默用户进行贷款。
     在2020-06-05 12:00:00 给一批用户发送提额券和抵扣券并短信提醒。
分析框架：1）测试组和对照组的申请到放款的转化率
        2）测试组和对照组用户的模型分，定价等级，期望额度，风控额度，申请额度的差异；
        3）测试组和对照组首逾的差异。

# 召回方案
客户类型		样本	方式	还款抵扣券	提额券
1）新客	拒绝	拒绝时间3/20-5/19	短信提醒		
2）新客到老客	未申请	结清时间距离现在>3个月	提额和抵扣券	15w	20w
	取消	取消时间在3个月之内	提额和抵扣券	15w	20w
	拒绝	拒绝时间在3个月之内	提额和抵扣券	10w	20w
3）老客结清	未申请	结清时间距离现在>3个月	提额和抵扣券	15w	20w
	取消	取消时间在3个月之内	提额和抵扣券	15w	20w
	拒绝	拒绝时间在3个月之内	提额和抵扣券	10w	20w
PS：					
1、逾期>3天结清或者上一笔展期次数>1的不提额					
2、本金>100w才可用抵扣券					
3、提额券的有效期7天，抵扣券的有效期30天					

'''


import os
import sys
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.2f' % x)
import datetime
import numpy as np
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)
sys.path.append('/Users/yantingting/Documents/MintechJob/newgenie1/utils3/')
from data_io_utils import *



today_date = datetime.date.today().strftime('%m%d')
today_date
file_path = '/Users/yantingting/Seafile/风控/模型/10 印尼/召回项目/0603_印尼召回'
if not os.path.exists(file_path):
    os.makedirs(file_path)




# '''--------------------- 新客拒绝  拒绝时间3/20-5/19 ---------------------'''
query0 = '''
with loan as (select * from (select id as loan_id,customer_id,loan_status,apply_time,denied_time,return_flag,
row_number()over(partition by customer_id order by apply_time desc) as rank1
from rt_t_gocash_core_loan )t
where rank1 = 1  and return_flag = 'false' and apply_time::date >='2020-03-20'::date and apply_time::date <='2020-05-19'::date),
customer as (select id as customer_id, cell_phone from rt_t_gocash_core_customer)
select loan.customer_id,customer.cell_phone,apply_time::date as apply_date
from loan 
inner join customer 
on loan.customer_id = customer.customer_id;
'''
df_new_den = DataBase().get_df_from_pg(query0)
df_new_den.shape
df_new_den['customer_id'] = df_new_den['customer_id'].astype(str)
df_new_den['cell_phone'] = df_new_den['cell_phone'].astype(str)
df_new_den.to_excel(os.path.join(file_path,'df_new_den.xlsx'),index=False)


# '''--------------------- 取消&拒绝 ---------------------'''
query1 = '''
with loan as (
select *
from (
select customer_id, loan_status, apply_time,row_number() over(partition by customer_id order by apply_time desc) as rn
from rt_t_gocash_core_loan
where return_flag = 'true'
) t
where rn=1
and loan_status in ('DENIED','RESCIND')
and apply_time between '2020-03-01' and '2020-06-04'
),
loan1 as (
select * from (select customer_id,id as loanid ,apply_time,return_flag,effective_date,due_date,paid_off_time,late_days,extend_times,loan_status,
row_number()over(partition by customer_id order by effective_date desc ) as rank1
from rt_t_gocash_core_loan 
where effective_date>='2017-10-24') t 
where rank1 = 1
),
customer as (select id as customer_id,cell_phone from rt_t_gocash_core_customer)
select loan.customer_id,customer.cell_phone,loan1.return_flag,loan1.late_days,loan1.extend_times,loan.loan_status
from loan 
left join loan1 
on loan.customer_id = loan1.customer_id
inner join customer 
on loan.customer_id = customer.customer_id
where loan.apply_time >loan1.apply_time
'''

df_den_can = DataBase().get_df_from_pg(query1)
df_den_can.shape
df_den_can.head()
df_den_can['customer_id'] = df_den_can['customer_id'].astype(str)
df_den_can['cell_phone'] = df_den_can['cell_phone'].astype(str)
df_den_can.to_excel(os.path.join(file_path,'df_den_can.xlsx'),index=False)



# '''--------------------- 未申请 ---------------------'''
query2 = '''
with loan as (select * from (
select customer_id,id as loanid ,return_flag,effective_date,due_date,paid_off_time,late_days,extend_times,loan_status,
row_number()over(partition by customer_id order by effective_date desc ) as rank1
from rt_t_gocash_core_loan 
where effective_date>='2017-10-24' )t
where rank1 = 1 and loan_status like '%PAIDOFF%'),
last_apply as (select customer_id,max(apply_time) as last_apply_time
from rt_t_gocash_core_loan 
group by 1),
customer as (select id as customer_id,cell_phone from rt_t_gocash_core_customer)
select loan.customer_id,customer.cell_phone,loan.late_days,loan.extend_times,loan.return_flag
from loan
left join last_apply 
on loan.customer_id = last_apply.customer_id
left join customer 
on loan.customer_id = customer.customer_id
where  paid_off_time >= last_apply_time
and to_timestamp(paid_off_time,'yyyy-mm-dd')<=  date('2020-06-03') - 90;
'''

df_noapply = DataBase().get_df_from_pg(query2)
df_noapply.shape
df_noapply.head()
df_noapply['late_days'].value_counts()
df_noapply['customer_id'] = df_noapply['customer_id'].astype(str)
df_noapply['cell_phone'] = df_noapply['cell_phone'].astype(str)
df_noapply.to_excel(os.path.join(file_path,'df_noapply.xlsx'),index=False)



# '''------------------------------------------- 数据汇总并上传 --------------------------------------'''
df_new_den = pd.read_excel(os.path.join(file_path,'df_new_den.xlsx'))
df_new_den['type'] = 'new_reject'
df_new_den.shape
df_new_den.head()
df_new_den['if_quota'] = 0
df_new_den['quota'] = '0w'
df_new_den['repayment'] = '0w'


df_den_can = pd.read_excel(os.path.join(file_path,'df_den_can.xlsx'))
df_den_can.shape
df_den_can.head()
df_den_can['return_flag'] = df_den_can['return_flag'].astype('str')
df_den_can['type'] = df_den_can.apply(lambda x:'new_rescind' if ((x['return_flag'] =='False') & (x['loan_status'] == 'RESCIND'))
                                      else 'new_denied' if ((x['return_flag'] =='False') & (x['loan_status'] == 'DENIED'))
                                      else 'old_rescind' if ((x['return_flag'] =='True') & (x['loan_status'] == 'RESCIND'))
                                      else 'old_denied', axis = 1)
df_den_can['if_quota'] = df_den_can.apply(lambda x: 0 if ((x['late_days']>3) | (x['extend_times']>1)) else 1 ,axis = 1)
df_den_can['quota'] = df_den_can.apply(lambda x: '10w' if ((x['if_quota'] == 1) & (x['loan_status'] == 'DENIED'))
                                                 else '15w' if ((x['if_quota'] == 1) & (x['loan_status'] == 'RESCIND'))
                                                 else '0w', axis =1)
df_den_can['repayment'] = '20w'

df_noapply = pd.read_excel(os.path.join(file_path,'df_noapply.xlsx'))
df_noapply.shape
df_noapply.head()
df_noapply['return_flag'] = df_noapply['return_flag'].astype('str')
df_noapply['type'] = df_noapply['return_flag'].apply(lambda x:'new_notapply' if x == 'False' else 'old_notapply')
df_noapply['if_quota'] = df_noapply.apply(lambda x: 0 if ((x['late_days']>3) | (x['extend_times']>1)) else 1 ,axis = 1)
df_noapply['quota'] = df_noapply['if_quota'].apply(lambda x: '15w' if x == 1 else '0w')
df_noapply['repayment'] = '20w'


remain_list = ['customer_id','type','if_quota','quota','repayment']
df_all = pd.concat([df_new_den[remain_list],df_den_can[remain_list],df_noapply[remain_list]],axis = 0)
df_all.head()
df_all.shape
df_all['quota'].value_counts()
df_all['customer_id'].nunique()
df_all['repayment'].value_counts()

DataBase().auto_upload_df_to_pg(df_all,tb='zh_0603')

