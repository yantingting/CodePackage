#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 0525_召回分析.py
@Time    : 2020-05-28 11:23
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

'''
背景：在放开模型阈值之后召回疫情期间模型分高但是因为阈值收紧而拒绝的人。
     在2020-05-27 12:00:00 给一批用户（6197个，临时表中的 '测试组1'）发送短信。
分析框架：1）测试组和对照组的申请到放款的转化率
        2）测试组和对照组用户的模型分，定价等级，期望额度，风控额度，申请额度的差异；
        3）测试组和对照组首逾的差异。
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
file_path = '/Users/yantingting/Seafile/风控/模型/10 印尼/召回项目/0525_印尼召回'



# '''------------------------ 样本加载到数据库 ------------------------'''
# 所有
df_all = pd.read_excel(os.path.join(file_path,'df_scored_0525.xlsx'))
df_all = df_all.query('flag ==1')
df_all.shape
df_all.head()

# 测试组
df_test = pd.read_excel(os.path.join(file_path,'0527-28_召回客户名单.xlsx'),sheet_name='summary')
df_test['group'] = df_test['meaaage_time'].apply(lambda x :'测试组1' if x == pd.to_datetime('2020-05-27 12:00:00') else '测试组2')
df_test.shape
df_test.head()
df_test.tail()

# 拼接
df_final = df_all.merge(df_test.drop('cell_phone', axis =1), on='customer_id', how='left')
df_final['group'] = df_final['group'].fillna('对照组')
df_final.info()

# 存数并上传
remain_list = ['customer_id','prob_score','prob','ruleresult1','apply_time','meaaage_time', 'group']
df = df_final[remain_list]
df.rename(columns = {'apply_time':'last_apply'}, inplace=True)
df.head()

DataBase().auto_upload_df_to_pg(df, tb = 'temp_zh0525')
print(1)




# '''------------------------ 申请到放款转化 ------------------------'''

query0 = '''
select t1."group",
count(distinct t1.customer_id) as all_cus,
count(distinct case when t2.apply_time>'2020-05-27 12:00:00' then t2.customer_id else null end ) as all_apply,
count(distinct case when t2.apply_time>'2020-05-27 12:00:00' and t2.effective_date >='2020-05-27'then t2.customer_id else null end ) as all_loan
from temp_zh0525 t1 
left join rt_t_gocash_core_loan t2
on t1.customer_id = t2.customer_id
group by 1;
'''

df0 = DataBase().get_df_from_pg(query0)
df0['apply%'] = df0['all_apply']/df0['all_cus']
df0['loan%'] = df0['all_loan']/df0['all_apply']
df0
df0.to_excel(os.path.join(file_path,'temp.xlsx'))



# '''------------------------ 同时间段用户表现 ------------------------'''
# 测试组1是发短信召回组
query1 = '''
with loan as (select id ,customer_id,apply_time,effective_date,request_principal,approved_period from dw_gocash_go_cash_loan_gocash_core_loan 
where apply_time >'2020-05-27 12:00:00' and apply_time<='2020-06-03' and return_flag = 'true'),
dj as (select t2.loan_id as loan_id,t2.loan_quota,t2.rc_quota,t2.request_amount,t2.one_time_fee,t2.risk_rating,
t3.prequota,t3.quota,t3.realquota,t3.groupquota
from rt_t_gocash_risk_control_pricing t2 
inner join (	select	orderno,
ruleresultmap :: json ->> 'preQuota' as prequota,
ruleresultmap :: json ->> 'quota' as quota,
ruleresultmap :: json ->> 'realQuota' as realquota,
(ruleresultmap :: json ->> 'groupPrice')::json ->>'groupQuota' as groupquota
from public.rt_risk_mongo_gocash_riskreport
where typeid = 'dj'
and ruleresultmap::json ->>'isWhiteList' is null
and date(createtime)>'2020-05-01') t3 
on t2.trace_id = t3.orderno),
risk as (select customerid,orderno,ruleresult,datasources
from rt_risk_mongo_gocash_riskreport 
where ruleresultname = 'oldUserModelV2Score'),
zh as (select * from temp_zh0525),
perf as (select t1.*,
case when loan_status = 'COLLECTION' then current_date-late_date
	else round(late_fee /(approved_principal*0.025))::int end as DPD
from (select id as loan_id,effective_date,loan_status,late_fee,approved_principal,approved_period,
case when current_date-effective_date::date>=approved_period then 1 else 0 end as due_f1
from rt_t_gocash_core_loan
WHERE return_flag = 'true' and effective_date>'2020-05-26') t1 
left join (select * from 			
(select order_id,late_date,row_number() over(partition by order_id order by late_date asc) as num
from dw_gocash_gocash_collection_col_case 
where (order_status = 'COLLECTION_PAIDOFF' or order_status = 'COLLECTION')
and app_id not in ('Credits','KASANDAAI'))t
where num =1) t2 
on t1.loan_id = t2.order_id
where due_f1 = 1)
select loan.id as loanid,
loan.apply_time,
loan.effective_date,
loan.request_principal,
loan.approved_period,
dj.*,
risk.ruleresult,
zh.group,
case when DPD >=1 then 1 else 0 end as  exerx
from loan 
left join dj on loan.id = dj.loan_id 
left join perf on loan.id = perf.loan_id
left join risk on loan.id::varchar = risk.orderno
left join zh on loan.customer_id = zh.customer_id;
'''

df1 = DataBase().get_df_from_pg(query1)
df1 = df1.drop_duplicates('loan_id',keep = 'first')
df1['group'] = df1['group'].apply(lambda x: x if x!='测试组2' else '对照组')
df1.shape








def json_to_var(data):
    # 解析sql从线上取的数
    data_json= from_json(from_json(data, 'datasources'), 'vars')
    data_json= data_json.drop(['dataSourceName'],1)
    # 变为一个变量一列
    index_list = ['loan_id','apply_time','effective_date',  'approved_period','risk_rating', 'loan_quota',\
                  'period', 'ruleresult',  'prob_score',  'ruleresult1', 'group','varName']
    data_prepared = data_json.set_index(index_list).unstack('varName')
    data_prepared.columns=[col[1] for col in data_prepared.columns]
    data_prepared=data_prepared.reset_index()
    data_prepared = data_prepared.fillna(-1).replace('', -1)
    return (data_prepared)

df_temp = df1[~df1['ruleresult'].isna()]
df_var1= json_to_var(df_temp)
df_var1.shape
df_var1.head()
df_var2 = df1[df1['ruleresult'].isna()]
df_var2.drop('datasources',1,inplace=True)
df_var2.shape
df_var2.head()
df_varall = df_var1.append(df_var2)
df_varall.head()
df_varall['loan_id'] = df_varall['loan_id'].astype(str)
df_varall = df_varall[df_var1.columns.tolist()]
df_varall.to_excel(os.path.join(file_path,'监测_'+today_date+'.xlsx'))



# 额度分析
df_varall.shape #(6288, 57)
df_varall['loan_quota'] = df_varall['loan_quota'].astype(float)
df_edu= df_varall[df_varall['loan_quota']>0]
df_edu.shape #(6079, 57)

pd.pivot_table(df_edu, index = 'group',values='loan_quota',aggfunc=('count','mean',))

df_edu['ruleresult'] = df_edu['ruleresult'].astype(float)
for cols in [-1,'对照组','测试组1']:
    df_temp = df_edu[df_edu['group'] == cols]
    n = df_temp['ruleresult'].describe()
    print(cols,n)



# 单变量
create_dict(df_edu, data_sorce=None, data_type=None,useless_vars = [])


df_varall['y'] = df_varall['effective_date'].apply(lambda x: 0 if x == -1 else 1)
df_varall['y'].value_counts()
df_varall['sample_type'].value_counts()
from temp import univariate_chart

df_varall.rename(columns = {'group':'sample_type'}, inplace= True)
for cols in df_varall.columns.tolist()[11:]:
    df_varall[cols] = df_varall[cols].astype(float)
    univariate_chart(df_varall,cols,'y',n_bins= 5,default_value=-1,draw_all=False,\
                     draw_list=[-1,'对照组','测试组1'],result_path=file_path)


from data_processing import *
df = df_varall[df_varall['sample_type'] !='对照组']
dftrain = df_varall[df_varall['sample_type'] ==-1]
dftest = df_varall[df_varall['sample_type'] =='测试组1']
for cols in df_varall.columns.tolist()[11:]:
    df_varall[cols] = df_varall[cols].astype(float)
    df6 = Numeric_Bin(df,cols,target='y',n=5,special_attribute=[-1],dftrain=dftrain, dftest=dftest,result_path=file_path)[6]
    df7 = Numeric_Bin(df,cols,target='y',n=5,special_attribute=[-1],dftrain=dftrain, dftest=dftest,result_path=file_path)[7]





