#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 模型阈值调整.py
@Time    : 2020-06-02 14:41
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import this

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

file_path = '/Users/yantingting/Seafile/风控/模型/10 印尼/印尼重启计划'

def model_per(sql,feature_grid):
    query = sql
    df = DataBase().get_df_from_pg(query)
    df['loan_id'] = df['loan_id'].astype(str)
    df['bin'] = pd.cut(df['score'].astype(float), feature_grid, include_lowest=True, duplicates='drop')
    return df



# '''--------------------------------- 老客 --------------------------------- '''
query = '''
with perf as (select t1.*,
case when loan_status = 'COLLECTION' then current_date-late_date
	else round(late_fee /(approved_principal*0.025))::int end as DPD
from (select id as loan_id,effective_date,loan_status,late_fee,approved_principal,approved_period,
--case when current_date::Date-effective_date::Date>=approved_period+6 then 1 else 0 end as due_f7,
case when current_date::Date-effective_date::Date>=approved_period then 1 else 0 end as due_f1
from rt_t_gocash_core_loan
WHERE ((effective_date between '2020-03-10' and '2020-03-20') or (effective_date between '2020-05-01' and '2020-06-20'))
and return_flag = 'true') t1 
left join (select * from 			
(select order_id,late_date,row_number() over(partition by order_id order by late_date asc) as num
from dw_gocash_gocash_collection_col_case 
where (order_status = 'COLLECTION_PAIDOFF' or order_status = 'COLLECTION')
and app_id not in ('Credits','KASANDAAI'))t
where num =1) t2 
on t1.loan_id = t2.order_id
where due_f1 = 1),
score as (
SELECT orderno
     , ruleresultname
     , pipelineid
     , ruleresult as score
FROM rt_risk_mongo_gocash_riskreport
WHERE ruleresultname ='oldUserModelV2Score'
)
SELECT perf.loan_id,
perf.effective_date,
perf.approved_period,
perf.loan_status,
case when DPD >=1 then 1 else 0 end as  exerx,
case when DPD >=3 then 1 else 0 end as  exer3,
case when DPD >=7 then 1 else 0 end as  exer7,
case when loan_status = 'COLLECTION' then 1 else 0 end as flag,
score.ruleresultname,score.score,score.pipelineid
FROM perf
INNER JOIN score on perf.loan_id :: varchar = score.orderno
;
'''

cut1 = [0,628.0, 638.0, 644.0, 650.0, 654.0, 659.0, 664.0, 670.0, 678.0, 1000]
df_v2 = model_per(query,cut1)
df_v2.shape
df_v2.head()
df_v2.to_excel(os.path.join(file_path,'v2_model.xlsx'))


# '''--------------------------------- 新客V5 --------------------------------- '''

query1 = '''with perf as (select t1.*,
case when loan_status = 'COLLECTION' then current_date-late_date
	else round(late_fee /(approved_principal*0.025))::int end as DPD
from (select id as loan_id,effective_date,loan_status,late_fee,approved_principal,approved_period,
--case when current_date::Date-effective_date::Date>=approved_period+6 then 1 else 0 end as due_f7,
case when current_date::Date-effective_date::Date>=approved_period then 1 else 0 end as due_f1
from rt_t_gocash_core_loan
WHERE effective_date!='1970-01-01' 
and ((effective_date between '2020-03-04'  and '2020-06-20'))
and return_flag = 'false') t1 
left join (select * from 			
(select order_id,late_date,row_number() over(partition by order_id order by late_date asc) as num
from dw_gocash_gocash_collection_col_case 
where (order_status = 'COLLECTION_PAIDOFF' or order_status = 'COLLECTION')
and app_id not in ('Credits','KASANDAAI'))t
where num =1) t2 
on t1.loan_id = t2.order_id
where due_f1 = 1),
score as (
SELECT orderno
     , ruleresultname
     , pipelineid
     , ruleresult as score
FROM rt_risk_mongo_gocash_riskreport
WHERE ruleresultname = 'newUserModelScoreV5'
)
SELECT perf.loan_id,
perf.effective_date,
perf.loan_status,
perf.approved_period,
case when DPD >=1 then 1 else 0 end as  exerx,
case when DPD >=3 then 1 else 0 end as  exer3,
case when DPD >=7 then 1 else 0 end as  exer7,
case when loan_status = 'COLLECTION' then 1 else 0 end as flag,
score.ruleresultname,score.score,score.pipelineid
FROM perf
INNER JOIN score on perf.loan_id :: varchar = score.orderno
;
'''

cut2 = [0, 613, 618, 626, 633, 638, 649, 656, 1000]
df_v5 = model_per(query1,cut2)
df_v5.shape
df_v5.head()
df_v5.to_excel(os.path.join(file_path,'v5_model.xlsx'))



# '''--------------------------------- 新客V6 --------------------------------- '''
# 放款人群的表现
query2 = '''with perf as (select t1.*,
case when loan_status = 'COLLECTION' then current_date-late_date
	else round(late_fee /(approved_principal*0.025))::int end as DPD
from (select id as loan_id,effective_date,loan_status,late_fee,approved_principal,approved_period,
--case when current_date::Date-effective_date::Date>=approved_period+6 then 1 else 0 end as due_f7,
case when current_date::Date-effective_date::Date>=approved_period then 1 else 0 end as due_f1
from rt_t_gocash_core_loan
WHERE effective_date!='1970-01-01' 
and ((effective_date between '2020-03-04' and '2020-06-20'))
and return_flag = 'false') t1 
left join (select * from 			
(select order_id,late_date,row_number() over(partition by order_id order by late_date asc) as num
from dw_gocash_gocash_collection_col_case 
where (order_status = 'COLLECTION_PAIDOFF' or order_status = 'COLLECTION')
and app_id not in ('Credits','KASANDAAI'))t
where num =1) t2 
on t1.loan_id = t2.order_id
where due_f1 = 1),
score as (
SELECT orderno
     , ruleresultname
     , pipelineid
     , ruleresult as score
FROM rt_risk_mongo_gocash_riskreport
WHERE ruleresultname = 'newUserModelScoreV6'
)
SELECT perf.loan_id,
perf.effective_date,
perf.loan_status,
perf.approved_period,
case when DPD >=1 then 1 else 0 end as  exerx,
case when DPD >=3 then 1 else 0 end as  exer3,
case when DPD >=7 then 1 else 0 end as  exer7,
case when loan_status = 'COLLECTION' then 1 else 0 end as flag,
score.ruleresultname,score.score,score.pipelineid
FROM perf
INNER JOIN score on perf.loan_id :: varchar = score.orderno
;
'''

cut3 = [0, 634, 638, 643, 648, 650, 653, 657, 661, 666, 671, 1000]
df_v6 = model_per(query2,cut3)
df_v6.shape
df_v6.head()
df_v6.to_excel(os.path.join(file_path,'v6_model.xlsx'))



# '''--------------------------------- 新客V4 --------------------------------- '''

query3 = '''with perf as (select t1.*,
case when loan_status = 'COLLECTION' then current_date-late_date
	else round(late_fee /(approved_principal*0.025))::int end as DPD
from (select id as loan_id,effective_date,loan_status,late_fee,approved_principal,approved_period,
--case when current_date::Date-effective_date::Date>=approved_period+6 then 1 else 0 end as due_f7
case when current_date::Date-effective_date::Date>=approved_period then 1 else 0 end as due_f1
from rt_t_gocash_core_loan
WHERE effective_date!='1970-01-01' 
and ((effective_date between '2020-03-04'  and '2020-06-20'))
and return_flag = 'false') t1 
left join (select * from 			
(select order_id,late_date,row_number() over(partition by order_id order by late_date asc) as num
from dw_gocash_gocash_collection_col_case 
where (order_status = 'COLLECTION_PAIDOFF' or order_status = 'COLLECTION')
and app_id not in ('Credits','KASANDAAI'))t
where num =1) t2 
on t1.loan_id = t2.order_id
where due_f1 = 1),
score as (
SELECT orderno
     , ruleresultname
     , pipelineid
     , ruleresult as score
FROM rt_risk_mongo_gocash_riskreport
WHERE ruleresultname = 'newUserModelV4Score'
)
SELECT perf.loan_id,
perf.effective_date,
perf.loan_status,
perf.approved_period,
case when DPD >=1 then 1 else 0 end as  exerx,
case when DPD >=3 then 1 else 0 end as  exer3,
case when DPD >=7 then 1 else 0 end as  exer7,
case when loan_status = 'COLLECTION' then 1 else 0 end as flag,
score.ruleresultname,score.score,score.pipelineid
FROM perf
INNER JOIN score on perf.loan_id :: varchar = score.orderno
;
'''

cut4 = [0, 0.103, 0.143, 0.176, 0.206, 0.234, 0.26, 0.297, 0.349, 0.435, 1]
df_v4 = model_per(query3,cut4)
df_v4.shape
df_v4.head()
df_v4.to_excel(os.path.join(file_path,'v4_model.xlsx'))



# '''--------------------------------- Linkaja --------------------------------- '''

query4 = '''with perf as (select t1.*,
case when loan_status = 'COLLECTION' then current_date-late_date
	else round(late_fee /(approved_principal*0.025))::int end as DPD
from (select id as loan_id,effective_date,loan_status,late_fee,approved_principal,approved_period,
--case when current_date::Date-effective_date::Date>=approved_period+6 then 1 else 0 end as due_f7
case when current_date::Date-effective_date::Date>=approved_period then 1 else 0 end as due_f1
from rt_t_gocash_core_loan
WHERE effective_date!='1970-01-01' 
and ((effective_date between '2020-03-04'  and '2020-06-10'))
and return_flag = 'false') t1 
left join (select * from 			
(select order_id,late_date,row_number() over(partition by order_id order by late_date asc) as num
from dw_gocash_gocash_collection_col_case 
where (order_status = 'COLLECTION_PAIDOFF' or order_status = 'COLLECTION')
and app_id not in ('Credits','KASANDAAI'))t
where num =1) t2 
on t1.loan_id = t2.order_id
where due_f1 = 1),
score as (
SELECT orderno
     , ruleresultname
     , pipelineid
     , ruleresult as score
FROM rt_risk_mongo_gocash_riskreport
WHERE ruleresultname = 'linkAjaNewModelV1Score'
)
SELECT perf.loan_id,
perf.effective_date,
perf.loan_status,
perf.approved_period,
case when DPD >=1 then 1 else 0 end as  exerx,
case when DPD >=3 then 1 else 0 end as  exer3,
case when DPD >=7 then 1 else 0 end as  exer7,
case when loan_status = 'COLLECTION' then 1 else 0 end as flag,
score.ruleresultname,score.score,score.pipelineid
FROM perf
INNER JOIN score on perf.loan_id :: varchar = score.orderno
;
'''

cut5 =[0, 581.0, 599.0, 609.0, 616.0, 621.0, 627.0, 632.0, 639.0, 648.0, 1000]

df_linkaja = model_per(query4,cut5)
df_linkaja.shape
df_linkaja.head()
df_linkaja.to_excel(os.path.join(file_path,'linkaja_model.xlsx'))



# ''' --------------------------------- 申请人群看分布 --------------------------------- '''
query_zhl = '''
with risk as (select orderno,nodename,pipelineid,
case when noderesult = 'R' then 1 else 0 end as noderesult,
ruleresultname,ruleresult,
case when ruleresult ='reject' then 1 
     when ruleresult in ('pass','nodata') then 0 
     else ruleresult::float end as rulerresult
from rt_risk_mongo_gocash_riskreport 
where ((ruleresultname like '%Score%' and ruleresultname like '%Model%') or isdecision = '1')
and createtime::date >='2020-06-01' and createtime::date <='2020-06-16' and typeid != 'dj'),
loan as (select id as loan_id,date(apply_time) as apply_date,return_flag,approved_period
from rt_t_gocash_core_loan )
select r.*,l.*
from loan as l 
right join risk r 
on l.loan_id::varchar = r.orderno
'''
df_apply = DataBase().get_df_from_pg(query_zhl)
df_apply.shape
df_apply = df_apply.drop('loan_id',axis = 1)
df_apply.rename(columns = {'orderno':'loan_id'}, inplace = True)
df_apply.head()
df_apply['loan_id'].nunique()

# 主表
tb = df_apply[['loan_id','apply_date','return_flag','approved_period','pipelineid']].drop_duplicates().copy()
tb.shape

# 节点
df_node = df_apply[['loan_id','nodename','noderesult']].drop_duplicates()
df_node.head()
tb_node = pd.pivot_table(df_node,index = ['loan_id'],columns=['nodename'],values=['noderesult'])
tb_node.columns = tb_node.columns.droplevel(0)
tb_node.head()
tb_node.shape

# 详细规则
df_rule = df_apply.copy()
tb_rule = pd.pivot_table(df_rule,index =['loan_id'], columns=['ruleresultname'],values=['rulerresult'])
tb_rule.columns = tb_rule.columns.droplevel(0)
tb_rule.head()
tb_rule.shape


df_all = tb.merge(tb_node,on = 'loan_id',how = 'inner').merge(tb_rule,on = 'loan_id',how = 'inner' )
df_all.head()
df_all.shape


# '''--------------------------------- 给申请样本打标签 --------------------------------- '''
thirdpart_rule = [ 'denyIziInquiriesByTypeTotal',
 'denyIziPhoneAge']

thirdpart_black = ['xhPrcBlacklist',
 'xhPrcGreylist',
 'ruhiaBlacklist']
model_node = [ 'linkAjaNewModelV1Result',
 'newUserModelResultV5',
 'newUserModelResultV6',
 'newUserModelV4Result',
 'oldUserModelV2Result',]

basic_node = [
 'bankAntifraud',\
 'blackList',\
 'callWithBadGuys',\
 'crossCheckRule7Result',\
 'crossCheckRule9Result',\
 'deniedByOccupation',\
 'deniedLoanWith5fields',\
 'duplicateCustomerWith5fields',\
 'existsLoanWith5fields',\
 'existsLoanWithContacts',\
 'industryBlacklist',\
 'installDangerApp',\
 'installSpecifyApp',\
 'invalidAge',\
 'sameGps3OneDayResult',\
 'sameGps3SevenDayResult',\
 'sameGps3ThreeDayResult',\
 'useSimulator',\
 'workLocationRestrict' ]

df_all['basic_node_new'] = df_all.apply(lambda x: x[basic_node].sum(),axis = 1)
df_all['basic_node_new1'] = df_all['basic_node_new'].apply(lambda x : 1 if x>=1 else 0)
df_all['loan_id'] = df_all['loan_id'].astype(str)
df_all.to_excel(os.path.join(file_path,'df_all.xlsx'))


df_all = pd.read_excel(os.path.join(file_path,'df_all.xlsx'))
# '''--------------------------------- 拆分数据集 --------------------------------- '''
# [i for i in df_all.columns.tolist() if i.find('Score')>-1]
# m_list= [ 'oldUserModelV2Score', 'newUserModelScoreV5', 'newUserModelScoreV6', 'newUserModelV4Score','linkAjaNewModelV1Score']
# cut_list = [cut1,cut2,cut3,cut4,cut5]

remain_list = ['loan_id','apply_date','return_flag','approved_period','pipelineid','basic_node_new1']

v2_apply = df_all[~df_all['oldUserModelV2Score'].isnull()][remain_list + ['oldUserModelV2Score']]
v2_apply['bin'] = pd.cut(v2_apply['oldUserModelV2Score'].astype(float), cut1, include_lowest=True, duplicates='drop')
v2_apply.shape
v4_apply = df_all[~df_all['newUserModelV4Score'].isnull()][remain_list+ ['newUserModelV4Score']]
v4_apply['bin'] = pd.cut(v4_apply['newUserModelV4Score'].astype(float), cut4, include_lowest=True, duplicates='drop')
v4_apply.shape
v5_apply = df_all[~df_all['newUserModelScoreV5'].isnull()][remain_list + ['newUserModelScoreV5']]
v5_apply['bin'] = pd.cut(v5_apply['newUserModelScoreV5'].astype(float), cut2, include_lowest=True, duplicates='drop')
v5_apply.shape
v6_apply = df_all[~df_all['newUserModelScoreV6'].isnull()][remain_list + ['newUserModelScoreV6']]
v6_apply['bin'] = pd.cut(v6_apply['newUserModelScoreV6'].astype(float), cut3, include_lowest=True, duplicates='drop')
v6_apply.shape
linkaja_apply = df_all[~df_all['linkAjaNewModelV1Score'].isnull()][remain_list + ['linkAjaNewModelV1Score']]
linkaja_apply['bin'] = pd.cut(linkaja_apply['linkAjaNewModelV1Score'].astype(float), cut5, include_lowest=True, duplicates='drop')
linkaja_apply.shape



# '''--------------------------------- V4&V5 --------------------------------- '''
writer1 = pd.ExcelWriter(os.path.join(file_path,'v4_v5.xlsx'))
v4 = pd.read_excel(os.path.join(file_path,'v4_model.xlsx'))
v4.rename(columns = {'bin': 'v4_bin'}, inplace= True)
v4['time_flag'] = v4['effective_date'].apply(lambda x: 1 if x<='2020-05-19'
                                                  else 2 if x<='2020-06-03'
                                                  else 3)
v5 = pd.read_excel(os.path.join(file_path,'v5_model.xlsx'))
v5.rename(columns = {'bin': 'v5_bin'}, inplace= True)
v5['time_flag'] = v5['effective_date'].apply(lambda x: 1 if x <='2020-04-09 '
                                                  else 2 if x<='2020-05-19'
                                                  else 3 if x<='2020-06-03'
                                                  else 4)
v5.rename(columns = {'time_flag': 'v5_time_flag'}, inplace= True)
print(v4.shape,v5.shape)
v4_v5 = v4.merge(v5[['loan_id','v5_bin','v5_time_flag']], on = 'loan_id',how ='inner')
v4_v5.shape

v5_apply.head()
v4_apply.rename(columns = {'bin': 'v4_bin'}, inplace= True)
v5_apply.rename(columns = {'bin': 'v5_bin'}, inplace= True)
print(v4_apply.shape,v5_apply.shape)
v4_v5_apply = v4_apply.merge(v5_apply[['loan_id','v5_bin']], on = 'loan_id',how ='left')
v4_v5_apply.shape

v4_v5.to_excel(writer1,sheet_name='v4_v5')
v4_v5_apply.to_excel(writer1,sheet_name='v4_v5_apply')
writer1.save()


# '''--------------------------------- V4&V6 --------------------------------- '''
writer2 = pd.ExcelWriter(os.path.join(file_path,'v4_v6.xlsx'))
v6 = pd.read_excel(os.path.join(file_path,'v6_model.xlsx'))
v6.rename(columns = {'bin': 'v6_bin'}, inplace= True)
v6['time_flag'] = v6['effective_date'].apply(lambda x: 1 if x <='2020-04-09'
                                                  else 2 if x<='2020-05-19'
                                                  else 3 if x<='2020-06-03'
                                                  else 4)

v6.rename(columns = {'time_flag': 'v6_time_flag'}, inplace= True)
print(v4.shape,v5.shape)
print(v4.shape,v6.shape)
v4_v6 = v4.merge(v6[['loan_id','v6_bin','v6_time_flag']], on = 'loan_id',how ='inner')
v4_v6.shape

v6_apply.rename(columns = {'bin': 'v6_bin'}, inplace= True)
print(v4_apply.shape,v6_apply.shape)
v4_v6_apply = v4_apply.merge(v6_apply[['loan_id','v6_bin']], on = 'loan_id',how ='left')
v4_v6_apply.shape

v4_v6.to_excel(writer2,sheet_name='v4_v6')
v4_v6_apply.to_excel(writer2,sheet_name='v4_v6_apply')
writer2.save()


# '''--------------------------------- V2 --------------------------------- '''
writer3 = pd.ExcelWriter(os.path.join(file_path,'V2.xlsx'))
v2 = pd.read_excel(os.path.join(file_path,'v2_model.xlsx'))
v2.rename(columns = {'bin': 'v2_bin'}, inplace= True)
v2['time_flag'] = v2['effective_date'].apply(lambda x: 1 if x <='2020-03-11'
                                                  else 2 if x<='2020-03-25'
                                                  else 3 if x<='2020-05-07'
                                                  else 4 if x<='2020-05-25'
                                                  else 5 if x<='2020-06-03'
                                                  else 6)

print(v2.shape)

v2_apply.rename(columns = {'bin': 'v2_bin'}, inplace= True)
print(v2_apply.shape)

v2.to_excel(writer3,sheet_name='v2')
v2_apply.to_excel(writer3,sheet_name='v2_apply')
writer3.save()


# '''--------------------------------- linkaja --------------------------------- '''
writer4 = pd.ExcelWriter(os.path.join(file_path,'linkaja.xlsx'))
linkaja = pd.read_excel(os.path.join(file_path,'linkaja_model.xlsx'))
linkaja.rename(columns = {'bin': 'linkaja_bin'}, inplace= True)
linkaja['time_flag'] = linkaja['effective_date'].apply(lambda x: 1 if x <='2020-04-08'
                                                  else 2 if x<='2020-05-14'
                                                  else 3 )
print(linkaja.shape)

linkaja_apply.rename(columns = {'bin': 'linkaja_bin'}, inplace= True)
print(linkaja_apply.shape)
linkaja.to_excel(writer4,sheet_name='linkaja')
linkaja_apply.to_excel(writer4,sheet_name='linkaja_apply')
writer4.save()








