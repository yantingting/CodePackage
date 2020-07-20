
"""
20190704to20190815放款人群回溯打分
10/25更新
"""

import os
import sys
import json
import logging
import warnings
warnings.filterwarnings('ignore')

sys.path.append('/Users/Mint/Desktop/repos/genie')

import pandas as pd
import numpy as np
from jinja2 import Template

#python3种reload函数不能直接使用
from imp import reload

from utils3.data_io_utils import *
import utils3.misc_utils as mu
import utils3.summary_statistics as ss
import utils3.metrics as mt
import xgboost as xgb
from xgboost import DMatrix

data_path = 'D:/Model/201908_qsfq_model/01_data/'
result_path = 'D:/Model/201908_qsfq_model/02_result/'

##
#读入模型数据
##


features_in_model = list(pd.read_excel('D:/Model/201908_qsfq_model/02_result_0928/result_0926_52.xlsx', sheet_name= 'features_in_model')[0])


#放款取performance
 perf_sql = """
 select order_no
 , business_id
 , effective_date
 , fst_term_late_day
 , fst_term_due_date
 , late_day
 , lst_term_due_date
 , max_late_day
 from t_loan_performance
 where business_id in ('rong360') and effective_date between '{{ start_date }}' and '{{ end_date }}' and dt = '{{ dt }}'
 """

perf_data = get_df_from_pg(Template(perf_sql).render(start_date = '2019-08-27', end_date = '2019-10-18', dt = '20191025'))
perf_data.order_no.nunique()
perf_data.max_late_day.value_counts().sort_index()
np.where(perf_data.max_late_day >= 7,1,0).sum()
np.where(perf_data.loc[perf_data.effective_date <= '2019-08-07']['max_late_day'] >= 7,1,0).sum() #相差13个bad


#银联
yl_huisu = pd.read_excel('D:/seafile/Seafile/风控/模型/数据测试/01银联/明特量化_定制化变量_20190929.xlsx')[['order_no','created_time','借记卡得分']].rename(columns = {'借记卡得分':'RMS002'})
yl_huisu = yl_huisu.loc[yl_huisu.created_time < '2019-09-24 00:00:00']
yl_huisu.created_time.max()

#百融
# br_debt_huisu = pd.read_excel('D:/seafile/Seafile/风控/模型/数据测试/02百融/01偿债压力指数.xlsx', sheet_name = '偿债压力指数').drop(0)
# br_debt_huisu = br_debt_huisu[['cus_num','drs_nodebtscore']].rename(columns = {'cus_num':'order_no'})


#百融线上数据
br_sql = """
with perf as (
select order_no
from t_loan_performance
where business_id in ('rong360') and effective_date between '2019-08-27' and '2019-10-23' and dt = '20191025'
),
related as (
select orderno, messageno
from risk_mongo_installmentmessagerelated
where businessid in ('rong360') and topicname in ('Application_thirdPart_bairongdebtrepaystress')
and databasename = 'installmentBairongDebtRepayStress'
),
 br_stress as (
select taskid
, cast(oss::json #>> '{result}' as json)::json #>>'{DebtRepayStress,nodebtscore}' as drs_nodebtscore
from risk_oss_bairong_debt_repay_stress
)
select t0.order_no, t2.orderno, t3.drs_nodebtscore
from perf t0
left join related t2 on t0.order_no = t2.orderno
left join br_stress t3 on t2.messageno = t3.taskid
"""
br_online_data = get_df_from_pg(br_sql)

br_online_data.shape #1607
br_online_data = br_online_data.drop_duplicates()

len([i for i in br_online_data.order_no.unique() if 'CL' in i]) #67

#合并百融数据
# br_debt_huisu.head()
# br_data = pd.concat([br_online_data.drop(['orderno'],1), br_debt_huisu])
# br_data.order_no.nunique()
#
# save_data_to_pickle(br_data, data_path, 'br_nodebtscore_0704to0815.pkl')

#
x_with_y = pd.read_excel(os.path.join(data_path, 'x_with_y_v6.xlsx'))
x_with_y.index = x_with_y.order_no

test_new_var = x_with_y.loc[x_with_y.sample_set == 'test_new'][features_in_model].drop(['RMS002','drs_nodebtscore'],1)

test_new_var2 = test_new_var.merge(yl_huisu.drop(['created_time'],1), left_index = True, right_on = 'order_no', how = 'left')
test_new_var2 = test_new_var2.merge(br_data, on = 'order_no', how = 'left')

test_new_var2 = test_new_var2.drop(['order_no_x','order_no_y'],1)
test_new_var2.index = test_new_var2.order_no

#打分
test_new_var2 = test_new_var2.fillna(-1)

for i in test_new_var2.columns:
    if i != 'order_no':
        print (i)
        test_new_var2[i] = test_new_var2[i].astype(float)

test_new_var2.dtypes


#读入模型数据
len(features_in_model)


#重新打分
# LOAD MODEL
mymodel = xgb.Booster()
mymodel.load_model( 'D:/Model/201908_qsfq_model/02_result_0928/dc 0926 52.model')
mymodel.load_model( 'D:/Model/201908_qsfq_model/samp_cond5 20190919 1920.model')


mydata = v6yl_data[features_in_model]
mydata = v6_data[features_in_model_v6]

# PREPARE DATA
var_selected = mydata[features_in_model_v6].values

# PREDICT SCORES
var_selected = DMatrix(var_selected)
ypred = mymodel.predict(var_selected)
ypred

score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([mydata.index, score, ypred]).T
data_scored.head(200)
data_scored = data_scored.rename(columns = {0:'order_no',1:'model_score',2:'prob'})


train_bin = [-np.inf,566, 582, 600, 610, 618, 625, 633, 642, 652, np.inf]
train_bin_20 = [-np.inf,550, 566, 574, 582, 591, 600, 605, 610, 615, 618, 621, 625, 629, 633, 637, 642, 646, 652, 659,np.inf]
train_bin_30 = [-np.inf,541, 557, 566, 571, 576, 582, 588, 594, 600, 603, 607, 610, 613, 616, 618, 620, 623, 625, 628, 630, 633,636,639, 642,645,648,652,656, 662, np.inf]




train_bin_v6 = [-np.inf,576, 590, 599, 606, 612, 618, 625, 632, 642, np.inf]

data_scored['score_bin'] = pd.cut(data_scored.model_score, bins = train_bin_v6)
data_scored['score_bin_20'] = pd.cut(data_scored.model_score, bins = train_bin_20)



data_scored.score_bin = data_scored.score_bin.astype(str)
data_scored.score_bin_20 = data_scored.score_bin_20.astype(str)


data_scored.head()

#合并变量和分数
data_scored_v6 = data_scored.merge(x_with_y2[['order_no','business_id', 'effective_date']], on = 'order_no')
data_scored_v6.head()


score_var.to_excel(data_path + "score_with_var0704to0807.xlsx",index=False)

#三十等分

test_new_score = score_var[['order_no','business_id','effective_date','model_score','score_bin','score_bin_20']]
test_new_score['sample_set'] = 'test_new'
test_new_score['score_bin_30'] = pd.cut(test_new_score.model_score, bins = train_bin_30)
test_new_score = test_new_score.rename(columns = {'model_score':'score'})

pd.qcut(pd.DataFrame(train_score)['score'], q=30,duplicates ='drop', precision=0).astype(str).value_counts().sort_index()



"""
分数上传到数据库
"""

train_score = pd.read_excel('D:/Model/201908_qsfq_model/02_result_0928/result_0926_52.xlsx', sheet_name= 'data_scored_train')[['order_no','effective_date','score']]
train_score['sample_set'] = 'train'
train_score['score_bin']  = pd.cut(train_score.score, bins = train_bin)
train_score['score_bin_20']  = pd.cut(train_score.score, bins = train_bin_20)
train_score['score_bin_30']  = pd.cut(train_score.score, bins = train_bin_30)

test_score = pd.read_excel('D:/Model/201908_qsfq_model/02_result_0928/result_0926_52.xlsx', sheet_name= 'data_scored_test')[['order_no','effective_date','score']]
test_score['sample_set'] = 'test'
test_score['score_bin']  = pd.cut(test_score.score, bins = train_bin)
test_score['score_bin_20']  = pd.cut(test_score.score, bins = train_bin_20)
test_score['score_bin_30']  = pd.cut(test_score.score, bins = train_bin_30)


model_score_all = pd.concat([train_score, test_score, test_new_score])
model_score_all.business_id = model_score_all.business_id.fillna('tb')

model_score_all.score_bin = model_score_all.score_bin.astype(str)
model_score_all.score_bin_20 = model_score_all.score_bin_20.astype(str)
model_score_all.score_bin_30 = model_score_all.score_bin_30.astype(str)

model_score_all['apply_time'] = ''

data_upload_1009 = score_with_var_1009.loc[score_with_var_1009.anti_fraud_rule_node == 'N'][['order_no','model_score','score_bin','score_bin_20','score_bin_30']]
data_upload_1009['sample_set'] = 'rong360_1009'
data_upload_1009['business_id'] = 'rong360'
data_upload_1009['effective_date'] = ''
data_upload_1009['apply_time'] = '2019-10-09'
data_upload_1009 = data_upload_1009[upload_var_order]


SQL_CREATE_TABLE = """
CREATE TABLE temp_credit_modelscore_v6yl(
    order_no VARCHAR,
    effective_date VARCHAR,
    business_id VARCHAR,
    sample_set VARCHAR,
    apply_time VARCHAR,
    model_score FLOAT,
    score_bin VARCHAR,
    score_bin_20 VARCHAR,
    score_bin_30 VARCHAR
)
"""
upload_df_to_pg(SQL_CREATE_TABLE)

insert = """
INSERT INTO temp_credit_modelscore_v6yl
 VALUES
{% for var in var_list %}
{{ var }},
{% endfor %}
"""

var_list = []

upload_var_order = ['order_no','effective_date','business_id','sample_set','apply_time','model_score','score_bin','score_bin_20','score_bin_30']

model_score_all = model_score_all[upload_var_order]


#for cols, rows in model_score_all.iterrows():
for cols, rows in data_upload_1009.iterrows():
    c = tuple(rows)
    var_list.append(c)

var_list

insert_sql = Template(insert).render(var_list=var_list)[:-2]
insert_sql = insert_sql.replace('\n\n','')
insert_sql = insert_sql.replace('\n','')

upload_df_to_pg(insert_sql)

SQL_ALTER_TABLE = """
ALTER  TABLE temp_credit_modelscore_v6yl
ADD COLUMN business_id VARCHAR,
ADD COLUMN effective_date VARCHAR,
ADD COLUMN sample_set VARCHAR,
ADD COLUMN apply_time VARCHAR,
ADD COLUMN score_bin_30 VARCHAR
--DROP COLUMN model_score_v6
"""
upload_df_to_pg(SQL_ALTER_TABLE)

model_score_all.to_excel(os.path.join(data_path,'score_upload.xlsx'))


#####10/30回溯打分

#v6yl
features_in_model = list(pd.read_excel('D:/Model/201908_qsfq_model/02_result_0928/result_0926_52.xlsx', sheet_name= 'features_in_model')[0])
len(set(x_with_y2.columns).intersection(set(features_in_model)))

v6yl_data = x_with_y2.loc[('2019-07-04' <=x_with_y2.effective_date) & (x_with_y2.effective_date <='2019-09-20')][features_in_model]
data_scored_v6yl.to_excel(local_data_path + 'v6yl_score_0704to0801.xlsx')



#v6
features_in_model_v6 = pd.read_excel('D:/seafile/Seafile/风控/模型/01 轻松分期模型/dcCreditV6/04 Document/v6/轻松分期申请模型（Bill模型）入模变量顺序 v6.xlsx', sheet_name = 'v6')[['模型变量名']]
features_in_model_v6 = list(features_in_model_v6['模型变量名'])
len(set(x_with_y2.columns).intersection(set(features_in_model_v6)))
v6_data = x_with_y2.loc[('2019-07-04' <=x_with_y2.effective_date) & (x_with_y2.effective_date <='2019-09-20')][features_in_model_v6]

data_scored_v6.to_excel(local_data_path + 'v6_score_0704to0801.xlsx')
