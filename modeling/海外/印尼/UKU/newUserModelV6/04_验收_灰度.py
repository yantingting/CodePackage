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


# 模型分分布
sql = '''
select orderno as loan_id, substring(ruleresult,1,3)::int as scorev6
from rt_risk_mongo_gocash_riskreportgray
where createtime::date between '2020-03-04' and '2020-03-08' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
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
psi.rename(columns={'0':'ct'}, inplace=True)

df.to_csv('D:\\Model Development\\202001 IDN new v6\\05 Implementation QC\\灰度分数分布.csv')

# 变量分布
sql = '''
select orderno as loan_id, 
sum(case when varname='altitude' then varvalue::float end) as altitude,
sum(case when varname='educationSeniorHighSchool' then varvalue::float end) as "education_senior_high_school",
sum(case when varname='educationRegularCollegeCourse' then varvalue::float end) as "education_regular_college_course",
sum(case when varname='iziTopup0to30Min' then varvalue::int end) as "topup_0_30_min",
sum(case when varname='iziTopup30to60Min' then varvalue::int end) as "topup_30_60_min",
sum(case when varname='iziTopup360to720Min' then varvalue::int end) as "topup_360_720_min",
sum(case when varname='iziB3d' then varvalue::int end) as "B_3d",
sum(case when varname='iziB7d' then varvalue::int end) as "B_7d",
sum(case when varname='iziB360d' then varvalue::int end) as "B_360d",
sum(case when varname='iziC30d' then varvalue::int end) as "C_30d",
sum(case when varname='iziC90d' then varvalue::int end) as "C_90d",
sum(case when varname='iziC360d' then varvalue::int end) as "C_360d",
sum(case when varname='advMultiscore' then varvalue::float end) as "multiscore",
sum(case when varname='GDM41' then varvalue::float end) as "GD_M_41",
sum(case when varname='GDM57' then varvalue::float end) as "GD_M_57",
sum(case when varname='GDM72' then varvalue::float end) as "GD_M_72",
sum(case when varname='GDM89' then varvalue::float end) as "GD_M_89",
sum(case when varname='GDM105' then varvalue::float end) as "GD_M_105",
sum(case when varname='GDM106' then varvalue::float end) as "GD_M_106",
sum(case when varname='GDM107' then varvalue::float end) as "GD_M_107",
sum(case when varname='GDM164' then varvalue::float end) as "GD_M_164",
sum(case when varname='GDM177' then varvalue::float end) as "GD_M_177",
sum(case when varname='GDM210' then varvalue::float end) as "GD_M_210",
sum(case when varname='GDM227' then varvalue::float end) as "GD_M_227",
sum(case when varname='GDM237' then varvalue::float end) as "GD_M_237",
sum(case when varname='GDM261' then varvalue::float end) as "GD_M_261",
sum(case when varname='GDM337' then varvalue::float end) as "GD_M_337",
sum(case when varname='GDM348' then varvalue::float end) as "GD_M_348"
from (
	select orderno, ruleresultname, 
	json_array_elements(datasources::json) ->> 'dataSourceName' dataSourceName,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varName'  varname,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varValue'  varvalue
	from rt_risk_mongo_gocash_riskreportgray
	where createtime::date between '2020-03-05' and '2020-03-08'  and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
	) tmp
group by orderno
'''
df = get_df_from_pg(sql)
print(df.shape)
print(df.loan_id.nunique())
print(df.columns)
df['appmon'] = 'test'

r_all_0221_0222 = pd.read_csv(path_rawdata + 'r_all_0221_0222.csv')
print(r_all_0221_0222.shape)
print(r_all_0221_0222.columns)
r_all_0221_0222 = r_all_0221_0222[['loan_id', 'altitude', 'education_senior_high_school',
       'education_regular_college_course', 'topup_0_30_min', 'topup_30_60_min',
       'topup_360_720_min', 'B_3d', 'B_7d', 'B_360d', 'C_30d', 'C_90d',
       'C_360d', 'multiscore', 'GD_M_41', 'GD_M_57', 'GD_M_72', 'GD_M_89',
       'GD_M_105', 'GD_M_106', 'GD_M_107', 'GD_M_164', 'GD_M_177', 'GD_M_210',
       'GD_M_227', 'GD_M_237', 'GD_M_261', 'GD_M_337', 'GD_M_348']]
r_all_0221_0222['appmon'] = 'train'

r_all = pd.concat([r_all_0221_0222, df],0)
r_all['Y'] = np.random.randint(0,2,size=(25746, 1))
r_all.shape

import sys
sys.path.append('D:/_Tools/genie')

import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *


args_dict = {}
methods = []

fs_obj = fs.FeatureSelection()
bin_obj = mt.BinWoe()

model_data_final = r_all.fillna(-1)
#model_data_final=model_data_final.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999],[-1,  -1, -1, -1, -1, -1, -1])

features_in_model = list(['altitude', 'education_senior_high_school',
       'education_regular_college_course', 'topup_0_30_min', 'topup_30_60_min',
       'topup_360_720_min', 'B_3d', 'B_7d', 'B_360d', 'C_30d', 'C_90d',
       'C_360d', 'multiscore', 'GD_M_41', 'GD_M_57', 'GD_M_72', 'GD_M_89',
       'GD_M_105', 'GD_M_106', 'GD_M_107', 'GD_M_164', 'GD_M_177', 'GD_M_210',
       'GD_M_227', 'GD_M_237', 'GD_M_261', 'GD_M_337', 'GD_M_348'])
var_dict = pd.DataFrame(columns= ['数据源', '指标英文', '指标中文', '数据类型'])
var_dict['数据源'] = features_in_model
var_dict['指标英文'] = features_in_model
var_dict['指标中文'] = features_in_model
var_dict['数据类型'] = 'float'

X_IV = model_data_final[features_in_model]
y_IV = model_data_final['Y'].astype(int)

X_cat_train, X_transformed, woe_iv_df, rebin_spec, ranking_result = fs_obj.overall_ranking(X_IV, y_IV, \
                                                                            var_dict, args_dict, \
                                                                            methods, num_max_bins=10)

rebin_spec_bin_adjusted = {k:v for k,v in rebin_spec.items() if k in features_in_model}

X_cat_train_with_y_appmon = pd.merge(X_cat_train, model_data_final[['Y','appmon']], left_index=True,right_index=True)

# 这一步按月统计了变量的分布以及缺失率，这一步可以将字段先存储出去，观察不合格的变量，在下一步可以提前删除
var_dist_badRate_by_time = ss.get_badRate_and_dist_by_time(X_cat_train_with_y_appmon, features_in_model,'appmon','Y')
                            
var_dist_badRate_by_time.to_excel("D:\\Model Development\\202001 IDN new v6\\05 Implementation QC\\灰度变量分布对比 0305_0308.xlsx")


# 模型打分验证
sql = '''
select orderno as loan_id, createtime, substring(ruleresult,1,3)::int as scorev6
,outputmap::json ->> 'newUserModelScoreV6InputModelParams' as newUserModelScoreV6InputModelParams
,outputmap::json ->> 'newUserModelScoreV6ModelParams' as newUserModelScoreV6ModelParams
from rt_risk_mongo_gocash_riskreportgray 
where substring(createtime,1,13) = '2020-03-06 15' and pipelineid in ('492','493') and nodename = 'modelNode' and ruleresultname = 'newUserModelScoreV6'
'''
df = get_df_from_pg(sql)
print(df.shape)
print(df.loan_id.nunique())
print(df.dtypes)

columnsSequence = ['jailbreakStatus', 'altitude', 'educationSeniorHighSchool',
                   'educationRegularCollegeCourse', 'iziTopup0to30Min', 'iziTopup30to60Min', 
                   'iziTopup360to720Min', 'iziB3d','iziB7d','iziB360d','iziC21d','iziC30d',
                   'iziC90d','iziC360d', 'rateHighFreqAppV6', 'rateMidFreqAppV6', 'AdaPundi', 'Gojek', 
                   'JULO', 'KreditPintar', 'LINE', 'Lite', 'MobileJKN', 'OVO', 'TIXID', 
                   'Traveloka', 'UangMe', 'advMultiscore', 'GDM1', 'GDM3', 'GDM8', 
                   'GDM41', 'GDM57', 'GDM59', 'GDM72', 'GDM77', 'GDM82', 
                   'GDM89', 'GDM93', 'GDM105', 'GDM106', 'GDM107', 'GDM140', 
                   'GDM150', 'GDM163', 'GDM164', 'GDM177', 'GDM178', 'GDM180', 
                   'GDM205', 'GDM210', 'GDM225', 'GDM227', 'GDM235', 'GDM237', 
                   'GDM238', 'GDM261', 'GDM303', 'GDM305', 'GDM328', 'GDM333', 
                   'GDM337', 'GDM348', 'GDM370', 'GDM371']

df.newusermodelscorev6inputmodelparams = df.newusermodelscorev6inputmodelparams.str.replace('[','')
df.newusermodelscorev6inputmodelparams = df.newusermodelscorev6inputmodelparams.str.replace(']','')
df[columnsSequence] = df.newusermodelscorev6inputmodelparams.str.split(',',expand = True)

for x in columnsSequence:
    df[x]= df[x].astype(float)
print(df.dtypes)


'''###################### 打分 ######################'''
import xgboost as xgb
import pandas as pd
import numpy as np
from pandas import read_csv
from pandas import read_excel
from xgboost import DMatrix

# LOAD FUNCTION
def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))
#.map(lambda x: int(x))
    
# LOAD MODEL
mymodel = xgb.Booster() 
mymodel.load_model("D:\\Model Development\\202001 IDN new v6\\04 Document\\开发文档\\model_perf_65_200221_142420.model")  

# PREDICT SCORES

data_lean = DMatrix(df[columnsSequence])
ypred = mymodel.predict(data_lean)

score = [round(Prob2Score(value, 600, 20)) for value in ypred]

data_scored = pd.DataFrame([df['loan_id'].values, df['scorev6'].values, ypred, score]).T
data_scored.columns= ['loan_id', 'scorev6', 'y_pred', 'score']

data_scored[data_scored.scorev6 != data_scored.score]

