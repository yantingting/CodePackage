import numpy as np
import pandas as pd
sys.path.append('C:/Users/Mint/Desktop/repos/newgenie/')


from utils3.data_io_utils import *

sql = """
WITH score as (
SELECT loanid, customerid, createtime, linkajanewmodelv1score, linkajanewmodelv1result
FROM risk_mongo_gocash_riskcontrolresultgray
WHERE linkajanewmodelv1result<>''
),
var as (
SELECT loanId
, maritalStatusCode
, religionCode
, educationCode
, mailAddressCode
, provinceCode
, gender
, age
, iziPhoneAgeAge
, iziInquiriesByType07d
, iziInquiriesByType14d
, iziInquiriesByType21d
, iziInquiriesByType30d
, iziInquiriesByType60d
, iziInquiriesByType90d
, iziInquiriesByTypeTotal
, iziPhoneVerifyResult
, referBroSis
, referParents
, referSpouse
, jobCode
, occupationOffice
, occupationEntre
, bankCodeBca
, bankCodePermata
, bankCodeBri
, bankCodeMandiri
, appId
, businessId
, riskNo
, rcType
, createTime
, religion
, education
, maritalStatus
, job
, occupationtype
, bankcode
FROM risk_mongo_gocash_installmentriskcontrolparamsgray
)
SELECT score.loanid
, score.customerid
, score.createtime
, score.linkajanewmodelv1score
, score.linkajanewmodelv1result
, var.*
FROM score
inner JOIN var on  score.loanid = var.loanid
"""
var_data = get_df_from_pg(sql)

var_dict =pd.read_excel('D:/Model/201911_uku_ios_model/04_最终文档\需求文档/印尼UKU产品ios和linkaja渠道模型入模变量.xlsx', sheet_name = '01_变量汇总')

var_data.linkajanewmodelv1score = var_data.linkajanewmodelv1score.astype(float)
var_data.columns

var_map = var_dict[['模型变量名','线上变量名','入模顺序']].sort_index( by = '入模顺序', ascending= True)
online_cols = [i.lower() for i in var_map.线上变量名]
var_data[online_cols] = var_data[online_cols].astype(float)

X = var_data[online_cols]


import xgboost as xgb
from xgboost import DMatrix

mymodel = xgb.Booster()
mymodel.load_model("D:/Model/201911_uku_ios_model/02_result/result2_grid_26_1218.model")
mymodel

data_lean = DMatrix(X)
ypred = mymodel.predict(data_lean)

score = [round(Prob2Score(value, 600, 20)) for value in ypred]
score

var_data.index = var_data.loanid
data_scored = pd.DataFrame([var_data.index, score, ypred]).T
data_scored = data_scored.rename(columns = {0:'index',1:'score',2:'y_pred'})
data_scored

apply_data = var_data.merge(data_scored, left_index = True, right_on = 'index')
apply_data.to_excel('D:/Model/201911_uku_ios_model/01_data/灰度data.xlsx')

score.loc[score.linkajanewmodelv1result>609]

base_info =

score.shape

6/14

def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))

""""""

VAR_SQL = """
WITH score as (
SELECT loanid
    ,  customerid
    ,  rc1riskno
    ,  createtime
    ,  risklevelfirst
    ,  newusermodelv4score
    ,  newusermodelv4result
FROM risk_gocash_mongo_riskcontrolresult
WHERE createtime between '2019-12-14 00:00:00' and '2019-12-20 00:00:00' and newusermodelv4score <> ''
 and businessid = 'uku'  and freerc = 'false'
),
var as (
SELECT riskno 
    ,  pipelineid
    ,  age
    ,  gender
    ,  iziphoneageage
    ,  iziinquiriesbytypetotal
--    ,  case when bankcode = 'BCA' then 1 else 0 end as bank_codeBCA
--    ,  case when bankcode = 'BNI' then 1 else 0 end as bank_codeBNI
--    ,  case when bankcode = 'BRI' then 1 else 0 end as bank_codeBRI
--    ,  case when bankcode = 'CIMB' then 1 else 0 end as bank_codeCIMB
--    ,  case when bankcode = 'MANDIRI' then 1 else 0 end as bank_codeMANDIRI
--    ,  case when bankcode not in ('BCA','BNI','BRI','CIMB','MANDIRI') then 1 else 0 end as bank_codeother
--    ,  case when city = 'BCA' then 1 else 0 end as bank_codeBCA
    ,  bankcode 
    ,  city
    ,  education
    ,  industryinvolved
    ,  job
    ,  maritalstatus
    ,  occupationtype
    ,  province
FROM risk_mongo_gocash_installmentriskcontrolparams
WHERE rctype = 'rc1' and createtime between '2019-12-14 00:00:00' and '2019-12-20 16:00:00'
)
SELECT score.loanid
     , score.customerid
     , score.createtime
     , score.risklevelfirst
     , score.newusermodelv4score
     , score.newusermodelv4result
     , var.*
FROM score 
left JOIN var on score.rc1riskno = var.riskno
"""

var_data = get_df_from_pg(VAR_SQL)

var_data.createtime.min()

var_data.newusermodelv4score = var_data.newusermodelv4score.astype(float)

var_data['prob_20bin'] = pd.cut(var_data.newusermodelv4score, bins = train_bin_20)
var_data['prob_10bin'] = pd.cut(var_data.newusermodelv4score, bins = train_bin_10)

train_bin_20 = [-np.inf, 0.08664, 0.11437, 0.13732, 0.15648, 0.17469, 0.19281, 0.21037, 0.22712, 0.2442, 0.26242, 0.28206, 0.3012, 0.32191, 0.34449, 0.371, 0.40199, 0.44105, 0.48989, 0.57749, np.inf]
train_bin_10 = [-np.inf,0.11437,0.15648,0.19281,0.22712,0.26242,0.3012,0.34449,0.40199,0.48989,np.inf]


var_data.createtime = var_data.createtime.apply(lambda x: str(x)[0:10])
var_data.loanid = var_data.loanid.astype(str)
var_data.to_excel('D:/project/monitoring/uku_new_model_v4/01_data/data.xlsx')



apply_score = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/201911 新用户模型 V4/03 Result/dat_sophie.xlsx', sheet_name = 'dat_sophie')
apply_score['score_bin'] = pd.cut(apply_score['xgb'], bins =  train_bin_10 )

apply_score.score_bin.value_counts().sort_index()