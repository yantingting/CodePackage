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
import utils3.feature_selection as fs
import utils3.plotting as pl
import utils3.modeling as ml
from functools import reduce
import xgboost as xgb

"""
申请样本上打分
"""

data_path='D:/Model/201909_qsfq_refit_model/02_result_v2/'
path_out='D:/Model Development/201901 Adam Model/'

# LOAD FUNCTION
def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))
#.map(lambda x: int(x))

# LOAD DATA
mydata = pd.read_csv('D:/Model/201909_qsfq_refit_model/01_data/data_final/rong360_application.csv')
mydata.shape
mydata = mydata.drop('Unnamed: 0',1)

mydata.loc[:, 'created_time'] = mydata.created_time.apply(lambda x: str(x)[:9])
mydata.created_time.value_counts().sort_index()
mydata.head()

mydata.order_no.nunique()
mydata.fillna(-1)

mydata = mydata.fillna(-1)
mydata = mydata.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999],[-1,  -1, -1, -1, -1, -1, -1])

# LOAD MODEL
mymodel = xgb.Booster()
mymodel.load_model(data_path + 'dc 20190909 42.model')

# LOAD VARIABLES
myvar = pd.read_excel('D:/Model/201909_qsfq_refit_model/04_最终文档/credit_42/轻松分期信用模型V5变量入模顺序.xlsx ', sheet_name = '变量名称for_python')  #credit
features = list(myvar['变量名称'])

len(features)

#mydata.rename(columns=dict(zip(myvar['变量名称'],myvar['线上变量名'])),inplace=True)
mydata.head()
mydata.columns
mydata.index = mydata.order_no
mydata.id_city_level.head()
mydata.id_city_level.value_counts()

mydata.id_city_level = mydata.id_city_level.replace('一线城市',0).replace('新一线城市',1).replace('二线城市',2).replace('三线城市',3).replace('四线城市',4).replace('五线城市',5)
mydata.loc[mydata.order_no == '157702633870590978'].T


157702633870590978

# ,case when b.city_level='一线城市' then 0
#       when b.city_level='新一线城市' then 1
#       when b.city_level='二线城市' then 2
#       when b.city_level='三线城市' then 3
#       when b.city_level='四线城市' then 4
#       when b.city_level='五线城市' then 5 else null end as id_city_level


# PREPARE DATA
#data_lean = pd.DataFrame(mydata, columns=list(myvar.iloc[:,1]))
var_selected = mydata[features].values


var_selected = pd.DataFrame(var_selected, columns=list(features)).astype(float)
var_selected.head()


# PREDICT SCORES
var_selected = DMatrix(var_selected)
ypred = mymodel.predict(var_selected)
ypred

score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([mydata.values[:,0],mydata.values[:,1], score, ypred]).T
data_scored.head(200)
data_scored = data_scored.rename(columns = {0:'order_no',1:'created_time',2:'model_score',3:'prob'})
data_scored.created_time.min() #08-27
data_scored.created_time.max() #09-09

data_scored.score_bin = data_scored.score_bin.astype(str)
data_scored.score_bin_20 = data_scored.score_bin_20.astype(str)

data_scored.to_excel(data_path + "data_scoredcredit_0827to0909_v2.xlsx",index=False)
data_scored.model_score.min()
data_scored.model_score.max()

score_bin = [400, 586, 596, 604, 610, 615, 620, 625, 631, 639, 800]
bin_20 = [-np.inf,584,591,596,599,602,606,608,610,613,615,617,620,622,625,627,631,636,642,np.inf]



data_scored['score_bin'] = pd.cut(data_scored.model_score, bins = score_bin)
data_scored['score_bin_20'] = pd.cut(data_scored.model_score, bins = bin_20)

data_scored.score_bin.value_counts().sort_index()
data_scored.head()



#------------------------变量分布-------------------
rebin_spec = load_data_from_pickle(data_path,'rebin_spec_v2.pkl')
var_dict = pd.read_excel('D:/Model/201909_qsfq_refit_model/建模代码可用变量字典_v2.xlsx')
X_data = mydata[features]

mydata.columns
bin_obj = mt.BinWoe()
X_cat = bin_obj.convert_to_category(X_data, var_dict, rebin_spec)
X_cat.head()
X_cat = X_cat.reset_index()
X_cat_with_time = X_cat.merge(mydata[['order_no','created_time']], on = 'order_no', how = 'left')

def calculate_day_psi(data, date_var):
    var_cnt_list = []
    var_dist_list = []
    date_list = data[date_var].unique()
    day_cnt = data.groupby([date_var])[date_var].count()
    for j in data.columns:
        if j not in ['order_no','created_time','date','hour','dccreditscore']:
            var_cnt = data.groupby([date_var,j])[j].count().unstack(level = 0,fill_value = 0)
            var_dist = var_cnt/day_cnt
            var_cnt['var_name'] = j
            var_dist['var_name'] = j
            var_cnt_list.append(var_cnt)
            var_dist_list.append(var_dist)
    var_cnt_list = pd.concat(var_cnt_list)
    var_dist_list = pd.concat(var_dist_list)
    var_cnt_list = var_cnt_list.rename(columns = dict(zip(date_list,['cnt_'+i for i in date_list]))).reset_index().rename(columns = {'index':'bin'})
    var_dist_list = var_dist_list.rename(columns = dict(zip(date_list,['dist_'+i for i in date_list]))).reset_index().rename(columns = {'index':'bin'})
    return(var_cnt_list, var_dist_list)


#a = X_cat_test.groupby(['date','td_id_cnt_loan_company_loan7d'])['td_id_cnt_loan_company_loan7d'].count().unstack(level = 0, fill_value = 0)
#b = X_cat_test.groupby(['date'])['td_id_cnt_loan_company_loan7d'].count()

var_cnt_list,var_dist_list = calculate_day_psi(X_cat_with_time,'created_time')
var_cnt_list

writer = pd.ExcelWriter(os.path.join(data_path, 'dist_0828to0909.xlsx'))
var_cnt_list.to_excel(writer, 'var_cnt_list', index=True)
var_dist_list.to_excel(writer, 'var_dist_list',  index=True)
writer.save()

"""
取线上数据
"""

online_var = """
select t0.orderno
,t0.customerid
,age
,ageMarital
,cnt_mobile_num_receiver
,cnt_order30d_60d_success_orderlist
,cnt_phone115d_170_hbw
,cnt_phone6180d_num
,cnt_postcode_num_receiver
,cnt_receiver
,sameSelfPhoneAndReceiver
,cnt_shop60d_90d_num_orderlist
,consfinOrgCount
,d30cntCash
,d30cntCf
,d60cnt
,d60cntCf
,d90cntCf
,historyFailFee
,id_city_level
,latestOneMonth
,latestOneMonthFail
,latestOneMonthSuc
,loansCashCount
,loansCredibility
,loansLongTime
,loanScore
,loansSettleCount
,m12cntCash
,m12cntOrgCash
,m18cntOrgCf
,m18pctCntCash 
,m24cntCf
,m24cntOrgCf
,m24pctCntOrgCash
,m4pctCntOrgAll
,m9cntOrgCf
,max_amount30d_60d_success_orderlist
,sameReferphoneAndReceiverRate
,sum_amount_success_orderlist
,sum_amount180d_success_orderlist
,sum_amount30d_success_orderlist
,tdIdCntLoanCompanyLoan30d
,vip_count_base
,dccreditscorev5score
from (select * from ods_rsk_installmentriskcontrolresult 
      where businessid  = 'rong360' and dccreditscorev5score <>'' and date(createtime) = '2019-09-11'
      ) t0
left join (select * 
from ods_risk_installmentriskcontrolparams where businessid = 'rong360'
) t1 on t0.orderno = t1.orderno
"""

online_var_data = get_df_from_pg(online_var)
online_var_data.shape
online_var_data.to_excel(os.path.join(data_path,'online_var0911_v2.xlsx'))
online_var_data.customerid ==

online_var_data.dccreditscorev5score.value_counts(dropna = False)
online_var_data.dccreditscorev5scoreresult.value_counts(dropna = False)
online_var_data.age.value_counts(dropna = False)
online_var_data.head(20)

online_var_data[['m18cntorgcf','m24cntcf','d30cntcf']].head(100)

online_var_data.loc[online_var_data.orderno == '158960617980429315'].T


a = online_var_data.loc[online_var_data.customerid.isin(['159067892304838659'])]
a.T

#a.rename(columns=dict(zip(myvar['变量名称'],myvar['线上变量名'])),inplace=True)
a.rename(columns=dict(zip(myvar['线上变量名'],myvar['变量名称'])),inplace=True)

myvar['线上变量名'] = [i.lower() for i in  myvar['线上变量名']]

a = a.astype(float)

a = a.replace('三线城市',3).replace(-9998.0,-1)

a


b.T
a.T


b = pd.DataFrame(a, columns=list(features)).astype(float).replace(-9998,-1)

b = b.rename(columns = {'m18pctcntcash':'m_18pct_cnt_cash'}).replace(-9998,-1)
b = b.fillna(0.9772)
b.T

a.T
# PREDICT SCORES
var_selected = DMatrix(b)
ypred = mymodel.predict(var_selected)
ypred

score = [round(Prob2Score(value, 600, 20)) for value in ypred]
score


data_scored = pd.DataFrame([mydata.values[:,0],mydata.values[:,1], score, ypred]).T
data_scored.head(200)



jxl_sql = """
select order_no, customer_id, created_time, businessid,oss
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_cf}'  as d_30cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_cash}'  as d_30cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt}'  as d_60cnt
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_cf}'  as d_60cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_cf}'  as d_90cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_cash}'  as m_12cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org_cash}'  as m_12cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org_cf}'  as m_18cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_cash}'  as m_18pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_cf}'  as m_24cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org_cf}'  as m_24cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_cash}'  as m_24pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_all}'  as m_4pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org_cf}'  as m_9cnt_org_cf
from dc_juxinli_application 
where order_no in ('158962776201494530','158553360591488003','158973901240534019') and oss <> '' 
"""

jxl_data = get_df_from_pg(jxl_sql)


