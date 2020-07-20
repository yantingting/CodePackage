import os
import numpy

sys.path.append('/Users/Mint/Desktop/repos/genie')
from utils3.data_io_utils import *
import utils3.misc_utils as mu
import utils3.summary_statistics as ss
import utils3.metrics as mt
from interval import Interval

import xgboost as xgb
from xgboost import DMatrix

data_path = 'D:/Model/201908_qsfq_model/05_灰度模型验收/'

v6yl_sql = """
select 
t0.orderno
, t0.createtime
, t0.pipelinename
, t0.anti_fraud_rule_node
, t0.dcCreditScoreV4Score
, t0.dcCreditScoreV4Result
, t0.dcCreditScoreV6yl
, t0.dcCreditScoreV6ylResult
, t0.dcCreditScoreV6Score
, t0.dcCreditScoreV6Result
,bairongNodebtscore
,d90pctCntCash
,d30cntOrg
,historySucFee
,d90pctCntCc
,latestOneMonthFail
,loansLatestTimeDiff
,d30cntCash
,m5cnt
,d90pctCntOrgCc
,loansOrgCount
,m9cntOrgCf
,d60pctCntCc
,m4pctCntOrgCash
,riskScore
,loansSettleCount
,d90cntOrgCf
,loansCredibility
,m6pctCntOrgCf
,loansCount
,m6pctCntCf
,latestSixMonth
,d90cntOrg
,d30pctCntOrgCc
,d30pctCntOrgCash
,m6pctCntOrgCash
,monthlyIncome
,d90pctCntOrgAll
,m4pctCntOrgCc
,education
,RMS002
,d90pctCntCf
,m5pctCntOrgCc
,m9pctCntOrgCc
,m5pctCntOrgCash
,d30pctCntOrgCf
,m5pctCntAll
,consfinOrgCount
,m9cntOrg
,m6pctCntOrgCc
,m4cnt
,m12pctCntOrgCc
,m5pctCntCc
,d30pctCntAll
,m6cntOrgCf
,d60cntOrgCash
,m6cnt
,riskCodeValue8
,d90pctCntOrgCash
,m4cntOrgCash
,d90cntOrgCash
,d90pctCntAll
from (select * 
      from  ods_rsk_installmentriskcontrolresultgray
      where businessid = 'xjbk'
      and createtime >= '2019-10-18 10:30:00'
      and anti_fraud_rule_node = 'N'
       )t0
left join (select * 
           from ods_risk_installmentriskcontrolparamsgray
           )t1 on t0.orderno = t1.orderno
"""
v6yl_data_or = get_df_from_pg(v6yl_sql)
v6yl_data = v6yl_data_or.copy()

v6yl_data.isnull().sum()
v6yl_data.loc[v6yl_data.orderno == '161923684779425794'].T

v6_sql = """
select 
t0.orderno
, t0.createtime
, t0.pipelinename
, t0.anti_fraud_rule_node
, t0.dcCreditScoreV4Score
, t0.dcCreditScoreV4Result
, t0.dcCreditScoreV6Score
, t0.dcCreditScoreV6Result
,education
,age
,monthlyIncome
,id_city_level
,ageMarital
,consfinOrgCount
,historyFailFee
,historySucFee
,latestOneMonthFail
,latestOneMonthSuc
,loansCredibility
,loansLongTime
,loansOrgCount
,loansOverdueCount
,loanScore
,loansSettleCount
,d30pctCntOrgCash
,d30cnt
,d30pctCntCc
,d30pctCntCf
,d30cntCash
,d30pctCntCash
,d30pctCntOrgCc
,d30cntOrgCash
,d60cnt
,d60pctCntCf
,d60cntCash
,d60pctCntCash
,d60pctCntAll
,d60cntOrgCash
,d90cntOrg
,d90pctCntOrgCf
,d90pctCntCc
,d90pctCntCf
,d90cntCash
,d90pctCntCash
,d90pctCntOrgCc
,d90pctCntAll
,m4pctCntOrgCash
,m4cntOrg
,m4pctCntOrgAll
,m4cnt
,m4pctCntCf
,m4cntCash
,m4pctCntOrgCc
,m4cntOrgCash
,m5pctCntOrgCash
,m5cnt
,m5cntOrgCf
,m5pctCntCf
,m5cntCash
,m5pctCntCash
,m5pctCntOrgCc
,m6pctCntOrgCash
,m6pctCntOrgCf
,m6pctCntCf
,m6cntCash
,m6pctCntCash
,m9pctCntOrgCash
,m9pctCntOrgCf
,m9cntOrgCf
,m9cntCash
,m9cntOrgCash
,m12cntCash
,m12cntOrgCash
,m24pctCntcc
,riskScore
,riskCodeValue2
,tdIdCntLoanCompanyLoan30d
,tdIdCntP2pLoan30d
,tdIdCntPartnerLoan30d
,tdMCntLoanCompanyLoan30d
,tdMCntP2pPartnerLoan30d
,tdMCntPartnerLoan30d
,tdIdCntLoanCompanyLoan90d
,tdIdCntFinanceLeaseLoan90d
,tdMCntLoanCompanyLoan90d
,tdMCntPartnerLoan90d
from (select * 
      from  ods_rsk_installmentriskcontrolresultgray where businessid = 'rong360' and date(createtime) between '2019-10-12' and '2019-10-13' and createtime <>''
       )t0
left join (select * 
           from ods_risk_installmentriskcontrolparamsgray where businessid = 'rong360'
           )t1 on t0.orderno = t1.orderno
"""
v6_data_or = get_df_from_pg(v6_sql)
v6_data.isnull().sum()

v6_data.id_city_level = v6_data.id_city_level.replace('一线城市',0).replace('新一线城市',1).replace('二线城市',2).replace('三线城市',3).replace('四线城市',4).replace('五线城市',5)
v6_data = v6_data.loc[v6_data.anti_fraud_rule_node == 'N']

v6_data = v6_data.fillna(-1).replace('null',-1)

v6yl_data = v6yl_data.fillna(-1).replace('null',-1)

v6yl_data.head()

for i in v6yl_data.columns:
   if i not in ['orderno','createtime','pipelinename','anti_fraud_rule_node','dccreditscorev4result','dccreditscorev6ylresult','dccreditscorev6result']:
       print(i)
       v6yl_data[i] = v6yl_data[i].astype(float)

v6yl_data['bairongnodebtscore_flag'] = 0
v6yl_data['d30cntcash_flag'] = 0
v6yl_data['loansorgcount_flag'] = 0
v6yl_data['riskscore_flag'] = 0
v6yl_data['loanssettlecount_flag'] = 0
v6yl_data['loanscount_flag'] = 0
v6yl_data['d90cntorg_flag'] = 0
v6yl_data['d90pctcntorgall_flag'] = 0
v6yl_data['rms002_flag'] = 0
v6yl_data['m6pctcntorgcc_flag'] = 0


for i in range(v6yl_data.shape[0]):
    print (i)
    if v6yl_data.loc[i,'bairongnodebtscore'] not in Interval(-1,100, closed= True):
        v6yl_data.loc[i, 'bairongnodebtscore'] = -1
        v6yl_data.loc[i, 'bairongnodebtscore_flag'] = 1
    if v6yl_data.loc[i,'d30cntcash'] not in Interval(-1,49, closed= True):
        v6yl_data.loc[i, 'd30cntcash'] = -1
        v6yl_data.loc[i, 'd30cntcash_flag'] = 1
    if v6yl_data.loc[i,'loansorgcount'] not in Interval(-1,22, closed= True):
        v6yl_data.loc[i, 'loansorgcount'] = -1
        v6yl_data.loc[i, 'loansorgcount_flag'] = 1
    if v6yl_data.loc[i, 'riskscore'] not in Interval(-1, 110, closed=True):
        v6yl_data.loc[i, 'riskscore'] = -1
        v6yl_data.loc[i, 'riskscore_flag'] = 1
    if v6yl_data.loc[i, 'loanssettlecount'] not in Interval(-1, 64, closed=True):
        v6yl_data.loc[i, 'loanssettlecount'] = -1
        v6yl_data.loc[i, 'loanssettlecount_flag'] = 1
    if v6yl_data.loc[i, 'loanscount'] not in Interval(-1, 68, closed=True):
        v6yl_data.loc[i, 'loanscount'] = -1
        v6yl_data.loc[i, 'loanscount_flag'] = 1
    if v6yl_data.loc[i, 'd90cntorg'] not in Interval(-1, 40, closed=True):
        v6yl_data.loc[i, 'd90cntorg'] = -1
        v6yl_data.loc[i, 'd90cntorg_flag'] = 1
    if v6yl_data.loc[i, 'd90pctcntorgall'] not in Interval(-1, 1, closed=True):
        v6yl_data.loc[i, 'd90pctcntorgall'] = -1
        v6yl_data.loc[i, 'd90pctcntorgall_flag'] = 1
    if v6yl_data.loc[i, 'rms002'] not in Interval(-1, 1317, closed=True):
        v6yl_data.loc[i, 'rms002'] = -1
        v6yl_data.loc[i, 'rms002_flag'] = 1
    if v6yl_data.loc[i, 'm6pctcntorgcc'] not in Interval(-1, 1, closed=True):
        v6yl_data.loc[i, 'm6pctcntorgcc'] = -1
        v6yl_data.loc[i, 'm6pctcntorgcc_flag'] = 1
    # if v6yl_data.loc[i, 'historysucfee'] not in Interval(-1, 92, closed=True):
    #     v6yl_data.loc[i, 'historysucfee'] = -1
    # if v6yl_data.loc[i, 'm9cntorgcf'] not in Interval(-1, 16, closed=True):
    #     v6yl_data.loc[i, 'm9cntorgcf'] = -1
    # if v6yl_data.loc[i, 'monthlyincome'] not in Interval(-1, 157846, closed=True):
    #     v6yl_data.loc[i, 'monthlyincome'] = -1

v6yl_data.shape

#线下打分

#读入模型数据
features = pd.read_excel(os.path.join('D:/seafile/Seafile/风控/模型/01 轻松分期模型/dcCreditV6/04 Document/v6yl/轻松分期申请模型（Bill模型）入模变量顺序 v6yl.xlsx'),sheet_name = 'v6yl')[['顺序编号','模型变量名','线上变量名']]
features.线上变量名 = [i.lower() for i in features.线上变量名]

v6yl_data = v6yl_data.rename(columns = dict(zip(features.线上变量名, features.模型变量名)))
v6yl_data.index = v6yl_data.orderno
#重新打分
# LOAD MODEL
mymodel = xgb.Booster()
mymodel.load_model( 'D:/Model/201908_qsfq_model/02_result_0928/dc 0926 52.model')

mydata = v6yl_data.loc[v6yl_data.anti_fraud_rule_node == 'N'][features.模型变量名]
mydata.loc[mydata.index == '162313305958908931'].T

mydata.loc[mydata.index == '162313305958908931'].T.values

# PREPARE DATA
var_selected = mydata[features.模型变量名].values

# PREDICT SCORES
var_selected = DMatrix(var_selected)

ypred = mymodel.predict(var_selected)
ypred

p_to_score(0.466)
Prob2Score(0.454495757818222, 600,20)


[i for i in ypred]
score = [round(Prob2Score(value, 600, 20)) for value in ypred]
score = [round(p_to_score(value, PDO=20, Base=600, Ratio=1)) for value in ypred]

data_scored = pd.DataFrame([mydata.index, score, ypred]).T
data_scored.head(200)
data_scored = data_scored.rename(columns = {0:'orderno',1:'model_score',2:'prob'})


train_bin = [-np.inf,566, 582, 600, 610, 618, 625, 633, 642, 652, np.inf]
train_bin_20 = [-np.inf,550, 566, 574, 582, 591, 600, 605, 610, 615, 618, 621, 625, 629, 633, 637, 642, 646, 652, 659,np.inf]
train_bin_30 = [-np.inf,541, 557, 566, 571, 576, 582, 588, 594, 600, 603, 607, 610, 613, 616, 618, 620, 623, 625, 628, 630, 633,636,639, 642,645,648,652,656, 662, np.inf]

data_scored['score_bin'] = pd.cut(data_scored.model_score, bins = train_bin)
data_scored['score_bin_20'] = pd.cut(data_scored.model_score, bins = train_bin_20)
data_scored['score_bin_30'] = pd.cut(data_scored.model_score, bins = train_bin_30)

for i in data_scored.columns:
    if 'score_bin' in i:
        data_scored[i] = data_scored[i].astype(str)

v6yl_data_offline = v6yl_data.merge(data_scored, on = 'orderno', how = 'inner')
v6yl_data_offline.to_excel(os.path.join(data_path, 'var_score_1018_ptoscore_xjbk.xlsx'))


"""
确认变量上限
"""
set(all_var3.loc[all_var3.loans_settle_count >= 64].index) - set(all_var2.loc[(all_var2.anti_fraud_rule_node == 'N') & (all_var2.loans_settle_count >= 64)].order_no)

# all_var3.loc[all_var3.loans_settle_count >= 64].shape[0]/len(all_var3)  #48, 0.0016482950448130215
# all_var3.loc[all_var3.loansf_org_count >= 22].shape[0]/len(all_var3)  #253, 0.008687888465368634
# all_var3.loc[all_var3.loans_count >= 68].shape[0]/len(all_var3)  #72, 0.008687888465368634
#
# all_var3.loc[(all_var3.loans_settle_count >= 64)|(all_var3.loans_org_count >= 22)|(all_var3.loans_count >= 68)].shape[0]/len(all_var3)  #0.0016482950448130215

all_var.loc[all_var.order_no == '160348035224700930'].T

v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.bairongnodebtscore> 100)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) #0
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.d30cntcash > 49)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) #0
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.loansorgcount > 22)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) # 0.007429829389102917
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.riskscore > 110)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) # 0
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.loanssettlecount > 64)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) # 0.001651073197578426
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.loanscount > 68)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) # 0.002201430930104568
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.d90cntorg > 40)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) # 0.00027517886626307
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.d90pctcntorgall > 1)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) # 0
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.rms002 > 1317)].shape[0]/len(v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N')]) # 0


v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.loans_settle_count >= 64)].shape[0]/len(all_var2.loc[(all_var2.anti_fraud_rule_node == 'N')]) #44, 0.0019279642450267286
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.loans_org_count >= 22)].shape[0]/len(all_var2.loc[(all_var2.anti_fraud_rule_node == 'N')]) #237, 0.010384716501621243
v6yl_data.loc[(v6yl_data.anti_fraud_rule_node == 'N') & (v6yl_data.loans_count >= 68)].shape[0]/len(all_var2.loc[(all_var2.anti_fraud_rule_node == 'N')]) #66, 0.002891946367540093

v6yl_data = v6yl_data.loc[v6yl_data.anti_fraud_rule_node == 'N']
v6yl_data.to_excel(os.path.join(data_path,'var_1012to1013.xlsx'))

v6yl_data['bairongnodebtscore_flag'] = 0
v6yl_data['d30cntcash_flag'] = 0
v6yl_data['loansorgcount_flag'] = 0
v6yl_data['riskscore_flag'] = 0
v6yl_data['loanssettlecount_flag'] = 0
v6yl_data['loanscount_flag'] = 0
v6yl_data['d90cntorg_flag'] = 0
v6yl_data['d90pctcntorgall_flag'] = 0
v6yl_data['rms002_flag'] = 0
v6yl_data['m6pctcntorgcc_flag'] = 0

v6yl_data = v6yl_data.reset_index(drop = True)

for i in range(v6yl_data.shape[0]):
    print (i)
    #if 0 <= v6yl_data.loc[i,'bairongnodebtscore'] <= 100:
    if v6yl_data.loc[i,'bairongnodebtscore'] not in Interval(-1,100, closed= True):
        v6yl_data.loc[i, 'bairongnodebtscore_flag'] = 1
    if v6yl_data.loc[i,'d30cntcash'] not in Interval(-1,49, closed= True):
        v6yl_data.loc[i, 'd30cntcash_flag'] = 1
    if v6yl_data.loc[i,'loansorgcount'] not in Interval(-1,22, closed= True):
        v6yl_data.loc[i, 'loansorgcount_flag'] = 1
    if v6yl_data.loc[i, 'riskscore'] not in Interval(-1, 110, closed=True):
        v6yl_data.loc[i, 'riskscore_flag'] = 1
    if v6yl_data.loc[i, 'loanssettlecount'] not in Interval(-1, 64, closed=True):
        v6yl_data.loc[i, 'loanssettlecount_flag'] = 1
    if v6yl_data.loc[i, 'loanscount'] not in Interval(-1, 68, closed=True):
        v6yl_data.loc[i, 'loanscount_flag'] = 1
    if v6yl_data.loc[i, 'd90cntorg'] not in Interval(-1, 40, closed=True):
        v6yl_data.loc[i, 'd90cntorg_flag'] = 1
    if v6yl_data.loc[i, 'd90pctcntorgall'] not in Interval(-1, 1, closed=True):
        v6yl_data.loc[i, 'd90pctcntorgall_flag'] = 1
    if v6yl_data.loc[i, 'rms002'] not in Interval(-1, 1317, closed=True):
        v6yl_data.loc[i, 'rms002_flag'] = 1
    if v6yl_data.loc[i, 'm6pctcntorgcc'] not in Interval(-1, 1, closed=True):
        v6yl_data.loc[i, 'm6pctcntorgcc_flag'] = 1
    # if v6yl_data.loc[i, 'historysucfee'] not in Interval(0, 92, closed=True):
    #     v6yl_data.loc[i, 'historysucfee'] = -1
    # if v6yl_data.loc[i, 'm9cntorgcf'] not in Interval(0, 16, closed=True):
    #     v6yl_data.loc[i, 'm9cntorgcf'] = -1
    # if v6yl_data.loc[i, 'monthlyincome'] not in Interval(0, 157846, closed=True):
    #     v6yl_data.loc[i, 'monthlyincome'] = -1

"""v6打分"""

for i in range(v6_data.shape[0]):
    print (i)
    #if 0 <= v6yl_data.loc[i,'bairongnodebtscore'] <= 100:
    if v6_data.loc[i, 'monthlyincome'] not in Interval(0, 157846, closed=True):
        v6yl_data.loc[i, 'monthlyincome'] = -1
    if v6_data.loc[i, 'historyfailfee'] not in Interval(0, 73, closed=True):
        v6_data.loc[i, 'historyfailfee'] = -1
    if v6_data.loc[i, 'historysucfee'] not in Interval(0, 92, closed=True):
        v6_data.loc[i, 'historysucfee'] = -1
    if v6_data.loc[i, 'loansLongTime'] not in Interval(0, 92, closed=True):
        v6_data.loc[i, 'loanslongtime'] = -1
    if v6_data.loc[i, 'riskscore'] not in Interval(0, 110, closed=True):
        v6_data.loc[i, 'riskscore'] = -1
    if v6_data.loc[i, 'm9cntorgcf'] not in Interval(0, 16, closed=True):
        v6_data.loc[i, 'm9cntorgcf'] = -1
    if v6_data.loc[i,'d30cntcash'] not in Interval(0,49, closed= True):
        v6_data.loc[i, 'd30cntcash'] = -1
    if v6_data.loc[i,'loansorgcount'] not in Interval(0,22, closed= True):
        v6_data.loc[i, 'loansorgcount'] = -1
    if v6_data.loc[i, 'loanssettlecount'] not in Interval(0, 64, closed=True):
        v6_data.loc[i, 'loanssettlecount'] = -1
    if v6_data.loc[i, 'd90cntorg'] not in Interval(0, 40, closed=True):
        v6_data.loc[i, 'd90cntorg'] = -1


def p_to_score(p, PDO=20, Base=600, Ratio=1):
    """
    逾期概率转换分数

    Args:
    p (float): 逾期概率
    PDO (float): points double odds. default = 20
    Base (int): base points. default = 600
    Ratio (float): bad:good ratio. default = 1

    Returns:
    化整后的模型分数
    """
    B = 1.0 * PDO / np.log(2)
    A = Base + B * np.log(Ratio)
    score = A - B * np.log(p / (1 - p))
    return round(score, 0)