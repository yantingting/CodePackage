import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/newgenie')

from utils3.data_io_utils import *

data_path = 'D:/Model/indn/202004_uku_old_model/01_data/'
result_path = 'D:/Model/indn/202004_uku_old_model/02_result/'


#############################
# test,oot上对比方案一和方案二效果
#############################

score_has_modify = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/grid_55_200513-19-01score.xlsx').drop(['Unnamed: 0'], 1)
score_has_modify.order_no = score_has_modify.order_no.astype(str)

score_no_modify = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/grid_59_200514-15-18score.xlsx').drop(['Unnamed: 0'], 1)
score_no_modify.order_no = score_no_modify.order_no.astype(str)

score_xuchao = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案一初步结果/grid_28_200515-15-23score.xlsx')
score_xuchao.order_no = score_xuchao.order_no.astype(str)


FIG_PATH = 'D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/figure/lifechart55'

score_xuchao.approved_period.value_counts()

#分期限看模型效果
def liftchart_by_period(df, save_labal, y, prob):
    for i in df.approved_period.unique():
        print(i)
        lc = pt.show_result_new(df.loc[df.approved_period == i], prob, y, n_bins=10, feature_label= 'period_%s'%i)
    path = os.path.join(FIG_PATH, "LiftChart_by_period_" + save_labal + ".png")
    plt.savefig(path, format='png', dpi=100)
    plt.close()

liftchart_by_period(score_xuchao, 'plan_one', 'Y','y_pred')
liftchart_by_period(score_no_modify, 'plan_two_no_modify', 'Y','y_pred')
liftchart_by_period(score_has_modify, 'plan_two_modify', 'Y','y_pred')

perf_sql = """
with loan as (
select id as loan_id
,customer_id
,apply_time :: varchar
,approved_period
,effective_date :: varchar
,paid_off_time :: varchar
,due_date :: varchar
,loan_status
,extend_times
,return_flag
,current_date :: varchar
,late_days
, case when late_days >= 3 then 1 else 0 end as ever3
, case when late_days >= 7 then 1 else 0 end as ever7
, case when late_days >= 15 then 1 else 0 end as ever15
, case when late_days >= 30 then 1 else 0 end as ever30
, case when current_date - due_date >= 3 then 1 else 0 end as due_flag3
, case when current_date - due_date >= 7 then 1 else 0 end as due_flag7
, case when current_date - due_date >= 15 then 1 else 0 end as due_flag15
, case when current_date - due_date >= 30 then 1 else 0 end as due_flag30
from dw_gocash_go_cash_loan_gocash_core_loan 
where effective_date!='1970-01-01' and effective_date between '2020-04-03' and '2020-05-08' 
and return_flag = 'true' and product_id = 1
)
select loan.*, t1.*
from loan
left join (select orderno, ruleresultname, ruleresult
           from rt_risk_mongo_gocash_riskreport
           where ruleresultname = 'oldUserModelV2Score'
            ) t1 on loan.loan_id :: text = t1.orderno
"""
perf_data = get_df_from_pg(perf_sql)
perf_data.loan_id = perf_data.loan_id.astype(str)
perf_data.customer_id = perf_data.customer_id.astype(str)

perf_data.to_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/01_data/perf_flag_0519.xlsx')
perf_data = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/01_data/perf_flag_0519.xlsx').drop('Unnamed: 0',1)

perf_data.shape[0] == perf_data.loan_id.nunique()
perf_data.due_flag15.value_counts()

"""2020/5/19 给4/2后的样本打分"""
import xgboost as xgb
from xgboost import DMatrix

model_has_modify_x = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/grid_55_200513-19-01.xlsx', sheet_name= '04_model_importance')[['index','varName']]
model_has_modify_x = model_has_modify_x.sort_values(by = 'index')
model_no_modify_x = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/grid_59_200514-15-18.xlsx', sheet_name= '04_model_importance')[['index','varName']]
model_no_modify_x = model_no_modify_x.sort_values(by = 'index')
model_xuchao = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案一初步结果/grid_28_200515-15-23.xlsx', sheet_name='04_model_importance')[['index','varName']]
model_xuchao = model_xuchao.sort_values(by = 'index')

save_data_to_pickle(x_with_y, data_path, 'x_with_y_0403to0508.pkl')

x_with_y = load_data_from_pickle(data_path,'x_with_y_0403to0508.pkl')

x_with_y = x_with_y.fillna(-1)

x_with_y.isnull().sum()

all_x_used =  x_with_y[model_has_modify_x['varName']]
all_x_used =  x_with_y[model_no_modify_x['varName']]
all_x_used =  x_with_y[model_xuchao['varName']]

for i in all_x_used.columns:
    try:
        all_x_used[i] = all_x_used[i].astype(float)
    except:
        print(i)

mymodel = xgb.Booster()
mymodel.load_model("D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/grid_55_200513-19-01.model")
mymodel.load_model("D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/grid_59_200514-15-18.model")
mymodel.load_model("D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案一初步结果/grid_28_200515-15-23.model")

data_lean = DMatrix(all_x_used)
ypred = mymodel.predict(data_lean)
score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([all_x_used.index, score, ypred]).T
data_scored.rename(columns = {0:'loan_id', 1:'score_var_59', 2:'prob_var_59'}, inplace=True)

data_scored.to_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/score_var_55_0403to0508.xlsx')
data_scored.to_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/score_var_59_0403to0508.xlsx')



def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))

#
""" Lift Charts """
import utils3.plot_tools as pt
import matplotlib.pyplot as plt

FIG_PATH = os.path.join('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/figure')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

perf_data.columns
perf_data.loc[(perf_data.due_flag15 == 1) & (perf_data.ever15 == 1), 'Y'] = 1
perf_data.loc[(perf_data.due_flag15 == 1) & (perf_data.ever15 == 0), 'Y'] = 0
perf_data.loc[(perf_data.due_flag15 == 0), 'Y'] = -1
perf_data.ruleresult = perf_data.ruleresult.astype(float)

import utils3.metrics as mt
perf_data['prob'] = mt.Performance().score_to_p(perf_data['ruleresult'])

data_scored_55 = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/score_var_55_0403to0508.xlsx')
data_scored_59 = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案二初步结果/score_var_59_0403to0508.xlsx')
data_scored_xuchao = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/02_result/05_18方案一初步结果/score_0403to0508.xlsx').drop(['Unnamed: 0'],1)

data_scored_55 = perf_data.merge(data_scored_55, on = 'loan_id')
data_scored_59 = perf_data.merge(data_scored_59, on = 'loan_id')
data_scored_xuchao = perf_data.merge(data_scored_xuchao, on = 'loan_id')

var55_lc = pt.show_result_new(data_scored_55.loc[data_scored_55.Y != -1], 'prob_var_55','Y', n_bins = 10, feature_label='var55')
var59_lc = pt.show_result_new(data_scored_59.loc[data_scored_59.Y != -1], 'prob_var_59','Y', n_bins = 10, feature_label='var59')
var28_lc = pt.show_result_new(data_scored_xuchao.loc[data_scored_xuchao.Y != -1], 'prob_var_28','Y', n_bins = 10, feature_label='var28')
v2_lc = pt.show_result_new(perf_data.loc[perf_data.Y != -1], 'prob','Y', n_bins = 10, feature_label='v2model')
path = os.path.join(FIG_PATH, "LiftChart_" + '04020427' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

#按周看
data_scored_55.rename(columns = {'effective_date':'applied_at'}, inplace = True)
data_scored_59.rename(columns = {'effective_date':'applied_at'}, inplace = True)
perf_data.rename(columns = {'effective_date':'applied_at'}, inplace = True)

pt.lift_chart_by_time(data_scored_59.loc[data_scored_59.Y != -1],'prob_var_59','Y', n_bins = 10, by="week")
path = os.path.join(FIG_PATH, "LiftChart_59_" + 'by_week' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

pt.lift_chart_by_time(perf_data.loc[perf_data.Y != -1],'prob','Y', n_bins = 10, by="week")
path = os.path.join(FIG_PATH, "LiftChart_v2_" + 'by_week' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

