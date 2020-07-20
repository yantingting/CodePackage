import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/newgenie')
from utils3.data_io_utils import *

data_path = 'D:/Model/indn/202004_uku_old_model/01_data/'
result_path = 'D:/Model/indn/202004_uku_old_model/02_result/'


perf_data_new = pd.read_excel('D:/Model/indn/202004_uku_old_model/01_data/flag_0511_v2.xlsx')

base_f = load_data_from_pickle(data_path, 'x_base_0124to0508.pkl').drop(['rn','apply_time','customer_id','create_time','update_time',
                                                                        'mail','marital_status','religion','education','mail_address1',
                                                                         'mail_address2','mail_address3','mail_address','marital_','religion_',
                                                                         'education_'], 1)


base_data2 = load_data_from_pickle(data_path, 'x_base2_0124to0508.pkl')

bank_f = load_data_from_pickle(data_path, 'x_bank_0124to0508.pkl').drop(['apply_time','effective_date','customer_id','update_time','bank_code','rn'], 1)
bank_f.columns

prof_f = load_data_from_pickle(data_path, 'x_prof_0124to0508.pkl')
prof_f.shape

refer_f = load_data_from_pickle(data_path, 'x_refer_0124to0508.pkl').drop(['customer_id'],1)


perf_f = load_data_from_pickle(data_path, 'x_perf_0124to0508.pkl')
perf_latedays_f = load_data_from_pickle(data_path, 'x_perf_latedays_0124to0508.pkl')
perf_paidhour_f = load_data_from_pickle(data_path, 'x_perf_paidhour_0124to0508.pkl')
perf_paychannel_f = load_data_from_pickle(data_path, 'x_perf_paychannel_0124to0508.pkl')
perf_preloan_f = load_data_from_pickle(data_path, 'x_perf_preloan_0124to0508.pkl')

perf_paychannel_f.rename(columns = {'loanid':'loan_id'}, inplace=True)
perf_preloan_f.rename(columns = {'loanid':'loan_id'}, inplace=True)

perf_col_f = load_data_from_pickle(data_path, 'yanshengbianliangbyjinwei_data.pkl').drop(['apply_time', 'effective_date', 'customer_id'],1)
perf_col_f.dtypes


izi_f = load_data_from_pickle(data_path, 'x_izi_0124to0508.pkl')


#整理所有历史行为的变量

x_with_y = perf_data.merge(base_f, on = 'loan_id', how = 'left').merge(base_data2, on = 'loan_id', how = 'left').merge(bank_f, on = 'loan_id', how = 'left').merge(prof_f, on = 'loan_id', how = 'left')\
            .merge(refer_f, on = 'loan_id', how = 'left').merge(perf_f, on = 'loan_id', how = 'left').merge(perf_latedays_f, on = 'loan_id', how = 'left').merge(perf_paidhour_f, on = 'loan_id', how = 'left')\
            .merge(perf_paychannel_f, on = 'loan_id', how = 'left').merge(perf_preloan_f, on = 'loan_id', how = 'left').merge(perf_col_f, on = 'loan_id', how = 'left')\
            .merge(izi_f, on = 'loan_id', how = 'left')
x_with_y.index = x_with_y.loan_id

#增加modify的历史行为变量

cols = list(var_dict.loc[(var_dict.数据源 == 'history_behavior') & (var_dict.是否可以直接使用 != '否') & (var_dict.是否受到期限影响 != '否'),'指标英文'])
cols = [i for i in cols if 'modify' not in i ]

for i in cols:
    print(i)
    x_with_y['%s_modify'%i] = (x_with_y[i] * x_with_y['pre_period'])/8

x_with_y = x_with_y.drop('%s_modify',1)

x_with_y = x_with_y.drop(['telcom','tel_-1','tel_'], 1)
x_with_y = x_with_y.fillna(-1)

drop_cols = [i for i in x_with_y.columns if 'modify_modify' in i ]
len(drop_cols)

x_with_y = x_with_y.drop(drop_cols, 1)

save_data_to_pickle(x_with_y, data_path, 'x_with_y_0513.pkl')
save_data_to_pickle(x_with_y, data_path, 'x_with_y_0403to0508.pkl')


#更新变量字典
var_dict = pd.read_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/V3模型建模代码可用变量字典.xlsx', sheet_name= 'Sheet1')
var_dict.head()

pd.DataFrame(set(x_with_y.columns) - set(var_dict['指标英文'])).to_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/var_cols.xlsx')


x_with_y.loc[x_with_y.age == -1].effective_date.value_counts().sort_index()

x_with_y.loc[x_with_y.age == -1][['loan_id', 'age']].head()

"""v2模型回溯打分"""

perf_latedays_sql = """
SELECT t1.loan_id
----平均
, 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then late_days else null end)/
	count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t2.id else null end)as avg_his_latedays_180d

FROM (SELECT loan_id as loan_id
              , apply_time
              , effective_date
              , customer_id
      FROM temp_uku_oldmodelv3_sample
      UNION
      SELECT id :: text as loan_id
               , apply_time :: varchar
               , effective_date :: varchar
               , customer_id :: text
      FROM dw_gocash_go_cash_loan_gocash_core_loan
      WHERE effective_date between '2020-03-04' and '2020-05-08' and return_flag = 'true'
       )t1
LEFT JOIN (SELECT id, apply_time, customer_id FROM dw_gocash_go_cash_loan_gocash_core_loan WHERE apply_time < '2020-05-10 00:00:00') t2 on t1.customer_id = t2.customer_id :: text
LEFT JOIN (SELECT * FROM public.dw_gocash_go_cash_loan_gocash_core_loan_pay_flow WHERE status = 'SUCCESS' ) t3 ON t2.id = t3.loan_id 
WHERE t1.apply_time :: timestamp > t2.apply_time :: timestamp
group by 1
"""

perf_latedays_datav2 = get_df_from_pg(perf_latedays_sql)
perf_latedays_datav2.loan_id = perf_latedays_datav2.loan_id.astype(str)

features_used = pd.read_excel('D:/Model/indn/202001_mvp_model/最终文档/需求文档/印尼UKU产品老客模型入模变量.xlsx',sheet_name='01_变量汇总')
set(features_used['模型变量名']) - set(x_with_y.columns)

temp = x_with_y.drop(['avg_his_latedays_180d'],1).merge(perf_latedays_datav2, on = 'loan_id', how = 'left')
temp.columns

temp.index = temp.loan_id

from utils3.misc_utils import convert_right_data_type

x_v2, _ = convert_right_data_type(temp[features_used['模型变量名']], var_dict)
x_v2 = x_v2.fillna(-1)
x_v2.head()

"""打分"""
import xgboost as xgb
from xgboost import DMatrix

mymodel = xgb.Booster()
mymodel.load_model("D:/Model/indn/202001_mvp_model/02_result/0205/grid_46_200225-20.model")


data_lean = DMatrix(x_v2)
ypred = mymodel.predict(data_lean)
score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([x_v2.index, score, ypred]).T

data_scored.rename(columns={0: 'loan_id', 1: 'score', 2: 'prob'}, inplace=True)
data_scored.score.max()

x_with_scoreV2 = x_v2.merge(data_scored, left_index = True, right_on = 'loan_id', how = 'left')
save_data_to_pickle(x_with_scoreV2, data_path, 'v2score.pkl')



