import numpy as np
import pandas as pd


data_path = 'D:/Model/201911_uku_ios_model/05_其他文档/'
data_path = 'D:/Model/201911_uku_ios_model/01_data/'
result_path = 'D:/Model/201911_uku_ios_model/02_result/'

rule_result = pd.read_excel('D:/Model/201911_uku_ios_model/05_其他文档/规则合集详情.xlsx', sheet_name='Dedup', dtype = {'order_no':str})
rule_result.dtypes
rule_result.flag30.value_counts()

oot_data = load_data_from_pickle(data_path, 'oot_with_prob_var26_1206.pkl')
test_data = pd.read_excel(os.path.join(result_path, 'result2_grid26_1206_score_v2.xlsx'), sheet_name= 'Sheet1')



oot_data['prob_bin'] = pd.cut(oot_data.y_pred, bins = train_p_bin,precision=6 ).astype(str)
test_data = test_data.rename(columns = {'Y':'flag_7','applied_at':'effective_date','proba_bin':'prob_bin'})

kept_cols = list(set(test_data.columns).intersection((oot_data.columns)))
kept_cols.remove('effective_date')

score = pd.concat([test_data[kept_cols], oot_data[kept_cols]],axis= 0)
score.loan_id = score.loan_id.astype(str)

rule_result2 = rule_result.merge(score, left_on = 'order_no', right_on = 'loan_id', how = 'left')
rule_result2.isnull().sum()
rule_result2.loc[rule_result2.loan_status.isnull()].effective_date.value_counts().sort_index()

rule_result2.order_no = rule_result2.order_no.astype(str)
rule_result2.loc[(rule_result2.effective_date >= '2019-06-27') & (rule_result2.effective_date <='2019-07-30'),'sample_set'] = 'train'
rule_result2.loc[(rule_result2.effective_date >= '2019-08-01') & (rule_result2.effective_date <='2019-08-28'),'sample_set'] = 'test'
rule_result2.loc[(rule_result2.effective_date >= '2019-08-30') & (rule_result2.effective_date <='2019-11-06'),'sample_set'] = 'oot'

rule_result2.to_excel(os.path.join(result_path, '模型分数和知识图谱规则交叉验证_v2.xlsx'))

rule_result2.isnull().sum()

score.loc[score.loan_id == '377160847570927616']