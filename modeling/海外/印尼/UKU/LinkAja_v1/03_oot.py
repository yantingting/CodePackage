import pandas as pd
import numpy as np
import os
import pickle
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score

import sys
sys.path.append('D:/Model/201911_uku_ios_model/03_code/newgenie/')

import utils3.misc_utils as mu
import utils3.summary_statistics as ss


reload(mt)
from imp import reload
from utils3.data_io_utils import *


"""  检查线下的v4分数和线上分数的是否一致 """

#读入线下分数
v4score = pd.read_csv(os.path.join(data_path, 'modelv4_score.csv'))
v4score.loan_id = v4score.loan_id.astype(str)
v4test = x_with_y.loc[x_with_y.sample_set == 'test'][['loan_id','extend_times','passdue_day','applied_type','applied_at','label']].merge(v4score, on = 'loan_id', how = 'left')

#v4test.rename(columns = {'effective_date':'applied_at', 'flag_7':'label'}, inplace=True)
v4test.loan_id = v4test.loan_id.astype(str)
v4test.bkl_v4_score = v4test.bkl_v4_score.astype(str)
v4test.to_excel(os.path.join(os.path.join(data_path, 'v4_test.xlsx')))

rm_id = list(v4test.loc[v4test.v4_score.isnull()]['loan_id'])

v4test = v4test.loc[~v4test.v4_score.isnull()]

v4_sql = """
select loanid, customerid, newusermodelv4score, createtime
from risk_gocash_mongo_riskcontrolresult
where newusermodelv4score <> ''
"""
v4score_online = get_df_from_pg(v4_sql)
v4score_online.shape
v4score_online.head()
temp.shape


temp = v4score.merge(v4score_online, right_on = 'loanid', left_on = 'loan_id')
temp.newusermodelv4score = temp.newusermodelv4score.astype(float)
temp.newusermodelv4score = round(temp.newusermodelv4score,6)
temp['diff'] = temp.newusermodelv4score - temp.v4_score
temp['diff'].min()
temp.to_excel(os.path.join(data_path,'v4score_compare.xlsx'))
temp.loc[temp['diff'] > 0.1].shape[0]/len(temp) #1.7%
temp.loc[temp['diff'] > 0.1].effective_date.max()


""" 清理 OOT的X和Y"""
all_x = load_data_from_pickle(data_path,'all_x_20191203_v2.pkl')
all_x.shape

flag_1205 = pd.read_excel(os.path.join(data_path, 'flag_20191205.xlsx'))
flag_1205 = flag_1205.loc[flag_1205.effective_date >= '2019-08-30']

flag_1205 = flag_1205.loc[flag_1205.flag_7.isin([0,1])]

oot_xy = flag_1205.merge(all_x.drop(['effective_date','customer_id'],1), on = 'loan_id', how = 'left')
oot_xy.loan_id = oot_xy.loan_id.astype(str)
oot_xy.index = oot_xy.loan_id

#变量衍生

oot_xy['occupation_office'] = oot_xy.occupation_type.apply(lambda x:1 if x == 0  else (-1 if x == -1 else 0))
oot_xy['occupation_entre'] = oot_xy.occupation_type.apply(lambda x:1 if x == 3 else (-1 if x == -1 else 0))
oot_xy['occupation_unemp'] = oot_xy.occupation_type.apply(lambda x:1 if x == 2 else (-1 if x == -1 else 0))
oot_xy['occupation_student'] = oot_xy.occupation_type.apply(lambda x:1 if x == 1 else (-1 if x == -1 else 0))

oot_xy['bank_code_BNI'] = oot_xy.bank_code.apply(lambda x:1 if x == 0  else (-1 if x == -1 else 0))
oot_xy['bank_code_MAYBANK'] = oot_xy.bank_code.apply(lambda x:1 if x == 1  else (-1 if x == -1 else 0))
oot_xy['bank_code_CIMB'] = oot_xy.bank_code.apply(lambda x:1 if x == 2  else (-1 if x == -1 else 0))
oot_xy['bank_code_BCA'] = oot_xy.bank_code.apply(lambda x:1 if x == 3  else (-1 if x == -1 else 0))
oot_xy['bank_code_PANIN'] = oot_xy.bank_code.apply(lambda x:1 if x == 4  else (-1 if x == -1 else 0))
oot_xy['bank_code_PERMATA'] = oot_xy.bank_code.apply(lambda x:1 if x == 5  else (-1 if x == -1 else 0))
oot_xy['bank_code_BRI'] = oot_xy.bank_code.apply(lambda x:1 if x == 6  else (-1 if x == -1 else 0))
oot_xy['bank_code_DANAMON'] = oot_xy.bank_code.apply(lambda x:1 if x == 7  else (-1 if x == -1 else 0))
oot_xy['bank_code_BTN'] = oot_xy.bank_code.apply(lambda x:1 if x == 8  else (-1 if x == -1 else 0))
oot_xy['bank_code_MANDIRI'] = oot_xy.bank_code.apply(lambda x:1 if x == 9  else (-1 if x == -1 else 0))

#打分
len(features_used)
oot_x = oot_xy[features_used]

for i in oot_x.columns:
    if i in list(var_map.keys()):
        print(i)
        if i != 'provincecode':
            oot_x[i] = oot_x[i].map(var_map[i])

oot_x.provincecode.value_counts()

oot_x.result = oot_x.result.replace('MATCH', 1).replace('NOT_MATCH', 0)
oot_x.whatsapp = oot_x.whatsapp.replace('yes', 1).replace('no', 0)

oot_x = oot_x.fillna(-1)
oot_x.provincecode = oot_x.provincecode.astype(int)

""" OOT样本上打分"""
y_oot_pred = best_xgb.predict_proba(oot_x)[:,1]

oot_with_prob = pd.DataFrame([oot_xy.index,y_oot_pred]).T.rename(columns = {0:'loan_id',1:'y_pred'})

oot_with_prob2 = oot_xy.merge(oot_with_prob, on = 'loan_id', how = 'left')
oot_with_prob2.isnull().sum()
oot_with_prob2.customer_id = oot_with_prob2.customer_id.astype(str)

save_data_to_pickle(oot_with_prob2, data_path, 'oot_with_prob_var26_1206.pkl')

"""
比较OOT上v4和新模型的效果
"""
v4oot = oot_xy[['loan_id','customer_id','effective_date','extend_times','flag_7']].merge(v4score, on = 'loan_id')

rmlist_oot = list(set(oot_xy.loan_id) - set(v4oot.loan_id))
oot_with_prob2 = oot_with_prob2.loc[~oot_with_prob2.loan_id.isin(rmlist_oot)]

FIG_PATH = os.path.join(result_path, 'figure', 'lifechart_26_1206')

v4_oot_lc = pt.show_result_new(v4oot, 'v4_score','flag_7', n_bins = 10, feature_label='v4')
test_lc = pt.show_result_new(oot_with_prob2, 'y_pred','flag_7', n_bins = 10, feature_label='var26')

path = os.path.join(FIG_PATH, "LiftChart_" + 'v4_26_oot' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()


#按月维度
v4oot = v4oot.rename(columns = {'effective_date_x':'effective_date'}).drop('effective_date_y',1)
v4oot["month"] = pd.to_datetime(v4oot.effective_date).dt.month
oot_with_prob2["month"] = pd.to_datetime(oot_with_prob2.effective_date).dt.month

v4_oot_lc_m11 = pt.show_result_new(v4oot.loc[v4oot.month == 9], 'v4_score','flag_7', n_bins = 10, feature_label='v4')
test_lc_m11 = pt.show_result_new(oot_with_prob2.loc[oot_with_prob2.month == 9], 'y_pred','flag_7', n_bins = 10, feature_label='var26')

path = os.path.join(FIG_PATH, "LiftChart_" + 'v4_26_m9' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()


"""
test上对比新模型和v4模型效果
"""
v4score = pd.read_csv(os.path.join(data_path, 'modelv4_score.csv'))
v4score.loan_id = v4score.loan_id.astype(str)
v4test = x_with_y.loc[x_with_y.sample_set == 'test'][['loan_id','extend_times','passdue_day','applied_type','applied_at','label']].merge(v4score, on = 'loan_id', how = 'left')
v4test.columns
#v4test.rename(columns = {'effective_date':'applied_at', 'flag_7':'label'}, inplace=True)
v4test.loan_id = v4test.loan_id.astype(str)
v4test.to_excel(os.path.join(os.path.join(data_path, 'v4_test.xlsx')))
v4test = v4test.loc[~v4test.v4_score.isnull()]

rm_id = list(v4test.loc[v4test.v4_score.isnull()]['loan_id'])

#test上对比v4和新模型的效果
v4_test_lc = pt.show_result_new(v4test, 'v4_score','label', n_bins = 10, feature_label='v4')
test_lc = pt.show_result_new(data_scored_all2.loc[(data_scored_all2.sample_set == 'test') & (~data_scored_all2.loan_id.isin(rm_id))], 'y_pred','Y', n_bins = 10, feature_label='var26')
#test28_lc = pt.show_result_new(test_pred_28, 'y_pred','Y', n_bins = 10, feature_label='test_var28')

path = os.path.join(FIG_PATH, "LiftChart_" + 'v4_26_test' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

v4test["week"] = pd.to_datetime(v4test.applied_at).dt.week
data_scored_all2.loc[data_scored_all2.sample_set == 'test',"week"] = pd.to_datetime(data_scored_all2.loc[data_scored_all2.sample_set == 'test'].applied_at).dt.week

#按照周对比v4和新模型结果
v4_test_lc_w34 = pt.show_result_new(v4test.loc[v4test.week == 34], 'v4_score','label', n_bins = 10, feature_label='v4')
test_lc_w34 = pt.show_result_new(data_scored_all2.loc[(data_scored_all2.sample_set == 'test') & (data_scored_all2.week == 34)], 'y_pred','Y', n_bins = 10, feature_label='var26')
path = os.path.join(FIG_PATH, "LiftChart_" + 'v4_26_34' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()


