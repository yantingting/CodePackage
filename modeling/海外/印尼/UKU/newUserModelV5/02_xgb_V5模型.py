import warnings
warnings.filterwarnings('ignore')

# 更新.pyc文件
import compileall
compileall.compile_dir(r'D:/_Tools/newgenie/utils3')

import pandas as pd
import numpy as np
import os
import pickle
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score

import sys
sys.path.append('D:/_Tools/newgenie/')
sys.setrecursionlimit(100000)


import utils3.misc_utils as mu
import utils3.summary_statistics as ss

#from utils3.feature_select import *  # from ml
import utils3.feature_selection as fs  # from genie
fs_obj = fs.FeatureSelection()

import utils3.metrics as mt
pf = mt.Performance()
bw = mt.BinWoe()


os.getcwd()
INPUT_PATH = 'D:\\Model Development\\201912 IDN new v5\\01 Data\\raw data'
RESULT_PATH = 'D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800 python3.6.8'
if not os.path.exists(RESULT_PATH):
    os.makedirs(RESULT_PATH)

r_all = pd.read_csv(INPUT_PATH + '\\r_all3.csv')
data_all = r_all[(r_all.flag7 == 1) | (r_all.flag7 == 0) ]
print(data_all.shape)


#data_all.label.value_counts(dropna = False)
data_all.set_index(["loan_id"], inplace=True)
print(data_all.shape)

data_all['passdue_day']=data_all.flag7.apply(lambda x: 0 if x==0 else 7)
data_all['applied_type']=data_all.sample_set.map({'train': 0, 'test': 1, 'oot': 2})
data_all['applied_from']="UKU"

data_all.rename(columns = {'loan_id':'order_no','effective_date':'applied_at','flag7':'label'}, inplace=True)

#data_all.columns

import utils3.generate_report as gr
#train_test = df_all[df_all.sample_set != 'oot']
cnt_label,cnt_month, cnt_qudao = gr.describe_sample(data_all)

print(cnt_label)
print(cnt_month)
print(cnt_qudao)

data_all = data_all.drop(['month', 'label_7','label_15'],axis=1)
print(data_all.shape)

base_col = ['applied_at', 'sample_set', 'passdue_day', 'applied_type', 'applied_from']
x_col = list(set(list(data_all.columns))-set(base_col)-set(['label']))
print(len(x_col))   #525-6=519  #518-6=512 #172 166 #1445

#var_dict = pd.read_excel(os.path.join(INPUT_PATH, '建模代码可用变量字典 20191106.xlsx'), sheet_name = '02_字典')
#var_dict = var_dict[var_dict.指标英文.isin(x_col)]
#var_dict.columns

var_dict = pd.DataFrame(columns= ['数据源', '指标英文'])
var_dict['数据源'] = x_col
var_dict['指标英文'] = x_col

data_all = data_all.fillna(-1)
data_all = data_all.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])

## EDA
"""  
utils3.misc_utils.process_missing
    处理缺失值。
    -8888: 如果某个数据源整条数据都没有就算是完整缺失未查得。
    -8887: 个别变量值缺失。

    Args:
    X (pd.DataFrame): 原始数值的分类变量
    var_dict (pd.DataFrame): 标准数据字典，需包含数据源，指标英文两列。
    known_missing (dict): 已知的代表缺失的值以及想要替换成的值。格式为：
        {-1: -9999, -9999999: -8887}
    downflagmap (dict): 中间层有些数据源有downflag字段用来标注是否宕机，是否查无此人等。
        格式为：
        {'Anrong_DownFlag': {1: -9999}, 'Tongdun_DownFlag': {1: -9999}}
"""  

X_cleaned = mu.process_missing(X = data_all,var_dict=var_dict, known_missing={-1:-9999, '-1':-9999}, downflagmap={}, verbose=True)
X_cleaned.head()

X_cleaned_sorted = X_cleaned.reindex(data_all.index.tolist())
data_cleaned = pd.concat([data_all[base_col+['label']], X_cleaned_sorted], axis=1)
data_cleaned = data_cleaned[data_all.columns.tolist()]
data_cleaned.head()


var_dict = pd.DataFrame(columns= ['数据源', '指标英文', '指标中文', '数据类型'])
var_dict['数据源'] = x_col
var_dict['指标英文'] = x_col
var_dict['指标中文'] = x_col
var_dict['数据类型'] = 'float'

"""  
utils3.summary_statistics.eda
    X (pd.DataFrame()): 是一个宽表，每一列是一个变量，每一行是一个obs
    var_dict (pd.DataFrame()): 标准变量字典表，需包含以下这些列：数据源，指标英文，指标中文，数据类型，指标类型。
    useless_vars (list): 无用变量名list
    exempt_vars (list): 豁免变量名list，这些变量即使一开始被既定原因定为exclude，也会被保留，比如前一版本模型的变量
    data_path (str): 存储数据文件路径
    save_label (str): summary文档存储将用'%s_variables_summary.xlsx' % save_label存
    uniqvalue_cutoff（float): 0-1之间，缺失率和唯一值阈值设定

* 无用变量标记（excelusion_reason）：缺失比例大于阈值，0值比例大于阈值，类别变量类别个数=1/>100
* 识别类别变量：①字典中数据类型为varchar,②dtype为object
"""          

ss.eda(X = data_cleaned, var_dict=var_dict, data_path = RESULT_PATH, useless_vars = [],\
       exempt_vars = [], save_label ='20191211', uniqvalue_cutoff=0.97)
summary = pd.read_excel(os.path.join(RESULT_PATH, '20191211_variables_summary.xlsx'), encoding='utf-8')

eda_remove = summary.loc[summary.exclusion_reason.notnull(), '指标英文'].tolist()
len(eda_remove)


 # 总模型
#app_pred = load_data_from_pickle('D:/Model Development/201912 IDN new v5/03 Result/py_output 20191212 1900', 'grid_33_191212_192429_data_scored_all.pkl')
#app_pred.to_csv('D:/Model Development/201912 IDN new v5/03 Result/py_output 20191212 1900/grid_33_191212_192429_data_scored_all.csv', index = False)
#app_pred = pd.read_csv('D:/Model Development/201912 IDN new v5/03 Result/py_output 20191212 1900/grid_33_191212_192429_data_scored_all.csv')
#app_pred.loan_id = app_pred.loan_id.str.replace('(','')
#app_pred.rename(columns={'y_pred': 'app_pred'}, inplace= True)
#
#data_all = data_all.drop(var_app, axis = 1)
#data_all = data_all.merge(app_pred[['loan_id','app_pred']], how = 'left', on = 'loan_id')
#print(data_all.shape)


## 划分train test testnew
'''******************** 融合模型 选变量 *********************'''
 # app 子模型
df_all = data_cleaned[base_col + list(['label']) + var_app]  
print(df_all.shape)

 # app 总模型
df_all = data_cleaned.drop(var_app, axis = 1)
df_all.reset_index(inplace = True)
df_all.loan_id = df_all.loan_id.astype(str)

#app_pred = load_data_from_pickle('D:/Model Development/201912 IDN new v5/03 Result/py_output 20191212 2100', 'grid_45_191213_002428_all_pred.pkl')
app_pred = all_pred[['loan_id', 'y_pred']]
app_pred.rename(columns={'y_pred': 'app_pred'}, inplace= True)
df_all = df_all.merge(app_pred[['loan_id','app_pred']], how = 'left', on = 'loan_id')
df_all.set_index(["loan_id"], inplace=True)
print(df_all.shape)
df_all = df_all.drop(['borrowing_purposes_OTHER','borrowing_purposes_MODAL USAHA',\
'borrowing_purposes_HIBURAN','borrowing_purposes_KEPERLUAN_SEHARI-HARI',\
'borrowing_purposes_DEKORASI','borrowing_purposes_EDUKASI'], axis = 1)
print(df_all.shape)
    
 # 一个模型
df_all = data_cleaned.copy()
df_all = df_all.drop(['borrowing_purposes_OTHER','borrowing_purposes_MODAL USAHA',\
'borrowing_purposes_HIBURAN','borrowing_purposes_KEPERLUAN_SEHARI-HARI',\
'borrowing_purposes_DEKORASI','borrowing_purposes_EDUKASI'], axis = 1)


#df_all = df_all.fillna(-1)
df_all = df_all.replace([-9999,'-9999',-8888,'-8888',-8887,'-8887'],[-1,-1,-1,-1,-1,-1])
print(df_all.sample_set.value_counts(dropna = False))

train_df = df_all[df_all.sample_set == 'train']
test_df = df_all[df_all.sample_set == 'test']
testnew_df = df_all[df_all.sample_set == 'oot']

train_df.shape
train_df.label.value_counts(dropna = False)
#train_df.head()
test_df.shape
test_df.label.value_counts(dropna = False)
#test_df.head()
testnew_df.shape
testnew_df.label.value_counts(dropna = False)
#testnew_df.head()

## Univariate Chart

# Univariate Chart (all)
#import utils3.plot_tools as pt
#import matplotlib.pyplot as plt
#
#FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'UniVarChart')
#if not os.path.exists(FIG_PATH):
#    os.makedirs(FIG_PATH)
#
#for i in x_col:
#    pt.univariate_chart(df_all, i,  'label', n_bins=10,
#                     default_value=-1,
#                     dftrain=train_df, dftest=test_df,
#                     draw_all=True, draw_train_test=True)
#    path = os.path.join(FIG_PATH, "uniVarChart_"+i+".png")
#    plt.savefig(path,format='png', dpi=100)
#    plt.close()
#    print(i+': done')


#train_df = train_df.drop(['bins','bin_no'],axis=1)
#test_df = test_df.drop(['bins','bin_no'],axis=1)
#testnew_df = testnew_df.drop(['bins','bin_no'],axis=1)

print(train_df.shape)
print(test_df.shape)
print(testnew_df.shape)

# 划分X_train X_test X_testnew
train = train_df.drop(base_col, axis=1)
test = test_df.drop(base_col, axis=1)
testnew = testnew_df.drop(base_col, axis=1)
#train.head()
print(train.shape)

# 若EDA有需要删除的变量
#train = train.drop(eda_remove, axis=1)
#test = test.drop(eda_remove, axis=1)
#testnew = testnew.drop(eda_remove, axis=1)

X_train = train.drop(['label'], axis=1)
y_train = train.label
X_test = test.drop(['label'], axis=1)
y_test = test.label
X_testnew = testnew.drop(['label'], axis=1)
y_testnew = testnew.label

print(X_train.shape)
#X_train.columns

''' ********************************************************************************************'''
## XGB importance
# fit model
model = XGBClassifier()
model.fit(X_train, y_train)
print(model)

## prediction
y_pred = model.predict(X_test)
predictions = [round(value) for value in y_pred]

## evaluation
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0))

features_in_model = list(X_train.columns)
feature_importance = model.feature_importances_

var_importance = pd.DataFrame(columns=["指标英文", 'importance'])
var_importance['指标英文'] = features_in_model
var_importance['importance'] = feature_importance
var_importance.loc[:, 'importance_rank'] = var_importance.importance.rank(ascending=False)
var_importance.head()

''' ********************************************************************************************'''
## 变量分布及各项指标

os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

args_dict = {
    'random_forest': {
        'grid_search': False,
        'param': None
    },
    'xgboost': {
        'grid_search': False,
        'param': None
    }
}
methods = [
#    'random_forest',
    #'lasso',
    'xgboost'
]


## Train 分箱
model_train_final = train.copy()
features_var_dict = list(var_dict['指标英文'])
X_train_IV = model_train_final[features_var_dict]
y_train_IV = model_train_final['label'].astype(int)

X_cat_train, X_transformed_train, woe_iv_df_train, rebin_spec_train, ranking_result_train = fs_obj.overall_ranking(X_train_IV, y_train_IV,
                                                                                           var_dict, args_dict,
                                                                                           methods, num_max_bins=5)

rebin_spec = mu.convert_rebin_spec2XGB_rebin_spec(rebin_spec_train)
rebin_spec_bin_adjusted = {k: v for k, v in rebin_spec.items() if k in features_var_dict}

## 输出变量的分箱
X_cat_train = bw.convert_to_category(
    model_train_final[features_var_dict],
    var_dict,
    rebin_spec_bin_adjusted)

model_test_final = test.copy()
X_cat_test = bw.convert_to_category(
    model_test_final[features_var_dict],
    var_dict,
    rebin_spec_bin_adjusted)

model_testnew_final = testnew.copy()
X_cat_testnew = bw.convert_to_category(
    model_testnew_final[features_var_dict],
    var_dict,
    rebin_spec_bin_adjusted)

# 变量分布
## train
train['appmon'] = '0_train'
var_cols = set(X_train.columns).intersection(var_dict.指标英文)
X_cat_train_with_y_appmon = pd.merge(X_cat_train,train[['label','appmon']] ,left_index=True,right_index=True)
var_dist_badRate_by_time = ss.get_badRate_and_dist_by_time(X_cat_train_with_y_appmon,var_cols,'appmon','label')

## test
test['appmon'] = '1_test'
X_cat_test_with_y_appmon = pd.merge(X_cat_test,test[['label','appmon']],left_index=True,right_index=True)
var_dist_badRate_by_time_test = ss.get_badRate_and_dist_by_time(X_cat_test_with_y_appmon,var_cols,'appmon','label')

## testnew
testnew['appmon'] = '2_testnew'
X_cat_testnew_with_y_appmon = pd.merge(X_cat_testnew,testnew[['label','appmon']],left_index=True,right_index=True)
var_dist_badRate_by_time_testnew = ss.get_badRate_and_dist_by_time(X_cat_testnew_with_y_appmon,var_cols,'appmon','label')

## all
all_cat = pd.concat([X_cat_train,X_cat_test,X_cat_testnew])
app_data = pd.concat([train[['label','appmon']],test[['label','appmon']],testnew[['label','appmon']]])
X_cat_with_y_appmon_all = pd.merge(all_cat,app_data[['label','appmon']] ,left_index=True,right_index=True)
var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,var_cols,'appmon','label')
var_dist_badRate_by_time_all.head()

# PSI
var_psi = pf.variable_psi(X_cat_train, X_cat_test, var_dict)
var_psi.loc[:, 'psi_rank'] = var_psi.PSI.rank(ascending=False)
var_psi.head()


# 汇总各项指标
ranking_result = ranking_result_train.merge(var_psi, on='指标英文', how='left')\
                                      .merge(var_importance, on='指标英文', how='left')
#ranking_result.columns
ranking_result.head()

#train.columns
#train.shape
train = train.drop(['appmon'],axis=1)
test = test.drop(['appmon'],axis=1)
testnew = testnew.drop(['appmon'],axis=1)

print(train.shape)
print(test.shape)
print(testnew.shape)

''' ******************************************** ********************************************'''
''' ******************************************** ********************************************'''

# feature selection

ranking_result_bk = ranking_result.copy()
ranking_result = ranking_result_bk.copy()


f_rmv = fs.feature_remove(train,test, ranking_result, RESULT_PATH, psi = 0.1, iv = 0.01, imp = 0.001, corr = 1, slope = 'FALSE')
print(len(f_rmv))

#f_rmv = f_rmv + list(['borrowing_purposes_KEPERLUAN_SEHARI-HARI'])
#print(len(f_rmv))

f_keep = [ele for ele in list(X_train.columns) if ele not in f_rmv] + list(['label'])
print(len(f_keep))

f_keep = list(['gender'
,'age'
,'phone_age'
,'07d'
,'14d'
,'21d'
,'30d'
,'60d'
,'90d'
,'total'
,'result'
,'refer_spouse'
,'religion_ISLAM'
,'education_SENIOR_HIGH_SCHOOL'
,'education_REGULAR_COLLEGE_COURSE'
,'bankcode_BCA'
,'bankcode_BRI'
,'job_MANAJER'
,'screen_(1424.0, 720.0)'
,'brand_SAMSUNG'
,'model_SH-04H'
,'mid_freq_app'
,'rate_high_freq_app'
,'rate_mid_freq_app'
,'rate_low_freq_app'
,'AdaKami'
,'Agen Masukan Market'
,'ConfigUpdater'
,'DanaRupiah'
,'Dokumen'
,'Facebook'
,'Facebook Services'
,'File'
,'Finmas'
,'Foto'
,'Gmail'
,'Google'
,'Instagram'
,'KTA KILAT'
,'Kalender'
,'Key Chain'
,'Kontak'
,'Kredinesia'
,'Kredit Pintar'
,'KreditQ'
,'Layanan Google Play'
,'Mesin Google Text-to-speech'
,'Messenger'
,'MyTelkomsel'
,'Pemasang Sertifikat'
,'Penyimpanan Kontak'
,'Rupiah Cepat'
,'SHAREit'
,'Setelan'
,'Setelan Penyimpanan'
,'SmartcardService'
,'Tema'
,'TunaiKita'
,'UKU'
,'UangMe'
,'Unduhan'
,'WPS Office'
,'com.android.carrierconfig'
,'com.android.smspush',
'label'])



#f_rmv = list(set(f_rmv+list(set(list(['whatsapp',
#'borrowing_purposes_DEKORASI',
#'borrowing_purposes_EDUKASI',
#'borrowing_purposes_HIBURAN',
#'borrowing_purposes_KEPERLUAN_SEHARI-HARI',
#'borrowing_purposes_MODAL USAHA',
#'borrowing_purposes_OTHER'])))))
#print(len(f_rmv))

try:
    train = train.drop(['slope'], axis=1)
    test = test.drop(['slope'], axis=1)
    #testnew = testnew.drop(['slope'], axis=1)
except:
    pass

print(train.shape)
print(test.shape)
print(testnew.shape)


#train_2 = train.drop(f_rmv, axis=1)
#test_2 = test.drop(f_rmv, axis=1)
#testnew_2 = testnew.drop(f_rmv, axis=1)
train_2 = train[f_keep]
test_2 = test[f_keep]
testnew_2 = testnew[f_keep]

print(train_2.shape)
print(test_2.shape)
print(testnew_2.shape)

## XGBoost

X_train = train_2.drop(['label'], axis=1)
y_train = train_2.label
X_test = test_2.drop(['label'], axis=1)
y_test= test_2.label
X_testnew = testnew_2.drop(['label'], axis=1)
y_testnew= testnew_2.label

#X_train.head()
print(X_train.shape)

features_used = X_train.columns.tolist()
len(features_used)

'''
# Random Search
import utils3.xgboost_model as xm
param = {
        'eta': [0.01,0.05,0.1],
        'max_depth': [2,3,4],
        'gamma': [0, 0.1, 0.2],
        'min_child_weight':[5,10],
        'subsample': [0.6,0.7,0.8],
        'colsample_bytree': [0.6,0.7,0.8],
        'n_estimators': [50,100,150,200,300]
        }

# 调参：sklearn.model_selection -- RandomizedSearchCV
# fit_model：xgboost.XGBClassifier
best_xgb, df_params, model_importance = xm.xgboost_randomgridsearch(X_train, y_train, X_test, y_test, NFOLD=5, param=None)
best_xgb
df_params.head()
model_importance.head()
# prediction
y_train_pred = best_xgb.predict_proba(X_train)[:,1]
y_test_pred = best_xgb.predict_proba(X_test)[:,1]
y_testnew_pred = best_xgb.predict_proba(X_testnew)[:,1]

# hyperopt
import utils3.xgboost_model as xm
from hyperopt import hp
params = {
    'num_boost_round':1000, # 最大迭代次数
    'nfold':3, # CV-K折
    'early_stopping_rounds':3,
    'silent':1, # 是否打印训练过程中间的一些信息到屏幕
    'seed':1123,
    'max_depth':hp.choice("max_depth", [2,3,4,5]),
    'min_child_weight':hp.quniform("min_child_weight", 1, 7, 1),
    'gamma':hp.uniform("gamma", 0, 0.4),
    'subsample':hp.uniform("subsample", 0.6, 1),
    'colsample_bytree':hp.uniform("colsample_bytree", 0.6, 1),
    'eta':hp.uniform("eta", 0.05, 0.2)           
}

import xgboost as xgb

a = -1
b = -1
c = -1
n=1
while a<0 or b<0 or c<0:
    best_xgb, df_params, model_importance = xm.xgb_hyperopt(X_train, y_train, param = None)
    # prediction
    dtrain = xgb.DMatrix(X_train)
    y_train_pred = model.predict(dtrain)
    dtest = xgb.DMatrix(X_test)
    y_test_pred = model.predict(dtest)
    dtestnew = xgb.DMatrix(X_testnew)
    y_testnew_pred = model.predict(dtestnew)
    a = min(y_train_pred)
    b = min(y_test_pred)
    c = min(y_testnew_pred)
    print(str(n)+'次模型结束')
    n += 1 

best_xgb
df_params.head()
model_importance.head()
'''

# Grid Search(local)
'''
-------------------------------
XGB tuning parameters
-------------------------------
'''
'''
参数调优的一般步骤
1. 确定学习速率和提升参数调优的初始值
2. max_depth 和 min_child_weight 参数调优
3. gamma参数调优
4. subsample 和 colsample_bytree 参数优
5. 正则化参数alpha调优
6. 降低学习速率和使用更多的决策树
'''

param_test1 = {
    'max_depth': range(2,4, 1),
        'min_child_weight': range(5, 15, 5)
}
gsearch1 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, gamma=0, subsample=0.7,
                                                max_depth=4, colsample_bytree=1, objective='binary:logistic',
                                                nthread=6, scale_pos_weight=1, seed=27),
                        param_grid=param_test1, scoring='roc_auc', n_jobs=4, iid=False, cv=5)

gsearch1.fit(X_train, y_train)

best_max_depth = gsearch1.best_params_['max_depth']
best_min_child_weight = gsearch1.best_params_['min_child_weight']
print('1:', gsearch1.best_score_)
print('best_max_depth:', best_max_depth)
print('best_min_child_weight:', best_min_child_weight)


#param_test2 = {'gamma': [i / 10.0 for i in range(0, 5)]}
param_test2 = {'gamma':[0,0.05,0.1]}
gsearch2 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, subsample=0.7, colsample_bytree=1,
                                                max_depth=best_max_depth,
                                                min_child_weight=best_min_child_weight, objective='binary:logistic',
                                                nthread=6, scale_pos_weight=1, seed=27),
                        param_grid=param_test2, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
gsearch2.fit(X_train, y_train)
best_gamma = gsearch2.best_params_['gamma']
print('2:', gsearch2.best_score_)
print('best_gamma:',best_gamma)


param_test3 = {
    #'subsample': [i / 100.0 for i in range(55, 95,5)],
    'subsample': [i / 10.0 for i in range(6, 9)],
    #'colsample_bytree': [i / 100.0 for i in range(55, 95,5)]
    'colsample_bytree': [i / 10.0 for i in range(6, 9)]
}
gsearch3 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, max_depth=best_max_depth, gamma=best_gamma,
                                                min_child_weight=best_min_child_weight, objective='binary:logistic', nthread=6,
                                                scale_pos_weight=1, seed=27),
                        param_grid=param_test3, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
gsearch3.fit(X_train, y_train)
best_subsample = gsearch3.best_params_['subsample']
best_colsample_bytree = gsearch3.best_params_['colsample_bytree']
print('3:', gsearch3.best_score_)
print('best_subsample:', best_subsample)
print('best_colsample_bytree:', best_colsample_bytree)


param_test4 = {'reg_alpha': [0, 0.001, 0.005, 0.01]}
#param_test4 = {'reg_alpha': [0, 0.001, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05]}
gsearch4 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, max_depth=best_max_depth, gamma=best_gamma,
                                                colsample_bytree=best_colsample_bytree, subsample=best_subsample,
                                                min_child_weight=best_min_child_weight, objective='binary:logistic', nthread=6,
                                                scale_pos_weight=1, seed=27),
                        param_grid=param_test4, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
gsearch4.fit(X_train, y_train)
best_reg_alpha = gsearch4.best_params_['reg_alpha']
print('4:', gsearch4.best_score_)
print('best_reg_alpha:', best_reg_alpha)


param_test5 = {'n_estimators': range(0, 200, 50)}
#param_test5 = {'n_estimators': range(0, 100, 10)}
gsearch5 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, max_depth=best_max_depth, gamma=best_gamma,
                                                colsample_bytree=best_colsample_bytree, subsample=best_subsample,
                                                reg_alpha=best_reg_alpha,
                                                min_child_weight=best_min_child_weight, objective='binary:logistic',
                                                nthread=6, scale_pos_weight=1, seed=27),
                        param_grid=param_test5, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
gsearch5.fit(X_train, y_train)
best_n_estimators = gsearch5.best_params_['n_estimators']
print('5:', gsearch5.best_score_)
print('best_n_estimators:', best_n_estimators)


param_test6 = {'learning_rate': [i / 10.0 for i in range(1, 3)]}
#param_test6 = {'learning_rate': [i / 100.0 for i in range(1, 20)]}
gsearch6 = GridSearchCV(estimator=XGBClassifier(max_depth=best_max_depth, gamma=best_gamma, n_estimators=best_n_estimators,
                                                colsample_bytree=best_colsample_bytree, subsample=best_subsample, reg_alpha=best_reg_alpha,
                                                min_child_weight=best_min_child_weight, objective='binary:logistic', nthread=6,
                                                scale_pos_weight=1, seed=27),
                        param_grid=param_test6, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
gsearch6.fit(X_train, y_train)
best_learning_rate = gsearch6.best_params_['learning_rate']
print('6:', gsearch6.best_score_)
print('best_learning_rate:', best_learning_rate)

best_learning_rate=0.1
best_n_estimators=150
best_max_depth=3
best_gamma=0
best_colsample_bytree=0.6
best_subsample=0.8
best_reg_alpha=0.01
best_min_child_weight=5


# 用获取得到的最优参数再次训练模型
best_xgb = XGBClassifier(learning_rate=best_learning_rate, n_estimators=best_n_estimators, max_depth=best_max_depth,
                         gamma=best_gamma, colsample_bytree=best_colsample_bytree, subsample=best_subsample, reg_alpha=best_reg_alpha,
                         min_child_weight=best_min_child_weight,
                         objective='binary:logistic', nthread=6, scale_pos_weight=1, eval_metric='auc', seed=27)
#print(best_xgb)
best_xgb.fit(X_train, y_train)

## df_params
param_dict = {"param": ['learning_rate', 'n_estimators','max_depth','gamma','colsample_bytree','subsample','reg_alpha','min_child_weight'],
"value": [best_learning_rate, best_n_estimators, best_max_depth, best_gamma, best_colsample_bytree, best_subsample, best_reg_alpha, best_min_child_weight ]}
df_params = pd.DataFrame(param_dict)
df_params

## model变量重要性
best_importance = best_xgb.feature_importances_
model_importance = pd.DataFrame(columns=["varName", 'importance'])
model_importance['varName'] = features_used
model_importance['importance'] = best_importance

model_importance.head()

# prediction
y_train_pred = best_xgb.predict_proba(X_train)[:,1]
y_test_pred = best_xgb.predict_proba(X_test)[:,1]
y_testnew_pred = best_xgb.predict_proba(X_testnew)[:,1]


## 打分&KS
train_pred = train_df.copy()
train_pred['y_pred'] = y_train_pred

test_pred = test_df.copy()
test_pred['y_pred'] = y_test_pred

testnew_pred = testnew_df.copy()
testnew_pred['y_pred'] = y_testnew_pred

train_pred.head()
#test_pred.head()
#testnew_pred.head()

data_scored_train, train_proba_ks, train_proba_ks_20, train_score_ks, train_score_ks_20, \
data_scored_test, test_proba_ks, test_proba_ks_20, test_score_ks, test_score_ks_20 = pf.data_score_KS(train_pred, test_pred, 'y_pred')

data_scored_train2, train_proba_ks2, train_proba_ks_202, train_score_ks2, train_score_ks_202, \
data_scored_testnew, testnew_proba_ks, testnew_proba_ks_20, testnew_score_ks, testnew_score_ks_20 = pf.data_score_KS(train_pred, testnew_pred, 'y_pred')

# AUC ACC
def auc_acc_table(df):
    from sklearn.metrics import roc_auc_score
    y = df.label.values
    y_pred = df.y_pred

    ## AUC
    auc = roc_auc_score(y, y_pred)
    print("auc: %.2f" % auc)

    ## Accuracy
    predictions = [round(value) for value in y_pred]
    accuracy = accuracy_score(y, predictions)
    print("Accuracy: %.4f%%" % (accuracy * 100.0))
    
    return auc, accuracy


train_auc, train_acc = auc_acc_table(train_pred)
test_auc, test_acc = auc_acc_table(test_pred)
testnew_auc, testnew_acc = auc_acc_table(testnew_pred)

## PDP(all)

import utils3.plot_tools as pt
import matplotlib.pyplot as plt
import utils3.generate_report as gr

FIG_PATH = os.path.join(RESULT_PATH, 'figure' , 'PDP')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

f_imp_list = gr.get_feature_importance(model_importance)
#f_imp_list
path = os.path.join(FIG_PATH,"top20importance.png")
plt.savefig(path, format='png', dpi=100)
plt.close()

select_features = model_importance['varName'].values.tolist()
all_pred = pd.concat([X_train, X_test])

n=0
while n <len(f_imp_list):
    m = n+9
    features_draw=[i for i in f_imp_list[n:m]]
    pt.pdpCharts9(best_xgb, all_pred, features_draw, select_features, n_bins=10, dfltValue = -1)
    
    path = os.path.join(FIG_PATH,"pdp_"+str(n)+"_"+str(m)+".png")
    plt.savefig(path, format='png', dpi=100)
    plt.close()
    
    n += 9
print('end')  

# excel集合结果

## 变量分布
var_dist_badRate_by_time_all['used_in_model'] = var_dist_badRate_by_time_all.varName.apply(lambda x: x in features_used)

## ranking result
ranking_result['used_in_model'] = ranking_result.指标英文.apply(lambda x: x in features_used)

## AUC + ACCURACY
# 若需要首申复申复贷的auc，可调用gerate_report.auc_tabel(df)
#auc_list = [auc_train, auc_test, auc_testnew]
#acc_list = [acc_train, acc_test, acc_testnew]

auc_list = [train_auc, test_auc, testnew_auc]
acc_list = [train_acc, test_acc, testnew_acc]

split_list = ['train','test','oot']
df_auc_acc = pd.DataFrame({"sample_set":split_list,"auc":auc_list,"accuracy":acc_list})

## df_prediction
all_pred = pd.concat([train_pred, test_pred, testnew_pred]).reset_index()
all_pred['loan_id'] = all_pred['loan_id'].astype(str)

## data_scored
data_scored_train['sample_set'] = "train"
data_scored_test['sample_set'] = "test"
data_scored_testnew['sample_set'] = "oot"
#data_scored_all = pd.concat([data_scored_train, data_scored_test])
data_scored_all = pd.concat([data_scored_train, data_scored_test, data_scored_testnew])
data_scored_all['order_no'] = data_scored_all['order_no'].astype(str)

# python3.6的dict是ordered，按照顺序这样定义，之后生成excel的时候会按照顺序创建sheet
xgb_dict = {}

# 如果sheet对应的内容是dict，则dict的key会出现在sheet第一列。value会从第二列开始插入
# 如果sheet对应的内容是df，则从sheet的A1位置开始插入整张表格，不包含pd.DataFrame的index
xgb_dict['01_sample_desc'] = {'整体':cnt_label,
                              '按月份':cnt_month,
                              '按渠道':cnt_qudao
                             }
# xgb_dict['02_EDA'] = summary
xgb_dict['03_AUC&ACC'] = df_auc_acc
xgb_dict['04_KS'] = {'train_proba_ks':train_proba_ks,
                     'test_proba_ks':test_proba_ks,
                     'testnew_proba_ks':testnew_proba_ks,
                     'train_score_ks':train_score_ks,
                     'test_score_ks':test_score_ks,
                     'testnew_score_ks':testnew_score_ks,
                     'train_proba_ks_20':train_proba_ks_20,
                     'test_proba_ks_20':test_proba_ks_20,
                     'testnew_proba_ks_20':testnew_proba_ks_20,
                     'train_score_ks_20':train_score_ks_20,
                     'test_score_ks_20':test_score_ks_20,
                     'testnew_score_ks_20':testnew_score_ks_20
                      }
xgb_dict['05_model_importance'] = model_importance.reset_index()
xgb_dict['06_model_params'] = df_params
#xgb_dict['07_ranking_result'] = ranking_result
#xgb_dict['08_data_scored'] = data_scored_all
#xgb_dict['09_data_prediction'] = all_pred
#xgb_dict['08_woe_iv_train'] = woe_iv_df_train
#xgb_dict['09_var_dist_badRate'] = var_dist_badRate_by_time_all


from datetime import datetime
import utils3.filing as fl

FILE_NAME = "grid_%d_%s"%(len(features_used), datetime.now().strftime('%y%m%d_%H%M%S'))

## SAVE MODEL
pickle.dump(best_xgb, open(os.path.join(RESULT_PATH, FILE_NAME+".pkl"), "wb"))
best_xgb.save_model(os.path.join(RESULT_PATH, FILE_NAME+".model"))
## SAVE data_scored_all
pickle.dump(data_scored_all, open(os.path.join(RESULT_PATH, FILE_NAME+"_data_scored_all.pkl"), "wb"))
pickle.dump(all_pred, open(os.path.join(RESULT_PATH, FILE_NAME+"_all_pred.pkl"), "wb"))
## SAVE data_prediction
#pickle.dump(data_scored_all, open(os.path.join(RESULT_PATH, FILE_NAME+"_data_prediction.pkl"), "wb"))


## 生成EXCEL
#result_path：excel的输出目录
#fig_path：KS、AUC、score_dis图片的存储主目录，也就是到figure目录即可
#file_name：统计结果的名称，例如：“summary.xlsx”
#data_dic:所有数据的汇总字典
fl.ModelSummary2Excel(result_path = RESULT_PATH, fig_path= RESULT_PATH, file_name = FILE_NAME+".xlsx", data_dic = xgb_dict).run()

## Lift Chart

import utils3.plot_tools as pt
import matplotlib.pyplot as plt
train_pred.head()

FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

# lift_chart - train test oot 
train_lc = pt.show_result_new(train_pred, 'y_pred','label', n_bins = 10, feature_label='train')
test_lc = pt.show_result_new(test_pred, 'y_pred','label', n_bins = 10, feature_label='test')
oot_lc = pt.show_result_new(testnew_pred, 'y_pred','label', n_bins = 10, feature_label='OOT')

path = os.path.join(FIG_PATH,"lift_chart_overall.png")
plt.savefig(path, format='png', dpi=100)
plt.close()

# lift_chart - month
##testnew_pred['applied_at'] = testnew_pred['applied_at'].astype(str)
pt.lift_chart_by_time(testnew_pred,'y_pred','label', n_bins = 10, by="month")

path = os.path.join(FIG_PATH,"lift_chart_by_month.png")
plt.savefig(path, format='png', dpi=100)
plt.close()

# lift_chart - week
pt.lift_chart_by_time(testnew_pred,'y_pred','label', n_bins = 10, by="week")

path = os.path.join(FIG_PATH,"lift_chart_by_week.png")
plt.savefig(path, format='png', dpi=100)
plt.close()


''' 按天看auc '''
# all_pred = load_data_from_pickle('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800', 'grid_64_191213_120304_all_pred.pkl')

testoot_pred = all_pred[(all_pred.sample_set=='oot') | (all_pred.sample_set=='test')]

FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

pt.lift_chart_by_time(testoot_pred,'y_pred','label', n_bins = 10, by="week")

path = os.path.join(FIG_PATH,"lift_chart_by_week_testoot.png")
plt.savefig(path, format='png', dpi=100)
plt.close()




''' 模型分对比 '''
score_v5 = all_pred[all_pred.sample_set=='test'].reset_index() 
score_v5 = score_v5[['loan_id', 'y_pred', 'label']]
print(score_v5.shape)

score_v4_r = pd.read_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\Linkaja\\modelv4_score.csv', dtype = {'loan_id': str})
score_v4 = score_v4_r[['loan_id', 'v4_score']].merge(score_v5[['loan_id', 'label']], how = 'right', on = 'loan_id')
score_v4 = score_v4[np.isfinite(score_v4['v4_score'])]
score_v4 = score_v4.rename(columns = {'v4_score': 'y_pred'})
score_v4.set_index(["loan_id"], inplace=True)
print(score_v4.shape)

#score_lkj_r = load_data_from_pickle('D:\\Model Development\\201912 IDN new v5\\01 Data\\Linkaja', 'oot_with_prob_var26_1206.pkl')
#score_lkj = score_lkj_r[['loan_id', 'y_pred']].merge(score_v5[['loan_id', 'label']], how = 'right', on = 'loan_id')
#score_lkj.y_pred = score_lkj.y_pred.astype(float)
#score_lkj = score_lkj[np.isfinite(score_lkj['y_pred'])]
#print(score_lkj.shape)
#score_lkj.set_index(["loan_id"], inplace=True)

FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

score_v4_lc = pt.show_result_new(score_v4, 'y_pred','label', n_bins = 10, feature_label='v4')
score_v5_lc = pt.show_result_new(score_v5, 'y_pred','label', n_bins = 10, feature_label='v5')
#score_lkj_lc = pt.show_result_new(score_lkj, 'y_pred','label', n_bins = 10, feature_label='linkaja')

path = os.path.join(FIG_PATH,"lift_chart_bymodel_test.png")
plt.savefig(path, format='png', dpi=100)
plt.close()


''' LIFT CHART 按展期 '''
r_extend_f = pd.read_csv(path_rawdata + 'r_extend_f.csv', dtype={'loan_id': str})
score_v5 = all_pred[(all_pred.sample_set=='oot') | (all_pred.sample_set=='test')].reset_index() 
#score_v5 = all_pred[all_pred.sample_set=='oot'].reset_index() 
score_v5 = score_v5[['loan_id', 'y_pred', 'label']]
score_v5 = score_v5.merge(r_extend_f, how = 'left', on = 'loan_id')
score_v5['extend_mark'] = score_v5.extend_times.apply(lambda x: '0' if x==0 else 
                                                   ('1,2,3' if x==1 else
                                                    ('1,2,3' if x==2 else 
                                                     ('1,2,3' if x==3 else '>3'))))
# score_v5['extend_mark'] = score_v5.extend_times.apply(lambda x: '0' if x==0 else 
#                                                    ('1' if x==1 else
#                                                     ('2' if x==2 else 
#                                                      ('3' if x==3 else '>3'))))
print(score_v5.shape)

score_v5_extend0 = score_v5[score_v5.extend_mark=='0']
score_v5_extend1 = score_v5[score_v5.extend_mark=='1,2,3']

FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

score_v5_extend0_lc = pt.show_result_new(score_v5_extend0, 'y_pred','label', n_bins = 10, feature_label='0')
score_v5_extend1_lc = pt.show_result_new(score_v5_extend1, 'y_pred','label', n_bins = 10, feature_label='1,2,3')


path = os.path.join(FIG_PATH,"lift_chart_byextend.png")
plt.savefig(path, format='png', dpi=100)
plt.close()

''' LIFT CHART 按金额 '''
score_v5 = all_pred[(all_pred.sample_set=='oot') | (all_pred.sample_set=='test')].reset_index() 
#score_v5 = all_pred[all_pred.sample_set=='test'].reset_index() 
score_v5 = score_v5[['loan_id', 'y_pred', 'label']]
score_v5 = score_v5.merge(r_extend_f, how = 'left', on = 'loan_id')
score_v5['amount_mark'] = score_v5.approved_principal.apply(lambda x: '1000000' if x==1000000 else '<1000000')

print(score_v5.shape)

score_v5_extend0 = score_v5[score_v5.amount_mark=='1000000']
score_v5_extend1 = score_v5[score_v5.amount_mark=='<1000000']

FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

score_v5_extend0_lc = pt.show_result_new(score_v5_extend0, 'y_pred','label', n_bins = 10, feature_label='10000000')
score_v5_extend1_lc = pt.show_result_new(score_v5_extend1, 'y_pred','label', n_bins = 10, feature_label='<1000000')
#score_lkj_lc = pt.show_result_new(score_lkj, 'y_pred','label', n_bins = 10, feature_label='linkaja')

path = os.path.join(FIG_PATH,"lift_chart_byamt.png")
plt.savefig(path, format='png', dpi=100)
plt.close()






tmp = all_pred[['loan_id', 'applied_at', 'sample_set', 'y_pred']]
tmp.to_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800 python3.6.8\\all_pred.xlsx')

data_scored_all.to_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800 python3.6.8\\data_scored_all.xlsx')


all_pred_original = load_data_from_pickle('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800', 'grid_64_191213_120304_all_pred.pkl')
data_scored_all_original = load_data_from_pickle('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800', 'grid_64_191213_120304_data_scored_all.pkl')

tmp = all_pred_original[['loan_id', 'applied_at', 'sample_set', 'y_pred']]
tmp.to_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800 python3.6.8\\all_pred_original.xlsx')

data_scored_all_original.to_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800 python3.6.8\\data_scored_all_original.xlsx')


