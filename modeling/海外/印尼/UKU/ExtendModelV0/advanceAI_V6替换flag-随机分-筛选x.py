import pandas as pd
import os
import pickle
import datetime
import sys
sys.path.append('C:/Users/Mint/Documents/GitHub/newgenie')
sys.setrecursionlimit(100000)
import utils3.model_training as tr
import utils3.filing as fl
# 更新.pyc文件
import compileall
compileall.compile_dir(r'C:/Users/Mint/Documents/GitHub/newgenie')

os.getcwd()


# 1. 读取数据
df=pd.read_csv('V6var_extflag.csv')
df=df.drop(['Unnamed: 0'],1)

in_dict = {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\03_建模',\
    'index_id':'loan_id',\
    'target':'ex_flag',\
    'date_col':'effective_date',\
    'extra_col':['effective_date','sample_set'],\
    'rmv_features':[],\
    'use_features':[],\
    'known_missing': {-999:-9999, -1111:-9999},\
    'sample_cutoff':{
        'by_time':False,\
        'test_cutoff': 0.3,\
        'oot_cutoff': None
    },\
    'ranking_methods':[
            'random_forest'
            #,'lasso'
            #,'xgboost'
    ],\
    'grid_param': {'param_test1' : {'max_depth': range(2,4, 1), 'min_child_weight': range(3, 15, 1)},\
        'param_test2': {'gamma': [i / 10.0 for i in range(0, 5)]} ,\
        'param_test3': {'subsample': [i / 10.0 for i in range(6, 9)], 'colsample_bytree': [i / 10.0 for i in range(6, 9)]},\
        'param_test4': {'reg_alpha': [0, 0.001, 0.005, 0.01]},\
        'param_test5': {'n_estimators': range(50, 200, 50)},\
        'param_test6' : {'learning_rate': [i / 10.0 for i in range(1, 3)]}
    },\
    'random_param': {
        'eta': [0.05,0.1],
        'max_depth': [2,3],
        'gamma': [0, 0.1],
        'min_child_weight':[5,10],
        'subsample': [0.8],
        'colsample_bytree': [0.8],
        'n_estimators': [100,200]
    }
}



# 2. 数据清洗、生成字典、EDA
data_cleaned, var_dict, eda = tr.data_preprocess(df=df, in_dict = in_dict)
#l = list(eda[eda.exclusion_reason.notnull()]['指标英文'].values)

# 3. 划分sample_set
oot_df = data_cleaned[data_cleaned.sample_set=='oot']
traintest_df = data_cleaned[data_cleaned.sample_set=='traintest']

test = traintest_df.sample(frac=0.3,axis=0,random_state=1)
train = traintest_df.drop(test.index)
train['sample_set']='train'
test['sample_set']='test'
df = pd.concat([train, test, oot_df],0)
df.sample_set.value_counts()

# 4. 样本信息
desc_tbl = tr.desc_sample(df=df, date_col='effective_date', target='label')

in_dict = {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\03_建模',\
    'index_id':'loan_id',\
    'target':'ex_flag',\
    'date_col':'effective_date',\
    'extra_col':['effective_date'],\
    'rmv_features':[],\
    'use_features':[],\
    'known_missing': {-999:-9999, -1111:-9999},\
    'sample_cutoff':{
        'by_time':False,\
        'test_cutoff': 0.3,\
        'oot_cutoff': None
    },\
    'ranking_methods':[
            'random_forest'
            #,'lasso'
            #,'xgboost'
    ],\
    'grid_param': {'param_test1' : {'max_depth': range(2,4, 1), 'min_child_weight': range(3, 15, 1)},\
        'param_test2': {'gamma': [i / 10.0 for i in range(0, 5)]} ,\
        'param_test3': {'subsample': [i / 10.0 for i in range(6, 9)], 'colsample_bytree': [i / 10.0 for i in range(6, 9)]},\
        'param_test4': {'reg_alpha': [0, 0.001, 0.005, 0.01]},\
        'param_test5': {'n_estimators': range(50, 200, 50)},\
        'param_test6' : {'learning_rate': [i / 10.0 for i in range(1, 3)]}
    },\
    'random_param': {
        'eta': [0.05,0.1],
        'max_depth': [2,3],
        'gamma': [0, 0.1],
        'min_child_weight':[5,10],
        'subsample': [0.8],
        'colsample_bytree': [0.8],
        'n_estimators': [100,200]
    }
}


# 先自动筛一遍trainVStest trainVSoot
df2 = df.drop(in_dict['extra_col'], axis=1)

tr = df2[df2.sample_set=='train']
te = df2[df2.sample_set=='test']
ot = df2[df2.sample_set=='oot']
tr = tr.drop(['sample_set'], axis=1)
te = te.drop(['sample_set'], axis=1)
ot = ot.drop(['sample_set'], axis=1)

import utils3.feature_selection as fs
fs_obj = fs.FeatureSelection()
f_in_model=set(list(df2.columns))-set(['label','sample_set'])

slope_dict1 = fs_obj.cal_slope(tr, te)
slope_select1 = [i[0] for i in slope_dict1.items() if i[1] >= 0]

slope_dict2 = fs_obj.cal_slope(tr, ot)
slope_select2 = [i[0] for i in slope_dict2.items() if i[1] >= 0]

slope_select = list(set(slope_select1).intersection(set(slope_select2)))
slope_remove = list(set(f_in_model)- set(slope_select))

df=df.drop(slope_remove,1)
df.to_csv('V6var_extflag_aftslope.csv', index=False)
df=pd.read_csv('V6var_extflag_aftslope.csv')

# 5. 画图-univarcahrt
df2 = df.drop(in_dict['extra_col'], axis=1) 
plot_list, wrong_list = tr.draw_univarchart(df2, RESULT_PATH=in_dict['RESULT_PATH'])
df2 = df2.drop(['bins','bin_no'],axis=1)

#看图筛x
#筛选过后的list
var_list = pd.read_excel('var_list.xlsx')
var_list = list(var_list['varname'])

df2=df2[var_list]
var_list.append('effective_date')
df=df[var_list]
df.to_csv('V6var_extflag_aftslope_aftX.csv',index=False)

# 6. 加random列
import numpy as np
np.random.seed(3)
df2['random'] = np.random.rand(df2.shape[0],1)
#df2['random'] = 1
var_dict = var_dict.append([{'数据源':'random','指标英文':'random','指标中文':'random','数据类型':'float64','指标类型':'原始字段'}],ignore_index=True)
# 若有强制删除的变量
var = df2.drop(in_dict['rmv_features'], axis=1)  # df2应包含且仅包含：可用x y sample_set

# 7. 计算PSI IV XGB-IMPORTANCE等
ranking_result = tr.var_ranking(df = var, var_dict = var_dict,num_max_bins=5,n_clusters=15,methods=in_dict['ranking_methods'])

# 筛选后的univarchart
plot_list, wrong_list = tr.draw_univarchart(var, RESULT_PATH=in_dict['RESULT_PATH'])
var = var.drop(['bins','bin_no'],axis=1)

# 8. 保存以上数据信息
eda=eda[eda.数据源.isin(var_list)]
var_dict=var_dict[var_dict.数据源.isin(var_list)]

eda.数据类型 = eda.数据类型.astype(str)
var_dict.数据类型 = var_dict.数据类型.astype(str)
data_dict = {}
data_dict['01_sample_desc'] = desc_tbl
data_dict['02_EDA'] = eda
data_dict['03_ranking_result'] = ranking_result
data_dict['04_var_dict'] = var_dict
FILE_NAME = "data_info_%s"%datetime.datetime.now().strftime('%y%m%d_%H%M%S')
pickle.dump(df, open(os.path.join(in_dict['RESULT_PATH'], FILE_NAME+"_df.pkl"), "wb"))
print('data saved!')
fl.ModelSummary2Excel(result_path = in_dict['RESULT_PATH'], fig_path= in_dict['RESULT_PATH'], file_name = FILE_NAME+".xlsx", data_dic = data_dict).run()
print('files saved!')


'''------------------------------------------------'''

ranking_result[ranking_result.数据源=='random']['importance_rank']

# 9. 特征选择
fs_in_dict = {
'1': {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\03_建模',\
        'slope':True,\
        'psi':None,\
        'iv':None,\
        'xgb_imp':None,\
        'corr':None,\
        'random':True  
    },\
'2': {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\03_建模',\
        'slope':True,\
        'psi':0.2,\
        'iv':None,\
        'xgb_imp':0.001,\
        'corr':None,\
        'random':False
    },\
'3': {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\03_建模',\
        'slope':True,\
        'psi':None,\
        'iv':0.01,\
        'xgb_imp':0.001,\
        'corr':None,\
        'random':False
    },\
'4': {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\03_建模',\
        'slope':True,\
        'psi':None,\
        'iv':0.02,\
        'xgb_imp':0.001,\
        'corr':None,\
        'random':False  
    },\
'5': {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\03_建模',\
        'slope':False,\
        'psi':0.1,\
        'iv':None,\
        'xgb_imp':0.001,\
        'corr':None,\
        'random':False
    },\
'6': {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\03_建模',\
        'slope':False,\
        'psi':None,\
        'iv':None,\
        'xgb_imp':0.001,\
        'corr':None,\
        'random':False
    }
}

# 按fs_in_dict中的选项进行特征选择
names = locals()
ks_list=[]
for i in range(1,len(fs_in_dict)+1):
    print('---- 第',str(i),'轮进行中 ----')
    names['ks_' + str(i)],names['f_rmv_' + str(i)],names['fs_dict_' + str(i)] = tr.fs_pipeline(df=var, ranking_result = ranking_result, fs_dict=fs_in_dict[str(i)], rs_dict=in_dict)
    ks_list.append(names['ks_' + str(i)])
    n = ks_list.index(max(ks_list))+1

# test KS最大的一组结果
#max_ks = names['ks_' + str(n)]
#max_fs_dict = names['fs_dict_' + str(n)]
max_f_rmv = names['f_rmv_' + str(n)]   #1
#max_f_rmv = list(set(max_f_rmv)-set(in_dict['use_features']))  #保留必用的特征

# 10. 根据特征选择结果进行Gridsearch XGB并自动保存结果
var2 = var.drop(max_f_rmv, axis=1)  # var2应包含：fs筛选后x y sample_set 
train = var2[var2.sample_set =='train']
test = var2[var2.sample_set =='test']
oot = var2[var2.sample_set =='oot']
df_auc_acc_ks = tr.model_performance(train=train, test=test, oot=oot, model='grid', in_dict = in_dict, save=True)

# feature_used的univarchart
plot_list, wrong_list = tr.draw_univarchart(var2, RESULT_PATH=in_dict['RESULT_PATH'])
var2 = var2.drop(['bins','bin_no'],axis=1)






























