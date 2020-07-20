import pandas as pd
import os
import pickle
import datetime
import sys
sys.path.append('/Users/yantingting/Documents/MintechJob/newgenie1/utils3/')
sys.setrecursionlimit(100000)
import utils3.model_training as tr
import utils3.filing as fl

# 更新.pyc文件
import compileall
compileall.compile_dir(r'C:/Users/Mint/Documents/GitHub/newgenie')

#os.getcwd()

in_dict = {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型',\
    'index_id':'loan_id',\
    'target':'flag7',\
    'date_col':'effective_date',\
    'extra_col':['effective_date','customer_id','apply_time', 'due_date'],\
    'rmv_features':[],\
    'use_features':[],\
    'known_missing': {-1:-9999, '-1':-9999},\
    'sample_cutoff':{
        'by_time':False,\
        'test_cutoff': 0.3,\
        'oot_cutoff': 0.2
    },\
    'ranking_methods':[
            'random_forest'
            #,'lasso'
            #,'xgboost'
    ],\
    'grid_param': {'param_test1' : {'max_depth': range(2,5, 1), 'min_child_weight': range(5, 10, 1)},\
        'param_test2': {'gamma': [i / 10.0 for i in range(0, 5)]} ,\
        'param_test3': {'subsample': [i / 10.0 for i in range(6, 9)], 'colsample_bytree': [i / 10.0 for i in range(6, 9)]},\
        'param_test4': {'reg_alpha': [0, 0.001, 0.005, 0.01]},\
        'param_test5': {'n_estimators': range(0, 200, 50)},\
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

# 1. 读取数据
data_all = pd.read_excel(os.path.join(INPUT_PATH, 'r_all.xlsx'), dtype={'order_no':object})
df=data_all.copy()

# 2. 数据清洗、生成字典、EDA
var_dict = pd.read_excel('')    # 如果有字典在这一步读取后输入下一步中，如果没有字典在下一步自动生成
data_cleaned, var_dict, eda = tr.data_preprocess(df=df, in_dict = in_dict, var_dict=None)

# 3. 划分sample_set
df = tr.sample_split(df = data_cleaned, in_dict = in_dict)

# 4. 样本信息
desc_tbl = tr.desc_sample(df=df, date_col='effective_date', target='label')

# 5. 画图-univarcahrt
df2 = df.drop(in_dict['extra_col'], axis=1)  # df2应包含且仅包含：全部x y sample_set
plot_list, wrong_list = tr.draw_univarchart(df2, RESULT_PATH=in_dict['RESULT_PATH'])
df2 = df2.drop(['bins','bin_no'],axis=1)    # 画图后dataframe中会多两列'bins','bin_no'

# 6. 加random列
#import numpy as np
#np.random.seed(3)
#df2['random'] = np.random.rand(df2.shape[0],1)
df2['random'] = 1
var_dict = var_dict.append([{'数据源':'random','指标英文':'random','指标中文':'random','数据类型':'float64','指标类型':'原始字段'}],ignore_index=True)
# 若有强制删除的变量
var = df2.drop(in_dict['rmv_features'], axis=1)  # df2应包含且仅包含：可用x y sample_set

# 7. 计算PSI IV XGB-IMPORTANCE等
ranking_result = tr.var_ranking(df = var, var_dict = var_dict,num_max_bins=5,n_clusters=15,methods=in_dict['ranking_methods'])

# 8. 保存以上数据信息
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

# 9. 特征选择
fs_in_dict = {
'1': {
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\07_MVP自动迭代',\
        'slope':True,\
        'psi':None,\
        'iv':0.01,\
        'xgb_imp':None,\
        'corr':None,\
        'random':True
    },\
'2': {
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\07_MVP自动迭代',\
        'slope':True,\
        'psi':0.2,\
        'iv':None,\
        'xgb_imp':None,\
        'corr':None,\
        'random':True
    },\
'3': {
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\07_MVP自动迭代',\
        'slope':True,\
        'psi':None,\
        'iv':None,\
        'xgb_imp':0.0001,\
        'corr':None,\
        'random':True
    },\
'4': {
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\07_MVP自动迭代',\
        'slope':True,\
        'psi':None,\
        'iv':None,\
        'xgb_imp':None,\
        'corr':None,\
        'random':True
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
max_f_rmv = names['f_rmv_' + str(n)]
#max_f_rmv = list(set(max_f_rmv)-set(in_dict['use_features']))  #保留必用的特征

# 10. 根据特征选择结果进行Gridsearch XGB并自动保存结果
var2 = var.drop(max_f_rmv, axis=1)  # var2应包含：fs筛选后x y sample_set 
train = var2[var2.sample_set =='train']
test = var2[var2.sample_set =='test']
oot = var2[var2.sample_set =='oot']
df_auc_acc_ks = tr.model_performance(train=train, test=test, oot=oot, model='grid', in_dict = in_dict, save=True)



# 附：两个模型LiftChart对比示例，待形成function
RESULT_PATH=os.getcwd()
with open('model_perf_49_200316_164258_data_scored_all.pkl', 'rb') as f:
    score_49 = pickle.load(f)

with open('model_perf_48_200319_150229_data_scored_all.pkl', 'rb') as f:
    score_48 = pickle.load(f)

score_49.rename(columns={'Y':'label'}, replace=True)
score_48.rename(columns={'Y':'label'}, replace=True)

train_pred_1 = score_49[score_49.sample_set=='train']
test_pred_1 = score_49[score_49.sample_set=='test']
oot_pred_1 = score_49[score_49.sample_set=='oot']
train_pred_2 = score_48[score_48.sample_set=='train']
test_pred_2 = score_48[score_48.sample_set=='test']
oot_pred_2 = score_48[score_48.sample_set=='oot']

import utils3.plot_tools as pt
import matplotlib.pyplot as plt

label_1='49'
label_2='48'

FIG_PATH = os.path.join(RESULT_PATH, 'figure')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

train_lc1 = pt.show_result_new(train_pred_1, 'y_pred','label', n_bins = 10, feature_label='train_'+label_1)
test_lc1 = pt.show_result_new(test_pred_1, 'y_pred','label', n_bins = 10, feature_label='test_'+label_1)
train_lc2 = pt.show_result_new(train_pred_2, 'y_pred','label', n_bins = 10, feature_label='train_'+label_2)
test_lc2 = pt.show_result_new(test_pred_2, 'y_pred','label', n_bins = 10, feature_label='test_'+label_2)
oot_lc1 = pt.show_result_new(oot_pred_1, 'y_pred','label', n_bins = 10, feature_label='oot_'+label_1)
oot_lc2 = pt.show_result_new(oot_pred_2, 'y_pred','label', n_bins = 10, feature_label='oot_'+label_2)

path = os.path.join(FIG_PATH,"lift_chart_%s_%s.png"%(label_1, label_2))
plt.savefig(path, format='png', dpi=100)
plt.close()