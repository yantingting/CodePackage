# 更新.pyc文件
import compileall
compileall.compile_dir(r'D:/_Tools/newgenie-master 20200217')

import pandas as pd
import numpy as np
import os
import pickle
import datetime
import sys
sys.path.append('D:/_Tools/newgenie-master 20200217')
sys.setrecursionlimit(100000)
import utils3.model_training as tr
import utils3.filing as fl

def load_data_from_pickle(file_path, file_name):
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'rb') as infile:
        result = pickle.load(infile)
    return result

import psycopg2
def get_df_from_pg(SQL):
    usename = "postgres"
    password = "Mintq2019"
    db = "risk_dm"
    host = "192.168.2.19"
    port = "5432"
    try:
        conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)
        print("Opened database successfully")
    except Exception as e:
        print(e)
    cur = conn.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    df = pd.DataFrame(rows,columns=[i.name for i in cur.description])
    df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
    return df

from math import log
def score_to_p(score, PDO=20.0, Base=600, Ratio=1.0):
        """
        分数转换逾期概率

        Args:
        score (float): 模型分数
        PDO (float): points double odds. default = 75
        Base (int): base points. default = 660
        Ratio (float): bad:good ratio. default = 1.0/15.0

        Returns:
        转化后的逾期概率
        """
        B = 1.0*PDO/log(2)
        A = Base+B*log(Ratio)
        alpha = (A - score) / B
        p = np.exp(alpha) / (1+np.exp(alpha))
        return p
    
    
    
#os.getcwd()

in_dict = {
    'RESULT_PATH':'D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618',\
    'index_id':'loan_id',\
    'target':'flag7',\
    'date_col':'effective_date',\
    'extra_col':['customer_id', 'apply_time', 'effective_date', 'grouping',
       'newusermodelscorev5', 'scorev5', 'extend_times', 'approved_principal',
       'approved_period','product_id'],\
    'rmv_features':[],\
        # var_adv_tel + var_adv_multi + var_adv_credit
        # 'bankcode_BNI','bankcode_BRI','bankcode_DANAMON','bankcode_MAYBANK',
        #             'bankcode_PANIN','bankcode_PERMATA','brand_ASUS','brand_LENOVO',
        #             'brand_OPPO','brand_REALME','','','','','','','','','','','','','','',
    'use_features':[],\
    'known_missing': {-1:-9999, '-1':-9999},\
    'sample_cutoff':{
        'by_time':False,\
        'test_cutoff': 0.3,\
        'oot_cutoff': 0.20
    },\
    'ranking_methods':[
            'random_forest'
            #,'lasso'
            #,'xgboost'
    ],\
    'grid_param': {'param_test1' : {'max_depth': range(2,3, 1), 'min_child_weight': range(25, 30, 1)},\
        'param_test2': {'gamma': [i / 10.0 for i in range(0, 5)]} ,\
        'param_test3': {'subsample': [i / 10.0 for i in range(2, 4)], 'colsample_bytree': [i / 10.0 for i in range(6, 9)]},\
        # 'param_test4': {'reg_alpha': [0, 0.001, 0.005, 0.01]},\
        'param_test4': {'reg_alpha': [0.08,0.1]},\
        'param_test5': {'n_estimators': range(20, 40, 5)},\
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

data_all = pd.read_csv('D:/Model Development/202001 IDN new v6/01 Data/raw data w600-618/r_all.csv', dtype = {'loan_id': str})

df = data_all[(pd.to_datetime(data_all.effective_date) >= pd.to_datetime('2020-01-23')) |
              ((pd.to_datetime(data_all.effective_date) < pd.to_datetime('2020-01-23')) & (data_all.scorev5>618))]

# df = data_all.copy()

# only cashcash and advanceAI
# r_extend = pd.read_csv('D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212/r_extend.csv', dtype={'loan_id': str})
# data_all = data_all.merge(r_extend[r_extend.product_id==2].loan_id,on='loan_id',how='inner')
# print(data_all.shape)

# df = data_all[['loan_id', 'flag7', 'flag7_raw', 'effective_date'] +
#                      var_adv_tel + var_adv_fraud + var_adv_multi + var_adv_credit]


print(df.shape)



# 2. 数据清洗、生成字典、EDA
data_cleaned, var_dict, eda = tr.data_preprocess(df=df, in_dict = in_dict)

# 3. 划分sample_set
df = tr.sample_split(df = data_cleaned, in_dict = in_dict)

# 4. 样本信息
desc_tbl = tr.desc_sample(df=df, date_col='effective_date', target='label')

# 5. 画图-univarcahrt
df2 = df.drop(in_dict['extra_col'], axis=1)  # df2应包含且仅包含：全部x y sample_set
plot_list, wrong_list = tr.draw_univarchart(df2, RESULT_PATH=in_dict['RESULT_PATH'])
df2 = df2.drop(['bins','bin_no'],axis=1)    # 画图后dataframe中会多两列'bins','bin_no'

# 6. 加random列
import numpy as np
np.random.seed(3)
df2['random'] = np.random.rand(df2.shape[0],1)
# df2['random'] = 1
var_dict = var_dict.append([{'数据源':'random','指标英文':'random','指标中文':'random','数据类型':'float64','指标类型':'原始字段'}],ignore_index=True)
# 若有强制删除的变量
var = df2.drop([], axis=1)  # df2应包含且仅包含：可用x y sample_set
# var_adv_tel + var_adv_fraud + var_adv_multi + var_adv_credit


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
        'RESULT_PATH':'D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618',\
        'slope':True,\
        'psi':0.1,\
        'iv':0.01,\
        'xgb_imp':0.0001,\
        'corr':None,\
        'random':False
    },\
'2': {
        'RESULT_PATH':'D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618',\
        'slope':True,\
        'psi':0.1,\
        'iv':0.01,\
        'xgb_imp':0.001,\
        'corr':None,\
        'random':False
    }
    ,\
'3': {
        'RESULT_PATH':'D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618',\
        'slope':True,\
        'psi':0.1,\
        'iv':0.015,\
        'xgb_imp':0.0001,\
        'corr':None,\
        'random':False
    },\
'4': {
        'RESULT_PATH':'D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618',\
        'slope':True,\
        'psi':0.1,\
        'iv':0.001,\
        'xgb_imp':0.0001,\
        'corr':None,\
        'random':False
    }
}

# 按fs_in_dict中的选项进行特征选择
names = locals()
ks_list=[]
for i in range(1,len(fs_in_dict)+1):
    print('-------- 第',str(i),'轮进行中 --------')
    names['ks_' + str(i)],names['f_rmv_' + str(i)],names['fs_dict_' + str(i)] = tr.fs_pipeline(df=var, ranking_result = ranking_result, fs_dict=fs_in_dict[str(i)], rs_dict=in_dict)
    ks_list.append(names['ks_' + str(i)])
    n = ks_list.index(max(ks_list))+1


# test KS最大的一组结果
max_ks = names['ks_' + str(n)]
max_fs_dict = names['fs_dict_' + str(n)]
max_f_rmv = names['f_rmv_' + str(n)]
# max_f_rmv = list(set(max_f_rmv)-set(in_dict['use_features']))  #保留必用的特征

# 10. 根据特征选择结果进行Gridsearch XGB并自动保存结果
# var_use = pd.read_excel('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618\\model_perf_117_200220_112821 ADVMULTI.xlsx',
#                         sheet_name = '03_model_importance')
# var_use = var_use.varName.tolist()
# var2 = var[var_use + list(['label','sample_set'])]
# var2 = var2.drop(['topup_30_60_min'],1)
# 'B_360d',
# 'GD_M_105',
# 'topup_360_720_min',
# 'Lite',
# 'high_freq_app',
# 'rate_high_freq_app'
var2 = var.drop(max_f_rmv, axis=1)  # var2应包含：fs筛选后x y sample_set 
train = var2[var2.sample_set =='train']
test = var2[var2.sample_set =='test']
oot = var2[var2.sample_set =='oot']
df_auc_acc_ks = tr.model_performance(train=train, test=test, oot=oot, model='grid', in_dict = in_dict, save=True)


'''************************** LIFT CHART **************************'''
import utils3.plot_tools as pt
import matplotlib.pyplot as plt

data_scored_all = load_data_from_pickle('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618',
                                        'model_perf_90_200226_212520_data_scored_all.pkl')


data_scored_618 = load_data_from_pickle('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618',
                                        'model_perf_100_200226_221600_data_scored_all.pkl')


'''************************** 模型分对比 **************************'''
score_v6 = data_scored_all[data_scored_all.sample_set=='oot'].reset_index() 
score_v6 = score_v6[['order_no', 'y_pred','Y']]
score_v6.rename(columns={'order_no':'loan_id'}, inplace=True)
print(score_v6.shape)
print(score_v6.dtypes)

score_618 = data_scored_618[data_scored_618.sample_set=='oot'].reset_index() 
score_618 = score_618[['order_no', 'y_pred','Y']]
score_618.rename(columns={'order_no':'loan_id'}, inplace=True)
print(score_618.shape)
print(score_618.dtypes)


score_v6 = score_v6.merge(score_618[['loan_id']], on='loan_id')
print(score_v6.shape)


FIG_PATH = os.path.join(in_dict['RESULT_PATH'], 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

score_v5_lc = pt.show_result_new(score_618, 'y_pred','Y', n_bins = 10, feature_label='618')
score_v6_lc = pt.show_result_new(score_v6, 'y_pred','Y', n_bins = 10, feature_label='all')

path = os.path.join(FIG_PATH,"lift_chart_bymodel_oot.png")
plt.savefig(path, format='png', dpi=100)
plt.close()



'''************************** 渠道对比 **************************'''
score_v6 = data_scored_all[data_scored_all.sample_set=='oot'].reset_index() 
score_v6 = score_v6[['order_no', 'y_pred','Y']]
score_v6.rename(columns={'order_no':'loan_id'}, inplace=True)
print(score_v6.shape)
print(score_v6.dtypes)

score_618 = data_scored_618[data_scored_618.sample_set=='oot'].reset_index() 
score_618 = score_618[['order_no', 'y_pred','Y']]
score_618.rename(columns={'order_no':'loan_id'}, inplace=True)
print(score_618.shape)
print(score_618.dtypes)

score_v6 = score_v6.merge(score_618[['loan_id']], on='loan_id')
print(score_v6.shape)


g1 = score_618.merge(data_all[data_all.product_id==1].loan_id, on = 'loan_id', how = 'inner')
g2 = score_618.merge(data_all[data_all.product_id==2].loan_id, on = 'loan_id', how = 'inner')
g3 = score_v6.merge(data_all[data_all.product_id==1].loan_id, on = 'loan_id', how = 'inner')
g4 = score_v6.merge(data_all[data_all.product_id==2].loan_id, on = 'loan_id', how = 'inner')

FIG_PATH = os.path.join(in_dict['RESULT_PATH'], 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

score_1_lc = pt.show_result_new(g1, 'y_pred','Y', n_bins = 10, feature_label='gt618 uku')
score_2_lc = pt.show_result_new(g2, 'y_pred','Y', n_bins = 10, feature_label='gt618 cashcash')
score_3_lc = pt.show_result_new(g3, 'y_pred','Y', n_bins = 10, feature_label='all uku')
score_4_lc = pt.show_result_new(g4, 'y_pred','Y', n_bins = 10, feature_label='all cashcash')

path = os.path.join(FIG_PATH,"lift_chart_byproduct_oot.png")
plt.savefig(path, format='png', dpi=100)
plt.close()


data_scored_all_1 = data_scored_all[data_scored_all.sample_set=='oot']
data_scored_all_1.rename(columns={'order_no':'loan_id'}, inplace=True)
data_scored_all_1= data_scored_all_1.merge(data_all[data_all.product_id==1].loan_id, on = 'loan_id', how = 'inner')
data_scored_all_2 = data_scored_all[data_scored_all.sample_set=='oot']
data_scored_all_2.rename(columns={'order_no':'loan_id'}, inplace=True)
data_scored_all_2= data_scored_all_2.merge(data_all[data_all.product_id==2].loan_id, on = 'loan_id', how = 'inner')

data_scored_all_1.to_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618\\data_scored_all_1.csv')
data_scored_all_2.to_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618\\data_scored_all_2.csv')

data_scored_618_1 = data_scored_618[data_scored_618.sample_set=='oot']
data_scored_618_1.rename(columns={'order_no':'loan_id'}, inplace=True)
data_scored_618_1= data_scored_618_1.merge(data_all[data_all.product_id==1].loan_id, on = 'loan_id', how = 'inner')

data_scored_618_2 = data_scored_618[data_scored_618.sample_set=='oot']
data_scored_618_2.rename(columns={'order_no':'loan_id'}, inplace=True)
data_scored_618_2= data_scored_618_2.merge(data_all[data_all.product_id==2].loan_id, on = 'loan_id', how = 'inner')

data_scored_618_1.to_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618\\data_scored_618_1.csv')
data_scored_618_2.to_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618\\data_scored_618_2.csv')


'''************************** 模型对比 OOT GT618 **************************'''
score_v6 = data_scored_all[data_scored_all.sample_set=='oot'].reset_index() 
score_v6 = score_v6[['order_no', 'y_pred','Y']]
score_v6.rename(columns={'order_no':'loan_id'}, inplace=True)
print(score_v6.shape)
print(score_v6.dtypes)

score_618 = data_scored_618[data_scored_618.sample_set=='oot'].reset_index() 
score_618 = score_618[['order_no', 'y_pred','Y']]
score_618.rename(columns={'order_no':'loan_id'}, inplace=True)
print(score_618.shape)
print(score_618.dtypes)

score_v6 = score_v6.merge(score_618[['loan_id']], on='loan_id')
print(score_v6.shape)


g1 = score_618.merge(data_all[data_all.scorev5>618].loan_id, on = 'loan_id', how = 'inner')
g2 = score_v6.merge(data_all[data_all.scorev5>618].loan_id, on = 'loan_id', how = 'inner')

FIG_PATH = os.path.join(in_dict['RESULT_PATH'], 'figure', 'LiftChart')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

score_1_lc = pt.show_result_new(g1, 'y_pred','Y', n_bins = 10, feature_label='gt618')
score_2_lc = pt.show_result_new(g2, 'y_pred','Y', n_bins = 10, feature_label='all')

path = os.path.join(FIG_PATH,"lift_chart_bymodel_ootgt618.png")
plt.savefig(path, format='png', dpi=100)
plt.close()

data_scored_all_gt618 = data_scored_all[data_scored_all.sample_set=='oot']
data_scored_all_gt618.rename(columns={'order_no':'loan_id'}, inplace=True)
data_scored_all_gt618= data_scored_all_gt618.merge(data_all[data_all.scorev5>618].loan_id, on = 'loan_id', how = 'inner')
data_scored_618_gt618 = data_scored_618[data_scored_618.sample_set=='oot']
data_scored_618_gt618.rename(columns={'order_no':'loan_id'}, inplace=True)
data_scored_618_gt618= data_scored_618_gt618.merge(data_all[data_all.scorev5>618].loan_id, on = 'loan_id', how = 'inner')

data_scored_all_gt618.to_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618\\data_scored_all_gt618.csv')
data_scored_618_gt618.to_csv('D:\\Model Development\\202001 IDN new v6\\03 Result\\py_output w600-618\\data_scored_618_gt618.csv')
