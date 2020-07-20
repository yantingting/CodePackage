import os
import time
import pandas as pd
import numpy as np
import psycopg2
import pickle as pickle

def load_data_from_pickle(file_path, file_name):
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'rb') as infile:
        result = pickle.load(infile)
    return result


usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)


import xgboost as xgb
import pandas as pd
import numpy as np
from pandas import read_csv
from pandas import read_excel
from xgboost import DMatrix

path_applydata = 'D:/Model Development/201912 IDN new v5/01 Data/apply data/'


'''###################### 取数 ######################'''

sql_model = '''
select a.loanid as loan_id, a.customerid, a.createtime, par.newUserModelscoreV5, 
newUserModelScoreV5InputModelParams, newUserModelScoreV5ModelParams
from rt_risk_mongo_gocash_installmentriskcontrolresult a
left join risk_mongo_gocash_installmentriskcontrolparams par on a.riskno = par.riskno
where a.pipelineid = '468' and anti_fraud_factor<>'R' and anti_fraud_result<>'R' and a.createtime::timestamp between '2019-12-27 14:00:00' and '2019-12-30 13:00:00'
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_model)
r_model = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_model.shape)
print(r_model.loan_id.nunique())
print(r_model.dtypes)

r_model.to_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\apply data\\r_model_1230.csv', index = False)


'''###################### 分布 ######################'''
r_use = r_model.copy()
r_use.newusermodelscorev5modelparams = r_use.newusermodelscorev5modelparams.str.replace('[','') 
r_use.newusermodelscorev5modelparams = r_use.newusermodelscorev5modelparams.str.replace(']','') 

data_features = r_use.newusermodelscorev5modelparams.str.split(',', expand=True)
data_features_all = pd.concat([data_features, r_use.newusermodelscorev5], axis=1, sort=False)

print(data_features_all.dtypes)
var_to_float = list(range(0, 64, 1))
for x in var_to_float:
    data_features_all[x] = data_features_all[x].astype(float)

data_features_all.newusermodelscorev5 = data_features_all.newusermodelscorev5.astype(float)

print(data_features_all.dtypes)

data_features_all['sample_set'] = 'd1230'

model_var = list(['gender'
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
,'com.android.smspush'
])

data_features_all.columns = list(model_var + ['newusermodelscorev5', 'sample_set'])


# r_1201_score = pd.read_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\score_apply_1129_1201.xlsx')
r_1201_score = data_scored.copy()
r_1201_score = r_1201_score[['loan_id', 'score']]
r_1201_score.loan_id = r_1201_score.loan_id.astype(str)
r_1201_score.rename(columns={'score': 'newusermodelscorev5'}, inplace = True)
print(r_1201_score.shape)
print(r_1201_score.dtypes)

# data_apply = pd.read_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\apply data\\apply 1129_1201\\data_apply.csv')
data_apply.loan_id = data_apply.loan_id.astype(str)
r_1201_var = data_apply[list(['loan_id']) + model_var]
 
print(r_1201_var.shape)
print(r_1201_var.dtypes)

for x in model_var:
    r_1201_var[x] = r_1201_var[x].astype(float)
    
r_1201 = r_1201_var.merge(r_1201_score, on = 'loan_id', how = 'left')
print(r_1201.shape)
print(r_1201.dtypes)

r_1201['sample_set'] = 'd1201'

r_all = pd.concat([data_features_all, r_1201.drop('loan_id', axis = 1)], ignore_index = True)
print(r_all.shape)


#################################### 变量分布及各项指标 ###################################
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
    # 'xgboost'
]

features_var_dict = list(model_var + ['newusermodelscorev5'])


var_dict = pd.DataFrame(columns= ['数据源', '指标英文', '数据类型'])
var_dict['数据源'] = features_var_dict
var_dict['指标英文'] = features_var_dict
var_dict['数据类型'] = 'float'

import random
r_all['label'] = [random.randint(0, 1) for p in range(0, len(r_all['sample_set']))]

r_all = r_all.fillna(-1)
r_all = r_all.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])

## Train 分箱
model_train_final = r_all.copy()
X_train_IV = model_train_final[features_var_dict]
y_train_IV = model_train_final['label'].astype(int)

X_cat_train, X_transformed_train, woe_iv_df_train, rebin_spec_train, ranking_result_train = fs_obj.overall_ranking(X_train_IV, y_train_IV,
                                                                                           var_dict, args_dict,
                                                                                           methods, num_max_bins=5)

rebin_spec = mu.convert_rebin_spec2XGB_rebin_spec(rebin_spec_train)
rebin_spec_bin_adjusted = {k: v for k, v in rebin_spec.items() if k in features_var_dict}


# X_cat_train = bw.convert_to_category(
#     model_train_final[features_var_dict],
#     var_dict,
#     rebin_spec)


# 变量分布
var_cols = features_var_dict.copy()

app_data = r_all.rename(columns={'sample_set': 'appmon'})

X_cat_with_y_appmon_all = pd.merge(X_cat_train,app_data[['label','appmon']] ,left_index=True,right_index=True)
var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,var_cols,'appmon','label')
var_dist_badRate_by_time_all.head()

var_dist_badRate_by_time_all.to_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\apply data\\var_dist_badRate_by_time_all.csv', index = False)





sql_time = '''
select distinct a.loanid as loan_id, a.createtime
from risk_gocash_mongo_riskcontrolresult  a
where a.businessid = 'uku' and existsloanwithcontacts <> 'reject' and installdangerapp <> 'reject' and callwithbadguys <> 'reject' and blacklist <> 'reject' and invalidage <> 'reject'
and deniedbyoccupation  <> 'reject' and installspecifyapp  <> 'reject' 
and a.createtime::date between '2019-12-01' and '2019-12-22'
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_time)
r_time = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_apply.shape)
print(r_apply.loan_id.nunique())
print(r_apply.dtypes)

'''###################### 打分 ######################'''
# LOAD FUNCTION
def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))
#.map(lambda x: int(x))
    
''' 读取数据 '''
# mydata = pd.read_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\raw data\\r_all3.csv')
# mydata.loan_id = mydata.loan_id.astype(str)

mydata = r_model.copy()

# mydata = mydata.fillna(-1)
# mydata = mydata.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])
# print(mydata.shape)
# print(mydata.dtypes)


# LOAD MODEL
mymodel = xgb.Booster() 
mymodel.load_model("D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800 python3.6.8\\grid_64_191219_145432.model")  

# LOAD VARIABLES 
# myvar = pd.read_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800\\grid_64_191213_120304.xlsx',\
#                       sheet_name='05_model_importance') 
# print(myvar.dtypes)

# PREDICT SCORES
# data_features = mydata[list(myvar['varName'])]







data_features = mydata.newusermodelscorev5inputmodelparams.str.split(',', expand=True)
print(data_features.dtypes)

var_to_float = list(range(0, 64, 1))
for x in var_to_float:
    data_features[x] = data_features[x].astype(float)
print(data_features.dtypes)

data_lean = DMatrix(data_features)
ypred = mymodel.predict(data_lean)


score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([mydata['loan_id'].values, 
                            mydata['createtime'].values, 
                            mydata['newusermodelscorev5'],
                            score, ypred]).T
data_scored.columns= ['loan_id', 'create_time', 'newusermodelscorev5', 
                      'score', 'prob']

print(data_scored.dtypes)
print(data_scored.shape)

writer = pd.ExcelWriter('D:\\Model Development\\201912 IDN new v5\\03 Result\\score_qc.xlsx')
data_scored.to_excel(writer, 'data_scored')
writer.save()


