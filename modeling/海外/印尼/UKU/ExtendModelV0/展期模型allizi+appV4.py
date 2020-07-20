import pandas as pd
import os
import pickle
import datetime
import sys
sys.path.append('C:/Users/Mint/Documents/Python Scripts/02_模型/10_展期模型/')
sys.setrecursionlimit(100000)
import utils3.model_training as tr
import utils3.filing as fl

os.getcwd()

in_dict = {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型\\',\
    'index_id':'loan_id',\
    'target':'ext_flag',\
    'date_col':'effective_date',\
    'extra_col':['effective_date','customer_id'],\
    'rmv_features':[],\
    'use_features':[],\
    'known_missing': {-999:-9999, -1111:-9999},\
    'sample_cutoff':{
        'by_time':False,\
        'test_cutoff': 0.3,\
        'oot_cutoff': 0.15
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

# 1. 读取数据
data_all = pd.read_csv('r_all_appV4.csv', dtype={'loan_id':object})
data_all = data_all.drop(['extend_times',
       'approved_amount', 'approved_period', 'due_date', 'late_days',
       'late_status', 'loan_status','paid_off_time', 'dt_date'],1)
df=data_all[data_all.ext_flag!=-1]


# 2. 数据清洗、生成字典、EDA
#data_cleaned, var_dict, eda = tr.data_preprocess(df=df, in_dict = in_dict)

df.columns = df.columns.str.replace(':','_').str.replace('/','_').str.replace('\\','_')
df.rename(columns = {in_dict['index_id']:'order_no', in_dict['target']:'label'}, inplace=True)
df.order_no = df.order_no.astype(str)
df.label = df.label.astype(int)
df.set_index(["order_no"], inplace=True)

#生成字典
x_col = list(set(list(df.columns))-set(in_dict['extra_col'])-set(['label']))
x_all = df[x_col]
var_dict = pd.DataFrame(columns=['数据源', '指标英文', '指标中文', '数据类型', '指标类型'])
var_dict['数据源'] = x_all.dtypes.index
var_dict['指标英文'] = x_all.dtypes.index
var_dict['指标中文'] = x_all.dtypes.index
var_dict['数据类型'] = x_all.dtypes.values
var_dict['指标类型'] = '原始字段'

#数据缺失处理
extra_col = in_dict['extra_col']
x_w_y = df.drop(extra_col,1)
#x_all = df[x_col]
import utils3.misc_utils as mu
X_cleaned = mu.process_missing(X = x_all,var_dict=var_dict, known_missing=in_dict['known_missing'], downflagmap={}, verbose=True)
X_cleaned_sorted = X_cleaned.reindex(x_all.index.tolist())
data_cleaned = pd.concat([df[extra_col+['label']], X_cleaned_sorted], axis=1)
data_cleaned = data_cleaned[df.columns.tolist()]

# EDA
import utils3.summary_statistics as ss
eda = ss.eda(X = X_cleaned_sorted, var_dict=var_dict, useless_vars = [],exempt_vars = [], uniqvalue_cutoff=0.97)

# 缺失值赋值
data_cleaned = data_cleaned.fillna(-1)
data_cleaned = data_cleaned.replace([-9999,'-9999',-8888,'-8888',-8887,'-8887'],[-1,-1,-1,-1,-1,-1])

l = list(eda[eda.exclusion_reason.notnull()]['指标英文'].values)

# 3. 划分sample_set
df = tr.sample_split(df = data_cleaned, in_dict = in_dict)
#df.sample_set.value_counts()
#df.label.value_counts()
#df[df.label==1]['sample_set'].value_counts()

# 4. 样本信息
desc_tbl = tr.desc_sample(df=df, date_col='effective_date', target='label')

# 5. 画图-univarcahrt
df2 = df.drop(in_dict['extra_col'], axis=1)
df_0_1 = df2[df2.label!=2]
df_0_2 = df2[df2.label!=1]
df_0_2['label']=df_0_2['label'].replace(2,1)
df_1_2 = df2[df2.label!=0]
df_1_2['label']=df_1_2['label'].replace(2,0)

# flag = 0 1 2
plot_list, wrong_list = tr.draw_univarchart(df2, RESULT_PATH=in_dict['RESULT_PATH'])
df2 = df2.drop(['bins','bin_no'],axis=1)
# flag = 0 1
plot_list, wrong_list = tr.draw_univarchart(df_0_1, RESULT_PATH=in_dict['RESULT_PATH'])
df_0_1 = df_0_1.drop(['bins','bin_no'],axis=1) 
# flag = 0 2
plot_list, wrong_list = tr.draw_univarchart(df_0_2, RESULT_PATH=in_dict['RESULT_PATH'])
df_0_2 = df_0_2.drop(['bins','bin_no'],axis=1) 
# flag = 1 2
plot_list, wrong_list = tr.draw_univarchart(df_1_2, RESULT_PATH=in_dict['RESULT_PATH'])
df_1_2 = df_1_2.drop(['bins','bin_no'],axis=1) 


# 6. 加random列
#import numpy as np
#np.random.seed(3)
#df2['random'] = np.random.rand(df2.shape[0],1)
df2['random'] = 1
var_dict = var_dict.append([{'数据源':'random','指标英文':'random','指标中文':'random','数据类型':'float64','指标类型':'原始字段'}],ignore_index=True)
# 若有强制删除的变量
var = df2.drop(in_dict['rmv_features'], axis=1)

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


'''
import pickle
with open('data_info_200226_181541_df.pkl', 'rb') as f:
    df = pickle.load(f)

eda = pd.read_excel('data_info_200226_181541.xlsx',sheet_name='02_EDA')
l = list(eda[eda.exclusion_reason.notnull()]['指标英文'].values)

ranking_result = pd.read_excel('data_info_200226_181541.xlsx',sheet_name='03_ranking_result')


df2 = df.drop(in_dict['extra_col'], axis=1)
df_0_1 = df2[df2.label!=2]
df_0_2 = df2[df2.label!=1]
df_0_2['label']=df_0_2['label'].replace(2,1)
df_1_2 = df2[df2.label!=0]
df_1_2['label']=df_1_2['label'].replace(2,0)

df2['random'] = 1
var = df2.drop(in_dict['rmv_features'], axis=1)
'''

# 9. 特征选择
## slope
fs_dict={
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型',\
        'slope':True,\
        'psi':None,\
        'iv':None,\
        'xgb_imp':None,\
        'corr':None,\
        'random':False
    }
list1,x,x= tr.feature_rmv(var, ranking_result, fs_dict)
list2,x,x= tr.feature_rmv(df_0_1, ranking_result, fs_dict)
list3,x,x= tr.feature_rmv(df_0_2, ranking_result, fs_dict)
list4,x,x= tr.feature_rmv(df_1_2, ranking_result, fs_dict)

slope_list = list(set(list1).union(set(list2)).union(set(list3)).union(set(list4)))
## random
fs_dict2={
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型',\
        'slope':False,\
        'psi':None,\
        'iv':None,\
        'xgb_imp':None,\
        'corr':None,\
        'random':True
    }
list5,x,x= tr.feature_rmv(var, ranking_result, fs_dict2)
## psi iv
fs_dict3={
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型',\
        'slope':False,\
        'psi':0.2,\
        'iv':0.01,\
        'xgb_imp':None,\
        'corr':None,\
        'random':False
    }
list6,x,x= tr.feature_rmv(var, ranking_result, fs_dict3)


fs_dict4={
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型',\
        'slope':False,\
        'psi':0.2,\
        'iv':0.015,\
        'xgb_imp':None,\
        'corr':None,\
        'random':False
    }
list7,x,x= tr.feature_rmv(var, ranking_result, fs_dict4)


fs_dict5={
        'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\10_展期模型',\
        'slope':False,\
        'psi':0.2,\
        'iv':0.005,\
        'xgb_imp':None,\
        'corr':None,\
        'random':False
    }
list8,x,x= tr.feature_rmv(var, ranking_result, fs_dict5)

rmv_list = list(set(slope_list).union(set(list5)).union(set(l)).union(set(list6)))
rmv_list2 = list(set(slope_list).union(set(list5)).union(set(list6)))  #58
rmv_list3 = list(set(list1).union(set(list5)).union(set(list6)))  #62
rmv_list4 = list(set(list1).union(set(list2)).union(set(list5)).union(set(list7))) #49
rmv_list5 = list(set(list1).union(set(list2)).union(set(list5)).union(set(list6))) #61
rmv_list6 = list(set(list2).union(set(list3)).union(set(list5)).union(set(list6))) #70


var2 = var.drop(rmv_list6, axis=1)  #--71var
var2 = var.drop(rmv_list3, axis=1)  #--63var
var2 = var.drop(rmv_list7, axis=1)  #--84var

train = var2[var2.sample_set =='train']
test = var2[var2.sample_set =='test']
oot = var2[var2.sample_set =='oot']
train=train.drop(['sample_set'],1)
test=test.drop(['sample_set'],1)
oot=oot.drop(['sample_set'],1)

X_train = train.drop(['label'], axis=1)
y_train = train.label
X_test = test.drop(['label'], axis=1)
y_test= test.label
X_oot = oot.drop(['label'], axis=1)
y_oot= oot.label
features_used = X_train.columns.tolist()


import numpy as np
from sklearn.utils import class_weight
class_weights = list(class_weight.compute_class_weight('balanced',
                                             np.unique(train['label']),
                                             train['label']))
w_array = np.ones(y_train.shape[0], dtype = 'float')
for i, val in enumerate(y_train):
    w_array[i] = class_weights[val]
class_weight_dict = dict(enumerate(class_weights))


from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier

param_test1 = {
    'max_depth': range(2,4, 1),
        'min_child_weight': range(1, 15, 1)
}
gsearch1 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, gamma=0, subsample=0.7, 
                                                max_depth=4, colsample_bytree=1, objective='multi:softprob',num_class=3,
                                                nthread=6, scale_pos_weight=1, seed=27),
                        param_grid=param_test1, n_jobs=4, iid=False, cv=5, scoring='f1_weighted')

#gsearch1.fit(X_train, y_train)
gsearch1.fit(X_train, y_train, sample_weight=w_array)

best_max_depth = gsearch1.best_params_['max_depth']
best_min_child_weight = gsearch1.best_params_['min_child_weight']
print('best_max_depth:', best_max_depth)
print('best_min_child_weight:', best_min_child_weight)
y_pred=gsearch1.predict_proba(X_test)
yprob = np.argmax(y_pred, axis=1)   # return the index of the biggest pro
yprob = pd.Series(yprob)
yprob.value_counts()


#param_test2 = {'gamma': [i / 10.0 for i in range(0, 5)]}
param_test2 = {'gamma':[0,0.05,0.1]}
gsearch2 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, subsample=0.7, colsample_bytree=1,
                                                max_depth=best_max_depth,
                                                min_child_weight=best_min_child_weight, objective='multi:softprob',num_class=3,
                                                nthread=6, scale_pos_weight=1, seed=27),
                        param_grid=param_test2, n_jobs=4, iid=False, cv=5, scoring='f1_weighted')
gsearch2.fit(X_train, y_train, sample_weight=w_array)
best_gamma = gsearch2.best_params_['gamma']
print('best_gamma:',best_gamma)
y_pred=gsearch2.predict_proba(X_test)
yprob = np.argmax(y_pred, axis=1)   # return the index of the biggest pro
yprob = pd.Series(yprob)
yprob.value_counts()


param_test3 = {
    #'subsample': [i / 100.0 for i in range(55, 95,5)],
    'subsample': [i / 10.0 for i in range(6, 9)],
    #'colsample_bytree': [i / 100.0 for i in range(55, 95,5)]
    'colsample_bytree': [i / 10.0 for i in range(6, 9)]
}
gsearch3 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, max_depth=best_max_depth, gamma=best_gamma,
                                                min_child_weight=best_min_child_weight, objective='multi:softprob', nthread=6,num_class=3,
                                                scale_pos_weight=1, seed=27),
                        param_grid=param_test3, n_jobs=4, iid=False, cv=5, scoring='f1_weighted')
gsearch3.fit(X_train, y_train, sample_weight=w_array)
best_subsample = gsearch3.best_params_['subsample']
best_colsample_bytree = gsearch3.best_params_['colsample_bytree']
print('best_subsample:', best_subsample)
print('best_colsample_bytree:', best_colsample_bytree)
y_pred=gsearch3.predict_proba(X_test)
yprob = np.argmax(y_pred, axis=1)   # return the index of the biggest pro
yprob = pd.Series(yprob)
yprob.value_counts()


param_test4 = {'reg_alpha': [0, 0.001, 0.005, 0.01]}
#param_test4 = {'reg_alpha': [0, 0.001, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05]}
gsearch4 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, max_depth=best_max_depth, gamma=best_gamma,
                                                colsample_bytree=best_colsample_bytree, subsample=best_subsample,
                                                min_child_weight=best_min_child_weight, objective='multi:softprob', nthread=6,num_class=3,
                                                scale_pos_weight=1, seed=27),
                        param_grid=param_test4, n_jobs=4, iid=False, cv=5, scoring='f1_weighted')
gsearch4.fit(X_train, y_train, sample_weight=w_array)
best_reg_alpha = gsearch4.best_params_['reg_alpha']
print('best_reg_alpha:', best_reg_alpha)
y_pred=gsearch4.predict_proba(X_test)
yprob = np.argmax(y_pred, axis=1)   # return the index of the biggest pro
yprob = pd.Series(yprob)
yprob.value_counts()


param_test5 = {'n_estimators': range(50, 200, 50)}
#param_test5 = {'n_estimators': range(0, 100, 10)}
gsearch5 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, max_depth=best_max_depth, gamma=best_gamma,
                                                colsample_bytree=best_colsample_bytree, subsample=best_subsample,
                                                reg_alpha=best_reg_alpha,
                                                min_child_weight=best_min_child_weight, objective='multi:softprob',num_class=3,
                                                nthread=6, scale_pos_weight=1, seed=27),
                        param_grid=param_test5, n_jobs=4, iid=False, cv=5, scoring='f1_weighted')
gsearch5.fit(X_train, y_train, sample_weight=w_array)
best_n_estimators = gsearch5.best_params_['n_estimators']
print('best_n_estimators:', best_n_estimators)
y_pred=gsearch5.predict_proba(X_test)
yprob = np.argmax(y_pred, axis=1)   # return the index of the biggest pro
yprob = pd.Series(yprob)
yprob.value_counts()


param_test6 = {'learning_rate': [i / 10.0 for i in range(1, 3)]}
#param_test6 = {'learning_rate': [i / 100.0 for i in range(1, 20)]}
gsearch6 = GridSearchCV(estimator=XGBClassifier(max_depth=best_max_depth, gamma=best_gamma, n_estimators=best_n_estimators,
                                                colsample_bytree=best_colsample_bytree, subsample=best_subsample, reg_alpha=best_reg_alpha,
                                                min_child_weight=best_min_child_weight, objective='multi:softprob', nthread=6,num_class=3,
                                                scale_pos_weight=1, seed=27),
                        param_grid=param_test6, n_jobs=4, iid=False, cv=5, scoring='f1_weighted')
gsearch6.fit(X_train, y_train, sample_weight=w_array)
best_learning_rate = gsearch6.best_params_['learning_rate']
print('best_learning_rate:', best_learning_rate)
y_pred=gsearch6.predict_proba(X_test)
yprob = np.argmax(y_pred, axis=1)   # return the index of the biggest pro
yprob = pd.Series(yprob)
yprob.value_counts()


# 用获取得到的最优参数再次训练模型
model = XGBClassifier(learning_rate=best_learning_rate, n_estimators=best_n_estimators, max_depth=best_max_depth,
                        gamma=best_gamma, colsample_bytree=best_colsample_bytree, subsample=best_subsample, reg_alpha=best_reg_alpha,
                        min_child_weight=best_min_child_weight,evel_metric='merror',
                        objective='multi:softprob',num_class=3,nthread=6, scale_pos_weight=1, seed=27)
model.fit(X_train, y_train, sample_weight=w_array)

y_pred=model.predict_proba(X_test)
yprob = np.argmax(y_pred, axis=1)
yprob = pd.Series(yprob)
yprob.value_counts()

## df_params
param_dict = {"param": ['learning_rate', 'n_estimators','max_depth','gamma','colsample_bytree','subsample','reg_alpha','min_child_weight'],
"value": [best_learning_rate, best_n_estimators, best_max_depth, best_gamma, best_colsample_bytree, best_subsample, best_reg_alpha, best_min_child_weight ]}
df_params = pd.DataFrame(param_dict)


## model变量重要性
best_importance = model.feature_importances_
model_importance = pd.DataFrame(columns=["varName", 'importance'])
model_importance['varName'] = features_used
model_importance['importance'] = best_importance



# prediction-------------------------------------------------------

y_train_pred = model.predict_proba(X_train)
y_train_pred=pd.DataFrame(y_train_pred)
y_train_pred.columns=['y0','y1','y2']
train_pred = train.reset_index()
train_pred = pd.concat([train_pred,y_train_pred],1)
y_train_lable = np.argmax(model.predict_proba(X_train), axis=1)
y_train_lable = pd.Series(y_train_lable)
y_train_lable.value_counts()
train_pred['y_pred']=y_train_lable


y_test_pred = model.predict_proba(X_test)
y_test_pred=pd.DataFrame(y_test_pred)
y_test_pred.columns=['y0','y1','y2']
test_pred = test.reset_index()
test_pred = pd.concat([test_pred,y_test_pred],1)
y_test_lable = np.argmax(model.predict_proba(X_test), axis=1)
y_test_lable = pd.Series(y_test_lable)
y_test_lable.value_counts()
test_pred['y_pred']=y_test_lable


y_oot_pred = model.predict_proba(X_oot)
y_oot_pred=pd.DataFrame(y_oot_pred)
y_oot_pred.columns=['y0','y1','y2']
oot_pred = oot.reset_index()
oot_pred = pd.concat([oot_pred,y_oot_pred],1)
y_oot_lable = np.argmax(model.predict_proba(X_oot), axis=1)
y_oot_lable = pd.Series(y_oot_lable)
y_oot_lable.value_counts()
oot_pred['y_pred']=y_oot_lable

train_pred['sample_set']='train'
test_pred['sample_set']='test'
oot_pred['sample_set']='oot'
pred_all = pd.concat([train_pred,test_pred,oot_pred],0)
pred_all=pred_all.rename(columns={'order_no':'loan_id'})
pred_all.loan_id=pred_all.loan_id.astype(str)


# f1score
from sklearn import metrics
f1_train_weighted = metrics.f1_score(train_pred['label'],train_pred['y_pred'], average='weighted')
f1_train_macro = metrics.f1_score(train_pred['label'],train_pred['y_pred'], average='macro')

f1_test_weighted = metrics.f1_score(test_pred['label'],test_pred['y_pred'], average='weighted')
f1_test_macro = metrics.f1_score(test_pred['label'],test_pred['y_pred'], average='macro')

f1_oot_weighted = metrics.f1_score(oot_pred['label'],oot_pred['y_pred'], average='weighted')
f1_oot_macro = metrics.f1_score(oot_pred['label'],oot_pred['y_pred'], average='macro')


f1_dict = {"param": ['f1_train_weighted', 'f1_train_macro','f1_test_weighted','f1_test_macro','f1_oot_weighted','f1_oot_macro'],
"value": [f1_train_weighted, f1_train_macro, f1_test_weighted, f1_test_macro, f1_oot_weighted, f1_oot_macro]}
df_f1 = pd.DataFrame(f1_dict)


## SAVING
import datetime
RESULT_PATH=os.getcwd()
xgb_dict = {}
xgb_dict['01_F1score'] = df_f1
xgb_dict['02_model_importance'] = model_importance.reset_index()
xgb_dict['03_model_params'] = df_params
xgb_dict['04_weight_array'] = w_array

FILE_NAME = "model_perf_%d_%s"%(len(features_used), datetime.datetime.now().strftime('%y%m%d_%H%M%S'))
## SAVE MODEL
pickle.dump(model, open(os.path.join(RESULT_PATH, FILE_NAME+".pkl"), "wb"))
model.save_model(os.path.join(RESULT_PATH, FILE_NAME+'.model'))
print('model saved!')
## SAVE files
pickle.dump(pred_all, open(os.path.join(RESULT_PATH, FILE_NAME+"_data_prediction.pkl"), "wb"))
fl.ModelSummary2Excel(result_path = RESULT_PATH, fig_path= RESULT_PATH, file_name = FILE_NAME+".xlsx", data_dic = xgb_dict).run()
print('files saved!')



##============================================
# P-R图
# precision recall curve
import matplotlib.pyplot as plt

from sklearn.metrics import precision_recall_curve
from sklearn.preprocessing import label_binarize
y_test = label_binarize(test_pred.label, classes=[*range(3)])
y_score = label_binarize(test_pred.y_pred, classes=[*range(3)])

precision = dict()
recall = dict()
for i in range(3):
    precision[i], recall[i], thresholds = precision_recall_curve(y_test[:, i],
                                                        y_score[:, i])
    plt.plot(recall[i], precision[i], lw=2, label='class {}'.format(i))

plt.xlabel("recall")
plt.ylabel("precision")
plt.legend(loc="best")
plt.title("precision vs. recall curve")
#plt.show()
plt.savefig(FILE_NAME+'p-r.png')


#=============================================
#classification_report
from sklearn.metrics import classification_report
target_names = ['class 0', 'class 1', 'class 2']

tbl1 = classification_report(test_pred['label'], test_pred['y_pred'], target_names=target_names)
with open("model_perf_75_200228_193325_test_tbl.txt","w") as f:
        f.write(tbl1)

trtbl2 = classification_report(train_pred['label'], train_pred['y_pred'], target_names=target_names)      
with open("model_perf_75_200228_193325_train_tbl.txt","w") as f:
        f.write(trtbl2)

