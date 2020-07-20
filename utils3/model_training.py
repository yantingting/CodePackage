'''
in_dict = {
    'RESULT_PATH':'C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型',\
    'index_id':'loan_id',\
    'target':'flag7',\
    'date_col':'effective_date',\
    'extra_col':['customer_id','apply_time', 'due_date'],\
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
    'fs_dict': {
        'slope':True,\
        'psi':None,\
        'iv':None,\
        'xgb_imp':None,\
        'corr':None
    },\
    'grid_param': {'param_test1' : {'max_depth': range(2,5, 1), 'min_child_weight': range(5, 10, 1)},\
        'param_test2': {'gamma': [i / 10.0 for i in range(0, 5)]} ,\
        'param_test3': {'subsample': [i / 10.0 for i in range(6, 9)], 'colsample_bytree': [i / 10.0 for i in range(6, 9)]},\
        'param_test4': {'reg_alpha': [0, 0.001, 0.005, 0.01]},\
        'param_test5': {'n_estimators': range(0, 200, 50)},\
        'param_test6' : {'learning_rate': [i / 10.0 for i in range(1, 3)]}
    },\
    'random_param': {
        'eta': [0.01,0.05,0.1],
        'max_depth': [2,3,4],
        'gamma': [0, 0.1, 0.2],
        'min_child_weight':[5,10],
        'subsample': [0.6,0.7,0.8],
        'colsample_bytree': [0.6,0.7,0.8],
        'n_estimators': [50,100,150,200,300]
    }
}
'''

import pandas as pd
import numpy as np
import os
import sys
import pickle
import time
from datetime import datetime
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score,roc_curve

import utils3.misc_utils as mu
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
fs_obj = fs.FeatureSelection()
import utils3.metrics as mt
pf = mt.Performance()
bw = mt.BinWoe()
import utils3.plot_tools as pt
import matplotlib.pyplot as plt
import utils3.xgboost_model as xm
import utils3.filing as fl
import utils3.data_io_utils as du


## 数据预处理、字典、eda、缺失填充
def data_preprocess(df, in_dict, var_dict=None):
    '''
    输入原始数据，输出缺失填充好的数据、字典、eda结果
    'known_missing': {-999:-9999, -1111:-9999}   #将data中已知表示缺失的值赋值为-9999
    '''
    #index_id=in_dict['index_id']
    #label=in_dict['target']
    df.columns = df.columns.str.replace(':','_').str.replace('/','_').str.replace('\\','_')
    df.rename(columns = {in_dict['index_id']:'order_no', in_dict['target']:'label'}, inplace=True)
    df.order_no = df.order_no.astype(str)
    df.label = df.label.astype(int)
    if df.label.nunique()>2:
        print('Warning: target种类大于2，仅取target=0或target=1')
        df = df[df.label.isin([0,1])]
    df.set_index(["order_no"], inplace=True)

    # 生成字典
    x_col = list(set(list(df.columns)) - set(in_dict['extra_col']) - set(['label']))
    x_all = df[x_col]
    if var_dict is None:
        var_dict = du.create_dict(x_all, data_sorce='默认', data_type='原始字段', useless_vars=[])

    #数据缺失处理
    extra_col = in_dict['extra_col']
    x_w_y = df.drop(extra_col,1)
    #x_all = df[x_col]
    X_cleaned = mu.process_missing(X = x_all,var_dict=var_dict, known_missing=in_dict['known_missing'], downflagmap={}, verbose=True)
    X_cleaned_sorted = X_cleaned.reindex(x_all.index.tolist())
    data_cleaned = pd.concat([df[extra_col+['label']], X_cleaned_sorted], axis=1)
    data_cleaned = data_cleaned[df.columns.tolist()]

    # EDA
    eda = ss.eda(X = X_cleaned_sorted, var_dict=var_dict, useless_vars = [],exempt_vars = [], uniqvalue_cutoff=0.97)

    # 缺失值赋值
    data_cleaned = data_cleaned.fillna(-1)
    data_cleaned = data_cleaned.replace([-9999,'-9999',-8888,'-8888',-8887,'-8887'],[-1,-1,-1,-1,-1,-1])

    return data_cleaned, var_dict, eda

## 划分sample_set
def sample_split(df, in_dict):
    '''
    'sample_cutoff':{
        'by_time':False,\      #True则train、test按时间划分，False则随机分
        'test_cutoff': 0.3,\
        'oot_cutoff': 0.2      #不为None，则按输入比例划分oot
    }
    输出df中增加一列'sample_set'
    '''
    test_cutoff = in_dict['sample_cutoff']['test_cutoff']
    oot_cutoff = in_dict['sample_cutoff']['oot_cutoff']
    bytime = in_dict['sample_cutoff']['by_time']
    effective_date = in_dict['date_col']
    df_sorted = df.sort_values([effective_date])
    # 
    if oot_cutoff is not None:
        #df[effective_date] = df[effective_date].astype(str)
        oot_df = df_sorted[int(df_sorted.shape[0] * (1-oot_cutoff)):]
        oot_date = min(oot_df[effective_date])
        traintest_df = df_sorted[:int(df_sorted.shape[0] * (1-oot_cutoff))]
        # 按时间划分
        if bytime:
            test_df = traintest_df[int(traintest_df.shape[0] * (1-test_cutoff)):]
            test_date = min(test_df[effective_date])
            df['sample_set'] = pd.to_datetime(df[effective_date]).apply(lambda x: 'train' \
                if x <= pd.to_datetime(str(test_date)) else ('test' if x <= pd.to_datetime(str(oot_date)) else 'oot'))
        # 随机分
        else:
            test = traintest_df.sample(frac=test_cutoff,axis=0,random_state=1)
            train = traintest_df.drop(test.index)
            train['sample_set']='train'
            test['sample_set']='test'
            oot_df['sample_set']='oot'
            df = pd.concat([train, test, oot_df],0)
    else:
        if bytime:
            test_df = df_sorted[int(df_sorted.shape[0] * (1-test_cutoff)):]
            test_date = min(test_df[effective_date])
            df['sample_set'] = pd.to_datetime(df[effective_date]).apply(lambda x: 'train' \
                if x <= pd.to_datetime(str(test_date)) else 'test')
        else:
            test = df.sample(frac=test_cutoff,axis=0,random_state=1)
            train = df.drop(test.index)
            train['sample_set']='train'
            test['sample_set']='test'
            df = pd.concat([train, test],0)
    return df

## 样本统计表
def desc_sample(df, date_col='effective_date', target='label'):
    '''
    输入用于划分sample_set的时间列名，target列名
    输出样本统计信息：样本划分,样本比例,起始时间,样本量,逾期数,逾期率
    '''
    desc_table = pd.pivot_table(df, index =['sample_set'], values = [date_col,target], \
                                aggfunc ={date_col:[min, max],target:['count', np.sum]})
    desc_table.columns = ['end_date','start_date','cnt_all','cnt_bad']
    desc_table['badRate'] = desc_table['cnt_bad']/desc_table['cnt_all']
    desc_table['sample_pct'] = desc_table['cnt_all']/df.shape[0]
    #合计
    desc_table.loc["ALL", "end_date"] = desc_table.end_date.astype(str).max()
    desc_table.loc["ALL", "start_date"] = desc_table.start_date.astype(str).min()
    desc_table.loc["ALL", "cnt_all"] = desc_table.cnt_all.sum()
    desc_table.loc["ALL", "cnt_bad"] = desc_table.cnt_bad.sum()
    desc_table.loc["ALL", "badRate"] = desc_table.badRate.mean()
    desc_table.loc["ALL", "sample_pct"] = desc_table.sample_pct.sum()
    #format
    desc_table['badRate'] = desc_table['badRate'].apply(lambda x: "{:.2%}".format(x))
    desc_table['sample_pct'] = desc_table['sample_pct'].apply(lambda x: "{:.0%}".format(x))
    desc_table['cnt_all'] = desc_table['cnt_all'].astype(int)
    desc_table['cnt_bad'] = desc_table['cnt_bad'].astype(int)
    desc_table['cnt_all'] = desc_table['cnt_all'].map(lambda x:format(x,','))
    desc_table['cnt_bad'] = desc_table['cnt_bad'].map(lambda x:format(x,','))
    desc_table=desc_table.reindex(['train','test','oot','ALL'])
    desc_table = desc_table.reset_index()
    desc_table = desc_table[['sample_set','sample_pct','start_date','end_date','cnt_all','cnt_bad','badRate']]
    return desc_table

## 画Univarchart
def draw_univarchart(df, RESULT_PATH):
    '''
    df仅包含x y sample_set
    univarchart中包含train test oot三条线
    '''
    if RESULT_PATH is None:
        RESULT_PATH = os.getcwd()
    
    train_df = df[df.sample_set == 'train']
    test_df = df[df.sample_set == 'test']
    oot_df = df[df.sample_set == 'oot']

    plot_list = []
    wrong_list= []
    x_col = list(set(list(df.columns))-set(['sample_set'])-set(['label']))

    FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'UniVarChart')
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)

    for i in x_col:
        print('{} is in processing'.format(i))
        try:
            pt.univariate_chart_oot(df, i,  'label', n_bins=10,
                            default_value=-1,
                            dftrain=train_df, dftest=test_df,dfoot = oot_df,
                            draw_all=True, draw_train_test=True)
            path = os.path.join(FIG_PATH, i+".png")
            plt.savefig(path, format='png', dpi=100)
            plt.close()
            plot_list.append(i)
        except:
            wrong_list.append(i)
            print('wrong!')
        ## 画图后dataframe中会多两列'bins','bin_no'
        #try:
        #    train_df = train_df.drop(['bins','bin_no'],axis=1)
        #    test_df = test_df.drop(['bins','bin_no'],axis=1)
        #    oot_df = oot_df.drop(['bins','bin_no'],axis=1)
    return plot_list, wrong_list

## Ranking：xgb_imp  IV  PSI等
def var_ranking(df, var_dict, num_max_bins=5, n_clusters=15,methods=[]):
    '''
    df仅包含x y sample_set
    'ranking_methods':[
        'random_forest'
        ,'lasso'
        ,'xgboost'
    ]
    '''
    train_df = df[df.sample_set == 'train']
    test_df = df[df.sample_set == 'test']
    train = train_df.drop(['sample_set'], axis=1)
    test = test_df.drop(['sample_set'], axis=1)
    X_train = train.drop(['label'], axis=1)
    y_train = train.label
    X_test = test.drop(['label'], axis=1)
    y_test = test.label
    features_var_dict = X_train.columns.tolist()
    var_dict = var_dict[var_dict.指标英文.isin(features_var_dict)]
    ## xgb model importance
    model1 = XGBClassifier()
    model1.fit(X_train, y_train)
    y_pred = model1.predict(X_test)
    predictions = [round(value) for value in y_pred]
    features_in_model = list(X_train.columns)
    feature_importance = model1.feature_importances_
    var_importance = pd.DataFrame(columns=["指标英文", 'importance'])
    var_importance['指标英文'] = features_in_model
    var_importance['importance'] = feature_importance
    var_importance.loc[:, 'importance_rank'] = var_importance.importance.rank(ascending=False)
    ## Ranking 
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
    # Train 分箱
    X_cat_train, X_transformed_train, woe_iv_df_train, rebin_spec_train, ranking_result_train = fs_obj.overall_ranking(X_train, y_train,
                                                                                            var_dict, args_dict,
                                                                                            methods=methods, 
                                                                                            num_max_bins=num_max_bins,
                                                                                            n_clusters=n_clusters)
    rebin_spec = mu.convert_rebin_spec2XGB_rebin_spec(rebin_spec_train)
    rebin_spec_bin_adjusted = {k: v for k, v in rebin_spec.items() if k in features_var_dict}
    # 输出变量的分箱
    X_cat_train = bw.convert_to_category(X_train, var_dict, rebin_spec_bin_adjusted)
    X_cat_test = bw.convert_to_category(X_test, var_dict, rebin_spec_bin_adjusted)
    # PSI
    var_psi = pf.variable_psi(X_cat_train, X_cat_test, var_dict)
    var_psi.loc[:, 'psi_rank'] = var_psi.PSI.rank(ascending=False)
    # 汇总各项指标
    ranking_result = ranking_result_train.merge(var_psi, on='指标英文', how='left')\
                                        .merge(var_importance, on='指标英文', how='left')

    return ranking_result

## Feature Selection
def feature_rmv(df, ranking_result, in_dict):
    '''
    df: x + y + sample_set
    'fs_dict': {
        'slope':True,\    #填True/False
        'psi':None,\      #填None/阈值
        'iv':None,\       #填None/阈值
        'xgb_imp':None,\  #填None/阈值
        'corr':None,\     #填None/阈值
        'random':True     #填true/False
    }
    '''
    RESULT_PATH = in_dict['RESULT_PATH']
    slope = in_dict['slope'] 
    psi = in_dict['psi']
    iv = in_dict['iv']
    imp = in_dict['xgb_imp']
    corr = in_dict['corr']
    random = in_dict['random']
    
    train_df = df[df.sample_set == 'train']
    test_df = df[df.sample_set == 'test']
    train = train_df.drop(['sample_set'], axis=1)
    test = test_df.drop(['sample_set'], axis=1)
    X_train = train.drop(['label'], axis=1)
    y_train = train.label
    X_test = test.drop(['label'], axis=1)
    y_test = test.label
    f_in_model = list(X_train.columns)
    print(u"变量总个数{}".format(len(f_in_model)))
    f_rmv = []
    
    if psi is not None:
        PSI_remove = ranking_result[ranking_result.PSI > psi]['指标英文'].values.tolist()
        print(u"PSI筛掉的个数{}".format(len(PSI_remove)))
        f_rmv = list(set(f_rmv + PSI_remove))
    else:
        PSI_remove=[]
    
    if iv is not None:
        IV_remove = ranking_result[ranking_result.IV < iv]['指标英文'].values.tolist()
        print(u"IV筛掉的个数{}".format(len(IV_remove)))
        f_rmv = list(set(f_rmv + IV_remove))
    else:
        IV_remove=[]
    
    if imp is not None:
        IMP_remove = ranking_result[ranking_result.importance < imp]['指标英文'].values.tolist()
        print(u"xgb重要性筛掉的个数{}".format(len(IMP_remove)))
        f_rmv = list(set(f_rmv + IMP_remove))
    else:
        IMP_remove=[]
    
    if corr is not None:
        cl = fs.Colinearity(X_train, RESULT_PATH)
        corr_remove, corr_table = cl.calculate_corr(ranking_result = ranking_result, corr_threshold=corr)
        corr_select = list(set(f_in_model) - set(corr_remove))
        print(u"相关性特征筛掉的个数{}".format(len(corr_remove)))
        f_rmv = list(set(f_rmv + corr_remove))
        pickle.dump(corr_table, open(os.path.join(RESULT_PATH, "CORR_TABLE.pkl"), "wb"))
    else:
        corr_remove=[]
        corr_table=[]

    if slope:
        slope_dict = fs_obj.cal_slope(train, test)
        slope_select = [i[0] for i in slope_dict.items() if i[1] >= 0]
        slope_remove = list(set(f_in_model)- set(slope_select))
        print(u"univariate筛掉的个数{}".format(len(slope_remove)))
        f_rmv = list(set(f_rmv + slope_remove))
    else:
        slope_remove=[]
    
    if random:
        rdm_imp = ranking_result.loc[ranking_result.指标英文=='random','importance'].values[0]
        rdm_remove = ranking_result[ranking_result.importance <= rdm_imp]['指标英文'].values.tolist()
        print(u"重要性低于random变量的个数{}".format(len(rdm_remove)))
        f_rmv = list(set(f_rmv + rdm_remove))
    else:
        rdm_remove=[]
    
    print(u"总共筛掉的个数{}".format(len(f_rmv)))
    f_used = list(set(f_in_model) - set(f_rmv))
    print(u"剩余变量个数{}".format(len(f_used)))
    
    fs_dict = {
        'PSI_cutoff':psi, 'PSI_rmv':len(PSI_remove),\
        'IV_cutoff':iv, 'IV_rmv':len(IV_remove), \
        'XGB_IMP_cutoff':imp, 'IMP_rmv':len(IMP_remove),\
        'CORR_cutoff':corr, 'CORR_rmv':len(corr_remove),\
        'UNIVAR':slope, 'slope_rmv':len(slope_remove),\
        'random':random, 'random_rmv':len(rdm_remove)
    }
    fs_df = pd.DataFrame.from_dict(fs_dict, orient='index')
    return f_rmv, slope_remove, fs_df


## 模型训练、表现并存储(prediction, data_scored, KS, AUC, pdp, liftchart)
## 用到了后面三个function：xgb_models  dict_pdp  dict_liftchart
def model_performance(train, test, oot, model, in_dict, save=True):
    '''
    train test oot：仅包含x y，也可带sample_set
    model：‘grid’或‘random’
    save: True则保存此次建模结果
    '''
    RESULT_PATH = in_dict['RESULT_PATH']
    try:
        train = train.drop(['sample_set'],1)
        test = test.drop(['sample_set'],1)
        oot = oot.drop(['sample_set'],1)
    except:
        pass
    # 模型训练
    if model=='grid':
        start = time.time()
        print('XGB grid search in process----')
        model, df_params, model_importance = xgb_models(train, test, model='grid',in_dict=in_dict)
        end = time.time()
        print('XGB grid search done!')
        print("run time = {}".format(end - start))
    elif model=='random':
        start = time.time()
        print('XGB random search in process----')
        model, df_params, model_importance = xgb_models(train, test, model='random',in_dict=in_dict)
        end = time.time()
        print('XGB random search done!')
        print("run time = {}".format(end - start))
    else:
        print('grid or random?')

    X_train = train.drop(['label'], axis=1)
    y_train = train.label
    X_test = test.drop(['label'], axis=1)
    y_test= test.label
    features_used = X_train.columns.tolist()
    X_traintest = pd.concat([X_train, X_test])
    # prediction
    y_train_pred = model.predict_proba(X_train)[:,1]
    y_test_pred = model.predict_proba(X_test)[:,1]
    train_pred = train.copy()
    train_pred['y_pred'] = y_train_pred
    train_pred['sample_set'] = 'train'
    test_pred = test.copy()
    test_pred['y_pred'] = y_test_pred
    test_pred['sample_set'] = 'test'
    print('prediction done')
    # KS
    data_scored_train, train_proba_ks, train_proba_ks_20, data_scored_test, test_proba_ks, test_proba_ks_20 = pf.data_KS(train_pred, test_pred, 'y_pred',method='proba')
    train_ks = max(train_proba_ks.KS)
    test_ks = max(test_proba_ks.KS)
    print("train_ks: %.2f" % train_ks)
    print("test_ks: %.2f" % test_ks)
    # AUC ACC
    auc_train, acc_train = pf.auc_acc(train_pred)
    print("train_auc: %.2f" % auc_train)
    auc_test, acc_test = pf.auc_acc(test_pred)
    print("test_auc: %.2f" % auc_test)
    # all prediction
    all_pred = pd.concat([train_pred, test_pred],0).reset_index()
    all_pred['order_no'] = all_pred['order_no'].astype(str)
    data_scored_train['order_no'] = data_scored_train['order_no'].astype(str)
    data_scored_test['order_no'] = data_scored_test['order_no'].astype(str)
    data_scored_train['sample_set'] = "train"
    data_scored_test['sample_set'] = "test"
    data_scored_all = pd.concat([data_scored_train, data_scored_test], 0)
    data_scored_all['order_no'] = data_scored_all['order_no'].astype(str)
    # all AUC ACC
    auc_list = [auc_train, auc_test]
    acc_list = [acc_train, acc_test]
    ks_list = [train_ks, test_ks]
    split_list = ['train','test']
    df_auc_acc = pd.DataFrame({"sample_set":split_list,"auc":auc_list,"accuracy":acc_list,"KS":ks_list})
    # draw lift chart
    print('draw lift chart----')
    #draw_liftchart(train_pred, test_pred, None, features_used, RESULT_PATH)
    lc_pic_dict=dict_liftchart(train_pred, test_pred, None, features_used, RESULT_PATH)
    oot_proba_ks=[]
    oot_proba_ks_20=[]
    
    if oot is not None:
        print('---- oot部分 ----')
        X_oot = oot.drop(['label'], axis=1)
        y_oot= oot.label
        y_oot_pred = model.predict_proba(X_oot)[:,1]
        oot_pred = oot.copy()
        oot_pred['y_pred'] = y_oot_pred
        oot_pred['sample_set'] = 'oot'
        data_scored_train, train_proba_ks, train_proba_ks_20, data_scored_oot, oot_proba_ks, oot_proba_ks_20 = pf.data_KS(train_pred, oot_pred, 'y_pred',method='proba')
        oot_ks = max(oot_proba_ks.KS)
        print("oot_ks: %.2f" % oot_ks)
        auc_oot, acc_oot = pf.auc_acc(oot_pred)
        print("oot_auc: %.2f" % auc_oot)
        # all prediction
        all_pred = pd.concat([all_pred, oot_pred],0).reset_index()
        all_pred['order_no'] = all_pred['order_no'].astype(str)
        data_scored_oot['order_no'] = data_scored_oot['order_no'].astype(str)
        data_scored_oot['sample_set'] = "oot"
        data_scored_all = pd.concat([data_scored_all, data_scored_oot], 0)
        data_scored_all['order_no'] = data_scored_all['order_no'].astype(str)
        # all AUC ACC
        auc_list = [auc_train, auc_test, auc_oot]
        acc_list = [acc_train, acc_test, acc_oot]
        ks_list = [train_ks, test_ks,oot_ks]
        split_list = ['train','test','oot']
        df_auc_acc = pd.DataFrame({"sample_set":split_list,"auc":auc_list,"accuracy":acc_list,"KS":ks_list})
        #draw_liftchart(train_pred, test_pred, oot_pred, features_used, RESULT_PATH)
        lc_pic_dict=dict_liftchart(train_pred, test_pred, oot_pred, features_used, RESULT_PATH)

    if save:
        print('start saving results----')
        # draw PDP
        print('draw PDP----')
        #draw_pdp(X_traintest, model, model_importance, features_used, RESULT_PATH)
        pdp_pic_dict = dict_pdp(X_traintest, model, model_importance, features_used, RESULT_PATH)

        xgb_dict = {}
        xgb_dict['01_AUC_ACC_KS'] = df_auc_acc
        xgb_dict['02_Lift_Chart'] = lc_pic_dict
        xgb_dict['03_KS'] = {'train_proba_ks':train_proba_ks,
                            'test_proba_ks':test_proba_ks,
                            'oot_proba_ks':oot_proba_ks,
                            'train_proba_ks_20':train_proba_ks_20,
                            'test_proba_ks_20':test_proba_ks_20,
                            'oot_proba_ks_20':oot_proba_ks_20
                            }
        xgb_dict['04_Model_Importance'] = model_importance.reset_index()
        xgb_dict['05_Model_Params'] = df_params
        xgb_dict['06_PDP_Chart'] = pdp_pic_dict

        FILE_NAME = "model_perf_%d_%s"%(len(features_used), datetime.now().strftime('%y%m%d_%H%M%S'))
        ## SAVE MODEL
        pickle.dump(model, open(os.path.join(RESULT_PATH, FILE_NAME+".pkl"), "wb"))
        model.save_model(os.path.join(RESULT_PATH, FILE_NAME+'.model'))
        print('model saved!')
        ## SAVE files
        pickle.dump(data_scored_all, open(os.path.join(RESULT_PATH, FILE_NAME+"_data_scored_all.pkl"), "wb"))
        pickle.dump(all_pred, open(os.path.join(RESULT_PATH, FILE_NAME+"_data_prediction.pkl"), "wb"))
        #data_scored_all.to_csv(os.path.join(RESULT_PATH, FILE_NAME+"_data_scored_all.csv"))
        #all_pred.to_csv(os.path.join(RESULT_PATH, FILE_NAME+"_data_prediction.csv"))
        fl.ModelSummary2Excel(result_path = RESULT_PATH, fig_path= os.path.join(RESULT_PATH,'figure'), file_name = FILE_NAME+".xlsx", data_dic = xgb_dict).run()
        print('files saved!')
    
    return  df_auc_acc

## 训练xgb模型 model='grid'/model='random'
def xgb_models(train, test, model, in_dict):
    '''
    train test：仅包含x y
    model：'grid'/'random'
    '''
    X_train = train.drop(['label'], axis=1)
    y_train = train.label
    X_test = test.drop(['label'], axis=1)
    y_test= test.label
    features_used = X_train.columns.tolist()

    if model == 'random':
        model, df_params, model_importance = xm.xgboost_randomgridsearch(X_train, y_train, X_test, y_test, NFOLD=5, param=in_dict['random_param'])

    elif model == 'grid':
        param_test1 = in_dict['grid_param']['param_test1']
        param_test2 = in_dict['grid_param']['param_test2']
        param_test3 = in_dict['grid_param']['param_test3']
        param_test4 = in_dict['grid_param']['param_test4']
        param_test5 = in_dict['grid_param']['param_test5']
        param_test6 = in_dict['grid_param']['param_test6']
        gsearch1 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, gamma=0, subsample=0.7,
                                                        max_depth=4, colsample_bytree=1, objective='binary:logistic',
                                                        nthread=6, scale_pos_weight=1, seed=27),
                                param_grid=param_test1, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
        gsearch1.fit(X_train, y_train)
        best_max_depth = gsearch1.best_params_['max_depth']
        best_min_child_weight = gsearch1.best_params_['min_child_weight']

        gsearch2 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, subsample=0.7, colsample_bytree=1,
                                                        max_depth=best_max_depth,
                                                        min_child_weight=best_min_child_weight, objective='binary:logistic',
                                                        nthread=6, scale_pos_weight=1, seed=27),
                                param_grid=param_test2, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
        gsearch2.fit(X_train, y_train)
        best_gamma = gsearch2.best_params_['gamma']

        gsearch3 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, max_depth=best_max_depth, gamma=best_gamma,
                                                        min_child_weight=best_min_child_weight, objective='binary:logistic', nthread=6,
                                                        scale_pos_weight=1, seed=27),
                                param_grid=param_test3, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
        gsearch3.fit(X_train, y_train)
        best_subsample = gsearch3.best_params_['subsample']
        best_colsample_bytree = gsearch3.best_params_['colsample_bytree']

        gsearch4 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, n_estimators=90, max_depth=best_max_depth, gamma=best_gamma,
                                                        colsample_bytree=best_colsample_bytree, subsample=best_subsample,
                                                        min_child_weight=best_min_child_weight, objective='binary:logistic', nthread=6,
                                                        scale_pos_weight=1, seed=27),
                                param_grid=param_test4, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
        gsearch4.fit(X_train, y_train)
        best_reg_alpha = gsearch4.best_params_['reg_alpha']

        gsearch5 = GridSearchCV(estimator=XGBClassifier(learning_rate=0.1, max_depth=best_max_depth, gamma=best_gamma,
                                                        colsample_bytree=best_colsample_bytree, subsample=best_subsample,
                                                        reg_alpha=best_reg_alpha,
                                                        min_child_weight=best_min_child_weight, objective='binary:logistic',
                                                        nthread=6, scale_pos_weight=1, seed=27),
                                param_grid=param_test5, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
        gsearch5.fit(X_train, y_train)
        best_n_estimators = gsearch5.best_params_['n_estimators']

        gsearch6 = GridSearchCV(estimator=XGBClassifier(max_depth=best_max_depth, gamma=best_gamma, n_estimators=best_n_estimators,
                                                        colsample_bytree=best_colsample_bytree, subsample=best_subsample, reg_alpha=best_reg_alpha,
                                                        min_child_weight=best_min_child_weight, objective='binary:logistic', nthread=6,
                                                        scale_pos_weight=1, seed=27),
                                param_grid=param_test6, scoring='roc_auc', n_jobs=4, iid=False, cv=5)
        gsearch6.fit(X_train, y_train)
        best_learning_rate = gsearch6.best_params_['learning_rate']

        # 用获取得到的最优参数再次训练模型
        model = XGBClassifier(learning_rate=best_learning_rate, n_estimators=best_n_estimators, max_depth=best_max_depth,
                                gamma=best_gamma, colsample_bytree=best_colsample_bytree, subsample=best_subsample, reg_alpha=best_reg_alpha,
                                min_child_weight=best_min_child_weight,
                                objective='binary:logistic', nthread=6, scale_pos_weight=1, eval_metric='auc', seed=27)
        model.fit(X_train, y_train)
        
        ## df_params
        param_dict = {"param": ['learning_rate', 'n_estimators','max_depth','gamma','colsample_bytree','subsample','reg_alpha','min_child_weight'],
        "value": [best_learning_rate, best_n_estimators, best_max_depth, best_gamma, best_colsample_bytree, best_subsample, best_reg_alpha, best_min_child_weight ]}
        df_params = pd.DataFrame(param_dict)

        ## model变量重要性
        best_importance = model.feature_importances_
        model_importance = pd.DataFrame(columns=["varName", 'importance'])
        model_importance['varName'] = features_used
        model_importance['importance'] = best_importance

    return model, df_params, model_importance


## 画PDP图for写入excel
def dict_pdp(X_traintest, model, model_importance, features_used, RESULT_PATH):
    FIG_PATH = os.path.join(RESULT_PATH, 'figure')
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)
    feature_importance = model_importance.sort_values("importance", ascending=False).reset_index()
    f_imp_list = feature_importance['varName'].values.tolist()
    #f_imp_list = gr.get_feature_importance(model_importance)
    #path = os.path.join(FIG_PATH,"top20importance.png")
    #plt.savefig(path, format='png', dpi=100)
    #plt.close()
    #select_features = model_importance['varName'].values.tolist()
    pic_dict={}
    n=0
    while n <len(f_imp_list):
        m = n+9
        features_draw=[i for i in f_imp_list[n:m]]
        pt.pdpCharts9(model, X_traintest, features_draw, features_used, n_bins=10, dfltValue = -1)
        save_label = "pdp_%d_"%(len(features_used))+str(n)+"_"+str(m)
        path = os.path.join(FIG_PATH,save_label+".png")
        plt.savefig(path, format='png', dpi=100)
        plt.close()
        names = save_label + '_picture'
        pic_dict[names] = save_label +'.png'
        n += 9
    #print('end')
    return pic_dict


## 画PDP
def draw_pdp(X_traintest, model, model_importance, features_used, RESULT_PATH):
    FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'PDP_%d_%s'%(len(features_used), datetime.now().strftime('%y%m%d_%H%M%S')))
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)

    f_imp_list = gr.get_feature_importance(model_importance)
    path = os.path.join(FIG_PATH,"top20importance.png")
    plt.savefig(path, format='png', dpi=100)
    plt.close()

    select_features = model_importance['varName'].values.tolist()
    n=0
    while n <len(f_imp_list):
        m = n+9
        features_draw=[i for i in f_imp_list[n:m]]
        pt.pdpCharts9(model, X_traintest, features_draw, select_features, n_bins=10, dfltValue = -1)
        path = os.path.join(FIG_PATH,"pdp_"+str(n)+"_"+str(m)+".png")
        plt.savefig(path, format='png', dpi=100)
        plt.close()
        n += 9
    #print('end')

## 画liftchart for写入excel
def dict_liftchart(train_pred, test_pred, oot_pred, features_used, RESULT_PATH):
    pic_dict ={}
    
    FIG_PATH = os.path.join(RESULT_PATH,'figure')
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)
    
    train_lc = pt.show_result_new(train_pred, 'y_pred','label', n_bins = 10, feature_label='train')
    test_lc = pt.show_result_new(test_pred, 'y_pred','label', n_bins = 10, feature_label='test')
    if oot_pred is not None:
        oot_lc = pt.show_result_new(oot_pred, 'y_pred','label', n_bins = 10, feature_label='OOT')
    
    pic_name = "lift_chart_%d_%s.png"%(len(features_used), datetime.now().strftime('%y%m%d_%H'))
    path = os.path.join(FIG_PATH,pic_name)
    plt.savefig(path, format='png', dpi=100)
    plt.close()
    names = 'liftchart_picture'
    pic_dict[names] = pic_name
    return pic_dict


## 画liftchart
def draw_liftchart(train_pred, test_pred, oot_pred, features_used, RESULT_PATH):
    FIG_PATH = os.path.join(RESULT_PATH, 'figure', 'LiftChart')
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)

    train_lc = pt.show_result_new(train_pred, 'y_pred','label', n_bins = 10, feature_label='train')
    test_lc = pt.show_result_new(test_pred, 'y_pred','label', n_bins = 10, feature_label='test')
    if oot_pred is not None:
        oot_lc = pt.show_result_new(oot_pred, 'y_pred','label', n_bins = 10, feature_label='OOT')

    path = os.path.join(FIG_PATH,"lift_chart_%d_%s.png"%(len(features_used), datetime.now().strftime('%y%m%d_%H%M%S')))
    plt.savefig(path, format='png', dpi=100)
    plt.close()


## 跑一个random search xgb，输出train_ks,test_ks
def randomgrid_for_fs(train, test, in_dict):
    print('random xgb processing---')
    model, df_params, model_importance = xgb_models(train, test, model='random',in_dict=in_dict)
    X_train = train.drop(['label'], axis=1)
    y_train= train.label
    X_test = test.drop(['label'], axis=1)
    y_test= test.label
    # prediction
    y_train_pred = model.predict_proba(X_train)[:,1]
    train_pred = train.copy()
    train_pred['y_pred'] = y_train_pred
    y_test_pred = model.predict_proba(X_test)[:,1]
    test_pred = test.copy()
    test_pred['y_pred'] = y_test_pred
    # train KS
    fpr1,tpr1,thresholds1 = roc_curve(train_pred['label'],train_pred['y_pred'])
    train_ks = max(tpr1-fpr1)   
    # test KS
    fpr,tpr,thresholds= roc_curve(test_pred['label'],test_pred['y_pred'])
    test_ks = max(tpr-fpr)
    
    return train_ks, test_ks

# 跑一套FS+randomXGB
def fs_pipeline(df,ranking_result,fs_dict,rs_dict):
    start = time.time()
    f_rmv, slope_rmv, fs_dict = feature_rmv(df, ranking_result, fs_dict)
    f_rmv = list(set(f_rmv)-set(rs_dict['use_features']))
    var2 = df.drop(f_rmv, axis=1)  # var2应包含：fs筛选后x y sample_set
    if var2.shape[1]>12:
        train = var2[var2.sample_set =='train']
        test = var2[var2.sample_set =='test']
        train=train.drop(['sample_set'],1)
        test=test.drop(['sample_set'],1)
        train_ks, test_ks = randomgrid_for_fs(train, test, rs_dict)
        print('train ks: ',train_ks)
        print('test ks: ',test_ks)
        if train_ks-test_ks>0.1:
            print('warning：过拟')
    else:
        print('变量个数过少！请更换阈值')
        test_ks=0
    end = time.time()
    print("run time = {}".format(end - start))
    return test_ks,f_rmv,fs_dict








# swap分析
def swap_tbl(df,target='label',old_score = 'v5',new_score = 'v6',old_cutoff = 0.45, new_cutoff = 0.4,multi=1.2):
    '''
    df: 包含旧模型概率，新模型概率，target
    target：target列名
    old_score：旧模型概率列名
    new_sore：新模型概率列名
    old_cutoff：旧模型通过率阈值
    new_cutoff：新模型同通过率下的阈值
    multi：swap—in人群逾期率乘数
    '''
    in_in = df[(df[old_score]<=old_cutoff) & (df[new_score]<=new_cutoff)]
    in_out = df[(df[old_score]<=old_cutoff) & (df[new_score]>new_cutoff)]
    out_in = df[(df[old_score]>old_cutoff) & (df[new_score]<=new_cutoff)]
    out_out = df[(df[old_score]>old_cutoff) & (df[new_score]>new_cutoff)]
    # 数量
    cnt_data = { 'old_IN' : [in_in.shape[0],in_out.shape[0]], 'old_OUT' : [out_in.shape[0],out_out.shape[0]] }
    cnt_tbl = pd.DataFrame.from_dict(cnt_data, orient='index')
    cnt_tbl.columns = ['new_IN','new_OUT']
    #cnt_tbl['Total']=cnt_tbl['new_IN']+cnt_tbl['new_OUT']
    #cnt_tbl.loc["Total", "new_IN"] = cnt_tbl.new_IN.sum()
    #cnt_tbl.loc["Total", "new_OUT"] = cnt_tbl.new_OUT.sum()
    #cnt_tbl.loc["Total", "Total"] = cnt_tbl.Total.sum()
    # 逾期
    lb_data = { 'old_IN' : [in_in[target].mean(),in_out[target].mean()], 'old_OUT' : [out_in[target].mean()*multi,out_out[target].mean()] }
    lb_tbl = pd.DataFrame.from_dict(lb_data, orient='index')
    lb_tbl.columns = ['new_IN','new_OUT']
    return cnt_tbl, lb_tbl













