# encoding=utf8
"""
Python 3.6.8
更新：2019-12-18
监控functions
"""
import sys

import dateutil.relativedelta as dr
import datetime
from jinja2 import Template
import pandas as pd
import numpy as np
import re
import math
import json
import itertools
import pandas as pd
import json
import numpy as np
import itertools
import time
import os
import xlsxwriter

try:
    sys.path.remove('/home/ops/repos/genie/')
except:
    pass

sys.path.append('/home/ops/repos/newgenie/')
from utils3.send_email import mailtest


import psycopg2
def get_df_from_pg(SQL):
    usename = "postgres"
    password = "Mintq2019"
    db = "risk_dm"
    host = "192.168.2.20"
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


def time_window(query_date,obs_window, time_type, per_window):
    """
    根据time_type的类型计算不同监控任务的时间长度
    time_type
    1: time_type = 'hour', obs_window = 24, per_window = 0: 每N小时取过去24小时的线上数据
    2: time_type = 'day', obs_window = 1, per_window = 0: 每日定时取过去1天的线上数据(不包括今天的数据)
    3: time_type = 'day', obs_window = 15, per_window = 21: 过去第15+21天开始，到过去第21天结束
    4: time_type = 'week', obs_window = 3, per_window = 0: 过去3周
    """
    out_dict = {}
    if time_type == 'hour':
        out_dict['ObStartDt'] = (pd.to_datetime(query_date) - dr.relativedelta(hours=obs_window+per_window)).strftime("%Y-%m-%d %H:%M:%S")
        out_dict['ObEndDt'] = (pd.to_datetime(query_date) - dr.relativedelta(hours=per_window)).strftime("%Y-%m-%d %H:%M:%S")
        start_hour = (pd.to_datetime(query_date) - dr.relativedelta(hours=obs_window)).strftime("%Y-%m-%d %H:%M:%S")
        end_hour = (pd.to_datetime(query_date) - dr.relativedelta(hours=0)).strftime("%Y-%m-%d %H:%M:%S")
        out_dict['hour_str'] = [start_hour,end_hour]
    elif time_type=='day':
        out_dict['ObStartDt'] = pd.to_datetime((pd.to_datetime(query_date) - dr.relativedelta(days=obs_window+per_window)).strftime("%Y-%m-%d")).strftime("%Y-%m-%d %H:%M:%S")
        #ObEndDt时间点为时间为监控时点前一天的59分59秒
        out_dict['ObEndDt'] = (pd.to_datetime((pd.to_datetime(query_date) - dr.relativedelta(days=per_window)).strftime("%Y-%m-%d")) - dr.relativedelta(seconds = 1 )).strftime("%Y-%m-%d %H:%M:%S")
        start_hour = (pd.to_datetime(query_date) - dr.relativedelta(days=obs_window + per_window)).strftime("%Y-%m-%d %H:%M:%S")
        end_hour = (pd.to_datetime(query_date) - dr.relativedelta(days=0+per_window)).strftime("%Y-%m-%d %H:%M:%S")
        out_dict['hour_str'] = [start_hour,end_hour]
    elif time_type == 'week':
        out_dict['ObStartDt'] = pd.to_datetime((pd.to_datetime(query_date) - dr.relativedelta(days= 7 * obs_window + per_window)).strftime("%Y-%m-%d")).strftime("%Y-%m-%d %H:%M:%S")
        #ObEndDt时间点为时间为监控时点前一天的59分59秒
        out_dict['ObEndDt'] = (pd.to_datetime((pd.to_datetime(query_date) - dr.relativedelta(days=0+per_window)).strftime("%Y-%m-%d")) - dr.relativedelta(seconds = 1 )).strftime("%Y-%m-%d %H:%M:%S")
        start_hour = (pd.to_datetime(query_date) - dr.relativedelta(days= 7 * obs_window + per_window)).strftime("%Y-%m-%d %H:%M:%S")
        end_hour = (pd.to_datetime(query_date) - dr.relativedelta(days=0+per_window)).strftime("%Y-%m-%d %H:%M:%S")
        out_dict['hour_str'] = [start_hour,end_hour]
    return out_dict

def get_current_data(product_name, in_dict):
    print(in_dict)
    time_dict = time_window(query_date = in_dict['query_date'], obs_window = in_dict['obs_window'], time_type= in_dict['time_type']
                            , per_window = in_dict['per_window'])
    print('start pulling data from',time_dict['ObStartDt'], 'to', time_dict['ObEndDt'])
    if isinstance(product_name, list):
        product_name = tuple(product_name)
    SQL = Template(in_dict['sql']).render(start_date = time_dict['ObStartDt']
                                                               , end_date = time_dict['ObEndDt']
                                                               , businessid = product_name)
    data = get_df_from_pg(SQL)
    if data.shape[0] == 0:
        print( '当前数据量为0, 执行SQL如下')
        print(SQL)
    else:
        data = data.fillna(-1).replace('', -1)
    return(data)


def eda(X, var_dict, useless_vars, exempt_vars, data_path, save_label, uniqvalue_cutoff=0.97):
#def eda(X, var_dict, useless_vars, exempt_vars, uniqvalue_cutoff=0.97):
    """
    计算N, missing value percent, mean, min, max, percentiles 等。

    Args:

    X (pd.DataFrame()): 是一个宽表，每一列是一个变量，每一行是一个obs
    var_dict (pd.DataFrame()): 标准变量字典表，包含以下这些列：数据源，数据类型，指标类型，
    指标英文，指标中文，变量解释，取值解释。
    useless_vars (list): 无用变量名list
    exempt_vars (list): 豁免变量名list，这些变量即使一开始被既定原因定为exclude，也会被
        保留，比如前一版本模型的变量
    data_path (str): 存储数据文件路径
    save_label (str): summary文档存储将用'%s_variables_summary.xlsx' % save_label存
    uniqvalue_cutoff（float): 0-1之间，缺失率和唯一值阈值设定

    Returns：
    None
    直接将结果存成xlsx
    """
    variable_summary = X.count(0).to_frame('N_available')
    variable_summary.loc[:, 'N'] = len(X)
    variable_summary.loc[:, 'N_-9999'] = (X.isin([-9999, '-9999', '-9999.0'])).sum()
    variable_summary.loc[:, 'pct_-9999'] = np.round(variable_summary['N_-9999'] * 1.0 / variable_summary.N, 3)
    variable_summary.loc[:, 'N_-8888'] = (X.isin([-8888, '-8888', '-8888.0'])).sum()
    variable_summary.loc[:, 'pct_-8888'] = np.round(variable_summary['N_-8888'] * 1.0 / variable_summary.N, 3)
    variable_summary.loc[:, 'N_-8887'] = (X.isin([-8887, '-8887', '-8887.0'])).sum()
    variable_summary.loc[:, 'pct_-8887'] = np.round(variable_summary['N_-8887'] * 1.0 / variable_summary.N, 3)
    variable_summary.loc[:, 'N_-1'] = (X.isin([-1, '-1', '-1.0'])).sum()
    variable_summary.loc[:, 'pct_-1'] = np.round(variable_summary['N_-1'] * 1.0 / variable_summary.N, 3)
    variable_summary.loc[:, 'pct_NA'] = variable_summary['pct_-8888'] \
                                        + variable_summary['pct_-8887'] \
                                        + variable_summary['pct_-9999'] \
                                        + variable_summary['pct_-1']
    variable_summary.loc[:, 'N_0'] = (X.isin([0,'0'])).sum()
    variable_summary.loc[:, 'pct_0'] = np.round(variable_summary.N_0 * 1.0 / variable_summary.N, 3)
    # the following can only be done for continuous variable
    try:
        numerical_vars = var_dict.loc[(var_dict['数据类型'] != 'varchar') & ~pd.isnull(var_dict['指标英文']), '指标英文'].tolist()
        categorical_vars = var_dict.loc[(var_dict['数据类型'] == 'varchar') & ~pd.isnull(var_dict['指标英文']), '指标英文'].tolist()
        numerical_vars = list(set(numerical_vars).intersection(set(X.columns)))
        categorical_vars = list(set(categorical_vars).intersection(set(X.columns)))
    except:
        numerical_vars = X.dtypes[X.dtypes!='object'].index.values
        categorical_vars = X.dtypes[X.dtypes=='object'].index.values

    if len(numerical_vars) > 0:
        X_numerical = X[numerical_vars].apply(lambda x: x.astype(float), 0)\
                                       .replace(-8888, np.nan)\
                                       .replace(-9999, np.nan)\
                                       .replace(-8887, np.nan) \
                                       .replace(-1, np.nan)
        numerical_vars_summary = X_numerical.mean().round(6).to_frame('mean')
        numerical_vars_summary.loc[:, 'std'] = X_numerical.std().round(6)
        numerical_vars_summary.loc[:, 'median'] = X_numerical.median().round(6)
        numerical_vars_summary.loc[:, 'min'] = X_numerical.min()
        numerical_vars_summary.loc[:, 'max'] = X_numerical.max()
        numerical_vars_summary.loc[:, 'p01'] = X_numerical.quantile(0.01)
        numerical_vars_summary.loc[:, 'p05'] = X_numerical.quantile(q=0.05)
        numerical_vars_summary.loc[:, 'p10'] = X_numerical.quantile(q=0.10)
        numerical_vars_summary.loc[:, 'p25'] = X_numerical.quantile(q=0.25)
        numerical_vars_summary.loc[:, 'p75'] = X_numerical.quantile(q=0.75)
        numerical_vars_summary.loc[:, 'p90'] = X_numerical.quantile(q=0.90)
        numerical_vars_summary.loc[:, 'p95'] = X_numerical.quantile(q=0.95)
        numerical_vars_summary.loc[:, 'p99'] = X_numerical.quantile(q=0.99)
    # the following are for categorical_vars
    if len(categorical_vars) > 0:
        X_categorical = X[categorical_vars].copy()
        X_categorical = X_categorical.replace('-8888.0', np.nan)\
                                     .replace('-9999.0', np.nan)\
                                     .replace('-8887.0', np.nan)\
                                     .replace('-8888', np.nan)\
                                     .replace('-9999', np.nan)\
                                     .replace('-8887', np.nan) \
                                     .replace('-1.0', np.nan)\
                                     .replace('-1', np.nan)
        categorical_vars_summary = X_categorical.nunique().to_frame('N_categories')
        cat_list = []
        for col in categorical_vars:
            if X_categorical[col].count() == 0:
                pass
            else:
                cat_count = X_categorical[col].value_counts().head(3)
                if len(cat_count) == 3:
                    col_result = pd.Series({'1st类别': str(cat_count.index.values[0]) + ' #=' + str(cat_count.iloc[0])\
                                                     + ', %=' + str(np.round(cat_count.iloc[0] * 1.0 / len(X), 2)),\
                                            '2nd类别': str(cat_count.index.values[1]) + ' #=' + str(cat_count.iloc[1])\
                                                     + ', %=' + str(np.round(cat_count.iloc[1] * 1.0 / len(X), 2)),\
                                            '3rd类别': str(cat_count.index.values[2]) + ' #=' + str(cat_count.iloc[2])\
                                                     + ', %=' + str(np.round(cat_count.iloc[2] * 1.0 / len(X), 2))\
                                            })\
                                            .to_frame().transpose()
                elif len(cat_count) == 2:
                    col_result = pd.Series({'1st类别': str(cat_count.index.values[0]) + ' #=' + str(cat_count.iloc[0])\
                                                     + ', %=' + str(np.round(cat_count.iloc[0] * 1.0 / len(X), 2)),\
                                            '2nd类别': str(cat_count.index.values[1]) + ' #=' + str(cat_count.iloc[1])\
                                                     + ', %=' + str(np.round(cat_count.iloc[1] * 1.0 / len(X), 2)),\
                                            })\
                                            .to_frame().transpose()
                elif len(cat_count) == 1:
                    col_result = pd.Series({'1st类别': str(cat_count.index.values[0]) + ' #=' + str(cat_count.iloc[0])\
                                                    + ', %=' + str(np.round(cat_count.iloc[0] * 1.0 / len(X), 2))})\
                                            .to_frame().transpose()
                else:
                    pass

                col_result.index = [col]
                cat_list.append(col_result)

        cat_df = pd.concat(cat_list)

    # merge all summaries
    if len(numerical_vars) > 0 and len(categorical_vars) > 0:
        all_variable_summary = variable_summary.merge(numerical_vars_summary, how='left', left_index=True, right_index=True)
        all_variable_summary = all_variable_summary.merge(categorical_vars_summary, how='left', left_index=True, right_index=True)
        all_variable_summary = all_variable_summary.merge(cat_df, how='left', left_index=True, right_index=True)
    elif len(numerical_vars) > 0 and len(categorical_vars) == 0:
        all_variable_summary = variable_summary.merge(numerical_vars_summary, how='left', left_index=True, right_index=True)
    elif len(categorical_vars) > 0 and len(numerical_vars) == 0:
        all_variable_summary = variable_summary.merge(categorical_vars_summary, how='left', left_index=True, right_index=True)
        all_variable_summary = all_variable_summary.merge(cat_df, how='left', left_index=True, right_index=True)
    else:
        return None

    all_variable_summary.loc[:, 'exclusion_reason'] = None
    all_variable_summary.loc[all_variable_summary.pct_NA > uniqvalue_cutoff, 'exclusion_reason'] = '缺失NA比例大于{}'.format(uniqvalue_cutoff)
    all_variable_summary.loc[all_variable_summary.pct_0 > uniqvalue_cutoff, 'exclusion_reason'] = '0值比例大于{}'.format(uniqvalue_cutoff)
    if len(categorical_vars) > 0:
        all_variable_summary.loc[all_variable_summary.N_categories == 1, 'exclusion_reason'] = '只有一个分类'
        all_variable_summary.loc[all_variable_summary.N_categories > 100, 'exclusion_reason'] = '类别变量的类别数过多'
    all_variable_summary.loc[useless_vars, 'exclusion_reason'] = '无用变量'
    '''
    vincio_vars = [i for i in all_variable_summary.index.values if isinstance(i, str) and 'vincio' in i]
    vincio_duplicate = [i.replace('vincio_', '') for i in vincio_vars]
    vincio_duplicate = list(set(vincio_duplicate).intersection(set(all_variable_summary.index.values)))
    vincio_vars = vincio_vars + vincio_duplicate
    all_variable_summary.loc[vincio_vars, 'exclusion_reason'] = 'vincio变量'

    bairong_vars = [i for i in all_variable_summary.index.values if isinstance(i, str) and 'al_m6' in i]
    all_variable_summary.loc[bairong_vars, 'exclusion_reason'] = 'bairong变量'
    '''
    # change exempt_vars exclusion_reason to None
    all_variable_summary.loc[exempt_vars, 'exclusion_reason'] = None


    # check
    # all_variable_summary.exclusion_reason.value_counts()
    # save
    all_variable_summary = all_variable_summary.drop('N_available', 1)
    final_output = all_variable_summary.reset_index()\
                             .rename(columns={'index':'var_code',\
                                              'pct_NA': 'NA占比',\
                                              'N_0': '0值数量',\
                                              'pct_0': '0值占比',\
                                              'N_-9999': '-9999值数量',\
                                              'pct_-9999': '-9999值占比',\
                                              'N_-8888': '-8888值数量',\
                                              'pct_-8888': '-8888值占比',\
                                              'N_-8887': '-8887值数量',\
                                              'pct_-8887': '-8887值占比', \
                                              'N_-1': '-1值数量', \
                                              'pct_-1': '-1值占比', \
                                              'N_categories': '类别数量'})

    # read var_code explanation
    final_output = var_dict.loc[:, ['数据源', '指标英文', '指标中文', '数据类型', '指标类型']]\
                    .merge(final_output, left_on='指标英文', right_on='var_code')\
                    .drop('var_code', 1)

    cols = list(final_output.columns.values)
    reorder_cols = cols[:5] + [cols[-1]] + [cols[5]] + [cols[14]] + cols[6:14] +  cols[15:-1]
    if len(set(reorder_cols).intersection(set(final_output.columns))) != len(set(final_output.columns)):
        return final_output.columns
    # reorder_cols = reduce(lambda x, y: x.append(y), order_list)
    final_output = final_output[reorder_cols]
    final_output.to_excel(os.path.join(data_path, '%s_variables_summary.xlsx' % save_label), \
                       encoding='utf-8', index=False)
    return final_output

# convert to the right data type
def convert_right_data_type(data, var_dict, quick_mode=False):
    """
    Convert to the right data type, have to apply it after processing the missing value.
    Even though the data are converted to the correct type before saving, reading
    it back will change it. For example, some categorical variables are coded
    as integers, they should be string type for usage, but reading it back, it
    will be loaded in as integers.

    Args:
    data (pd.DataFrame): X数据, 已经处理过缺失值
    var_dict(pd.DataFrame): 变量字典
    quick_mode (bool): default=False. If set True, 将会批量转化数据类型，如果某一列
        有问题，则数据整体将会转化失败。

    Returns:
    failed_conversion (list): if quick_mode=False, 转换失败的变量名list

    """
    data = data.copy()

    cols = data.columns.values
    failed_conversion = []

    if quick_mode:
        col_types = var_dict.loc[var_dict['指标英文'].isin(cols), ['指标英文', '数据类型']]
        for col_type in ['varchar', 'float', 'integer']:
            onetype_cols = col_types.loc[col_types['数据类型']==col_type, '指标英文'].tolist()
            try:
                if col_type == 'varchar':
                    data[onetype_cols] = data[onetype_cols].astype(str)
                elif col_type == 'float':
                    data[onetype_cols] = data[onetype_cols].astype(float)
                elif col_type == 'integer':
                    data[onetype_cols] = data[onetype_cols].astype(int)
            except:
                print("one column may have problem, so conversion for type %s failed" % col_type)

        return data

    else:
        cols = list(set(cols).intersection(set(var_dict['指标英文'].tolist())))
        for col in cols:
            right_type = var_dict.loc[var_dict['指标英文']==col, '数据类型'].iloc[0]
            non_missing_data = data.loc[~data[col].isin([-8888, -9999, -8887]), col]
            if non_missing_data.empty:
                current_type = right_type
            else:
                current_type = str(type(data.loc[~data[col].isin([-8888, -9999, -8887]), col].iloc[0]))

            if right_type == 'varchar':
                if 'float' in current_type:
                    try:
                        data[col] = data[col].astype(int).astype(str)
                    except:
                        print(col + ' conversion failed')
                        failed_conversion.append(col)
                else:
                    try:
                        data[col] = data[col].astype(str)
                    except:
                        print(col + ' conversion failed')
                        failed_conversion.append(col)
                if 'object' in str(data[col].dtype):
                    pass
                else:
                    print(col + ' conversion failed. Type should be object, but is still %s' % str(data[col].dtype))
                    failed_conversion.append(col)

            if right_type == 'float':
                try:
                    data[col] = data[col].astype(float)
                except:
                    print(col + ' conversion failed')
                    failed_conversion.append(col)

                if 'float' in str(data[col].dtype):
                    pass
                else:
                    print(col + ' conversion failed. Type should be float, but is still %s' % str(data[col].dtype))
                    failed_conversion.append(col)

            if right_type == 'integer':
                try:
                    data[col] = data[col].astype(int)
                except:
                    print(col + ' conversion failed')
                    failed_conversion.append(col)

                if 'int' in str(data[col].dtype):
                    pass
                else:
                    print(col + ' conversion failed. Type should be int, but is still %s' % str(data[col].dtype))
                    failed_conversion.append(col)

        return data, np.unique(failed_conversion)


def var_process(bankCode, city, education, industryInvolved, job,
                maritalStatus, occupationType, province, appNums=None):
    # App
    appOriginalVarList = ["v1", "v3", "v4", "v5", "v6", "v7", "v8", "v10", "v11", "v12", "v13", "v14", "v15", "v17",
                          "v18", "v19", "v20", "v21", "v22", "v24", "v25", "v26", "v27", "v28", "v29", "v31", "v32",
                          "v33", "v34", "v35", "v36", "v38", "v39", "v40", "v41", "v42", "v43", "v45", "v46", "v47",
                          "v48", "v49", "v50", "v52", "v53", "v54", "v55", "v56", "v57", "v59", "v60", "v61", "v62",
                          "v63", "v64", "v66", "v67", "v68", "v69", "v70", "v71", "v73", "v74", "v75", "v76", "v77",
                          "v78", "v80", "v81", "v82", "v83", "v84", "v85", "v87", "v88", "v89", "v90", "v91", "v92",
                          "v94", "v95", "v96", "v97", "v98", "v99", "v101", "v102", "v103", "v104", "v105", "v106",
                          "v108", "v109", "v110", "v111", "v112", "v113", "v115", "v116", "v117", "v118", "v119",
                          "v120", "v122", "v123", "v124", "v125", "v126", "v127", "v129", "v130", "v131", "v132",
                          "v133", "v134", "v136", "v137", "v138", "v139", "v140", "v141", "v143", "v144", "v145",
                          "v146", "v147", "v148", "v150", "v151", "v152", "v153", "v154", "v155", "v157", "v158",
                          "v159", "v160", "v161", "v162", "v164", "v165", "v166", "v167", "v168", "v169", "v171",
                          "v172", "v173", "v174", "v175", "v176", "v178", "v179", "v180", "v181", "v182", "v183",
                          "v185", "v186", "v187", "v188", "v189", "v190", "v192", "v193", "v194", "v195", "v196",
                          "v197", "v199", "v200", "v201", "v202", "v203", "v204", "v206", "v207", "v208", "v209",
                          "v210", "v211", "v213", "v214", "v215", "v216", "v217", "v218", "v220", "v221", "v222",
                          "v223", "v224", "v225", "v227", "v228", "v229", "v230", "v231", "v232", "v234", "v235",
                          "v236", "v237", "v238", "v239", "v241", "v242", "v243", "v244", "v245", "v246", "v248",
                          "v249", "v250", "v251", "v252", "v253", "v255", "v256", "v257", "v258", "v259", "v260",
                          "v262", "v263", "v264", "v265", "v266", "v267", "v269", "v270", "v271", "v272", "v273",
                          "v274", "v276", "v277", "v278"]
    appVarList = ["v41_app_1", "v41_app_3", "v41_app_4", "v41_app_5", "v41_app_6", "v41_app_7", "v41_app_8",
                  "v41_app_10", "v41_app_11", "v41_app_12", "v41_app_13", "v41_app_14", "v41_app_15", "v41_app_17",
                  "v41_app_18", "v41_app_19", "v41_app_20", "v41_app_21", "v41_app_22", "v41_app_24", "v41_app_25",
                  "v41_app_26", "v41_app_27", "v41_app_28", "v41_app_29", "v41_app_31", "v41_app_32", "v41_app_33",
                  "v41_app_34", "v41_app_35", "v41_app_36", "v41_app_38", "v41_app_39", "v41_app_40", "v41_app_41",
                  "v41_app_42", "v41_app_43", "v41_app_45", "v41_app_46", "v41_app_47", "v41_app_48", "v41_app_49",
                  "v41_app_50", "v41_app_52", "v41_app_53", "v41_app_54", "v41_app_55", "v41_app_56", "v41_app_57",
                  "v41_app_59", "v41_app_60", "v41_app_61", "v41_app_62", "v41_app_63", "v41_app_64", "v41_app_66",
                  "v41_app_67", "v41_app_68", "v41_app_69", "v41_app_70", "v41_app_71", "v41_app_73", "v41_app_74",
                  "v41_app_75", "v41_app_76", "v41_app_77", "v41_app_78", "v41_app_80", "v41_app_81", "v41_app_82",
                  "v41_app_83", "v41_app_84", "v41_app_85", "v41_app_87", "v41_app_88", "v41_app_89", "v41_app_90",
                  "v41_app_91", "v41_app_92", "v41_app_94", "v41_app_95", "v41_app_96", "v41_app_97", "v41_app_98",
                  "v41_app_99", "v41_app_101", "v41_app_102", "v41_app_103", "v41_app_104", "v41_app_105",
                  "v41_app_106", "v41_app_108", "v41_app_109", "v41_app_110", "v41_app_111", "v41_app_112",
                  "v41_app_113", "v41_app_115", "v41_app_116", "v41_app_117", "v41_app_118", "v41_app_119",
                  "v41_app_120", "v41_app_122", "v41_app_123", "v41_app_124", "v41_app_125", "v41_app_126",
                  "v41_app_127", "v41_app_129", "v41_app_130", "v41_app_131", "v41_app_132", "v41_app_133",
                  "v41_app_134", "v41_app_136", "v41_app_137", "v41_app_138", "v41_app_139", "v41_app_140",
                  "v41_app_141", "v41_app_143", "v41_app_144", "v41_app_145", "v41_app_146", "v41_app_147",
                  "v41_app_148", "v41_app_150", "v41_app_151", "v41_app_152", "v41_app_153", "v41_app_154",
                  "v41_app_155", "v41_app_157", "v41_app_158", "v41_app_159", "v41_app_160", "v41_app_161",
                  "v41_app_162", "v41_app_164", "v41_app_165", "v41_app_166", "v41_app_167", "v41_app_168",
                  "v41_app_169", "v41_app_171", "v41_app_172", "v41_app_173", "v41_app_174", "v41_app_175",
                  "v41_app_176", "v41_app_178", "v41_app_179", "v41_app_180", "v41_app_181", "v41_app_182",
                  "v41_app_183", "v41_app_185", "v41_app_186", "v41_app_187", "v41_app_188", "v41_app_189",
                  "v41_app_190", "v41_app_192", "v41_app_193", "v41_app_194", "v41_app_195", "v41_app_196",
                  "v41_app_197", "v41_app_199", "v41_app_200", "v41_app_201", "v41_app_202", "v41_app_203",
                  "v41_app_204", "v41_app_206", "v41_app_207", "v41_app_208", "v41_app_209", "v41_app_210",
                  "v41_app_211", "v41_app_213", "v41_app_214", "v41_app_215", "v41_app_216", "v41_app_217",
                  "v41_app_218", "v41_app_220", "v41_app_221", "v41_app_222", "v41_app_223", "v41_app_224",
                  "v41_app_225", "v41_app_227", "v41_app_228", "v41_app_229", "v41_app_230", "v41_app_231",
                  "v41_app_232", "v41_app_234", "v41_app_235", "v41_app_236", "v41_app_237", "v41_app_238",
                  "v41_app_239", "v41_app_241", "v41_app_242", "v41_app_243", "v41_app_244", "v41_app_245",
                  "v41_app_246", "v41_app_248", "v41_app_249", "v41_app_250", "v41_app_251", "v41_app_252",
                  "v41_app_253", "v41_app_255", "v41_app_256", "v41_app_257", "v41_app_258", "v41_app_259",
                  "v41_app_260", "v41_app_262", "v41_app_263", "v41_app_264", "v41_app_265", "v41_app_266",
                  "v41_app_267", "v41_app_269", "v41_app_270", "v41_app_271", "v41_app_272", "v41_app_273",
                  "v41_app_274", "v41_app_276", "v41_app_277", "v41_app_278"]
    appList = ['4shared', 'AdaKami', 'AdaKita', 'Adobe Acrobat', 'Agoda', 'Airy', 'AKULAKU', 'Alkitab', 'Anak-anak',
               'AppStation', 'B612', 'BABE', 'baca', 'BCA mobile', 'BeautyPlus', 'bima+', 'Biugo', 'Blibli.com',
               'BNIMobilenew', 'BPJSTKU', 'BRI Mobile', 'Browser', 'Bubble Shooter', 'Bukalapak', 'Cair Kilat',
               'Calculator', 'Camera360', 'CamScanner', 'Candy Crush Saga', 'Candy Crush Soda', 'Canva', 'Cashcash Pro',
               'Cashcepat', 'Cashwagon', 'Cermati', 'CGV CINEMAS', 'Cinema 21', 'Clean Master', 'CM Launcher',
               'ColorNote', 'com.mfashiongallery.emag', 'com.samsung.updatecarriermatch', 'DANA', 'Danabijak',
               'DanaRupiah', 'DanaSegera', 'DelimaKotak', 'detikcom', 'Digibank Indonesia', 'Dokumen', 'DompetKilat',
               'Drive', 'DU Recorder', 'Duit Plus', 'Duo', 'EasyShare', 'Email', 'Excel', 'Facebook',
               'Facemoji Keyboard for Xiaomi', 'FastSave', 'Forum MIUI', 'Foto', 'Free Fire', 'Go-bantu', 'GO-LIFE',
               'Go Deti', 'GO Keyboard', 'Go Mobile', 'GOJEK', 'Google Berita', 'Google Pinyin Input',
               'Google Play Film', 'Google Play Games', 'Google Play Movies u0026 TV', 'Google Play Music',
               'Google Play Musik', 'Google Play services for Instant Apps', 'Grab', 'Hago', 'Helix Jump',
               'Homescapes', 'HOOQ', 'i.saku', 'iDana-Pinjam', 'iflix', 'Indodana', 'Indonesia live sticker',
               'InShot', 'Instagram', 'iReader', 'JD.id', 'Jenius', 'JobStreet', 'JOOX', 'Ju0026T<U+00A0>Express',
               'JULO', 'Kalkulator', 'Keyboard SwiftKey', 'KineMaster', 'KLO Bugreport', 'Koi Tunai',
               'Komedi putar Wallpaper', 'Kredit Pintar', 'KreditCepat', 'Kreditpedia', 'KreditQ', 'KreditUang',
               'Kredivo', 'KTA KILAT', 'Kunci', 'Kunci Layar Satu Ketuk', 'Kwai Go', 'Layout', 'Lazada', 'LINE',
               'LinkAja', 'LinkedIn', 'Lite', 'Ludo King', 'mab', 'maucash', 'MAXstream', 'Mega Ekspres', 'Messenger',
               'Messenger Lite', 'Mi Community', 'Mi Roaming', 'MiChat', 'Mobile JKN', 'Mobile Legends: Bang Bang',
               'Modal Nasional', 'MP3 Video Converter', 'MX Player', 'My Home Credit Indonesia', 'My JNE',
               'My Smartfren', 'myIM3', 'myIndiHome', 'MyTelkomsel', 'myXL', 'OLX', 'Opera Mini', 'Outlook', 'OVO',
               'Parallel Space', 'Parallel Space 64Bit Support', 'Peel Mi Remote', 'Pegadaian Digital', 'Pegipegi',
               'Pengunduh Video - untuk Instagram', 'Perekam Suara', 'Permata VIP', 'PhotoGrid', 'Photos', 'PicMix',
               'PicsArt', 'Pinjam Modal', 'pinjam tunai', 'Pinjam Yuk', 'PinjamanGo', 'Pinjamania', 'PinjamDuit',
               'PKV Games', 'POKETO', 'Pouch', 'PowerPoint', 'PrivyID', 'PUBG MOBILE', 'Rupiah Cepat', 'S LIME',
               'Samsung Gift Indonesia', 'Samsung Health', 'Samsung Internet', 'Samsung Max', 'Samsung Notes',
               'Saya Modalin', 'SDK Game Space', 'SeDana', 'SHAREit', 'Shopee', 'Skype', 'Smart Switch', 'Smart Tutor',
               'Smart<U+200B>Things', 'Smule', 'Snapchat', 'SoundCloud', 'Spotify', 'Spreadsheet',
               'Stiker pribadi untuk WhatsApp', 'Subway Surf', 'SwiftKey factory settings', 'Tantan', 'Telegram',
               'Temukan Perangkat Saya', 'Terjemahan', 'tiket.com', 'TikTok', 'Tokopedia', 'TouchPal',
               'TouchPal Englishgb Pack', 'TouchPal for OPPO', 'TouchPal Indonesian Pack', 'TouchPal Pinyin/Bihua Pack',
               'Translate', 'Traveloka', 'Truecaller', 'TubeMate', 'TunaiKita', 'Tunaiku', 'TunaiSaku', 'Turbo VPN',
               'Twitter', 'UangKita', 'UangMe', 'UangTeman', 'Utang Dulu', 'Video Player', 'Vidio', 'VidMate', 'Viu',
               'VivaVideo', 'VSCO', 'Wall In', 'Waze', 'WEBTOON', 'WeChat', 'WeSing', 'WhatsApp Business',
               'WhatsApp Wallpaper', 'Word', 'WPS Office', 'Yahoo Mail', 'YouTube Go', 'ZALORA', 'Zedge', 'Zilingo']
    idfList = [3.35416638, 1.00903038, 2.653731296, 3.490538046, 3.410034828, 3.432382126, 0.425302054, 3.166980324,
               3.454084863, 3.616739653, 2.809439204, 2.304083056, 3.287475005, 0.904080524, 2.763828693, 2.726324298,
               3.461037382, 2.29679705, 2.602671431, 1.768413229, 2.453249226, 3.399045706, 3.666889436, 1.215268764,
               3.072773636, 3.611319585, 3.08387605, 2.882987892, 2.784303231, 3.652704801, 3.537115092, 2.179780591,
               1.012943494, 1.195647149, 2.884946757, 3.403426874, 2.288483248, 2.92835925, 3.61267185, 3.382785185,
               1.698778577, 2.386749557, 1.894604077, 3.063354414, 0.665317106, 3.701776695, 3.948873488, 3.090276072,
               3.267155003, 3.578093937, 3.001260285, 1.339611731, 3.205854477, 3.003465357, 1.398304422, 2.588965882,
               2.58218291, 2.353934112, 0.572864422, 2.930409131, 3.246301167, 2.139591668, 1.50883478, 2.85595922,
               3.473910585, 2.514059562, 3.34584167, 3.428998028, 3.49413733, 0.612753493, 3.375286489, 2.248266188,
               1.536279608, 2.804602436, 3.54720345, 3.503799241, 1.51015622, 0.917971527, 0.711979157, 1.885661419,
               3.537115092, 3.582023216, 2.461346437, 3.605928736, 2.994674092, 2.7196539, 1.286021425, 3.418913909,
               3.436912145, 0.358068447, 2.446902685, 2.420678498, 3.254779785, 3.139628192, 1.507185431, 2.010840603,
               1.575554615, 2.586053258, 3.27195578, 3.665461884, 2.922234699, 3.568985221, 1.995416133, 0.189260254,
               2.757479465, 2.578327483, 1.062508061, 3.675497811, 1.169207595, 0.877654323, 2.473833144, 2.284531384,
               2.994674092, 3.259521378, 0.717350701, 1.23641383, 2.627622826, 2.673108703, 1.920177145, 2.541496125,
               3.557394621, 2.243092871, 2.833359388, 3.54720345, 0.627824038, 2.894139205, 2.082622822, 2.445638196,
               2.720762548, 2.304448754, 1.761801282, 1.268864302, 3.666889436, 3.458714501, 1.429071462, 3.565106742,
               2.975171848, 2.554092513, 3.276779716, 1.003039947, 2.777236064, 1.64989337, 2.417397117, 2.684276061,
               1.284041879, 3.054023087, 2.902086267, 2.967334259, 3.605928736, 2.992488298, 2.892820813, 2.80099015,
               2.644438509, 1.923420256, 3.478633144, 3.00052634, 2.483848468, 3.149797771, 3.463365672, 0.648906554,
               1.530358928, 3.393596101, 1.504059143, 3.502586385, 3.877610468, 2.736977608, 2.675756308, 2.946961233,
               2.434745755, 0.351976423, 2.768471766, 2.752887035, 2.879081637, 2.608603233, 3.017544878, 2.652176489,
               2.327761558, 2.944183453, 3.282599325, 0.739818456, 0.661489757, 2.857229868, 3.342737684, 2.201321894,
               3.052476296, 2.80099015, 2.494849679, 3.549741523, 2.267819884, 3.549741523, 3.143006573, 3.309214992,
               2.859138873, 3.376354295, 2.166310214, 3.406725393, 2.396326645, 2.376083842, 2.209271588, 0.913410724,
               2.565899025, 2.004859074, 2.893479792, 1.902647996, 1.991663783, 3.349995362, 1.197338175, 3.224954648,
               3.495339976, 0.629535488, 1.234028243, 3.406725393, 2.335654852, 2.020981844, 2.835223322, 0.794940306,
               3.142160907, 3.125396055, 3.599230788, 2.863607441, 3.301254751, 2.664683166, 3.007890143, 3.661191415,
               2.549876062, 2.192126427, 3.088672222, 3.406725393, 3.644289604, 3.197779063, 3.491736369, 2.298977283,
               1.076122396, 2.119326928, 2.068945903, 3.533358044, 3.204054296, 3.620824069]

    appVars_df = pd.DataFrame({'var': appVarList, 'app': appList, 'idf': idfList, 'value': [-1] * len(appList)})
    appVars_df['original_var'] = appOriginalVarList
    if appNums is not None:
        appVars_df['value'] = appVars_df.apply(
            lambda x: x['idf'] * appNums[x['app']] if x['app'] in appNums.keys() else 0, axis=1)


    # bankCode
    bankOriginalVarList = ["bank_code.BNI.", "bank_code.BCA.", "bank_code.BRI.", "bank_code.MANDIRI.",
                           "bank_code.CIMB.", "bank_code.other."]
    bankList = ['BNI', 'BCA', 'BRI', 'MANDIRI', 'CIMB']
    bankCode_df = pd.DataFrame({'var': ['v41_bank_code_BNI', 'v41_bank_code_BCA', 'v41_bank_code_BRI',
                                        'v41_bank_code_MANDIRI', 'v41_bank_code_CIMB', 'v41_bank_code_other'],
                                'value': [0, 0, 0, 0, 0, 0], 'original_var': bankOriginalVarList})
    if bankCode in bankList:
        bankCode_df.iloc[bankList.index(bankCode), 1] = 1
    else:
        bankCode_df.iloc[-1, 1] = 1

    # city
    cityOriginalVarList = ['city.KAB__BANDUNG.', 'city.KAB__BEKASI.', 'city.KAB__BOGOR.', 'city.KAB__SIDOARJO.',
                           'city.KAB__TANGERANG.', 'city.KOTA_ADM__JAKARTA_BARAT.', 'city.KOTA_ADM__JAKARTA_PUSAT.',
                           'city.KOTA_ADM__JAKARTA_SELATAN.', 'city.KOTA_ADM__JAKARTA_TIMUR.',
                           'city.KOTA_ADM__JAKARTA_UTARA.', 'city.KOTA_BANDUNG.', 'city.KOTA_BEKASI.',
                           'city.KOTA_BOGOR.', 'city.KOTA_DEPOK.', 'city.KOTA_MEDAN.', 'city.KOTA_SEMARANG.',
                           'city.KOTA_SURABAYA.', 'city.KOTA_TANGERANG.', 'city.KOTA_TANGERANG_SELATAN.', 'city.other.']
    cityList = ['KAB. BANDUNG', 'KAB. BEKASI', 'KAB. BOGOR', 'KAB. SIDOARJO', 'KAB. TANGERANG',
                'KOTA ADM. JAKARTA BARAT', 'KOTA ADM. JAKARTA PUSAT', 'KOTA ADM. JAKARTA SELATAN',
                'KOTA ADM. JAKARTA TIMUR', 'KOTA ADM. JAKARTA UTARA', 'KOTA BANDUNG', 'KOTA BEKASI',
                'KOTA BOGOR', 'KOTA DEPOK', 'KOTA MEDAN', 'KOTA SEMARANG', 'KOTA SURABAYA', 'KOTA TANGERANG',
                'KOTA TANGERANG SELATAN']
    cityVarList = ['v41_city_' + c for c in cityList] + ['v41_city_other']
    city_df = pd.DataFrame({'var': cityVarList, 'value': [0] * len(cityVarList), 'original_var': cityOriginalVarList})
    if city in cityList:
        city_df.iloc[cityList.index(city), 1] = 1
    else:
        city_df.iloc[-1, 1] = 1

    # education
    educationOriginalVarList = ['education.ASSOCIATE_BACHELOR_1.', 'education.ASSOCIATE_BACHELOR_3.',
                                'education.JUNIOR_HIGH_SCHOOL.', 'education.REGULAR_COLLEGE_COURSE.',
                                'education.SENIOR_HIGH_SCHOOL.', 'education.other.']
    educationList = ['ASSOCIATE_BACHELOR_1', 'ASSOCIATE_BACHELOR_3', 'JUNIOR_HIGH_SCHOOL',
                     'REGULAR_COLLEGE_COURSE', 'SENIOR_HIGH_SCHOOL']
    educationVarList = ['v41_education_' + e for e in educationList] + ['v41_education_other']
    education_df = pd.DataFrame({'var': educationVarList, 'value': [0] * len(educationVarList),
                                 'original_var': educationOriginalVarList})
    if education in educationList:
        education_df.iloc[educationList.index(education), 1] = 1
    else:
        education_df.iloc[-1, 1] = 1

    # industryInvolved
    industryInvolvedOriginalVarList = ['industry_involved.ARCHITECTURE.', 'industry_involved.EDUCATION.',
                                       'industry_involved.FINANCIAL.', 'industry_involved.OTHER.',
                                       'industry_involved.PABRIK.', 'industry_involved.RETAIL.',
                                       'industry_involved.ROOM_BOARD.', 'industry_involved.TRADING_INDUSTRY.',
                                       'industry_involved.TRANSPORTASI_LOGISTIK.', 'industry_involved.other.']
    industryInvolvedList = ['ARCHITECTURE', 'EDUCATION', 'FINANCIAL', 'OTHER', 'PABRIK',
                            'RETAIL', 'ROOM_BOARD', 'TRADING_INDUSTRY', 'TRANSPORTASI_LOGISTIK']
    industryInvolvedVarList = ['v41_industry_involved_' + i for i in industryInvolvedList] + ['v41_industry_involved_other']
    industryInvolved_df = pd.DataFrame({'var': industryInvolvedVarList, 'value': [0] * len(industryInvolvedVarList),
                                        'original_var': industryInvolvedOriginalVarList})
    if industryInvolved in industryInvolvedList:
        industryInvolved_df.iloc[industryInvolvedList.index(industryInvolved), 1] = 1
    else:
        industryInvolved_df.iloc[-1, 1] = 1

    # job
    jobOriginalVarList = ['job.MANAJER.', 'job.Missing.', 'job.STAF.', 'job.SUPERVISO.', 'job.other.']
    jobList = ['MANAJER', 'Missing', 'STAF', 'SUPERVISO']
    jobVarList = ['v41_job_' + j for j in jobList] + ['v41_job_other']
    job_df = pd.DataFrame({'var': jobVarList, 'value': [0] * len(jobVarList), 'original_var': jobOriginalVarList})
    if job in jobList:
        job_df.iloc[jobList.index(job), 1] = 1
    else:
        job_df.iloc[-1, 1] = 1

    # maritalStatus
    maritalStatusOriginalVarList = ['marital_status.BEREFT.', 'marital_status.DIVORCED.', 'marital_status.MARRIED.',
                                     'marital_status.SPINSTERHOOD.']
    maritalStatusList = ['BEREFT', 'DIVORCED', 'MARRIED', 'SPINSTERHOOD']
    maritalStatusVarList = ['v41_marital_status_' + j for j in maritalStatusList]
    maritalStatus_df = pd.DataFrame({'var': maritalStatusVarList, 'value': [0] * len(maritalStatusVarList),
                                     'original_var': maritalStatusOriginalVarList})
    if maritalStatus in maritalStatusList:
        maritalStatus_df.iloc[maritalStatusList.index(maritalStatus), 1] = 1

    # occupationType
    occupationTypeOriginalVarList = ['occupation_type.ENTREPRENEUR.', 'occupation_type.OFFICE.',
                                     'occupation_type.other.']
    occupationTypeList = ['ENTREPRENEUR', 'OFFICE']
    occupationTypeVarList = ['v41_occupation_type_' + j for j in occupationTypeList] + ['v41_occupation_type_other']
    occupationType_df = pd.DataFrame({'var': occupationTypeVarList, 'value': [0] * len(occupationTypeVarList),
                                      'original_var': occupationTypeOriginalVarList})
    if occupationType in occupationTypeList:
        occupationType_df.iloc[occupationTypeList.index(occupationType), 1] = 1
    else:
        occupationType_df.iloc[-1, 1] = 1

    # province
    provinceOriginalVarList = ['province.BALI.', 'province.BANTEN.', 'province.DAERAH_ISTIMEWA_YOGYAKARTA.',
                               'province.DKI_JAKARTA.', 'province.JAWA_BARAT.', 'province.JAWA_TENGAH.',
                               'province.JAWA_TIMUR.', 'province.SULAWESI_SELATAN.', 'province.SUMATERA_UTARA.',
                               'province.other.']
    provinceList = ['BALI', 'BANTEN', 'DAERAH ISTIMEWA YOGYAKARTA', 'DKI JAKARTA',
                    'JAWA BARAT', 'JAWA TENGAH', 'JAWA TIMUR', 'SULAWESI SELATAN', 'SUMATERA UTARA']
    provinceVarList = ['v41_province_' + j for j in provinceList] + ['v41_province_other']
    province_df = pd.DataFrame({'var': provinceVarList, 'value': [0] * len(provinceVarList),
                                'original_var': provinceOriginalVarList})
    if province in provinceList:
        province_df.iloc[provinceList.index(province), 1] = 1
    else:
        province_df.iloc[-1, 1] = 1

    # concat all variable
    var_df = pd.concat([appVars_df[['var', 'value', 'original_var']], bankCode_df, city_df, education_df,
                        industryInvolved_df, job_df, maritalStatus_df, occupationType_df, province_df], ignore_index=True)

    # data_list -- To predict score
    columnsSequence = ["v1", "v3", "v4", "v5", "v6", "v7", "v8", "v10", "v11", "v12", "v13", "v14", "v15", "v17", "v18",
                       "v19", "v20", "v21", "v22", "v24", "v25", "v26", "v27", "v28", "v29", "v31", "v32", "v33", "v34",
                       "v35", "v36", "v38", "v39", "v40", "v41", "v42", "v43", "v45", "v46", "v47", "v48", "v49", "v50",
                       "v52", "v53", "v54", "v55", "v56", "v57", "v59", "v60", "v61", "v62", "v63", "v64", "v66", "v67",
                       "v68", "v69", "v70", "v71", "v73", "v74", "v75", "v76", "v77", "v78", "v80", "v81", "v82", "v83",
                       "v84", "v85", "v87", "v88", "v89", "v90", "v91", "v92", "v94", "v95", "v96", "v97", "v98", "v99",
                       "v101", "v102", "v103", "v104", "v105", "v106", "v108", "v109", "v110", "v111", "v112", "v113",
                       "v115", "v116", "v117", "v118", "v119", "v120", "v122", "v123", "v124", "v125", "v126", "v127",
                       "v129", "v130", "v131", "v132", "v133", "v134", "v136", "v137", "v138", "v139", "v140", "v141",
                       "v143", "v144", "v145", "v146", "v147", "v148", "v150", "v151", "v152", "v153", "v154", "v155",
                       "v157", "v158", "v159", "v160", "v161", "v162", "v164", "v165", "v166", "v167", "v168", "v169",
                       "v171", "v172", "v173", "v174", "v175", "v176", "v178", "v179", "v180", "v181", "v182", "v183",
                       "v185", "v186", "v187", "v188", "v189", "v190", "v192", "v193", "v194", "v195", "v196", "v197",
                       "v199", "v200", "v201", "v202", "v203", "v204", "v206", "v207", "v208", "v209", "v210", "v211",
                       "v213", "v214", "v215", "v216", "v217", "v218", "v220", "v221", "v222", "v223", "v224", "v225",
                       "v227", "v228", "v229", "v230", "v231", "v232", "v234", "v235", "v236", "v237", "v238", "v239",
                       "v241", "v242", "v243", "v244", "v245", "v246", "v248", "v249", "v250", "v251", "v252", "v253",
                       "v255", "v256", "v257", "v258", "v259", "v260", "v262", "v263", "v264", "v265", "v266", "v267",
                       "v269", "v270", "v271", "v272", "v273", "v274", "v276", "v277", "v278",
                       "marital_status.MARRIED.", "marital_status.SPINSTERHOOD.", "marital_status.BEREFT.",
                       "marital_status.DIVORCED.", "education.SENIOR_HIGH_SCHOOL.", "education.ASSOCIATE_BACHELOR_3.",
                       "education.REGULAR_COLLEGE_COURSE.", "education.ASSOCIATE_BACHELOR_1.", "education.other.",
                       "education.JUNIOR_HIGH_SCHOOL.", "occupation_type.OFFICE.", "occupation_type.ENTREPRENEUR.",
                       "occupation_type.other.", "industry_involved.OTHER.", "industry_involved.ARCHITECTURE.",
                       "industry_involved.other.", "industry_involved.FINANCIAL.", "industry_involved.PABRIK.",
                       "industry_involved.TRADING_INDUSTRY.", "industry_involved.EDUCATION.",
                       "industry_involved.RETAIL.", "industry_involved.TRANSPORTASI_LOGISTIK.",
                       "industry_involved.ROOM_BOARD.", "job.STAF.", "job.Missing.", "job.MANAJER.",
                       "job.SUPERVISO.", "job.other.", "bank_code.BNI.", "bank_code.BCA.", "bank_code.BRI.",
                       "bank_code.MANDIRI.", "bank_code.CIMB.", "bank_code.other.", "province.DKI_JAKARTA.",
                       "province.JAWA_BARAT.", "province.BALI.", "province.BANTEN.", "province.JAWA_TIMUR.",
                       "province.SUMATERA_UTARA.", "province.other.", "province.JAWA_TENGAH.",
                       "province.DAERAH_ISTIMEWA_YOGYAKARTA.", "province.SULAWESI_SELATAN.",
                       "city.KOTA_ADM__JAKARTA_BARAT.", "city.KAB__BOGOR.", "city.KOTA_BEKASI.",
                       "city.other.", "city.KOTA_TANGERANG_SELATAN.", "city.KAB__TANGERANG.", "city.KOTA_TANGERANG.",
                       "city.KOTA_SURABAYA.", "city.KOTA_MEDAN.", "city.KOTA_ADM__JAKARTA_SELATAN.", "city.KOTA_BOGOR.",
                       "city.KOTA_ADM__JAKARTA_PUSAT.", "city.KOTA_BANDUNG.", "city.KOTA_ADM__JAKARTA_TIMUR.",
                       "city.KOTA_ADM__JAKARTA_UTARA.", "city.KAB__BANDUNG.", "city.KAB__BEKASI.", "city.KOTA_DEPOK.",
                       "city.KAB__SIDOARJO.", "city.KOTA_SEMARANG."]
    data_list = var_df[['original_var', 'value']].set_index('original_var').reindex(columnsSequence).T

    # save_list -- To save values in mongoDB
    # save_list = var_df[['var', 'value']]

    return data_list  # , save_list

# json解析函数
def from_json(data, var_name: str):
    """
    :param data: dataframe
    :param var_name: column name of json in dataframe, json object: dict or [dict]
    :return:
    """
    a1 = data.copy()
    a1 = a1[~a1[var_name].isna()].reset_index(drop=True)
    other_col_list = list(a1.columns)
    other_col_list.remove(var_name)

    a1[var_name] = a1[var_name].map(lambda x: json.loads(x) if isinstance(x, str) else x)

    if not isinstance(a1[var_name][0], dict) or not isinstance(a1[var_name][1], dict):
        a1[var_name] = a1[var_name].map(lambda x: [{'temp': None}] if len(x) == 0 else x)
        list_len = list(map(len, a1[var_name].values))
        newvalues = np.hstack((np.repeat(a1[other_col_list].values, list_len, axis=0),
                               np.array([np.concatenate(a1[var_name].values)]).T))
        a1 = pd.DataFrame(data=newvalues, columns=other_col_list + [var_name])

    start = time.time()
    # 新增一列'columns'用于存储每一列的json串的字段名
    a1['columns'] = a1[str(var_name)].map(
        lambda x: list(x.keys()) if isinstance(x, dict) else list(json.loads(x).keys()))
    print('new columns done')
    # 获取json串中的所有字段名称
    add_columns_list = list(set(list(itertools.chain(*a1['columns']))))
    for columns in add_columns_list:
        # 将json串展开
        a1[str(columns)] = a1[str(var_name)].map(
            lambda x: x.get(str(columns)) if isinstance(x, dict) else json.loads(x).get(str(columns)))
        print(str(columns))
    if 'temp' in a1.columns:
        del a1['temp']
    del a1['columns'], a1[str(var_name)]
    end = time.time()
    print("run time = {}".format(end - start))

    return a1

def get_newusemodelv4_var(data):
    raw_var = []
    app_data = data[['loanid','packages']]
    x = app_data.copy()
    for row in range(app_data.shape[0]):
        #print(row)
        try:
            json.loads(app_data.iloc[row, 1])
        except:
            x.drop(index=row, inplace=True)
    x.reset_index(drop=True, inplace=True)
    a = from_json(x, 'packages')
    b = a.loc[:, ['loanid', 'appName']]
    # 只需要筛选出模型里用到的app，然后进行统计
    appList = ['4shared', 'AdaKami', 'AdaÎKita', 'Adobe Acrobat', 'Agoda', 'Airy', 'AKULAKU', 'Alkitab', 'Anak-anak',
                   'AppStation', 'B612', 'BABE', 'baca', 'BCA mobile', 'BeautyPlus', 'bima+', 'Biugo', 'Blibli.com',
                   'BNIMobilenew', 'BPJSTKU', 'BRI Mobile', 'Browser', 'Bubble Shooter', 'Bukalapak', 'Cair Kilat',
                   'Calculator', 'Camera360', 'CamScanner', 'Candy Crush Saga', 'Candy Crush Soda', 'Canva', 'Cashcash Pro',
                   'Cashcepat', 'Cashwagon', 'Cermati', 'CGV CINEMAS', 'Cinema 21', 'Clean Master', 'CM Launcher',
                   'ColorNote', 'com.mfashiongallery.emag', 'com.samsung.updatecarriermatch', 'DANA', 'Danabijak',
                   'DanaRupiah', 'DanaSegera', 'DelimaKotak', 'detikcom', 'Digibank Indonesia', 'Dokumen', 'DompetKilat',
                   'Drive', 'DU Recorder', 'Duit Plus', 'Duo', 'EasyShare', 'Email', 'Excel', 'Facebook',
                   'Facemoji Keyboard for Xiaomi', 'FastSave', 'Forum MIUI', 'Foto', 'Free Fire', 'Go-bantu', 'GO-LIFE',
                   'Go Deti', 'GO Keyboard', 'Go Mobile', 'GOJEK', 'Google Berita', 'Google Pinyin Input',
                   'Google Play Film', 'Google Play Games', 'Google Play Movies u0026 TV', 'Google Play Music',
                   'Google Play Musik', 'Google Play services for Instant Apps', 'Grab', 'Hago', 'Helix Jump',
                   'Homescapes', 'HOOQ', 'i.saku', 'iDana-Pinjam', 'iflix', 'Indodana', 'Indonesia live sticker',
                   'InShot', 'Instagram', 'iReader', 'JD.id', 'Jenius', 'JobStreet', 'JOOX', 'Ju0026T<U+00A0>Express',
                   'JULO', 'Kalkulator', 'Keyboard SwiftKey', 'KineMaster', 'KLO Bugreport', 'Koi Tunai',
                   'Komedi putar Wallpaper', 'Kredit Pintar', 'KreditCepat', 'Kreditpedia', 'KreditQ', 'KreditUang',
                   'Kredivo', 'KTA KILAT', 'Kunci', 'Kunci Layar Satu Ketuk', 'Kwai Go', 'Layout', 'Lazada', 'LINE',
                   'LinkAja', 'LinkedIn', 'Lite', 'Ludo King', 'mab', 'maucash', 'MAXstream', 'Mega Ekspres', 'Messenger',
                   'Messenger Lite', 'Mi Community', 'Mi Roaming', 'MiChat', 'Mobile JKN', 'Mobile Legends: Bang Bang',
                   'Modal Nasional', 'MP3 Video Converter', 'MX Player', 'My Home Credit Indonesia', 'My JNE',
                   'My Smartfren', 'myIM3', 'myIndiHome', 'MyTelkomsel', 'myXL', 'OLX', 'Opera Mini', 'Outlook', 'OVO',
                   'Parallel Space', 'Parallel Space 64Bit Support', 'Peel Mi Remote', 'Pegadaian Digital', 'Pegipegi',
                   'Pengunduh Video - untuk Instagram', 'Perekam Suara', 'Permata VIP', 'PhotoGrid', 'Photos', 'PicMix',
                   'PicsArt', 'Pinjam Modal', 'pinjam tunai', 'Pinjam Yuk', 'PinjamanGo', 'Pinjamania', 'PinjamDuit',
                   'PKV Games', 'POKETO', 'Pouch', 'PowerPoint', 'PrivyID', 'PUBG MOBILE', 'Rupiah Cepat', 'S LIME',
                   'Samsung Gift Indonesia', 'Samsung Health', 'Samsung Internet', 'Samsung Max', 'Samsung Notes',
                   'Saya Modalin', 'SDK Game Space', 'SeDana', 'SHAREit', 'Shopee', 'Skype', 'Smart Switch', 'Smart Tutor',
                   'Smart<U+200B>Things', 'Smule', 'Snapchat', 'SoundCloud', 'Spotify', 'Spreadsheet',
                   'Stiker pribadi untuk WhatsApp', 'Subway Surf', 'SwiftKey factory settings', 'Tantan', 'Telegram',
                   'Temukan Perangkat Saya', 'Terjemahan', 'tiket.com', 'TikTok', 'Tokopedia', 'TouchPal',
                   'TouchPal Englishgb Pack', 'TouchPal for OPPO', 'TouchPal Indonesian Pack', 'TouchPal Pinyin/Bihua Pack',
                   'Translate', 'Traveloka', 'Truecaller', 'TubeMate', 'TunaiKita', 'Tunaiku', 'TunaiSaku', 'Turbo VPN',
                   'Twitter', 'UangKita', 'UangMe', 'UangTeman', 'Utang Dulu', 'Video Player', 'Vidio', 'VidMate', 'Viu',
                   'VivaVideo', 'VSCO', 'Wall In', 'Waze', 'WEBTOON', 'WeChat', 'WeSing', 'WhatsApp Business',
                   'WhatsApp Wallpaper', 'Word', 'WPS Office', 'Yahoo Mail', 'YouTube Go', 'ZALORA', 'Zedge', 'Zilingo']
    b = b[b['appName'].isin(appList)].reset_index(drop=True)

    # 统计每笔订单，对应用户的app安装情况（该用户手机里，每个app安装个数，大部分都是1，少数情况有人手机里的某一个app安装了多个）
    c = pd.DataFrame(b.groupby(by=['loanid', 'appName'])['loanid'].count()).rename(columns = {'loanid':'count'}).reset_index()
    data.loanid = data.loanid.astype(str)
    c.loanid = c.loanid.astype(str)
    raw_var = []
    for index, row in data.iterrows():
        print(index)
        a = pd.DataFrame()
        appNums = {}
        if row['loanid'] in c.loanid.values:
            a = c[c.loanid == row['loanid']].reset_index(drop=True)
            for i in range(a.shape[0]):
                appNums[a['appName'][i]] = a['count'][i]
        else:
            appNums = None
        var_list = var_process(bankCode=row['bankcode'], city=row['city'], education=row['education'],
                               industryInvolved=row['industryinvolved'], job=row['job'],
                               maritalStatus=row['maritalstatus'],
                               occupationType=row['occupationtype'], province=row['province'], appNums=appNums)
        raw_var.append(var_list)
    raw_var_2 = pd.concat(raw_var)
    raw_var_2 = raw_var_2.reset_index(drop=True)
    data_2 = data.merge(raw_var_2, left_index=True, right_index=True, how='left')
    return(data_2)




def make_patch_spines_invisible(ax):
    ax.set_frame_on(True)
    ax.patch.set_visible(False)
    for sp in ax.spines.values():
        sp.set_visible(False)

# def var_chart9(data, 'createtime', eda_default):
#     #还未debug
#     plt.figure(figsize=(16, 8))
#     length = len(topn_var)
#     cntPlt = int(np.ceil(length / 9.0))
#     figlist = []
#     for i in list(range(1, cntPlt + 1)):
#         fig = plt.figure(i)
#         figlist.append(fig)
#         j = 1
#         for col in length[(i - 1) * 9:i * 9]:
#             plt.subplot(3, 3, j)
#             for varcols in topn_var:
#                 print(varcols)
#                 if eda_default.loc[eda_default.指标英文 == varcols, '数据类型'] == 'varchar':
#                     categorical_var_chart(data_out['x_categorical'], 'createtime', eda_default)
#                 else:
#                     numerical_var_chart(data_out['x_numerical'], 'createtime', eda_default)
#         #pdpChart_compat(model, df, col, predictors, n_bins, dfltValue=dfltValue, maxVal=df[col].quantile(maxValRatio))
#         j += 1
#     plt.tight_layout()


# def categorical_var_chart(data,time_cols, eda_default, report_path):
#     """
#     categorical柱状图
#     """
#     colors = ['khaki','steelblue','indianred','yellow']
#     pic_dict = {}
#     FIG_PATH = os.path.join(report_path, 'figure')
#     if not os.path.exists(FIG_PATH):
#         os.makedirs(FIG_PATH)
#     bar_width = 0.3
#     for i in data.keys():
#         data[i] = data[i].sort_values(time_cols,  ascending= True)
#         x_data = data[i][time_cols]
#         #print(x_data)
#         color_index = 0
#         space_scale = len(data[i].keys()) -3
#         #print(len(data[i].keys()) -3 )
#         plt.figure(figsize=(space_scale * 1 + 10, 10)) #每多一个类别，图的长宽比调大
#         for (k,v) in data[i].items():
#             if k not in ([time_cols,'%s_cnt'%i]):#[time_cols,'%s_cnt'%i, '%s_na'%i]
#                 y_data = v.copy()
#                 # plt.figure(figsize=(20, 10))
#                 k = k.replace('NA', 'missing rate')
#                 plt.bar(left= np.arange(len(x_data)) * (1 + bar_width*space_scale) + color_index*bar_width, height=y_data, label=k, align='edge',color=colors[color_index % len(colors)], alpha=0.8,width=bar_width)
#                 color_index += 1
#         plt.xticks(np.arange(len(x_data)) * (1 + bar_width*space_scale) + color_index * bar_width / 2, x_data, rotation=20)
#         plt.title("%s distribution" %i)
#
#         plt.xlabel("apply_time")
#         plt.ylabel("distribution")
#
#         plt.legend(loc = 'upper right')
#         i = i.replace('.','_')
#         path = os.path.join(FIG_PATH, "categorical" + "_" + i + ".png")
#         plt.savefig(path, format='png', dpi=100)
#         plt.close()
#         names = i + '_picture'
#         pic_dict[names] = "categorical" + "_" + i + ".png"
#     return(pic_dict)

def calculate_week(df, time_cols, n_week):
    df[time_cols] = df[time_cols].apply(lambda x:str(x)[0:10])
    for i in range(1,n_week + 1):
        start_day = (pd.to_datetime(df.createtime.max(), format='%Y-%m-%d') - dr.relativedelta(days = 7 * i)).strftime("%Y-%m-%d")
        end_day = (pd.to_datetime(df.createtime.max(), format='%Y-%m-%d') - dr.relativedelta(days = 7 * (i-1))).strftime("%Y-%m-%d")
        df.loc[(df.createtime> start_day) & (df.createtime <= end_day), 'week_time'] = '%sTo%s'%(start_day.replace('-','/'),end_day.replace('-','/'))
    return(df)

def send_weekly_report(query_time,in_dict, var_dict, psi_dict, auc_dict = None, send_report = True):
    #输出结果
    print('start')
    print(in_dict['report_path'])
    os.chdir(os.path.join(in_dict['report_path'],'figure' ))
    print('success')
    save_file_name = in_dict['report_path'] + '%s %s monitoring weekly.xlsx' %(in_dict['product_name'], in_dict['model_name'])
    print(save_file_name)
    workbook = xlsxwriter.Workbook(save_file_name)
    worksheet = workbook.add_worksheet('变量及分监控')
    i = 0
    height = 800
    for name, data in var_dict.items():
        print(data)
        worksheet.insert_image('B1', data,  options = {'y_offset': 0 + i * height, 'x_scale':0.8, 'y_scale':1})
        i = i + 1
    worksheet2 = workbook.add_worksheet('模型PSI监控')
    for name, data in psi_dict.items():
        worksheet2.insert_image('B1',data, options={'y_offset': 0, 'x_scale': 0.5, 'y_scale': 0.5})
    if auc_dict is not None:
        worksheet3 = workbook.add_worksheet('模型AUC监控')
        worksheet3.insert_image('B1',auc_dict['auc_pict'] , options={'y_offset': 0, 'x_scale': 0.8, 'y_scale': 0.8})
    workbook.close()

    if send_report:
        mailtest(["riskcontrol-business@huojintech.com"]
                 , in_dict['weekly_param']['recipients']
                 , "%s周报 %s" %(in_dict['model_name'], query_time)
                 , "【模型监控周报】\
                    \n 产品: %s\
                    \n 模型名: %s" %(in_dict['product_name'], in_dict['model_name'])
                 ,save_file_name)

def returnSum(myDict):
    sum = 0
    for i in myDict:
        sum = sum + myDict[i]
    return sum

def var_process_V5(df, high_freq_list, mid_freq_list, low_freq_list, appNums=None):
    # APP-idf
    appVarList = [
                'AdaKami','Agen Masukan Market','ConfigUpdater','DanaRupiah','Dokumen','Facebook',
                'Facebook Services','File','Finmas','Foto','Gmail','Google','Instagram',
                'KTA KILAT','Kalender','Key Chain','Kontak','Kredinesia','Kredit Pintar','KreditQ','Layanan Google Play',
                'Mesin Google Text-to-speech','Messenger','MyTelkomsel','Pemasang Sertifikat','Penyimpanan Kontak',
                'Rupiah Cepat','SHAREit','Setelan','Setelan Penyimpanan','SmartcardService','Tema','TunaiKita',
                'UKU','UangMe','Unduhan','WPS Office','com.android.carrierconfig','com.android.smspush'
                ]

    idfList = [
                0.609512319728349,0.116859362855901,0.0705971720851384,0.381625615114676,0.564089030401483,0.166831590740874,
                0.254862212694988,0.296673400064685,0.634021595871793,0.130317349354273,0.0686055814737916,0.0977660358379332,
                0.202797495889244,0.538575047364931,0.131167856083055,0.249920044250338,0.290379556788847,0.534058946199967,
                0.200620358374656,0.501606922418829,0.115714787706426,0.122700522395568,0.331462599775473,0.516360131615968,
                0.231907225034456,2.82036568504324,0.255256085962573,0.391960891327856,0.560990840081422,0.128400582275882,
                0.327896319602918,0.423948373334698,0.415817032090843,0.0357186797922932,0.344573472757627,0.566402054992248,
                0.494542266040498,0.148344836502514,0.220119034478748
                ]
    appVars_df = pd.DataFrame({'var': appVarList, 'app': appVarList, 'idf': idfList, 'value': [-1] * len(appVarList)})
    if appNums is not None:
        appVars_df['value'] = appVars_df.apply(lambda x: x['idf'] * appNums[x['app']]/ len(list(appNums.keys())) if x['app'] in appNums.keys() else 0, axis=1)
    ## all tfidf variable
    var_df = appVars_df[['var', 'value']].reset_index(drop=True)
    data_df = var_df[['var', 'value']].set_index('var').T
    # APP-freq
    if appNums is not None:
        df_appnums = pd.DataFrame.from_dict(appNums, orient='index', columns=['cnt']).reset_index()
        df_appnums.columns = ['app','cnt']
        high = pd.DataFrame(high_freq_list)
        high.columns = ['app']
        mid = pd.DataFrame(mid_freq_list)
        mid.columns = ['app']
        low = pd.DataFrame(low_freq_list)
        low.columns = ['app']
        df_appnums_high = high.merge(df_appnums, on='app', how='left')
        df_appnums_mid = mid.merge(df_appnums, on='app', how='left')
        df_appnums_low = low.merge(df_appnums, on='app', how='left')
        data_df['highFreqAppV5'] = df_appnums_high.cnt.sum()
        data_df['midFreqAppV5'] = df_appnums_mid.cnt.sum()
        data_df['lowFreqAppV5'] = df_appnums_low.cnt.sum()
        cnt_app = returnSum(appNums)
        data_df['rateHighFreqAppV5'] = data_df['highFreqAppV5']/cnt_app
        data_df['rateMidFreqAppV5'] = data_df['midFreqAppV5']/cnt_app
        data_df['rateLowFreqAppV5'] = data_df['lowFreqAppV5']/cnt_app
    else:
        data_df['highFreqAppV5'] = -1
        data_df['midFreqAppV5'] = -1
        data_df['lowFreqAppV5'] = -1
        data_df['rateHighFreqAppV5'] = -1
        data_df['rateMidFreqAppV5'] = -1
        data_df['rateLowFreqAppV5'] = -1
    # all variables
    df=df.reset_index(drop=True)
    data_df=data_df.reset_index(drop=True)
    data_df=data_df.drop(['highFreqAppV5','lowFreqAppV5'],1)
    var_all = pd.concat([df, data_df], axis=1)
    return var_all



def get_v5_app_var(data_prepared, app_list_high, app_list_mid, app_list_low):
    # 读取高中低频app列表
    f = open(app_list_high, "r", encoding='utf-8')
    str_high = f.read()
    high_freq_list = str_high[2:-2].split("\", \"")
    len(high_freq_list)  # 141
    f.close()

    f = open(app_list_mid, "r", encoding='utf-8')
    str_mid = f.read()
    mid_freq_list = str_mid[2:-2].split("\", \"")
    len(mid_freq_list)  # 103
    f.close()

    f = open(app_list_low, "r", encoding='utf-8')
    str_low = f.read()
    low_freq_list = str_low[2:-7].split("\", \"")
    low_freq_list.append('')
    len(low_freq_list)  # 27522
    f.close()

    # 衍生APP变量
    data_all = pd.DataFrame()
    for i in range(data_prepared.shape[0]):
        df = data_prepared[i:i + 1]
        appNums = df['appNums'][i]
        if type(appNums) == str:
            if appNums.find(",\"Bixby\"") >= 0:
                a = appNums.replace(",\"Bixby\"", "Bixby")
                appNums = eval(a)
        if type(appNums) == dict:
            df_app = var_process_V5(df, high_freq_list, mid_freq_list, low_freq_list, appNums=appNums)
        else:
            df_app = var_process_V5(df, high_freq_list, mid_freq_list, low_freq_list, appNums=None)
        data_all = pd.concat([data_all, df_app], 0)
    data_all = data_all.drop(['appNums'], 1)
    data_all = data_all.fillna(-1)
    data_all.gender = data_all.gender.astype(float)
    data_all.iziPhoneVerifyResult = data_all.iziPhoneVerifyResult.astype(float)
    data_all.gender = data_all.gender.astype(int)
    data_all.iziPhoneVerifyResult = data_all.iziPhoneVerifyResult.astype(int)
    return (data_all)




