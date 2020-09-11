# encoding=utf8
"""
对数据进行Exploratory data analysis和极值处理

Owner： 胡湘雨

最近更新：2017-12-07
"""
#import os
import pandas as pd
import numpy as np
#from utils3.misc_utils import *
#from jinja2 import Template

#try:
#    import xgboost as xgb
#except:
#    pass


def eda(X, var_dict, useless_vars, exempt_vars, uniqvalue_cutoff=0.97):
    """
    计算N, missing value percent, mean, min, max, percentiles 等。

    Args:

    X (pd.DataFrame()): 是一个宽表，每一列是一个变量，每一行是一个obs
    var_dict (pd.DataFrame()): 标准变量字典表，包含以下这些列：'数据源', '指标英文', '指标中文', '数据类型', '指标类型', '是否可用'
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
    final_output = var_dict.loc[:, ['数据源', '指标英文', '指标中文', '数据类型', '指标类型', '是否可用']]\
                    .merge(final_output, left_on='指标英文', right_on='var_code')\
                    .drop('var_code', 1)

    cols = list(final_output.columns.values)
    reorder_cols = cols[:5] + [cols[-1]] + [cols[5]] + [cols[14]] + cols[6:14] +  cols[15:-1]
    if len(set(reorder_cols).intersection(set(final_output.columns))) != len(set(final_output.columns)):
        return final_output.columns
    # reorder_cols = reduce(lambda x, y: x.append(y), order_list)

    final_output = final_output[reorder_cols]
    #final_output.to_excel(os.path.join(data_path, '%s_variables_summary.xlsx' % save_label), \
    #                    encoding='utf-8', index=False)
    return final_output

def get_badRate_and_dist_by_time(cat_data_with_y_and_time,select_vars,time_var_name,y_name):
    '''
    该函数为统计变量按统计时间每个分箱的逾期率以及分布

    Args:

    cat_data_with_y_and_time(DataFrame):含有时间以及y数据的data
    select_vars(list):要统计的字段
    time_var_name(varchar)：时间相关的字段名
    y_name(varchar):y的名称

    Returns：
    New DataFrame:变量按统计时间每个分箱的逾期率以及分布
    '''
    group_data_list = []
    for i in select_vars:
        tmp = cat_data_with_y_and_time.groupby([i,time_var_name])[y_name]\
        .agg([('allNum',len),('badNum',sum)\
          ,('badRate',lambda x : sum(x)/len(x))]).unstack()
        tmp2 = tmp.reset_index().rename(columns = {i:'bins'})
        tmp2['varName'] = i
        columns = pd.MultiIndex.from_arrays([['dist' for i in range(len(list((tmp2['allNum']/tmp2['allNum'].sum()).columns)))],
                                    list((tmp2['allNum']/tmp2['allNum'].sum()).columns)]\
                                    , names=[time_var_name,'bins'])
        tmp3 = pd.DataFrame((tmp2['allNum']/tmp2['allNum'].sum()).values, columns=columns)
        group_data_list.append(pd.concat([tmp2,tmp3],axis=1)[['varName','bins','allNum','badNum','dist','badRate']])
    return pd.concat(group_data_list)