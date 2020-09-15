#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : summary_statistics.py
@Time    : 2020-09-11 23:15
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import os
import pandas as pd
import numpy as np

def eda(X, useless_vars = [], special_value = [], var_dict = None, result_path = None, save_label = None, cutoff=0.97):
    """
    :param X: DataFrame,数据集
    :param useless_vars: 不需要分析的列
    :param special_value: 需单独统计的数值或者类型
    :param var_dict: 数据字典，默认为None，如果有 包含['数据源', '指标英文', '指标中文', '数据类型', '指标类型', '是否可用']
    :param result_path: 文件存储位置，默认为None
    :param save_label: 文件名，格式为'%s_EDA.xlsx' % save_label
    :param cutoff:  判断异常的阈值
    :return: DataFrame  EDA
    """
    variable_summary = X.count().to_frame('N_available')
    variable_summary.loc[:, 'N'] = X.shape[0]
    if special_value:
        for values in special_value:
            if values != '':
                variable_summary.loc[:, 'N_%s' % values] = (X.isin([values])).sum()
                variable_summary.loc[:, 'pct_%s' % values] = np.round(variable_summary['N_%s' % values] * 1.0 / variable_summary.N, 3)
            else:
                variable_summary.loc[:, 'N_NA'] = (X.isna()).sum()
                variable_summary.loc[:, 'pct_NA'] = np.round(variable_summary['N_NA'] * 1.0 / variable_summary.N, 3)
    # 如果有字典就按照字典中的类型来，如果没有就系统自己判断
    try:
        numerical_vars = var_dict.loc[(var_dict['数据类型'] != 'varchar') & ~pd.isnull(var_dict['指标英文']), '指标英文'].tolist()
        categorical_vars = var_dict.loc[(var_dict['数据类型'] == 'varchar') & ~pd.isnull(var_dict['指标英文']), '指标英文'].tolist()
        numerical_vars = list(set(numerical_vars).intersection(set(X.columns)))
        categorical_vars = list(set(categorical_vars).intersection(set(X.columns)))
    except:
        numerical_vars = X.select_dtypes(exclude=['object']).columns
        categorical_vars = X.select_dtypes('O').columns

    # the following can only be done for continuous variable
    if len(numerical_vars) > 0:
        X_numerical = X[numerical_vars].apply(lambda x: x.astype(float), 0).replace(special_value, [np.nan]* len(special_value))

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
        X_categorical = X_categorical.replace(special_value, [np.nan]* len(special_value))
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
    all_variable_summary.loc[all_variable_summary.pct_NA > cutoff, 'exclusion_reason'] = '缺失NA比例大于{}'.format(cutoff)
    all_variable_summary.loc[all_variable_summary.pct_0 > cutoff, 'exclusion_reason'] = '0值比例大于{}'.format(cutoff)
    if len(categorical_vars) > 0:
        all_variable_summary.loc[all_variable_summary.N_categories == 1, 'exclusion_reason'] = '只有一个分类'
        all_variable_summary.loc[all_variable_summary.N_categories > 100, 'exclusion_reason'] = '类别变量的类别数过多'
    all_variable_summary.loc[useless_vars, 'exclusion_reason'] = '无用变量'

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
    final_output = final_output[reorder_cols]
    final_output.to_excel(os.path.join(result_path, '%s_EDA.xlsx' % save_label),encoding='utf-8', index=False)
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