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
    vars_summary = X.count().to_frame('N_available')
    vars_summary.loc[:, 'N'] = X.shape[0]
    if special_value:
        for values in special_value:
            if (values == '') or (np.isnan(values)):
                vars_summary.loc[:, 'N_NA'] = (X.isna()).sum()
                vars_summary.loc[:, 'pct_NA'] = np.round(vars_summary['N_NA'] * 1.0 / vars_summary.N, 3)
            else:
                vars_summary.loc[:, 'N_%s' % values] = (X.isin([values])).sum()
                vars_summary.loc[:, 'pct_%s' % values] = np.round(vars_summary['N_%s' % values] * 1.0 / vars_summary.N, 3)
    # 如果有字典就按照字典中的类型来，如果没有就系统自己判断
    try:
        no_vars = var_dict.loc[(var_dict['数据类型'] != 'varchar') & ~pd.isnull(var_dict['指标英文']), '指标英文'].tolist()
        cate_vars = var_dict.loc[(var_dict['数据类型'] == 'varchar') & ~pd.isnull(var_dict['指标英文']), '指标英文'].tolist()
        no_vars = list(set(no_vars).intersection(set(X.columns)))
        cate_vars = list(set(cate_vars).intersection(set(X.columns)))
    except:
        no_vars = X.select_dtypes(exclude=['object']).columns
        cate_vars = X.select_dtypes('O').columns

    # the following can only be done for continuous vars
    if len(no_vars) > 0:
        X_no = X[no_vars].apply(lambda x: x.astype(float), 0).replace(special_value, [np.nan]* len(special_value))

        no_vars_summary = X_no.mean().round(6).to_frame('mean')
        no_vars_summary.loc[:, 'std'] = X_no.std().round(6)
        no_vars_summary.loc[:, 'median'] = X_no.median().round(6)
        no_vars_summary.loc[:, 'min'] = X_no.min()
        no_vars_summary.loc[:, 'max'] = X_no.max()
        no_vars_summary.loc[:, 'p01'] = X_no.quantile(0.01)
        no_vars_summary.loc[:, 'p05'] = X_no.quantile(q=0.05)
        no_vars_summary.loc[:, 'p10'] = X_no.quantile(q=0.10)
        no_vars_summary.loc[:, 'p25'] = X_no.quantile(q=0.25)
        no_vars_summary.loc[:, 'p75'] = X_no.quantile(q=0.75)
        no_vars_summary.loc[:, 'p90'] = X_no.quantile(q=0.90)
        no_vars_summary.loc[:, 'p95'] = X_no.quantile(q=0.95)
        no_vars_summary.loc[:, 'p99'] = X_no.quantile(q=0.99)

    # the following are for cate_vars
    if len(cate_vars) > 0:
        X_cate = X[cate_vars].copy()
        X_cate = X_cate.replace(special_value, ['']* len(special_value))
        cate_vars_summary = X_cate.nunique().to_frame('N_categories')
        cat_list = []
        for col in cate_vars:
            if X_cate[col].count() == 0:
                pass
            else:
                cat_count = X_cate[col].value_counts().head(3)
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
    if len(no_vars) > 0 and len(cate_vars) > 0:
        all_vars_summary = vars_summary.merge(no_vars_summary, how='left', left_index=True, right_index=True)
        all_vars_summary = all_vars_summary.merge(cate_vars_summary, how='left', left_index=True, right_index=True)
        all_vars_summary = all_vars_summary.merge(cat_df, how='left', left_index=True, right_index=True)
    elif len(no_vars) > 0 and len(cate_vars) == 0:
        all_vars_summary = vars_summary.merge(no_vars_summary, how='left', left_index=True, right_index=True)
    elif len(cate_vars) > 0 and len(no_vars) == 0:
        all_vars_summary = vars_summary.merge(cate_vars_summary, how='left', left_index=True, right_index=True)
        all_vars_summary = all_vars_summary.merge(cat_df, how='left', left_index=True, right_index=True)
    else:
        return None

    all_vars_summary.loc[:, 'exclusion_reason'] = None
    all_vars_summary.loc[all_vars_summary.pct_NA > cutoff, 'exclusion_reason'] = '缺失NA比例大于{}'.format(cutoff)
    if 0 in special_value:
        all_vars_summary.loc[all_vars_summary.pct_0 > cutoff, 'exclusion_reason'] = '0值比例大于{}'.format(cutoff)
    if len(cate_vars) > 0:
        all_vars_summary.loc[all_vars_summary.N_categories == 1, 'exclusion_reason'] = '只有一个分类'
        all_vars_summary.loc[all_vars_summary.N_categories > 100, 'exclusion_reason'] = '类别变量的类别数过多'
    all_vars_summary.loc[useless_vars, 'exclusion_reason'] = '无用变量'

    final_output = all_vars_summary.reset_index().rename(columns = {'index': '指标英文'})
    if var_dict.values.any():
        final_output = pd.merge(var_dict, final_output, on = '指标英文')
    try:
        final_output.to_excel(os.path.join(result_path, '%s_EDA.xlsx' % save_label), encoding='utf-8', index=False)
    except:
        print('There is no storage path!')
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