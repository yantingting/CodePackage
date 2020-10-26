#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : common_fun.py
@Time    : 2020-09-10 00:39
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import numpy as np
import pandas as pd



'''
把woebin的返回值转换成dataframe
'''

def convert_bins_to_df(bins):
    df_v = pd.DataFrame()
    for col in bins.keys():
        df_v = df_v.append(bins[col], ignore_index=True)
    for col2 in ['count_distr', 'badprob', 'cum_good', 'cum_bad']:
        df_v[col2] = df_v[col2].apply(lambda x: '%.3f%%' % round(x, 4) * 100)
    for col3 in ['woe', 'bin_iv', 'total_iv', 'bin_ks', 'KS']:
        df_v[col3] = round(df_v[col3].astype(float), 4)

    return df_v.sort_values(by=['total_iv', 'variable'], ascending=False)

'''
card转换成dataframe，作为最终评分卡形式展现
'''






def get_cols_lessthan_twobins(bins):
    '''
    取得bins中分箱小于等于2的变量列表，分箱小于等于2的变量在使用stepwise选择变量过程中，会出现异常，因此需要剔除掉
    '''
    df_bins = convert_bins_to_df(bins)
    ll = []
    for k, group in df_bins.groupby('variable'):
        if group.shape[0] <= 2:
            ll.append(k)
    return ll


'''
从bins中获取每个feature的total_iv
'''
def get_ivlist_from_bins(bins):
    list_iv = []
    for col in bins.keys():
        iv = round(bins[col]['total_iv'][0], 3)
        list_iv.append([col, iv])
    df_iv = pd.DataFrame(np.array(list_iv), columns=['feature_name', 'iv'])
    df_iv['iv'] = df_iv['iv'].astype(float)
    return df_iv.sort_values(by='iv', axis=0, ascending=False).reset_index()


