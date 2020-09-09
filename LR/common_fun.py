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
import datetime
import re
import time


def print_time(strMsg=''):
    if strMsg != '':
        print(strMsg + ': ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    else:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


def GetFileNameWithTime(FileName=''):
    txt = FileName.split(".", 1)
    if len(txt) == 2:
        strTime = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        strFileName = txt[0] + '_' + strTime + '.' + txt[1]
        return strFileName
    else:
        return FileName


'''
把woebin的返回值转换成dataframe
'''


def convert_bins_to_df(bins):
    df_v = pd.DataFrame()
    for col in bins.keys():
        df_v = df_v.append(bins[col], ignore_index=True)
    df_v['count_distr'] = round(df_v['count_distr'].astype(float), 4) * 100
    df_v['badprob'] = round(df_v['badprob'].astype(float), 4) * 100
    df_v['woe'] = round(df_v['woe'].astype(float), 4)
    df_v['bin_iv'] = round(df_v['bin_iv'].astype(float), 4)
    df_v['total_iv'] = round(df_v['total_iv'].astype(float), 4)

    return df_v.sort_values(by=['total_iv', 'variable'], ascending=False)


'''
card转换成dataframe，作为最终评分卡形式展现
'''


def get_scorecard(card):
    df_v = pd.DataFrame()
    for col in card.keys():
        df_v = df_v.append(card[col], ignore_index=True)
    return df_v


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


'''
从bins中获取指定feature的相关信息
'''


def get_feature_info_from_bins(bins, list_feature, list_info=['variable', 'total_iv']):
    bins_feature = {}
    for name in list_feature:
        str_name = re.sub('_woe$', '', name)
        if str_name in bins.keys():
            bins_feature[name] = bins[str_name]
        else:
            print("{}不存在bins中".format(str_name))

    df_total_col = convert_bins_to_df(bins_feature)
    list_col = []
    if list_info is not None:
        for col in list_info:
            if col in df_total_col.columns:
                list_col.append(col)
    else:
        list_col = df_total_col.columns

    return df_total_col[list_col].drop_duplicates().reset_index()


def drop_columns(df, list_col_to_drop):
    exist_col = []
    for col in list_col_to_drop:
        if col in df.columns:
            exist_col.append(col)
    exist_col = list(set(exist_col))
    print('删除前总列数：{}, 需要删除列数：{}, 被删除列数：{}, 删除后总列数：{}'.format(len(df.columns), len(list_col_to_drop), len(exist_col),
                                                             len(df.columns) - len(exist_col)))
    return df.drop(exist_col, axis=1)


def print_current_time():
    print(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))


def fillna(df, list_col, fillvalue):
    for col in list_col:
        if col in df.columns:
            df[col] = df[col].fillna(fillvalue)
        else:
            print("{}列不存在".format(col))
    return df


'''
total_row：在打印train，test样本占总体样本比例时，需要输入总体样本行数
'''


def print_sample_info(df, y_header, title='', total_row=0):
    print('\n-----------{}-----------'.format(title))
    print_time()
    print('数据集行列', df.shape)
    print('逾期率:{}%'.format(round(len(df[df[y_header] == 1]) / df.shape[0], 4) * 100))
    if total_row > 0:
        print('{}样本占总体样本比例:{}%'.format(title, round(df.shape[0] / total_row, 4) * 100))
    print(df[y_header].value_counts())


def agg_col(df, list_col, new_col_name, func='sum'):
    exist_col = []
    for col in list_col:
        if col not in df.columns:
            exist_col.append(col)

    if len(exist_col):
        print('数据集中不存在指定的列:{}'.format(list_col))

    if func == 'sum':
        df[new_col_name] = df[list_col].apply(lambda x: x.sum(), axis=1)
    elif func == 'max':
        df[new_col_name] = df[list_col].apply(lambda x: x.max(), axis=1)
    elif func == 'min':
        df[new_col_name] = df[list_col].apply(lambda x: x.min(), axis=1)
    return df


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


def calc_psi(col_list, df_train, df_test):
    psi_list = []
    for var in col_list:
        df_var = pd.DataFrame({'train_var': df_train[var].value_counts(), 'test_var': df_test[var].value_counts()})
        df_var['train_var_per'] = df_var['train_var'] / df_var['train_var'].sum()
        df_var['test_var_per'] = df_var['test_var'] / df_var['test_var'].sum()
        df_var['psi'] = np.nansum((df_var['test_var_per'] - df_var['train_var_per']) * np.log(
            df_var['test_var_per'] / df_var['train_var_per']))
        var_psi = df_var['psi'].mean()
        psi_list.append(var_psi)

    #         print('{}:\tPSI={}'.format(var, var_psi))

    df_psi = pd.DataFrame({'var': col_list, 'psi': psi_list})
    return df_psi

