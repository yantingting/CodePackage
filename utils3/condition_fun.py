#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : condition_fun.py
@Time    : 2020-09-10 00:42
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import pandas as pd
import numpy as np
import warnings
import re
from pandas.api.types import is_numeric_dtype


def str_to_list(x):
    if x is not None and isinstance(x, str):
        x = [x]
    return x


# remove date time columns # rm_datetime_col
# remove columns if len(x.unique()) == 1

def rmcol_datetime_unique1(dat, check_char_num=False):  # add more datatime types later
    '''
    对特征待分箱中特征类数检验，大于50进行警告
    对日期型数据检查并删除
    '''
    if check_char_num:
        # character columns with too many unique values
        # 字符列有太多分组值
        char_cols = [i for i in list(dat) if not is_numeric_dtype(dat[i])]  # 选择字符型值的特征列
        char_cols_too_many_unique = [i for i in char_cols if len(dat[i].unique()) >= 50]  # 选择字符型特征分类大于50的特征列
        if len(char_cols_too_many_unique) > 0:
            print(
                '>>> There are {} variables have too many unique non-numberic values, which might cause the binning process slow. Please double check the following variables: \n{}'.format(
                    len(char_cols_too_many_unique), ', '.join(char_cols_too_many_unique)))
            print('>>> Continue the binning process?')  # 分组过多，对后续分箱效率有很大影响
            print('1: yes \n2: no \n')
            cont = int(input("Selection: "))
            while cont not in [1, 2]:
                cont = int(input("Selection: "))
            if cont == 2:  # 选择2就停止运行
                raise SystemExit(0)

    # remove only 1 unique vlaues variable
    # 删除仅有一类值的字符变量
    '''
    unique1_cols = [i for i in list(dat) if len(dat[i].unique()) == 1]
    if len(unique1_cols) > 0:
        print("There are {} columns have only one unique values, which are removed from input dataset. \n (ColumnNames: {})".format(len(unique1_cols), ', '.join(unique1_cols)))
        dat = dat.drop(unique1_cols, axis=1)
    '''
    # remove date time variable # isinstance
    # 删除时间日期变量
    datetime_cols = dat.dtypes[dat.dtypes == 'datetime64[ns]'].index.tolist()
    if len(datetime_cols) > 0:
        print("There are {} date/time type columns are removed from input dataset. \n (ColumnNames: {})".format(
            len(datetime_cols), ', '.join(datetime_cols)))
        dat = dat.drop(datetime_cols, axis=1)
    # return dat

    return dat


# replace blank by NA
# ' @import data.table
# '
def rep_blank_na(dat):  # cant replace blank string in categorical value with nan无法用nan替换分类值中的空白字符串
    # remove duplicated index 移除重复的索引，即dat索引重排
    if dat.index.duplicated().any():  # any() 函数用于判断给定的可迭代参数 iterable 是否全部为 False，则返回 False，如果有一个为 True，则返回 True。
        dat = dat.reset_index()
        print('There are duplicated index in dataset. The index has been reseted.')

    blank_cols = [index for index, x in dat.isin(['', ' ']).sum().iteritems() if x > 0]  # 检查数据框dat中数据包含空值的列的名字
    if len(blank_cols) > 0:
        print('There are blank strings in {} columns, which are replaced with NaN. \n (ColumnNames: {})'.format(
            len(blank_cols), ', '.join(blank_cols)))
        dat = dat.replace(' ', np.nan, regex=True)
        dat = dat.replace('', np.nan, regex=True)
        # dat[dat == [' ','']] = np.nan#将数据框中空值替换为np.nan
        # dat = dat.apply(lambda x: x.str.strip()).replace(r'^\s*$', np.nan, regex=True)#将数据框中所有字符特征含有^\s*$替换为np.nan
        # string.strip([chars])用来去除头尾字符、空白符(包括\n、\r、\t、' '，即：换行、回车、制表符、空格)
        dat = dat.replace(r'^\s*$', np.nan, regex=True)  # 将dat中的’r'^\s*$'‘ 替换为np.nan
    return dat


# check y
# ' @import data.table
# '
def check_y(dat, y, positive):
    '''
    检查dat是否是数据框，检查y是否是一个特征，是否存在，检查y是否是整型0 1，是二类，把它转换为0 1，positive为y中积极的一类转换为1
    '''
    positive = str(positive)
    # ncol of dt 检查数据框
    if isinstance(dat, pd.DataFrame) & (dat.shape[1] <= 1):
        raise Exception("Incorrect inputs; dat should be a DataFrame with at least two columns.")

    # y ------
    y = str_to_list(y)  # 转换为list
    # length of y == 1
    if len(y) != 1:  # y特征只能是一个
        raise Exception("Incorrect inputs; the length of y should be one")

    y = y[0]
    # y not in dat.columns
    if y not in dat.columns:  # y不在dat中
        raise Exception("Incorrect inputs; there is no \'{}\' column in dat.".format(y))

    # remove na in y
    if pd.isna(dat[y]).any():  # 删除y中为空记录
        print("There are NaNs in \'{}\' column. The rows with NaN in \'{}\' were removed from dat.".format(y, y))
        dat = dat.dropna(subset=[y])  # 删除y列中有空值的的记录
        # dat = dat[pd.notna(dat[y])]

    # numeric y to int #y数值为整型
    if is_numeric_dtype(dat[y]):
        dat.loc[:, y] = dat[y].apply(lambda x: x if pd.isnull(x) else int(x))  # dat[y].astype(int)
    # length of unique values in y  #检查y值是否都是一样的
    unique_y = np.unique(dat[y].values)
    if len(unique_y) == 2:
        # if [v not in [0,1] for v in unique_y] == [True, True]: y是二类但不是0 1类型转换为0 1类型
        if True in [bool(re.search(positive, str(v))) for v in unique_y]:
            y1 = dat[y]
            y2 = dat[y].apply(lambda x: 1 if str(x) in re.split('\|', positive) else 0)
            if (y1 != y2).any():
                dat.loc[:, y] = y2  # dat[y] = y2
                print("The positive value in \"{}\" was replaced by 1 and negative value by 0.".format(y))
        else:
            raise Exception("Incorrect inputs; the positive value in \"{}\" is not specified".format(y))
    else:
        raise Exception("Incorrect inputs; the length of unique values in y column \'{}\' != 2.".format(y))

    return dat


# check print_step
# ' @import data.table
# '
def check_print_step(print_step):
    if not isinstance(print_step, (int, float)) or print_step < 0:
        print("Incorrect inputs; print_step should be a non-negative integer. It was set to 1.")
        print_step = 1
    return print_step


# x variable
def x_variable(dat, y, x):
    '''
    判断x变量是否在dat中，是否存在不在的
    '''
    y = str_to_list(y)
    x_all = list(set(dat.columns) - set(y))

    if x is None:
        x = x_all
    else:
        x = str_to_list(x)

        if any([i in list(x_all) for i in x]) is False:
            # x种特征都不在x_all中
            x = x_all
        else:
            # x种特征部分在x_all中
            x_notin_xall = set(x).difference(x_all)  # 选择x中不在x_all中的特征
            if len(x_notin_xall) > 0:
                print(
                    "Incorrect inputs; there are {} x variables are not exist in input data, which are removed from x. \n({})".format(
                        len(x_notin_xall), ', '.join(x_notin_xall)))
                x = set(x).intersection(x_all)  # 选出交集
    return list(x)


# check breaks_list
def check_breaks_list(breaks_list, xs):
    if breaks_list is not None:
        # is string
        if isinstance(breaks_list, str):
            breaks_list = eval(breaks_list)  # 转换字符串
        # is not dict
        if not isinstance(breaks_list, dict):
            raise Exception("Incorrect inputs; breaks_list should be a dict.")
    return breaks_list


# check special_values
def check_special_values(special_values, xs):
    if special_values is not None:
        # # is string
        # if isinstance(special_values, str):
        #     special_values = eval(special_values)
        if isinstance(special_values, list):
            print(
                "The special_values should be a dict. Make sure special values are exactly the same in all variables if special_values is a list.")
            sv_dict = {}
            for i in xs:
                sv_dict[i] = special_values
            special_values = sv_dict
        elif not isinstance(special_values, dict):
            raise Exception("Incorrect inputs; special_values should be a list or dict.")
    return special_values
