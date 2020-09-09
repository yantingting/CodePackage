#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : musk_woebin.py
@Time    : 2020-09-10 00:46
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import numpy as np
import pandas as pd
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import re
import multiprocessing as mp
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus']=False
import time


def woebin_ply(dt, bins, no_cores=None, print_step=0):
    '''
    WOE Transformation
    ------
    `woebin_ply` converts original input data into woe values
    based on the binning information generated from `woebin`.

    Params
    ------
    dt: A data frame.
    bins: Binning information generated from `woebin`.
    no_cores: Number of CPU cores for parallel computation.
      Defaults NULL. If no_cores is NULL, the no_cores will
      set as 1 if length of x variables less than 10, and will
      set as the number of all CPU cores if the length of x
      variables greater than or equal to 10.
    print_step: A non-negative integer. Default is 1. If
      print_step>0, print variable names by each print_step-th
      iteration. If print_step=0 or no_cores>1, no message is print.

    Returns
    -------
    DataFrame
        a dataframe of woe values for each variables

    Examples
    -------
    import scorecardpy as sc
    import pandas as pd

    # load data
    dat = sc.germancredit()

    # Example I
    dt = dat[["creditability", "credit.amount", "purpose"]]
    # binning for dt
    bins = sc.woebin(dt, y = "creditability")

    # converting original value to woe
    dt_woe = sc.woebin_ply(dt, bins=bins)

    # Example II
    # binning for germancredit dataset
    bins_germancredit = sc.woebin(dat, y="creditability")

    # converting the values in germancredit to woe
    ## bins is a dict
    germancredit_woe = sc.woebin_ply(dat, bins=bins_germancredit)
    ## bins is a dataframe
    germancredit_woe = sc.woebin_ply(dat, bins=pd.concat(bins_germancredit))
    '''
    # start time
    start_time = time.time()
    # remove date/time col
    dt = rmcol_datetime_unique1(dt)
    # replace "" by NA
    dt = rep_blank_na(dt)
    # ncol of dt
    # if len(dt.index) <= 1: raise Exception("Incorrect inputs; dt should have at least two columns.")
    # print_step
    print_step = check_print_step(print_step)

    # bins # if (is.list(bins)) rbindlist(bins)
    if isinstance(bins, dict):
        bins = pd.concat(bins, ignore_index=True)
    # x variables
    xs_bin = bins['variable'].unique()
    xs_dt = list(dt.columns)
    xs = list(set(xs_bin).intersection(xs_dt))
    # length of x variables
    xs_len = len(xs)
    # initial data set
    dat = dt.loc[:, list(set(xs_dt) - set(xs))]

    # loop on xs
    if (no_cores is None) or (no_cores < 1):
        no_cores = 1 if xs_len < 10 else mp.cpu_count()
    #
    if no_cores == 1:
        for i in np.arange(xs_len):
            x_i = xs[i]
            # print xs
            # print(x_i)
            if print_step > 0 and bool((i + 1) % print_step):
                print(('{:' + str(len(str(xs_len))) + '.0f}/{} {}').format(i, xs_len, x_i), flush=True)
            #
            binx = bins[bins['variable'] == x_i].reset_index()
            # bins.loc[lambda x: x.variable == x_i]
            # bins.loc[bins['variable'] == x_i] #
            # bins.query('variable == \'{}\''.format(x_i))
            dtx = dt[[x_i]]
            dat = pd.concat([dat, woepoints_ply1(dtx, binx, x_i, woe_points="woe")], axis=1)
    else:
        pool = mp.Pool(processes=no_cores)
        # arguments
        args = zip(
            [dt[[i]] for i in xs],
            [bins[bins['variable'] == i] for i in xs],
            [i for i in xs],
            ["woe"] * xs_len
        )
        # bins in dictionary
        dat_suffix = pool.starmap(woepoints_ply1, args)
        dat = pd.concat([dat] + dat_suffix, axis=1)
        pool.close()
    # runingtime
    runingtime = time.time() - start_time
    if (runingtime >= 10):
        # print(time.strftime("%H:%M:%S", time.gmtime(runingtime)))
        print('Woe transformating on {} rows and {} columns in {}'.format(dt.shape[0], xs_len, time.strftime("%H:%M:%S", time.gmtime(runingtime))))
    return dat


# ' @import data.table
def woepoints_ply1(dtx, binx, x_i, woe_points):
    '''
    Transform original values into woe or porints for one variable.

    Params
    ------

    Returns
    ------

    '''
    # woe_points: "woe" "points"
    # binx = bins.loc[lambda x: x.variable == x_i]
    binx = pd.merge(
        pd.DataFrame(binx['bin'].str.split('%,%').tolist(), index=binx['bin']) \
            .stack().reset_index().drop('level_1', axis=1),
        binx[['bin', woe_points]],
        how='left', on='bin'
    ).rename(columns={0: 'V1', woe_points: 'V2'})

    # dtx
    ## cut numeric variable
    if is_numeric_dtype(dtx[x_i]):
        is_sv = pd.Series(not bool(re.search(r'\[', str(i))) for i in binx.V1)
        binx_sv = binx.loc[is_sv]
        binx_other = binx.loc[~is_sv]
        # create bin column
        breaks_binx_other = np.unique(list(map(float, ['-inf'] + [re.match(r'.*\[(.*),.+\).*', str(i)).group(1) for i in binx_other['bin']] + ['inf'])))
        labels = ['[{},{})'.format(breaks_binx_other[i], breaks_binx_other[i + 1]) for i in range(len(breaks_binx_other) - 1)]

        dtx = dtx.assign(xi_bin=lambda x: pd.cut(x[x_i], breaks_binx_other, right=False, labels=labels)) \
            .assign(xi_bin=lambda x: [i if (i != i) else str(i) for i in x['xi_bin']])
        # dtx.loc[:,'xi_bin'] = pd.cut(dtx[x_i], breaks_binx_other, right=False, labels=labels)
        # dtx.loc[:,'xi_bin'] = np.where(pd.isnull(dtx['xi_bin']), dtx['xi_bin'], dtx['xi_bin'].astype(str))
        #
        mask = dtx[x_i].isin(binx_sv['V1'])
        dtx.loc[mask, 'xi_bin'] = dtx.loc[mask, x_i].astype(str)
        dtx = dtx[['xi_bin']].rename(columns={'xi_bin': x_i})
    ## to charcarter, na to missing
    if not is_string_dtype(dtx[x_i]):
        dtx.loc[:, x_i] = dtx.loc[:, x_i].astype(str).replace('nan', 'missing')
    # dtx.loc[:,x_i] = np.where(pd.isnull(dtx[x_i]), dtx[x_i], dtx[x_i].astype(str))
    # dtx = dtx.replace(np.nan, 'missing').assign(rowid = dtx.index)
    dtx = dtx.fillna('missing').assign(rowid=dtx.index)
    # rename binx
    binx.columns = ['bin', x_i, '_'.join([x_i, woe_points])]
    # merge
    dtx_suffix = pd.merge(dtx, binx, how='left', on=x_i).sort_values('rowid') \
        .set_index(dtx.index)[['_'.join([x_i, woe_points])]]
    return dtx_suffix


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


# check print_step
# ' @import data.table
# '
def check_print_step(print_step):
    if not isinstance(print_step, (int, float)) or print_step < 0:
        print("Incorrect inputs; print_step should be a non-negative integer. It was set to 1.")
        print_step = 1
    return print_step