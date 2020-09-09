#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : info_value.py
@Time    : 2020-09-10 00:45
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import pandas as pd
import numpy as np
# from .condition_fun import *
import condition_fun


def iv(dt, y, x=None, iv_number=None, positive='bad|1', order=True):
    '''
    Information Value信息值
    ------
    This function calculates information value (IV) for multiple x variables.
    It treats each unique x value as a group and counts the number of y
    classes. If there is a zero number of y class, it will be replaced by
    0.99 to make sure it calculable.
    Params
    ------
    dt: A data frame with both x (predictor/feature) and
      y (response/label) variables.
    y: Name of y variable.
    x: Name of x variables. Default is NULL. If x is NULL, then
      all variables except y are counted as x variables.
    positive: Value of positive class, default is "bad|1".默认为bad|1
    order: Logical, default is TRUE. If it is TRUE, the output
      will descending order via iv.排序默认为降序

    Returns
    ------
    DataFrame
        Information Value

    Examples
    ------
    import scorecardpy as sc

    # load data
    dat = sc.germancredit()

    # information values
    dt_info_value = sc.iv(dat, y = "creditability")
    '''

    dt = dt.copy(deep=True)
    if isinstance(y, str):
        y = [y]
    if isinstance(x, str) and x is not None:
        x = [x]
    if x is not None:
        dt = dt[y + x]
    # remove date/time col  检查日期，以及分箱特征类数,数目大于50，报错
    dt = condition_fun.rmcol_datetime_unique1(dt)
    # replace "" by NA  检查空值，空值替换为np.nan
    dt = condition_fun.rep_blank_na(dt)
    # check y   检查y，以及对y二类检查替换为positive示例类型
    dt = condition_fun.check_y(dt, y, positive)
    # x variable names  检查x变量，输出x变量特征list
    xs = condition_fun.x_variable(dt, y, x)
    # info_value  计算x变量，对应的iv值
    if iv_n is None:
        ivlist = pd.DataFrame({
            'variable': xs,
            'info_value': [iv_xy(dt[i], dt[y[0]]) for i in xs]
        }, columns=['variable', 'info_value'])
        # sorting iv
    else:
        ivlist = iv_n(dt, y, n=iv_number)
    if order:
        ivlist = ivlist.sort_values(by='info_value', ascending=False)  # 递增

    return ivlist


# ivlist = iv(dat, y='creditability')

# ' @import data.table

def iv_xy(x, y):
    # good bad func
    def goodbad(df):
        names = {'good': (df['y'] == 0).sum(), 'bad': (df['y'] == 1).sum()}
        # 'y'与下面iv——total数据框中的‘y’一致
        return pd.Series(names)

    # iv calculation
    iv_total = pd.DataFrame({'x': x.astype('str'), 'y': y}) \
        .fillna('missing') \
        .groupby('x') \
        .apply(goodbad) \
        .replace(0, 0.9) \
        .assign(
        DistrBad=lambda x: x.bad / sum(x.bad),
        DistrGood=lambda x: x.good / sum(x.good)
    ) \
        .assign(iv=lambda x: (x.DistrGood - x.DistrBad) * np.log(x.DistrGood / x.DistrBad)) \
        .iv.sum()  ##########################################################################################################
    # return iv
    return iv_total


def iv_n(df, target, n=10):
    bad = df[target].sum()  # 坏客户总人数
    good = df[target].count() - bad  # 好客户总人数
    chat = list(df.columns[df.dtypes == 'object'])
    name = df.columns.drop(target)
    ivs = []

    for i in name:
        X = df[i]
        Y = df[target[0]]
        nuniq = X.nunique()
        if nuniq <= n:
            chat.append(i)
        if i in chat:
            d1 = pd.DataFrame({"X": X, "Y": Y})
            d1['X'] = d1['X'].astype(str)
            d2 = d1.groupby('X', as_index=False)
        else:
            d1 = pd.DataFrame({"X": X, "Y": Y, "bin": pd.qcut(X, n, duplicates='drop')})
            d1['bin'] = d1['bin'].astype(str)
            d2 = d1.groupby('bin', as_index=False)
        d3 = pd.DataFrame(columns=['min'])
        d3['sum'] = d2.sum().Y
        d3['total'] = d2.count().Y
        d3['bad_rate'] = d2.mean().Y
        d3['group_rate'] = d3['total'] / (bad + good).values
        d3['woe'] = d3['bad_rate'].apply(
            lambda x: np.log((x / (1 - x)) / (bad / good).values[0]) if (x != 0 and x != 1) else 0)

        d3['iv'] = (d3['sum'] / bad.values - ((d3['total'] - d3['sum']) / good.values)) * d3['woe']
        iv = d3['iv'].sum()
        ivs.append(iv)
    d = pd.DataFrame({"variable": name, "info_value": ivs})
    d.sort_values('info_value', inplace=True, ascending=False)
    return d


# print(iv_xy(x,y))


# #' Information Value
# #'
# #' calculating IV of total based on good and bad vectors
# #'
# #' @param good vector of good numbers
# #' @param bad vector of bad numbers
# #'
# #' @examples
# #' # iv_01(good, bad)
# #' dtm = melt(dt, id = 'creditability')[, .(
# #' good = sum(creditability=="good"), bad = sum(creditability=="bad")
# #' ), keyby = c("variable", "value")]
# #'
# #' dtm[, .(iv = lapply(.SD, iv_01, bad)), by="variable", .SDcols# ="good"]
# #'
# #' @import data.table
# ' @import data.table
# '
def iv_01(good, bad):
    # iv calculation
    iv_total = pd.DataFrame({'good': good, 'bad': bad}) \
        .replace(0, 0.9) \
        .assign(
        DistrBad=lambda x: x.bad / sum(x.bad),
        DistrGood=lambda x: x.good / sum(x.good)
    ) \
        .assign(iv=lambda x: (x.DistrGood - x.DistrBad) * np.log(x.DistrGood / x.DistrBad)) \
        .iv.sum()
    # return iv
    return iv_total


# #' miv_01
# #'
# #' calculating IV of each bin based on good and bad vectors
# #'
# #' @param good vector of good numbers
# #' @param bad vector of bad numbers
# #'
# #' @import data.table
# #'
# ' @import data.table
# '
def miv_01(good, bad):
    # iv calculation
    infovalue = pd.DataFrame({'good': good, 'bad': bad}) \
        .replace(0, 0.9) \
        .assign(
        DistrBad=lambda x: x.bad / sum(x.bad),
        DistrGood=lambda x: x.good / sum(x.good)
    ) \
        .assign(iv=lambda x: (x.DistrGood - x.DistrBad) * np.log(x.DistrGood / x.DistrBad)) \
        .iv
    # return iv
    return infovalue


# #' woe_01
# #'
# #' calculating WOE of each bin based on good and bad vectors
# #'
# #' @param good vector of good numbers
# #' @param bad vector of bad numbers
# #'
# #' @import data.table
# ' @import data.table
# '
def woe_01(good, bad):
    # woe calculation
    woe = pd.DataFrame({'good': good, 'bad': bad}) \
        .replace(0, 0.9) \
        .assign(
        DistrBad=lambda x: x.bad / sum(x.bad),
        DistrGood=lambda x: x.good / sum(x.good)
    ) \
        .assign(woe=lambda x: np.log(x.DistrGood / x.DistrBad)) \
        .woe
    # return woe
    return woe
