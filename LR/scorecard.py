#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : scorecard.py
@Time    : 2020-09-10 00:47
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import pandas as pd
import numpy as np
import re


def ab(points0=600, odds0=20, pdo=50):
    # sigmoid function
    # library(ggplot2)
    # ggplot(data.frame(x = c(-5, 5)), aes(x)) + stat_function(fun = function(x) 1/(1+exp(-x)))

    # log_odds function
    # ggplot(data.frame(x = c(0, 1)), aes(x)) + stat_function(fun = function(x) log(x/(1-x)))

    # logistic function
    # p(y=1) = 1/(1+exp(-z)),
    # z = beta0+beta1*x1+...+betar*xr = beta*x
    ##==> z = log(p/(1-p)),
    # odds = p/(1-p) # bad/good <==>
    # p = odds/1+odds
    ##==> z = log(odds)
    ##==> score = a - b*log(odds)

    # two hypothesis
    # points0 = a - b*log(odds0)
    # points0 - PDO = a - b*log(2*odds0)
    # if pdo > 0:
    #     b = pdo/np.log(2)
    # else:
    #     b = -pdo/np.log(2)
    b = pdo / np.log(2)
    a = points0 - b * np.log(odds0)  # log(odds0/(1+odds0))

    return {'a': a, 'b': b}


def scorecard(bins, model, xcolumns, points0=600, odds0=20, pdo=50, weight={'weight_bad': 1, 'weight_good': 1},
              basepoints_eq0=False, missing_avg='min0', missing_add='missing_min'):
    '''
    Creating a Scorecard
    ------
    `scorecard` creates a scorecard based on the results from `woebin`
    and LogisticRegression of sklearn.linear_model

    Params
    ------
    bins: Binning information generated from `woebin` function.
    model: A LogisticRegression model object.
    points0: Target points, default 600.
    odds0: Target odds, default 20..
    pdo: Points to Double the Odds, default 50.
    weight: In(bad_weight/good/weight)
    missing_avg: 'min0': add min, 'min1':  add second min  ,'mean' :add avg, 'none' dont deal
    missing_add: 'missing_min' :nutural people  add missing  scord min  'none' :dont deal
    basepoints_eq0: Logical, default is FALSE. If it is TRUE, the
      basepoints will equally distribute to each variable.

    Returns
    ------
    DataFrame
        scorecard dataframe

    Examples
    ------
    import scorecardpy as sc

    # load data
    dat = sc.germancredit()

    # filter variable via missing rate, iv, identical value rate
    dt_sel = sc.var_filter(dat, "creditability")

    # woe binning ------
    bins = sc.woebin(dt_sel, "creditability")
    dt_woe = sc.woebin_ply(dt_sel, bins)

    y = dt_woe.loc[:,'creditability']
    X = dt_woe.loc[:,dt_woe.columns != 'creditability']

    # logistic regression ------
    from sklearn.linear_model import LogisticRegression
    lr = LogisticRegression(penalty='l1', C=0.9, solver='saga')
    lr.fit(X, y)

    # # predicted proability
    # dt_pred = lr.predict_proba(X)[:,1]
    # # performace
    # # ks & roc plot
    # sc.perf_eva(y, dt_pred)

    # scorecard
    # Example I # creat a scorecard
    card = sc.scorecard(bins, lr, X.columns)

    # credit score
    # Example I # only total score
    score1 = sc.scorecard_ply(dt_sel, card)
    # Example II # credit score for both total and each variable
    score2 = sc.scorecard_ply(dt_sel, card, only_total_score = False)
    '''

    # coefficients
    aabb = ab(points0, odds0, pdo)
    a = aabb['a']
    b = aabb['b']
    # odds = pred/(1-pred); score = a - b*log(odds)

    # bins # if (is.list(bins)) rbindlist(bins)
    if isinstance(bins, dict):
        bins = pd.concat(bins, ignore_index=True)
    xs = [re.sub('_woe$', '', i) for i in xcolumns]
    # coefficients
    coef_df = pd.Series(model.params.values[1:], index=np.array(xs)) \
        .loc[lambda x: x != 0]  # .reset_index(drop=True)
    # b = pdo/np.log(2)
    # a = points0 - b*np.log(odds0) #log(odds0/(1+odds0))
    # scorecard
    len_x = len(coef_df)
    #    basepoints = a - b*model.params.values[0]

    weight1 = np.log(weight['weight_bad'] / weight['weight_good'])
    basepoints = a - b * (model.params.values[0] - weight1)
    card = {}
    if basepoints_eq0:
        card['basepoints'] = pd.DataFrame({'variable': "basepoints", 'bin': np.nan, 'points': 0}, index=np.arange(1))
        for i in coef_df.index:
            card[i] = bins.loc[bins['variable'] == i, ['variable', 'bin', 'woe']] \
                .assign(points=lambda x: round(-b * x['woe'] * coef_df[i] + basepoints / len_x)) \
                [["variable", "bin", "points"]]
    else:
        card['basepoints'] = pd.DataFrame({'variable': "basepoints", 'bin': np.nan, 'points': round(basepoints)},
                                          index=np.arange(1))
        for i in coef_df.index:
            card[i] = bins.loc[bins['variable'] == i, ['variable', 'bin', 'woe']] \
                .assign(points=lambda x: round(-b * x['woe'] * coef_df[i])) \
                [["variable", "bin", "points"]]

    if missing_avg == 'mean':
        for i in card:
            if i == 'basepoints':
                continue
            ss = int(card[i][card[i].bin != 'missing'].points.mean())
            card[i]['points'] = np.where([d == 'missing' for d in card[i]['bin']], ss, card[i]['points'])

    if missing_avg == 'min0':
        for i in card:
            if i == 'basepoints':
                continue
            ss = int(sorted(list(card[i][card[i].bin != 'missing'].points))[int(missing_avg[-1])])
            card[i]['points'] = np.where([d == 'missing' for d in card[i]['bin']], ss, card[i]['points'])

    if missing_avg == 'None':
        pass

    if missing_add == 'missing_min':
        for i in bins:
            print(i)
            ds = bins[i][bins[i].bin == 'missing']
            if len(ds) > 0 and ds.count == 0:
                ss = int(
                    sorted(list(card[re.sub('_woe$', '', i)][card[re.sub('_woe$', '', i)].bin != 'missing'].points))[0])
                card[re.sub('_woe$', '', i)]['points'] = np.where(
                    [d == 'missing' for d in card[re.sub('_woe$', '', i)]['bin']], ss,
                    card[re.sub('_woe$', '', i)]['points'])

    return card


# 将样本得分分n区间，统计每个区间的好坏样本数
def group_score(train_score, y_train, n=10, brk=None):
    def n0(x):
        return sum(x == 0)

    def n1(x):
        return sum(x == 1)

    if brk == None:
        train = pd.Series([int(i) for i in sorted(train_score['score'].values)]).reset_index(drop=True)
        point = []
        for i in range(n - 1):
            point.append(train[len(train) // n * (i + 1)])

        point = list(set(point))
        brk = [float('-inf')] + sorted(point) + [float('inf')]
        print("brk:", brk)

    # initial binning datatable
    # cut
    labels = ['[{},{})'.format(brk[i], brk[i + 1]) for i in range(len(brk) - 1)]
    print("labels:", labels)

    train_score.loc[:, 'score_bin'] = pd.cut(train_score['score'], brk, right=False, labels=labels)
    train_score['lable'] = y_train.values
    train_group = train_score.groupby('score_bin')['lable'].agg([n0, n1]).rename(
        columns={'n0': 'good_cnt', 'n1': 'bad_cnt'})

    train_group['total_cnt'] = train_group['good_cnt'] + train_group['bad_cnt']
    train_group['bad_ratio'] = round(train_group['bad_cnt'] / train_group['total_cnt'], 4) * 100

    train_group["total_ratio"] = round(train_group['total_cnt'] / np.sum(train_group['total_cnt']), 4) * 100

    return brk, labels, train_group