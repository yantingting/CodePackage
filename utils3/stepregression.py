#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : stepregression.py
@Time    : 2020-10-16 10:51
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd


def forward_selected(data, response):
    """
    前向逐步回归算法，调整R平方，增加变量,R值变大则该特征对模型有效，R值减小则该特征对模型无效
    data: 数据框
    response ：人为去除特征
    """
    remaining = set(data.columns)
    remaining.remove(response)
    selected = []  # 入模特征初始化
    current_score, best_new_score = 0.0, 0.0
    while remaining and current_score == best_new_score:
        scores_with_candidates = []
        for candidate in remaining:
            # selected.append(candidate)
            x = sm.add_constant(data.loc[:, candidate])
            y = data[response]
            score = smf.OLS(y, x).fit().rsquared_adj  # 获取模型调整R平方值
            scores_with_candidates.append((score, candidate))
        scores_with_candidates.sort()  # 升序排序
        best_new_score, best_candidate = scores_with_candidates.pop()  # 取最大调整R平方值
        if current_score < best_new_score:
            remaining.remove(best_candidate)
            selected.append(best_candidate)
            current_score = best_new_score

    return list([remaining][0])


def stepwise_selection(X, y, initial_list=[], threshold_in=0.01, threshold_out=0.05, print_msg=True):
    """
   基于p值的both逐步回归
    Arguments:
        X：自变量
        y：因变量
        initial_list：初始x变量list，默认为空
        threshold_in ：进入模型stepregressionp值的上限，默认0.01
        threshold_out：剔出模型特征p值的下限，默认0.05
        verbose：逻辑值，是否输出进入模型的特征与p值
    Returns: list of selected features
    引入变量检验p值小于0.01引入，同时检验模型中所有变量p值大于0.05剔除
    """
    included = list(initial_list)

    icount = 0  # 记录循环次数
    while True:
        changed = False  # 停止标志
        icount = icount + 1
        strAddMsg = ''
        strDropMsg = ''

        # forward step
        excluded = list(set(X.columns) - set(included))
        new_pval = pd.Series(index=excluded)

        for new_column in excluded:
            model = sm.Logit(y, sm.add_constant(pd.DataFrame(X[included + [new_column]]))).fit(disp=print_msg)
            new_pval[new_column] = model.pvalues[new_column]  # 获取特征p值
            best_pval = new_pval.min()

        if best_pval < threshold_in and len(excluded) > 0:
            best_feature = new_pval.argmin()  # 获取最小值对应索引
            included.append(best_feature)
            changed = True
            strAddMsg = 'Add  {:30} with p-value {:.6}'.format(best_feature, best_pval)

        # backward step
        model = sm.Logit(y, sm.add_constant(pd.DataFrame(X[included]))).fit(disp=print_msg)
        # use all coefs except intercept
        pvalues = model.pvalues.iloc[1:]
        worst_pval = pvalues.max()  # null if pvalues is empty

        if worst_pval > threshold_out:
            changed = True
            worst_feature = pvalues.argmax()
            included.remove(worst_feature)
            strDropMsg = 'Drop {:30} with p-value {:.6}'.format(worst_feature, worst_pval)

        print('######################## round {} ########################'.format(icount))
        # print(new_pval)
        # print(strAddMsg)
        # print(strDropMsg)
        print('included {} variables:{}\n'.format(len(included), included))

        if not changed:
            break
    return included


def back_selection(x, y):
    """
   基于AIC值最小的后向逐步回归
    Arguments:
        X：自变量
        y：因变量
    Returns: list of selected features
    """
    model = sm.Logit(y, sm.add_constant(x)).fit()
    all_aic = model.aic
    fea_list = list(x)
    changed = True
    while changed:
        changed = False
        aic = []
        for i in range(len(fea_list)):
            print(len(fea_list))
            fea_li = fea_list.copy()
            fea_li.remove(fea_list[i])
            model = sm.Logit(y, sm.add_constant(x[fea_li])).fit()
            aic.append((model.aic, fea_li))
            aic.sort()
            choose_aic, columns = aic[0]
        if choose_aic < all_aic:
            fea_list = columns
            all_aic = choose_aic
            print(all_aic)
            changed = True
    return fea_list


def stepregression(x, y, method='both', initial_list=[], threshold_in=0.01, threshold_out=0.05, print_msg=True):
    '''
    x:自变量特征数据框
    y：因变量series
    '''
    if method == 'both':
        feature = stepwise_selection(x, y, initial_list, threshold_in, threshold_out, print_msg)
    if method == 'back':
        feature = back_selection(x, y)
    if method == 'forward':
        feature = forward_selected(pd.concat((x, y), axis=1), pd.DataFrame(y).columns.values[0])
    return feature


