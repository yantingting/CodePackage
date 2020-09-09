#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : var_filter.py
@Time    : 2020-09-10 00:48
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import warnings
import time
# from .condition_fun import *
# from .info_value import *
import condition_fun
import info_value


def var_filter(dt, y, x=None, iv_limit=0.02, iv_number=None, missing_limit=0.95,
               identical_limit=0.95, var_rm=None, var_kp=None,
               return_rm_reason=False, positive='bad|1'):
    '''
    Variable Filter
    ------
    This function filter variables base on specified conditions, such as
    information value, missing rate, identical value rate.

    Params
    ------
    dt: A data frame with both x (predictor/feature) and y
      (response/label) variables.
                   数据源y为目标变量特征名，字符串
    y: Name of y variable.
                    y为目标变量特征名，字符串
    x: Name of x variables. Default is NULL. If x is NULL, then all
      variables except y are counted as x variables.
                 自变量x的名字，=null则默认为除y外所有数据特征list，训练特征变量名list
    iv_limit: The information value of kept variables should>=iv_limit.
      The default is 0.02.
                     iv值限制为0.02，iv值最低限制
    missing_limit: The missing rate of kept variables should<=missing_limit.
      The default is 0.95.
                        缺失率删除限制为0.95，最低限制
    identical_limit: The identical value rate (excluding NAs) of kept
      variables should <= identical_limit. The default is 0.95.
                  保留的相同价值率（不包括NAs），最多类数量占总体数量之比
             变量应该<= identical_limit。 默认值为0.95
    var_rm: Name of force removed variables, default is NULL.
            强制删除变量的名称为list，默认为NULL。
    var_kp: Name of force kept variables, default is NULL.
             强制名称保存变量，默认为NULL
    return_rm_reason: Logical, default is FALSE.
            是否返回删除变量理由，默认为FALSE,
            如果为真，返回一个字典df,其中df['dt']为返回选择后特征数据，df['rm']返回删除特征理由数据框
            如果为假， 返回选择后特征数据df
    positive: Value of positive class, default is "bad|1".
                反类的值，默认为“坏| 1”。

    Returns
    ------
    DataFrame
        A data.table with y and selected x variables

    Dict(if return_rm_reason == TRUE)
        A DataFrame with y and selected x variables and
          a DataFrame with the reason of removed x variable.

    Examples
    ------
    import scorecardpy as sc

    # load data
    dat = sc.germancredit()

    # variable filter
    dt_sel = sc.var_filter(dat, y = "creditability")
    '''
    # start time
    start_time = time.time()
    print('[INFO] filtering variables ...')

    dt = dt.copy(deep=True)
    if isinstance(y, str):
        y = [y]
    if isinstance(x, str) and x is not None:
        x = [x]
    if x is not None:
        dt = dt[y + x]
    # remove date/time col  删除时间日期特征列
    dt = condition_fun.rmcol_datetime_unique1(dt)

    # replace "" by NA  替换''为na
    dt = condition_fun.rep_blank_na(dt)
    # check y 对数据框dt和标签y进行检查替换，positive为y中的正例标签想，返回处理后dat
    dt = condition_fun.check_y(dt, y, positive)
    # x variable names  检查x是否在dat中，并返回处理后x
    x = condition_fun.x_variable(dt, y, x)

    # force removed variables 强制删除特征list var_rm
    if var_rm is not None:
        if isinstance(var_rm, str):
            # 转化为list
            var_rm = [var_rm]
        x = list(set(x).difference(set(var_rm)))  # 将x中的var-rm特征删除
    # check force kept variables 强制保留变量
    if var_kp is not None:
        if isinstance(var_kp, str):
            var_kp = [var_kp]
        var_kp2 = list(set(var_kp) & set(x))
        len_diff_var_kp = len(var_kp) - len(var_kp2)
        if len_diff_var_kp > 0:
            print(
                "Incorrect inputs; there are {} var_kp variables are not exist in input data, which are removed from var_kp. \n {}".format(
                    len_diff_var_kp, list(set(var_kp) - set(var_kp2))))
        var_kp = var_kp2 if len(var_kp2) > 0 else None

    # -iv 计算iv值

    iv_list = info_value.iv(dt, y, x, iv_number=iv_number,
                            order=False)  ##############################################################################################################

    # -na percentage
    nan_rate = lambda a: a[a.isnull()].size / a.size  # 缺失率定义（缺失数/总数）
    na_perc = dt[x].apply(nan_rate).reset_index(name='missing_rate').rename(columns={'index': 'variable'})  # 缺失率计算
    # -identical percentage 相同特征值百分比
    idt_rate = lambda a: a.value_counts().max() / a.size  # 变量值中不同类别的最大出现次数/总数
    identical_perc = dt[x].apply(idt_rate).reset_index(name='identical_rate').rename(columns={'index': 'variable'})

    # dataframe iv na idt  将x变量的缺失率和相同百分比合并在iv值表
    dt_var_selector = iv_list.merge(na_perc, on='variable').merge(identical_perc, on='variable')
    # remove na_perc>95 | ele_perc>0.95 | iv<0.02 删除x中 na_perc>95或者ele_perc>0.95或者iv<0.02的变量
    # variable datatable selected  删除x中 na_perc>95或者ele_perc>0.95或者iv<0.02的变量
    dt_var_sel = dt_var_selector.query(
        '(info_value >= {}) & (missing_rate <= {}) & (identical_rate <= {})'.format(iv_limit, missing_limit,
                                                                                    identical_limit))

    # add kept variable #将强制要加入的x变量特征加入
    x_selected = dt_var_sel.variable.tolist()  # 筛选出的变量特征x，并以list输出
    if var_kp is not None:
        x_selected = np.unique(x_selected + var_kp).tolist()  # 筛选出变量和强制加入变量，去重，输出list
    # data kept
    dt_kp = dt[x_selected + y]  # 构建最新数据集

    # runingtime #输出特征选取数据，和特征选取个数，删除特征个数，样本量
    runingtime = time.time() - start_time
    if (runingtime >= 10):
        # print(time.strftime("%H:%M:%S", time.gmtime(runingtime)))
        print('Variable filtering on {} rows and {} columns in {} \n{} variables are removed'.format(dt.shape[0],
                                                                                                     dt.shape[1],
                                                                                                     time.strftime(
                                                                                                         "%H:%M:%S",
                                                                                                         time.gmtime(
                                                                                                             runingtime)),
                                                                                                     dt.shape[1] - len(
                                                                                                         x_selected + y)))
    # return remove reason
    if return_rm_reason:
        dt_var_rm = dt_var_selector.query(
            '(info_value < {}) | (missing_rate > {}) | (identical_rate > {})'.format(iv_limit, missing_limit,
                                                                                     identical_limit)) \
            .assign(
            info_value=lambda x: ['info_value<{}'.format(iv_limit) if i else np.nan for i in (x.info_value < iv_limit)],
            missing_rate=lambda x: ['missing_rate>{}'.format(missing_limit) if i else np.nan for i in
                                    (x.missing_rate > missing_limit)],
            identical_rate=lambda x: ['identical_rate>{}'.format(identical_limit) if i else np.nan for i in
                                      (x.identical_rate > identical_limit)]
        )
        dt_rm_reason = pd.melt(dt_var_rm, id_vars=['variable'], var_name='iv_mr_ir').dropna() \
            .groupby('variable').apply(lambda x: ', '.join(x.value)).reset_index(name='rm_reason')
        '''
        dt_var_rm=dt_var_rm.replace([None],'')
        dt_var_rm['rm_reason']=dt_var_rm['info_value']+''+dt_var_rm['missing_rate']+''+dt_var_rm['identical_rate']
        dt_rm_reason=dt_var_rm.loc[:,('variable','rm_reason')]
        '''
        if var_rm is not None:
            dt_rm_reason = pd.concat([
                dt_rm_reason,
                pd.DataFrame({'variable': var_rm, 'rm_reason': "force remove"}, columns=['variable', 'rm_reason'])
            ], axis=0)
        if var_kp is not None:
            dt_rm_reason = dt_rm_reason.query('variable not in {}'.format(var_kp))

        dt_rm_reason = pd.merge(dt_rm_reason, dt_var_selector, how='outer', on='variable')
        dt_kp = dt_kp.reset_index(drop=True)
        dt_rm_reason['rm_reason'] = dt_rm_reason['rm_reason'].fillna('keep')
        dt_rm_reason = dt_rm_reason[dt_rm_reason['rm_reason'] != 'keep']
        return {'dt': dt_kp, 'rm': dt_rm_reason}
    else:
        dt_kp = dt_kp.reset_index(drop=True)
        return dt_kp
