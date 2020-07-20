#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : plot_tools.py
@Time    : 2020-06-30 23:26
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import os
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore')

import statsmodels.api as sm
from sklearn import metrics
from matplotlib.pylab import rcParams
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
rcParams['figure.figsize'] = 16,8
plt.switch_backend('agg')


def cal_rate(df, resp, lenth):
    return pd.DataFrame.from_dict(
    {
            'cntLoan' : len(df),
            'event' : df[resp].sum(),
            #'rate'    : len(df)/lenth,
            'eventRate' : df[resp].mean()
        },
    orient = 'index').T


def calc_rate(df, y_true):
    return pd.DataFrame.from_dict(
        {
            'cntLoan': len(df),
            'event': df[y_true].sum(),
            'eventRate': df[y_true].mean()
        },
        orient='index').T


def showna(data, thresh=0.70):
    ## 返回一个降序的缺失值占比的列数据框
    col_name = []
    ratio = []
    columns = data.columns.tolist()
    for i in range(len(columns)):
        num = data.iloc[:, i].isna().sum()
        ratio_i = num * 100 / data.shape[0]
        if (1 - ratio_i / 100) >= thresh:
            col_name.append(columns[i])
            ratio.append(1 - ratio_i / 100)
    rst = pd.DataFrame({"col_name": col_name, "ratio": ratio}).sort_values("ratio", ascending=False)
    return rst



def univariate_chart(df,feature,target,sample_set,n_bins = 10,default_value = -9999,draw_all = True,\
                     draw_list = [],result_path = None):

    df.rename(columns = {sample_set:'sample_type'}, inplace=True)
    idx_not_default = (df[feature] != default_value)
    df_bin = df.loc[idx_not_default,feature]

    if n_bins > df[feature].nunique():  # 数据元素种类过少.
        feature_grid = sorted(df_bin.unique().tolist())
        bins_dict = {}
        for i, v in enumerate(feature_grid):
            bins_dict[v] = i
        df['bins'] = df[feature].apply(lambda x: x if x !=default_value else -99999)
        df['bin_no'] = df['bins'].apply(lambda x: bins_dict.get(x) if x != -99999 else -1)

    else:  # 特征元素比较多的时候,采用等频分箱.
        n = round(1 / n_bins, 2)
        feature_grid = sorted(list(set(df_bin.describe(percentiles=[n * i for i in range(1, n_bins)])[3:].values)))
        feature_grid.append(max(df[feature]))
        df['bins'] = -99999
        bins = pd.cut(df_bin, feature_grid, include_lowest=True, duplicates='drop')
        df.loc[idx_not_default, 'bins'] = bins
        df.loc[idx_not_default, 'bin_no'] = bins.cat.codes
        df['bin_no'].fillna(-1, inplace=True)
        df['bin_no'] = df['bin_no'].astype(float)
        bins_dict = dict(zip(df['bins'], df['bin_no']))

    # 画图
    df_fig = pd.DataFrame.from_dict(bins_dict,orient='index',columns=['bin_no'])\
        .sort_values(by='bin_no').reset_index().rename(columns = {'index':'bins'})

    if draw_all:
        g_df = df.groupby(['bin_no'], observed=True)[target].agg({'count', 'sum', 'mean'}).reset_index()
        g_df.rename(columns={'count': 'all', 'sum': 'event', 'mean': 'event_rate'}, inplace=True)
        g_df = g_df[['bin_no', 'all','event','event_rate']]
        df_fig = df_fig.merge(g_df,on = 'bin_no', how='left')
        plt.plot(g_df.bin_no, g_df['event_rate'], 'bo-', label='%s' % 'all')

    df_fig = df_fig

    if draw_list:
        for df_type in draw_list:
            g_df = df[df['sample_type'] == df_type]\
                .groupby(['bin_no'], observed=True)[target]\
                .agg({'count', 'sum', 'mean'}).reset_index()

            g_df.rename(columns={'count': '%s'%df_type + '_a', 'sum': '%s'%df_type + '_b', 'mean': '%s'%df_type + '_r'}, inplace=True)
            g_df = g_df[['bin_no', '%s'%df_type + '_a', '%s'%df_type + '_b', '%s'%df_type + '_r']]
            df_fig = df_fig.merge(g_df,on = 'bin_no', how='left')
            x = g_df.bin_no; y = g_df['%s'%df_type + '_r']
            plt.plot(x, y, 'o-', label='%s' % df_type)
    df_fig = df_fig.T
    plt.axhline(y=df[target].mean(), color='k', linestyle='-.', label='eventR_all')
    plt.axhline(y=df[~idx_not_default][target].mean(), color='r', linestyle='--',label = 'eventR_dft')
    plt.gcf().text(0.6, 0.3, '%s' % df_fig, fontsize=10)
    plt.subplots_adjust(right=0.6)
    plt.subplots_adjust(right=0.6)
    plt.title(u'Univariate Chart of %s' % feature, fontsize=10)
    plt.ylabel('event rate')
    plt.legend(fontsize=10, framealpha=0.5, loc='best')
    plt.grid()
    if result_path :
        result_path = os.path.join(result_path, 'figure/univariate/')
    if not os.path.exists(result_path):
        os.makedirs(result_path)
    plt.savefig(os.path.join(result_path, feature + '.png'), format='png', dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()






#
# def univariate_chart(df, feature, y_true, n_bins=10,
#                      default_value=-99999,
#                      dftrain=None, dftest=None,
#                      draw_all=True, draw_train_test=False):
#     '''
#     在样本集上画出指定变量的单变量分析图.
#     :param df: 数据集, DataFrame.
#                至少包含特征和标签列.
#     :param feature: 要画的特征, str.
#     :param y_true: 标签列, string.
#                    仅仅包含0/1.
#     :param n_bins: 最大分成的箱数, int.
#                    默认为10, 仅仅对于包含数值的特征.
#     :param default_value: 缺省值, int.
#                           默认-99999.
#     :param dftrain: 训练集, DataFrame.
#     :param dftest: 测试集, Dataframe.
#     :param draw_all: 是否在全样本上作图, bool.
#                      默认为True.
#     :param draw_train_test: 是否分别在训练集和测试集上作图, bool.
#                             默认为False.
#     :return: 单变量分析图, fig.
#     '''
#
#     idx_not_default = (df[feature] != default_value)  # 非缺省数据位置.
#
#     if n_bins > df[feature].nunique():  # 数据元素种类过少.
#         y_mean_all, y_mean_train, y_mean_test = [], [], []
#         count_all, count_train, count_test = [], [], []
#
#         feature_grid = sorted(df.loc[idx_not_default, feature].unique().tolist())
#         for feature_value in feature_grid:  # 对全样本记录每个bin的信息.
#             y_mean_all.append(df.loc[df[feature] == feature_value, y_true].mean())
#             count_all.append(df.loc[df[feature] == feature_value, y_true].count())
#         y_mean_all = np.round(y_mean_all, 3)
#
#         if draw_train_test:  # 对训练集和测试集记录每个bin的信息.
#             for feature_value in feature_grid:
#                 y_mean_train.append(dftrain.loc[dftrain[feature] == feature_value, y_true].mean())
#                 y_mean_test.append(dftest.loc[dftest[feature] == feature_value, y_true].mean())
#                 count_train.append(dftrain.loc[dftrain[feature] == feature_value, y_true].count())
#                 count_test.append(dftest.loc[dftest[feature] == feature_value, y_true].count())
#             y_mean_train = np.round(y_mean_train, 3)
#             y_mean_test = np.round(y_mean_test, 3)
#
#
#         # 画图.
#         fig = plt.figure()
#         plt.figure(figsize=(8, 6))
#         x_index = list(range(1, len(feature_grid) + 1))
#         if draw_all:  # 画全样本.
#             plt.plot(x_index, y_mean_all, 'bo-', label='%s' % 'all')
#             plt.gcf().text(0.6, 0.60, 'All data sample: %s' % count_all, fontsize=9)
#
#
#         if draw_train_test:
#             plt.plot(x_index, y_mean_train, 'co-', label='%s' % 'train')
#             plt.plot(x_index, y_mean_test, 'mo-', label='%s' % 'test')
#             plt.gcf().text(0.6, 0.55, 'Train data sample: %s' % count_train, fontsize=9)
#             plt.gcf().text(0.6, 0.50, 'Train data eventR: %s' % y_mean_train, fontsize=9)
#             plt.gcf().text(0.6, 0.45, 'Test data sample: %s' % count_test, fontsize=9)
#             plt.gcf().text(0.6, 0.40, 'Test data eventR: %s' % y_mean_test, fontsize=9)
#
#
#         plt.axhline(y=df[y_true].mean(), color='k', linestyle='-.', label='eventR_all')
#         plt.axhline(y=df.loc[df[feature] == default_value, y_true].mean(), color='r', linestyle='--', label='eventR_dft')
#         plt.gcf().text(0.6, 0.7, 'Categorical value:', fontsize=9)
#         plt.gcf().text(0.6, 0.65, 'Feature grid: %s' % [str(int(x)) for x in feature_grid], fontsize=9)
#         plt.subplots_adjust(right=0.59)
#
#
#     else:  # 特征元素比较多的时候,采用等频分箱.
#         feature_grid = sorted(list(set(df.loc[idx_not_default, feature].
#                                        describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9])[3:].values)))
#         #feature_grid[-1] = feature_grid[-1] + 1
#         feature_grid.append(max(df[feature]))
#         df['bins'] = -99999
#         bins = pd.cut(df.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
#         df.loc[idx_not_default, 'bins'] = bins
#         df.loc[idx_not_default, 'bin_no'] = bins.cat.codes
#         g_df = df[idx_not_default].groupby(['bins', 'bin_no'], observed=True)[y_true].agg({'mean', 'count', 'sum'})
#         g_df.rename(columns={'mean': 'allEvntR', 'count': 'allSpl', 'sum': 'allEvnt'}, inplace=True)
#
#         if draw_train_test:
#             # Train sample
#             dftrain['bins'] = -99999
#             bins = pd.cut(dftrain.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
#             dftrain.loc[idx_not_default, 'bins'] = bins
#             dftrain.loc[idx_not_default, 'bin_no'] = bins.cat.codes
#             g_dtrain = dftrain[idx_not_default].groupby(['bins', 'bin_no'], observed=True)[y_true].agg({'mean', 'count', 'sum'})
#             g_dtrain.rename(columns={'mean': 'trEvntR', 'count': 'trSpl', 'sum': 'trEvnt'}, inplace=True)
#
#             # Test sample
#             dftest['bins'] = -99999
#             bins = pd.cut(dftest.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
#             dftest.loc[idx_not_default, 'bins'] = bins
#             dftest.loc[idx_not_default, 'bin_no'] = bins.cat.codes
#             g_dtest = dftest[idx_not_default].groupby(['bins', 'bin_no'], observed=True)[y_true].agg({'mean', 'count', 'sum'})
#             g_dtest.rename(columns={'mean': 'teEvntR', 'count': 'teSpl', 'sum': 'teEvnt'}, inplace=True)
#             g_all = pd.concat([g_df, g_dtrain, g_dtest], axis=1)
#         else:
#             g_all = g_df
#         g_all = g_all.sort_index(level=1)
#         #g_all = g_all.sortlevel(1)  # 按区间排序.
#
#         if len(feature_grid) != len(g_all['allEvntR']) + 1:
#             strss = '\n有的分段内没有数据！！！-----------------------------------'
#         else:
#             strss = '\n'
#         print(strss)
#
#         # 画图.
#         fig = plt.figure(1)
#         x_index = list(g_all.index.get_level_values('bin_no'))
#         if draw_all:
#             plt.plot(x_index, g_all['allEvntR'], 'bo-', label='%s' % 'all')
#
#         if draw_train_test:
#             plt.plot(x_index, g_all['trEvntR'], 'co-', label='%s' % 'train')
#             plt.plot(x_index, g_all['teEvntR'], 'mo-', label='%s' % 'test')
#
#         plt.axhline(y=df[y_true].mean(), color='k', linestyle='-.', label='eventR_all')
#         plt.axhline(y=df.loc[df[feature] == default_value, y_true].mean(), color='r', linestyle='--', label='eventR_dft')
#         plt.gcf().text(0.6, 0.7, '%s' % strss, fontsize=10)
#         plt.gcf().text(0.6, 0.3, '%s' % g_all, fontsize=10)
#         plt.subplots_adjust(right=0.59)
#         plt.subplots_adjust(right=0.59)
#
#     plt.title('Univariate Chart of %s' % feature)
#     plt.ylabel('event r ate')
#     plt.legend(fontsize=10, loc=4, framealpha=0.5)
#     plt.grid()
#     #plt.show()
#     #return fig

def univariate_chart_oot(df, feature, y_true, n_bins=10,
                     default_value=-99999,
                     dftrain=None, dftest=None, dfoot = None,
                     draw_all=True, draw_train_test=False):
    '''
    在样本集上画出指定变量的单变量分析图.
    :param df: 数据集, DataFrame.
               至少包含特征和标签列.
    :param feature: 要画的特征, str.
    :param y_true: 标签列, string.
                   仅仅包含0/1.
    :param n_bins: 最大分成的箱数, int.
                   默认为10, 仅仅对于包含数值的特征.
    :param default_value: 缺省值, int.
                          默认-99999.
    :param dftrain: 训练集, DataFrame.
    :param dftest: 测试集, Dataframe.
    :param draw_all: 是否在全样本上作图, bool.
                     默认为True.
    :param draw_train_test: 是否分别在训练集和测试集上作图, bool.
                            默认为False.
    :return: 单变量分析图, fig.
    '''

    idx_not_default = (df[feature] != default_value)  # 非缺省数据位置.

    if n_bins > df[feature].nunique():  # 数据元素种类过少.
        y_mean_all, y_mean_train, y_mean_test, y_mean_oot = [], [], [], []
        count_all, count_train, count_test, count_oot = [], [], [], []

        feature_grid = sorted(df.loc[idx_not_default, feature].unique().tolist())
        for feature_value in feature_grid:  # 对全样本记录每个bin的信息.
            y_mean_all.append(df.loc[df[feature] == feature_value, y_true].mean())
            count_all.append(df.loc[df[feature] == feature_value, y_true].count())
        y_mean_all = np.round(y_mean_all, 3)

        if draw_train_test:  # 对训练集和测试集记录每个bin的信息.
            for feature_value in feature_grid:
                y_mean_train.append(dftrain.loc[dftrain[feature] == feature_value, y_true].mean())
                y_mean_test.append(dftest.loc[dftest[feature] == feature_value, y_true].mean())
                y_mean_oot.append(dfoot.loc[dfoot[feature] == feature_value, y_true].mean())

                count_train.append(dftrain.loc[dftrain[feature] == feature_value, y_true].count())
                count_test.append(dftest.loc[dftest[feature] == feature_value, y_true].count())
                count_oot.append(dfoot.loc[dfoot[feature] == feature_value, y_true].count())

            y_mean_train = np.round(y_mean_train, 3)
            y_mean_test = np.round(y_mean_test, 3)
            y_mean_oot = np.round(y_mean_oot, 3)


        # 画图.
        fig = plt.figure()
        plt.figure(figsize=(8, 6))
        x_index = list(range(1, len(feature_grid) + 1))
        if draw_all:  # 画全样本.
            plt.plot(x_index, y_mean_all, 'bo-', label='%s' % 'all')
            plt.gcf().text(0.6, 0.60, 'All data sample: %s' % count_all, fontsize=9)


        if draw_train_test:
            plt.plot(x_index, y_mean_train, 'co-', label='%s' % 'train')
            plt.plot(x_index, y_mean_test, 'mo-', label='%s' % 'test')
            plt.plot(x_index, y_mean_oot, 'go-', label='%s' % 'oot')
            plt.gcf().text(0.6, 0.55, 'Train data sample: %s' % count_train, fontsize=9)
            plt.gcf().text(0.6, 0.50, 'Train data eventR: %s' % y_mean_train, fontsize=9)
            plt.gcf().text(0.6, 0.45, 'Test data sample: %s' % count_test, fontsize=9)
            plt.gcf().text(0.6, 0.40, 'Test data eventR: %s' % y_mean_test, fontsize=9)
            plt.gcf().text(0.6, 0.35, 'Oot data sample: %s' % count_oot, fontsize=9)
            plt.gcf().text(0.6, 0.30, 'Oot data eventR: %s' % y_mean_oot, fontsize=9)

        plt.axhline(y=df[y_true].mean(), color='k', linestyle='-.', label='eventR_all')
        plt.axhline(y=df.loc[df[feature] == default_value, y_true].mean(), color='r', linestyle='--', label='eventR_dft')
        plt.gcf().text(0.6, 0.7, 'Categorical value:', fontsize=9)
        plt.gcf().text(0.6, 0.65, 'Feature grid: %s' % [str(int(x)) for x in feature_grid], fontsize=9)
        plt.subplots_adjust(right=0.59)


    else:  # 特征元素比较多的时候,采用等频分箱.
        feature_grid = sorted(list(set(df.loc[idx_not_default, feature].
                                       describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9])[3:].values)))
        #feature_grid[-1] = feature_grid[-1] + 1
        feature_grid.append(max(df[feature]))
        df['bins'] = -99999
        bins = pd.cut(df.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
        df.loc[idx_not_default, 'bins'] = bins
        df.loc[idx_not_default, 'bin_no'] = bins.cat.codes
        g_df = df[idx_not_default].groupby(['bins', 'bin_no'],observed=True)[y_true].agg({'mean', 'count', 'sum'})
        g_df.rename(columns={'mean': 'allEvntR', 'count': 'allSpl', 'sum': 'allEvnt'}, inplace=True)

        if draw_train_test:
            # Train sample
            dftrain['bins'] = -99999
            bins = pd.cut(dftrain.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
            dftrain.loc[idx_not_default, 'bins'] = bins
            dftrain.loc[idx_not_default, 'bin_no'] = bins.cat.codes
            g_dtrain = dftrain[idx_not_default].groupby(['bins', 'bin_no'], observed=True)[y_true].agg({'mean', 'count', 'sum'})
            g_dtrain.rename(columns={'mean': 'trEvntR', 'count': 'trSpl', 'sum': 'trEvnt'}, inplace=True)

            # Test sample
            dftest['bins'] = -99999
            bins = pd.cut(dftest.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
            dftest.loc[idx_not_default, 'bins'] = bins
            dftest.loc[idx_not_default, 'bin_no'] = bins.cat.codes
            g_dtest = dftest[idx_not_default].groupby(['bins', 'bin_no'], observed=True)[y_true].agg({'mean', 'count', 'sum'})
            g_dtest.rename(columns={'mean': 'teEvntR', 'count': 'teSpl', 'sum': 'teEvnt'}, inplace=True)

            # Oot sample
            dfoot['bins'] = -99999
            bins = pd.cut(dfoot.loc[idx_not_default, feature], feature_grid, include_lowest=True, duplicates='drop')
            dfoot.loc[idx_not_default, 'bins'] = bins
            dfoot.loc[idx_not_default, 'bin_no'] = bins.cat.codes
            g_doot = dfoot[idx_not_default].groupby(['bins', 'bin_no'], observed=True)[y_true].agg({'mean', 'count', 'sum'})
            g_doot.rename(columns={'mean': 'ootEvntR', 'count': 'ootSpl', 'sum': 'ootEvnt'}, inplace=True)
            g_all = pd.concat([g_df, g_dtrain, g_dtest, g_doot], axis=1)
        else:
            g_all = g_df
        g_all = g_all.sort_index(level=1)
        #g_all = g_all.sortlevel(1)  # 按区间排序.

        if len(feature_grid) != len(g_all['allEvntR']) + 1:
            strss = '\n有的分段内没有数据！！！-----------------------------------'
        else:
            strss = '\n'
        print(strss)

        # 画图.
        fig = plt.figure(1)
        x_index = list(g_all.index.get_level_values('bin_no'))
        if draw_all:
            plt.plot(x_index, g_all['allEvntR'], 'bo-', label='%s' % 'all')

        if draw_train_test:
            plt.plot(x_index, g_all['trEvntR'], 'co-', label='%s' % 'train')
            plt.plot(x_index, g_all['teEvntR'], 'mo-', label='%s' % 'test')
            plt.plot(x_index, g_all['ootEvntR'], 'go-', label='%s' % 'oot')

        plt.axhline(y=df[y_true].mean(), color='k', linestyle='-.', label='eventR_all')
        plt.axhline(y=df.loc[df[feature] == default_value, y_true].mean(), color='r', linestyle='--', label='eventR_dft')
        plt.gcf().text(0.6, 0.7, '%s' % strss, fontsize=10)
        plt.gcf().text(0.6, 0.3, '%s' % g_all, fontsize=10)
        plt.subplots_adjust(right=0.59)
        plt.subplots_adjust(right=0.59)

    plt.title('Univariate Chart of %s' % feature)
    plt.ylabel('event r ate')
    plt.legend(fontsize=10, loc=4, framealpha=0.5)
    plt.grid()
    #plt.show()
    #return fig

def show_result(df, var, resp, n_bins, pre):
    # 这个要改，如果无法等分10份就会报错
    """
    Draw Lift-Chart and AccumLift-Chart for certain score

    Parameters
    ----------
    df : pd.DataFrame
        at least contains score and resp
    var : string, score need to draw
    resp : string, resp column
        only contain 0/1 value
    n_bins: int

    Returns
    -------
    fig : 2 figures
    """
    df['bkl_%s' %var] = pd.qcut(df[var], n_bins, duplicates="drop")
    lenth = len(df)
    r1 = df.groupby('bkl_%s' %var).apply(lambda x: cal_rate(x, resp, lenth)).reset_index(level = 1, drop = True)
    #r1['accumRate'] = r1['rate'].cumsum()
    r1['acmLoan'] = r1['cntLoan'].cumsum()
    r1['acmEvent'] = r1['event'].cumsum()
    r1['acmEventRate'] = r1['acmEvent']/r1['acmLoan']
    print (r1)

    # plot lift_chart - marginal
    plt.subplot(1, 2, 1)
    # xtickss = r1.index
    r1.index = range(1, r1.shape[0]+1)
    plt.plot(r1.index, r1['eventRate'], marker='o',
             label='sample: %d,Auc of %s %s:%.3f' %(df.shape[0],pre,var, np.round(metrics.roc_auc_score(df[resp], df[var]), 3)))# linestyle='--'
    plt.title('EventRate in %d Quantiles' %n_bins)
    plt.ylabel('eventRate')
    plt.grid(True)
    # plt.xticks(r1.index, xtickss, rotation = 70)
    plt.legend(fontsize = 13, loc = 2, framealpha = 0.5)

    # plot lift_chart - accumulative
    plt.subplot(1, 2, 2)
    plt.plot(r1.index, r1['acmEventRate'], marker='o',
             label='Auc of %s  %s:%.3f' %(pre, var, np.round(metrics.roc_auc_score(df[resp], df[var]), 3)))# linestyle='--'
    plt.title('Accum-EventRate in %d Quantiles' %n_bins)
    plt.ylabel('accumEventRate')
    # plt.xticks(r1.index, xtickss, rotation = 70)
    plt.grid(True)
    plt.legend(fontsize = 13, loc = 2, framealpha = 0.5)
    plt.tight_layout()


def show_result_new(df, y_pred, y_true, n_bins, feature_label=''):
    '''
    模型预测结果的Lift Chart.

    :param df: 数据集, DataFrame.
    :param y_pred: 模型预测分数, str.
    :param y_true: 是否违约标签列, array[0, 1].
    :param n_bins: 分箱数量, int.
    :param feature_label:
    :return: Lift Chart, fig.
    '''

    if feature_label == '':
        feature_label = y_pred
    df['bkl_%s' % y_pred] = pd.qcut(df[y_pred], n_bins, duplicates='drop')
    n_bins = len(df['bkl_%s' % y_pred].unique())
    print('分箱数量: ', n_bins)

    g_df = df.groupby('bkl_%s' % y_pred).apply(lambda x: calc_rate(x, y_true)).reset_index(level=1, drop=True)
    g_df['acmLoan'] = g_df['cntLoan'].cumsum()
    g_df['acmEvent'] = g_df['event'].cumsum()
    g_df['acmEventRate'] = g_df['acmEvent'] / g_df['acmLoan']
    g_df = g_df.reset_index()
    #print(g_df)
    # plot lift_chart - marginal
    plt.subplot(1, 2, 1)

    g_df.index = range(1, n_bins + 1)
    plt.plot(g_df.index, g_df['eventRate'], marker='o',
             label='Auc of %s:%d:%.3f' % (
                 feature_label, df.shape[0], np.round(metrics.roc_auc_score(df[y_true], df[y_pred]), 3)))  # linestyle='--'
    plt.title('EventRate in %d Quantiles' % n_bins)
    plt.ylabel('eventRate')
    plt.grid(True)
    plt.legend(fontsize=13, loc=2, framealpha=0.5)


    plt.subplot(1, 2, 2)
    plt.plot(g_df.index, g_df['acmEventRate'], marker='o',
             label='Auc of %s:%d:%.3f' % (
                 feature_label, df.shape[0], np.round(metrics.roc_auc_score(df[y_true], df[y_pred]), 3)))  # linestyle='--'
    plt.title('Accum-EventRate in %d Quantiles' % n_bins)
    plt.ylabel('accumEventRate')
    plt.grid(True)
    plt.legend(fontsize=13, loc=2, framealpha=0.5)
    rcParams['figure.figsize'] = 16, 8
    plt.tight_layout()
    #plt.savefig(PIC_NAME+'.png', dpi=100)
    return g_df



def lift_chart_by_time(df, var, resp, n_bins, by="month"):
    df['applied_at'] = df['applied_at'].astype(str)

    if by=="month":
        df["month"] = df.applied_at.str[:7]
    elif by=="week":
        df["week"] = pd.to_datetime(df.applied_at).dt.week
    else:
        print("error")

    granularity_dict = {"week": u"周", "month": u"月"}
    for value in sorted(df[by].unique()):
        df_value = df[(df[by]==value)&(df[var].notnull())]
        if df_value.shape[0] > 0 and df_value[resp].nunique() == 2:
            show_result_new(df_value, var, resp, n_bins, feature_label = str(value))

    print(u"按照{}的粒度的lift_chart".format(granularity_dict[by]))
    return

#PDP_chart --------------------------------------------------------------------------------------------------
def pdpChart(model, df, var, predictors, n_bins, dfltValue, maxVal):
    """
    Draw PDP-Chart for certain feature

    Parameters
    ----------
    model : trained model
    df : pd.DataFrame
        contains all features used in model
    var : string, feature need to draw
    predictors : list of string
        all features used in model
    n_bins: int
        only works with numeric data
    dfltValue : numeric, default value for this feature
    maxVal : boolean or numeric
        designed max value for this feature

    Returns
    -------
    fig : figure
    """

    import xgboost as xgb
    from xgboost import XGBClassifier
    from xgboost import cv, DMatrix

    idx = (df[var] != dfltValue)

    if n_bins > df[var].nunique():
        n_bins = df[var].nunique()
        feature_grid = [dfltValue] + sorted(df.loc[idx, var].unique().tolist())
    else:
        feature_grid = range(n_bins)
        if maxVal==0:
            feature_grid = [dfltValue] + [df.loc[idx, var].min()+val*(maxVal-df.loc[idx, var].min())/n_bins for val in feature_grid]
        else:
            #feature_grid = [dfltValue] + [df.loc[idx, var].min()+val*(df.loc[idx, var].max()-df.loc[idx, var].min())/n_bins for val in feature_grid]

            feature_grid = [dfltValue] + list(pd.qcut(df.loc[idx, var], q=n_bins, duplicates="drop",retbins=True)[1])

    #print feature_grid
    if df.shape[0] > 10000:
        x_small = df.sample(n = 10000, random_state = 77)
    else:
        x_small = df

    predictions = []
    for feature_val in feature_grid:
        x_copy = x_small.copy()
        x_copy[var] = feature_val
        ##xgbClassifier
        if isinstance(model, xgb.XGBClassifier):
            predictions.append(model.predict(x_copy[predictors]).mean())
        ##xgb
        else:
            predictions.append(model.predict(xgb.DMatrix(x_copy[predictors])).mean())

    xindex = feature_grid[1:]

    plt.plot(range(len(xindex)), predictions[1:], 'bo-', label = '%s' %var)
    plt.xticks(range(len(xindex)), ["%.2g"%i for i in xindex], rotation=45)
    plt.axhline(y=model.predict(x_small[predictors]).mean(), color='k', linestyle='--', label = 'scoreAvg')
    plt.axhline(y=predictions[0], color='r', linestyle='--', label = 'dfltValue')
    plt.title('pdp Chart of %s' %var)
    plt.ylabel('Score')
    plt.legend(fontsize = 10, loc = 4, framealpha = 0.5)
    plt.grid()
    plt.tight_layout()

def pdpChart_new(model, df, var, predictors, n_bins, dfltValue, maxValRatio=1):
    """
    Draw PDP-Chart for certain feature

    Parameters
    ----------
    model : trained model
    df : pd.DataFrame
        contains all features used in model
    var : string, feature need to draw
    predictors : list of string
        all features used in model
    n_bins: int
        only works with numeric data
    dfltValue : numeric,value to sample bin max
    maxVal : boolean or numeric
        designed max value for this feature

    Returns
    -------
    fig : figure
    """
    maxVal=df[var][df[var] > dfltValue].quantile(maxValRatio)
    #feature_grid
    idx = ((df[var] > dfltValue)&(df[var]<=maxVal))
    #是否包含所需单一分箱的取值区间
    if sum((df[var]<=dfltValue))>0:
        feature_grid=[dfltValue]
    else:
        feature_grid=[]
    bin_index=[]
    for i in range(0,n_bins+1):
        bin_index.append(i*1.0*maxValRatio/n_bins)
    feature_grid=sorted(list(df.loc[idx, var].quantile(bin_index))+feature_grid)
    print (var + ': NotMiss_samplenum:'+ str(len(df.loc[idx, var]))+';bin:',(feature_grid))
    #取观察样本 原始样本大于1w时随机抽取1w
    if df.shape[0] > 10000:
        x_small = df.sample(n = 10000, random_state = 77)
    else:
        x_small = df
    #score
    predictions = []
    for feature_val in feature_grid:
        x_copy = x_small.copy()
        x_copy[var] = feature_val
        predictions.append(model.predict_proba(x_copy[predictors])[:,1].mean())
    #制图
    if feature_grid[0] != dfltValue:
        xindex = feature_grid[:]
        plt.plot(bin_index, predictions[:], 'bo-', label = 'Variable distribution')
        plt.xticks(bin_index,['%.2f'%i for i in feature_grid],rotation=45)
        plt.axhline(y=model.predict_proba(x_small[predictors])[:,1].mean(), color='k', linestyle='--', label = 'scoreAvg')
    else:
        xindex = feature_grid[1:]
        plt.plot(bin_index, predictions[1:], 'bo-', label = 'Variable distribution')
        plt.xticks(bin_index,['%.2f'%i for i in feature_grid[1:]],rotation=45)
        plt.axhline(y=model.predict_proba(x_small[predictors])[:,1].mean(), color='k', linestyle='--', label = 'scoreAvg')
        plt.axhline(y=predictions[0], color='r', linestyle='--', label = 'dfltValue')
    plt.title(var)
    plt.ylabel('Score')
    plt.legend(fontsize = 10, loc = 4, framealpha = 0.5)
    plt.grid()

def pdpCharts9(model, df, collist, predictors, n_bins = 10, dfltValue = -99999, maxValRatio = 1):
    """
    Draw PDP-Chart for certain features

    Parameters
    ----------
    model : trained model
    df : pd.DataFrame
        contains all features used in model
    collist : list of string, features need to draw
    predictors : list of string
        all features used in model
    n_bins: int, default 10
        only works with numeric data
    dfltValue : numeric, default -99999
        default value for this feature,
    maxValRatio : numeric, default 1
        assign max value with x quantile

    Returns
    -------
    fig : figure with at most 9 subplots

    predictors 所有模型需要的特征 collist你需要画的
    """

    plt.figure(figsize=(16,8))
    lenth = len(collist)
    cntPlt = int(np.ceil(lenth/9.0))
    figlist = []

    for i in list(range(1, cntPlt+1)):
        fig = plt.figure(i)
        figlist.append(fig)
        j = 1
        for col in collist[(i-1)*9:i*9]:
            plt.subplot(3, 3, j)
            pdpChart_compat(model, df, col, predictors, n_bins, dfltValue = dfltValue, maxVal = df[col].quantile(maxValRatio))
            j+=1
        plt.tight_layout()
        #plt.savefig(pic_name)

        #plt.close()

    return figlist

def pdpChart_compat(model, df, var, predictors, n_bins, dfltValue, maxVal):
    """
    Draw PDP-Chart for certain feature

    Parameters
    ----------
    model : trained model
    df : pd.DataFrame
        contains all features used in model
    var : string, feature need to draw
    predictors : list of string
        all features used in model
    n_bins: int
        only works with numeric data
    dfltValue : numeric, default value for this feature
    maxVal : boolean or numeric
        designed max value for this feature

    Returns
    -------
    fig : figure
    """

    idx = (df[var] != dfltValue)

    if n_bins > df[var].nunique():
        n_bins = df[var].nunique()
        feature_grid = [dfltValue] + sorted(df.loc[idx, var].unique().tolist())
    else:
        feature_grid = range(n_bins)
        if maxVal == 0:
            feature_grid = [dfltValue] + [df.loc[idx, var].min() + val * (maxVal - df.loc[idx, var].min()) / n_bins for
                                          val in feature_grid]
        else:
            feature_grid = [dfltValue] + list(pd.qcut(df.loc[idx, var], q=n_bins, duplicates="drop", retbins=True)[1])

    if df.shape[0] > 10000:
        x_small = df.sample(n=10000, random_state=77)
    else:
        x_small = df

    predictions = []
    n=0
    for feature_val in feature_grid:
        import xgboost as xgb
        x_copy = x_small.copy()
        x_copy[var] = feature_val
        n=n+1

        ##xgbClassifier
        if isinstance(model, xgb.XGBClassifier):
            predictions.append(model.predict_proba(x_copy[predictors])[:,1].mean())
        ##xgb
        elif isinstance(model, xgb.core.Booster):
            predictions.append(model.predict(xgb.DMatrix(x_copy[predictors])).mean())
        ##线性回归
        else:
            predictions.append(model.predict(sm.add_constant(x_copy[predictors])).mean())

    xindex = feature_grid[1:]

    plt.plot(range(len(xindex)), predictions[1:], 'bo-', label='%s' % var)
    plt.xticks(range(len(xindex)), ["%.2g" % i for i in xindex], rotation=45)

    ## dfltvalue
    # XGB
    if isinstance(model, xgb.core.Booster):
        plt.axhline(y=model.predict(xgb.DMatrix(x_small[predictors])).mean(), color='k', linestyle='--', label='scoreAvg')
        plt.axhline(y=predictions[0], color='r', linestyle='--', label='dfltValue')
    elif isinstance(model, xgb.XGBClassifier):
        plt.axhline(y=model.predict_proba(x_copy[predictors])[:,1].mean(), color='k', linestyle='--', label='scoreAvg')
        plt.axhline(y=predictions[0], color='r', linestyle='--', label='dfltValue')
    # LR
    else:
       #plt.axhline(y=model.predict(sm.add_constant(x_small[predictors])).mean(), color='k', linestyle='--', label='scoreAvg')
       pass

    plt.title('pdp Chart of %s' % var)
    plt.ylabel('Score')
    plt.legend(fontsize=10, loc=4, framealpha=0.5)
    plt.grid()
    plt.tight_layout()

##box-cox from Ms.Li-----------------------------------------------------------------------------------------
def univerChart_new(method, df, bin_num, var, target, missing_val):
    tmp_df = df[[var, target]]
    tmp_df[var].fillna(missing_val, inplace=True)

    #####################等频分箱#################################################
    if method == 'freq':
        tmp_df['bin'] = pd.qcut(tmp_df[var][tmp_df[var] != missing_val], bin_num, duplicates='drop')

    #####################等宽分箱###################################################
    if method == 'width':
        tmp_df["bin"] = pd.cut(tmp_df[var][tmp_df[var] != missing_val], bin_num, duplicates='drop')

    tmp_df['bin'] = tmp_df['bin'].cat.add_categories(['missing'])
    tmp_df['bin'][tmp_df[var] == missing_val] = ['missing']

    group_by_bin = tmp_df.groupby(["bin"], as_index=True)

    data_bin = pd.DataFrame()  # 用来记录每个箱体的最大最小值
    data_bin["bin_mean"] = group_by_bin[var].mean()  # 统计每组内的均值
    data_bin["count"] = group_by_bin[var].count()  # 统计每组内的样本数
    data_bin["bad_count"] = group_by_bin[target].sum()  # 统计每组内的坏样本数
    data_bin["event_rate"] = round(data_bin['bad_count'] / data_bin["count"], 4) * 100  # 统计每组内的事件率

    data_bin.reset_index(inplace=True)

    data_bin['group'] = data_bin.index
    data_bin['group'][data_bin.bin == 'missing'] = 'missing'

    # print(var)

    return data_bin

def binEventRate(df, var, fig, j):
    b = df['event_rate']
    l = [i for i in range(df.group.shape[0])]

    ax1 = fig.add_subplot(1, 2, j)
    plt.bar(df.group.astype(str), df['count'], color='blue', label='样本数')
    ax1.legend(loc=1)
    plt.legend(prop={'family': 'SimHei', 'size': 11}, loc="upper left")
    plt.xticks(rotation=0)
    plt.xlabel('分组 - {}'.format(var))

    # 设置百分比形式的坐标轴
    fmt = '%.2f%%'
    yticks = mtick.FormatStrFormatter(fmt)

    ax2 = ax1.twinx()  # this is the important function 镜像
    ax2.plot(df.group.astype(str), df['event_rate'], 'or-', label='逾期率')
    ax2.yaxis.set_major_formatter(yticks)
    # ax2.legend(loc=1);ax2.set_ylabel('逾期率')
    plt.legend(prop={'family': 'SimHei', 'size': 11}, loc="upper right")

    # 将数值显示在图形上
    # for i,(_x,_y) in enumerate(zip(l,b)):
    #     plt.text(_x,_y,b[i],color='black',fontsize=10,)

    plt.title('分组样本数及逾期率 - {}'.format(var))

    # return plt.show()
