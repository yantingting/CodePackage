#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : metrics.py
@Time    : 2020-07-12 19:38
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import os
import json
import logging
from itertools import chain
from math import log
try:
    import xgboost as xgb
except:
    pass
import pandas as pd
import numpy as np
from jinja2 import Template
from sklearn import preprocessing, tree, metrics
from sklearn.linear_model import Lasso, LassoCV
from scipy.stats import stats
from copy import deepcopy
from sklearn.metrics import roc_auc_score
from sklearn.metrics import accuracy_score

#from utils3.data_io_utils import *
from utils3.misc_utils import *
from functools import reduce
from math import log


class BinWoe(object):
    def __init__(self):
        pass


    def categorical_cut_bin(self, x, y, min_size=None, num_max_bins=10):
        """
        单变量：用decision tree给分类变量分箱

        Args:
        x (pd.Series): the original cateogorical variable x，process_missing()处理过的
        y（pd.Series): label
        min_size (int): the minimum size of each bin. If not provided, will be set
        as the max of 200 or 3% of sample size
        num_max_bins (int): the max number of bins to device. default=6

        Returns:
        new_x (pd.Series): the new x with new cateogory labeled with numbers
        encode_mapping (dict): the mapping dictionary between original x and label
            encoded x used for tree building
        spec_dict (dict): the mapping dictionary of new categorical label and original
            categories. Format as rebin_spec as shown above
        """
        if not min_size:
            min_size = max(200, int(len(x)*0.03))

        x_encoded = label_encode(x)
        # label encoded mapping to original x
        map_df = pd.DataFrame({'original': x, 'encoded': x_encoded}).drop_duplicates()
        encode_mapping = dict(list(zip(map_df.original, map_df.encoded)))

        # 因为之后missing的这一个level会被单独拎出来。要确保拎出来之后的level依然有至少3%或50条数据
        num_missing1 = 0
        num_missing2 = 0
        num_missing3 = 0

        if -9999 in x.tolist() or '-9999' in x.tolist():
            num_missing1 = sum(x==-9999) + sum(x=='-9999')
        if -8888 in x.tolist() or '-8888' in x.tolist():
            num_missing2 = sum(x==-8888) + sum(x=='-8888')
        if -8887 in x.tolist() or '-8887' in x.tolist():
            num_missing3 = sum(x==-8887) + sum(x=='-8887')
        if -1 in x.tolist() or '-1' in x.tolist():
            num_missing3 = sum(x==-1) + sum(x=='-1')

        min_samples_leaf = min_size + num_missing1 + num_missing2 + num_missing3
        tree_obj = tree.DecisionTreeClassifier(criterion ='entropy', min_samples_leaf=min_samples_leaf,\
                                               max_leaf_nodes=num_max_bins)
        tree_obj.fit(x_encoded.values.reshape(-1, 1), y)
        # -2 is TREE_UNDEFINED default value
        cut_point = tree_obj.tree_.threshold[tree_obj.tree_.threshold != -2]
        #print cut_point
        new_x = pd.Series(np.zeros(x.shape), index=x.index)
        if len(cut_point) > 0:
            cut_point.sort()
            new_x.loc[x_encoded < cut_point[0]] = 1
            for i in range(len(cut_point)-1):
                new_x[(x_encoded>=cut_point[i]) & (x_encoded<cut_point[i+1])] = i+2
            new_x[x_encoded>=cut_point[len(cut_point)-1]] = len(cut_point) + 1
        else:
            new_x = pd.Series(np.ones(x_encoded.shape), index=x.index)

        if -9999 in x.tolist() or '-9999' in x.tolist():
            new_x.loc[x.isin([-9999, '-9999'])] = '-9999'
        if -8888 in x.tolist() or '-8888' in x.tolist():
            new_x.loc[x.isin([-8888, '-8888'])] = '-8888'
        if -8887 in x.tolist() or '-8887' in x.tolist():
            new_x.loc[x.isin([-8887, '-8887'])] = '-8887'
        if -1 in x.tolist() or '-1' in x.tolist():
            new_x.loc[x.isin([-1, '-1'])] = '-1'

        df1 = x.to_frame('original_x')
        df2 = new_x.to_frame('new_x_category')
        df3 = pd.concat([df1,df2], axis=1)
        df4 = df3.drop_duplicates()
        df4 = df4.loc[(~df4.new_x_category.isin([-8888, -8887, -9999, -1,'-8888', '-8887', '-9999','-1']))]
        spec_dict = {}
        for index, row in df4.iterrows():
            if row['new_x_category'] not in spec_dict:
                spec_dict[row['new_x_category']] = []
            spec_dict[row['new_x_category']].append(row['original_x'])

        return new_x, encode_mapping, spec_dict


    def numerical_cut_bin(self, x, var_type, num_max_bins=10, min_size=None, missing_alone=True):
        """
        Auto classing for one numerical variable. If x has more than 100 unique
        values, then divide it into 20 bins, else if x has more than 10, then divide
        it into 10 bins. else if x has more than 3 unique values, divide accordingly,
        else, keep it as it is and convert to str.
        All 0 and -8888, -8887, -9999 自己持有一箱。当数量unique值<=10且>3时，则<=1
        & > -8887的数值每一个值是一箱

        Args:
        x (pd.Series): original x values, numerical values. process_missing()处理过的
        var_type (str): ['integer', 'float']
        num_max_bins (int): number of max bins to cut. default = 10
        min_size (int): the minimum size of each bin.If not provided, will be set
        as the max of 200 or 3% of sample size
        missing_alone (bool): default=True. -9999, -8888, -8887各自单独一箱.
            If false，缺失值被当成是正常的数据数值参与分箱，可能和实际值的最低箱分在一起

        Returns:
        x_category (pd.Series): binned x
        """
        num_max_bins = int(num_max_bins)
        if num_max_bins <= 2 or pd.isnull(num_max_bins):
            raise "num_max_bins is too small or None. value is %s" % num_max_bins

        if x.dtypes == 'O':
            x = x.astype(float)
        if not min_size:
            min_size = max(200, int(len(x)*0.03))

        precision = 0
        if var_type == 'float':
            if max(x) <= 1:
                precision = 4
            elif max(x) <= 10:
                precision = 3


        if x.nunique() > 3:
            if (-8888 in x.tolist() or -8887 in x.tolist() or -9999 in x.tolist() or -1 in x.tolist()) & missing_alone:
                x_missing1 = x[x.isin([-8888, -8887, -9999,-1])].copy().astype('str')
                x_not_missing = x[(~x.isin([-8888, -8887, -9999,-1]))].copy()
                quantiles_list = np.linspace(0, 1, num=num_max_bins)
                bounds = np.unique(x_not_missing.quantile(quantiles_list).round(precision).tolist())[1:-1]#去掉最小和最大
                min_value = min(x_not_missing)
                if min_value > -1:
                    bounds = np.unique([-1] + list(bounds) + [np.inf])
                else:
                    bounds = np.unique([-np.inf] + list(bounds) + [np.inf])
                x_not_missing_binned = pd.cut(x_not_missing, bounds)
                x_category = pd.concat([x_missing1, x_not_missing_binned]).sort_index().astype('str')
            else:
                quantiles_list = np.linspace(0, 1, num=num_max_bins)
                bounds = np.unique(x.quantile(quantiles_list).round(3).tolist())[1:-1]
                bounds = [-np.inf] + list(bounds) + [np.inf]
                x_category = pd.cut(x, bounds).astype('str')
        else:
            x_category = x.copy().astype('str')

        return x_category



    def binning(self, X, y, var_dict, num_max_bins=10, verbose=True, min_size=None, missing_alone=True):
        """
        Auto Classing
        如果分类变量类别<=5,则保留原始数据

        Args:
        X (pd.Series): 变量宽表，原始值的X
        y (pd.Series): label
        var_dict (pd.DataFrame): 标准变量字典表
        num_max_bins (int): 分类变量auto classing的分箱数
        verbose (bool): default=True. If set True, will print process logging
        min_size (int): the minimum size of each bin.If not provided, will be set
        as the max of 200 or 3% of sample size
        missing_alone (bool): 用于self.numerical_cut_bin()。default=True. -9999, -8888, -8887各自单独一箱.
            If false，缺失值被当成是正常的数据数值参与分箱，可能和实际值的最低箱分在一起

        Returns：
        X_cat (pd.Series): binned X
        all_encoding_map (dict): encoding map for all variables, keys are variable names
                类别变量原始值对应分箱值
                 {'client': {'Android': 0,
                  'DmAppAndroid': 1,
                  'DmAppIOS': 2,
                  'IOS': 3,
                  'JmAppAndroid': 4,
                  'JmAppIOS': 5,
                  'Touch': 6,
                  'WeChat': 7,
                  'Web': 8}}
        all_spec_dict (dict): rebin spec dictionary for all variables, keys are variable names
                类别变量分箱值对应的原始值列表
                {'client': {1.0: ['Android', 'DmAppAndroid', 'DmAppIOS'],
                  2.0: ['IOS', 'JmAppAndroid', 'JmAppIOS'],
                  3.0: ['WeChat', 'Touch'],
                  4.0: ['Web']}}
        """
        X_cat = X.copy()
        all_encoding_map = {}
        all_spec_dict = {}
        for col in X_cat.columns:
            if verbose:
                logging.log(18, col + ' starts binning')

            if '数据类型' in var_dict.columns.values:
                var_type = str(var_dict.loc[var_dict['指标英文']==col, '数据类型'].iloc[0])
            else:
                if X[col].dtype == 'object':
                    var_type = 'varchar'
                else:
                    var_type = 'float'
            if var_type in ['int', 'float', 'int64', 'float64']:
                X_cat.loc[:, col] = self.numerical_cut_bin(X[col], var_type, num_max_bins=num_max_bins,
                    min_size=min_size, missing_alone=missing_alone)
            elif var_type in ['varchar','object']:
                if X[col].nunique() > 5:
                    X_cat.loc[:, col], encode_mapping, spec_dict = self.categorical_cut_bin(X[col].astype(str), y, min_size=min_size, num_max_bins=num_max_bins)
                    all_encoding_map[col] = encode_mapping
                    all_spec_dict[col] = spec_dict


        return X_cat, all_encoding_map, all_spec_dict


    def obtain_boundaries(self, var_bin, missing_alone=True):
        """
        根据已分好箱的变量值，提取分箱界限值.主要应用于连续变量自动分箱的情况下，需要从分好箱
        的数据中提取分箱界限

        Args:
        var_bin (pd.Series): 单变量，已经binning好了的x
        missing_alone (bool): default=True 将min_bound和0作为分箱边界。If False不单独处理，
            仅将最小值和最大值替换为inf

        Returns:
        result (dict): 分箱界限stored in a dict
        {
            'other_categories': [-8888.0, 0.0],
            'cut_boundaries': [0.0, 550.0, 2110.0, 5191.0, 9800.0, 16000.0, 28049.0, 54700.0, inf]
        }
        """
        unique_bin_values = var_bin.astype('str').unique()
        boundaries = [i.replace('(', '').replace(']', '').replace('[', '').replace(')', '').split(', ') for i in unique_bin_values if ',' in i]
        if boundaries:
            boundaries = [float(i) for i in list(chain.from_iterable(boundaries)) if i not in ['nan', 'missing']]
            min_bound = np.min(boundaries)
            max_bound = np.max(boundaries)
            boundaries = [i for i in boundaries if i != min_bound and i != max_bound]
            if missing_alone:
                if ('0' in unique_bin_values or '0.0' in unique_bin_values) and min_bound == 0:
                    boundaries.extend([np.inf, min_bound, -np.inf])#新数据集中可能出现小于min_bound的数据
                elif min_bound == -1:
                    boundaries.extend([np.inf, min_bound, -np.inf])
                else:
                    boundaries.extend([np.inf, -np.inf])
            else:
                boundaries.extend([np.inf, -np.inf])
            boundaries = sorted(set(boundaries))

        not_bin_cat = [float(i) for i in unique_bin_values if ',' not in i]
        result = {
            'cut_boundaries': boundaries,
            'other_categories': not_bin_cat
        }

        return result


    def convert_to_category(self, X, var_dict, rebin_spec, verbose=True,
                                replace_value='-9999'):
        """
        将原始数值的变量宽表按照rebin_spec进行分箱， rebin_spec格式如上所示。

        Args:
        X (pd.DataFrame): X原始值宽表, process_missing()处理过的
        var_dict(pd.DataFrame): 标准变量字典
        rebin_spec(dict): 定义分箱界限的dict
        verbose (bool): default=True. If set True, will print the process logging

        Returns:
        X_cat (pd.DataFrame): X after binning
        """
        X_cat = X.copy()
        for col, strategy in list(rebin_spec.items()):
            if verbose:
                logging.log(18, col + ' starts checking 数据类型 before the conversion')

            right_type = var_dict.loc[var_dict['指标英文']==col, '数据类型'].iloc[0]

            if col in X_cat.columns:
                if verbose:
                    logging.log(18, col + ' starts the binning conversion')

                if right_type == 'object':
                    for category, cat_range in list(strategy.items()):
                        if isinstance(cat_range, list):
                            X_cat.loc[X_cat[col].isin(cat_range), col] = category
                        else:
                            X_cat.loc[(X_cat[col]==cat_range), col] = category
                    #替换后，还有新类别的需要再处理
                    checker_value = list(strategy.keys())+['-9999','-8887','-8888','-1',-9999,-8887,-8888,-1]
                    newobs=~X_cat[col].isin(checker_value)
                    if newobs.sum()>0:
                        if replace_value=='min_value':
                            replace_value = min([str(x) for x in checker_value])
                        logging.warning('{}有新的类别{}出现,替换 {} 条数据值为{}:'.format(col,list(X_cat.loc[newobs, col].unique()),
                                        newobs.sum(),replace_value))
                        X_cat.loc[newobs, col] = replace_value
                else:
                    if X[col].dtypes == 'O':
                        X[col] = X[col].astype(float)

                    if isinstance(strategy, dict) and 'cut_boundaries' in strategy and 'other_categories' in strategy:
                        cut_spec = strategy['cut_boundaries']
                        other_categories = strategy['other_categories']
                        x = X[col].copy()
                        x1 = x.loc[x.isin(other_categories)].copy().astype('str')
                        x2 = x.loc[~x.isin(other_categories)].copy()
                        x2_cutted = pd.cut(x2, cut_spec).astype('str')
                        X_cat.loc[:, col] = pd.concat([x1, x2_cutted]).sort_index()
                    elif isinstance(strategy, list):
                        X_cat.loc[:, col] = pd.cut(X[col], strategy).astype(str)


        return X_cat




    def transform_x_to_woe(self, x, woe):
        """
        Transform a single binned variable to woe value

        Args:
        x (pd.Series): original x that is already converted to categorical, each level should match the name used
            in woe
        woe (pd.Series): contains the categorical level name and the corresponding woe

        Returns:
        x2 (pd.Series): WOE-transformed x

        example:
        >>> x.head()
        0    (-inf, 1.0]
        1    (-inf, 1.0]
        2    (-inf, 1.0]
        3     (3.0, inf]
        4    (-inf, 1.0]
        Name: n_mealsNum, dtype: object
        >>> woe.head()
                   bin       WOE
        0  (-inf, 1.0]  0.027806
        1   (1.0, 3.0] -0.106901
        2   (3.0, inf] -0.179868
        3        -8888  0.443757
        """
        woe.index = woe.bin.astype(str)
        woe_dict = woe.WOE.to_dict()
        x2 = x.copy().astype('str').replace(woe_dict)
        value_type = x2.apply(lambda x: str(type(x)))
        not_converted = value_type.str.contains('str')
        if sum(not_converted) > 0:
            logging.warning("""
            %s 变量包含新值，不在原来的分箱中。
            WOE转换数据为：%s
            未转换成功数据count：%s
            """ % (x.name, json.dumps(woe_dict), x2.loc[not_converted].value_counts()))

            x2.loc[value_type.str.contains('str')] = 0
        return x2.astype(float)


    def transform_x_all(self, X, woe_iv_df):
        """
        Transform binned X to woe value

        Args:
        X (pd.DataFrame): original X that is already converted to categorical,
            each level should match the name used in woe
        woe_iv_df (pd.DataFrame): contains the categorical level name and the corresponding woe. Must
            contain columns: 'bin', 'WOE'. Usually calculate_woe() 返回的result
            should work

        Returns:
        X_woe (pd.DataFrame): WOE-transformed x
        """
        woe_iv_df = woe_iv_df.copy()
        if '指标英文' in woe_iv_df.columns.values:
            woe_iv_df.rename(columns={'指标英文': 'var_code'}, inplace=True)
        if '分箱' in woe_iv_df.columns.values:
            woe_iv_df.rename(columns={'分箱': 'bin'}, inplace=True)

        X_woe = X.copy()
        woe_vars = woe_iv_df.var_code.unique()
        x_vars = X.columns.values
        transform_cols = list(set(woe_vars).intersection(set(x_vars)))
        for var in transform_cols:
            woe = woe_iv_df.loc[woe_iv_df.var_code == var, ['bin', 'WOE']]
            X_woe.loc[:, var] = self.transform_x_to_woe(X_woe[var], woe)
        return X_woe



    def order_bin_output(self, result, col):
        """
        将分箱结果排序，生成一列拍序列。

        Args:
        result (pd.DataFrame): 需要添加排序的数据
        col (str): 分箱的列名
        """
        result = result.copy()
        result.loc[:, col] = result.loc[:, col].astype('str')
        for index, rows in result.iterrows():
            if (',' in rows[col]) & (']' in rows[col] or '(' in rows[col]):
                sort_val = rows[col].replace('(', '')\
                                       .replace(')', '')\
                                       .replace('[', '')\
                                       .replace(']', '')\
                                       .split(', ')[0]
                if 'inf' in sort_val and '-inf' not in sort_val:
                    result.loc[index, 'sort_val'] = 1e10
                elif '-inf' in sort_val:
                    result.loc[index, 'sort_val'] = -1e10
                else:
                    result.loc[index, 'sort_val'] = float(sort_val)
            elif '-1' in rows[col]:
                result.loc[index, 'sort_val'] = -1e9
            elif '-8887' in rows[col]:
                result.loc[index, 'sort_val'] = -1e10
            elif '-8888' in rows[col]:
                result.loc[index, 'sort_val'] = -1e11
            elif '-9999' in rows[col]:
                result.loc[index, 'sort_val'] = -1e12
            elif rows[col] == '0' or rows[col] == '0.0' or rows[col] == '0.00':
                result.loc[index, 'sort_val'] = -0.01
            elif rows[col] == '1' or rows[col] == '1.0' or rows[col] == '1.00':
                result.loc[index, 'sort_val'] = 0.9999
            else:
                try:
                    result.loc[index, 'sort_val'] = float(rows[col])
                except:
                    result.loc[index, 'sort_val'] = 0
        result = result.sort_values('sort_val')
        return result

    def calculate_woe(self, x, y,ks_order='eventrate_order'):
        """
        计算某一个变量的WOE和IV

        Args:
        x (pd.Series): 变量列，已经binning好了的
        y (pd.Series): 标签列
        ks_order(str): 选择KS计算的排序方式，默认eventrate_order，可选bin_order

        Returns:
        result (pd.DataFrame): contains the following columns
        ['var_code', 'bin', 'N', 'PercentDist', 'WOE', 'EventRate', 'PercentBad', 'PercentGood', 'IV']
        """
        x = x.astype(str)
        N = x.value_counts().to_frame('N')
        N.loc[:, 'PercentDist'] = (N * 1.0 / N.sum()).N
        dd = pd.crosstab(x, y)
        dd.index.name = None
        dd2 = dd * 1.0 / dd.sum()
        dd2 = dd2.rename(columns={1: 'PercentBad', 0: 'PercentGood'})
        if 1 in dd.columns:
            dd2.loc[:, 'EventRate'] = dd.loc[:, 1] * 1.0 / dd.sum(1)
            dd2.loc[:, 'NBad'] = dd.loc[:, 1]
        else:
            dd2.loc[:, 'EventRate'] = 0
            dd2.loc[:, 'NBad'] = 0
            dd2.loc[:, 'PercentBad'] = 0

        dd2.loc[:, 'PercentBad'] = dd2.PercentBad.round(4)
        dd2.loc[:, 'PercentGood'] = dd2.PercentGood.round(4)

        woe = np.log(dd2['PercentBad'] / dd2['PercentGood'])
        woe.loc[np.isneginf(woe)] = 0
        woe = woe.replace(np.inf, 0).fillna(0)
        iv = sum((dd2.loc[np.isfinite(woe), 'PercentBad'] - dd2.loc[np.isfinite(woe), 'PercentGood']) * woe[np.isfinite(woe)])
        result = dd2.merge(woe.to_frame('WOE'), left_index=True, right_index= True)\
                    .merge(N, left_index=True, right_index= True)
        result.loc[:, 'var_code'] = x.name
        result.loc[:, 'IV'] = iv

        result = result.reset_index()\
                       .rename(columns={'index': 'bin'})

        # 计算KS之前，要按照逾期率从高到低sort
        if ks_order == 'eventrate_order':
            result = result.sort_values('EventRate', ascending=False)
        elif ks_order == 'bin_order':
            result = self.order_bin_output(result,'bin')
        else:
            logging.info("默认排序'eventrate_order'")
            result = result.sort_values('EventRate', ascending=False)

        result.loc[:, 'cumGood'] = result.PercentGood.cumsum()
        result.loc[:, 'cumBad'] = result.PercentBad.cumsum()
        result.loc[:, 'cum_diff'] = abs(result.cumBad - result.cumGood)
        result.loc[:, 'KS'] = max(result.cum_diff)

        # sum all rows
        # add_row = {'var_code': result.var_code.iloc[0],\
        #            'bin': 'ALL', \
        #            'N': result.N.sum(), \
        #            'PercentDist': result.PercentDist.sum(),\
        #            'NBad': result.NBad.sum(),\
        #            'EventRate': result.NBad.sum() * 1.0 / result.N.sum(),
        #            'PercentBad': result.PercentBad.sum(),\
        #            'PercentGood': result.PercentGood.sum()}
        # result = result.append(pd.DataFrame(add_row, [len(result)]))

        # 添加排序列:按照分箱值排序
        result = self.order_bin_output(result, 'bin')

        return result[['var_code', 'bin', 'N', 'PercentDist', 'WOE', 'NBad', \
                       'EventRate', 'PercentBad', 'PercentGood', 'cumBad', \
                       'cumGood', 'cum_diff', 'IV', 'KS', 'sort_val']]


    def calculate_woe_all(self, X, y, var_dict=None, all_spec_dict0=None,
                              verbose=True, ks_order='eventrate_order'):
        """
        计算所有变量的WOE和IV

        Args:
        x (pd.DataFrame): 变量列，已经binning好了的
        y (pd.Series): 标签列
        var_dict (pd.DataFrame): 标准变量字典. 请做一些初期的数据源筛选。因为有些变量在
            不同数据源当中都有，用的也是相同的英文名称
        all_spec_dict (dict): 分类变量的数据在用tree分类后返回的数字标签和相应的原始
            分类对应关系。 categorical_cut_bin()返回的all_spec_dict格式
        verbose (bool): default=True. If set True, will print the process logging
        ks_order: 'eventrate_order' 按照badrate排序后计算ks；'bin_order' 按照变量的分箱顺序计算ks

        Returns:
        woe_iv_df (pd.DataFrame): contains the following columns
        [u'数据源', u'指标英文', u'指标中文', u'数据类型', u'指标类型',\
        u'分箱', 'N', u'分布占比', 'WOE', u'逾期率', u'Bad分布占比',\
        u'Good分布占比', 'IV']
        """
        if all_spec_dict0 is None:
            spec_dict_flag = False
        else:
            spec_dict_flag = True
            all_spec_dict = deepcopy(all_spec_dict0)
            all_spec_dict = {k:v for k,v in list(all_spec_dict.items()) \
                                if var_dict.loc[var_dict['指标英文']==k, '数据类型'].iloc[0]=='varchar'}

        woe_iv_result = []
        for col in X.columns.values:
            if verbose:
                logging.log(18, col + ' woe calculation starts')

            woe_iv_result.append(self.calculate_woe(X[col], y,ks_order=ks_order))

        woe_iv_df = pd.concat(woe_iv_result)
        woe_iv_df.loc[:, 'comment'] = 'useless_0.02_minus'
        woe_iv_df.loc[(woe_iv_df.IV <0.1) & (woe_iv_df.IV >= 0.02), 'comment'] = 'weak_predictor_0.02_0.1'
        woe_iv_df.loc[(woe_iv_df.IV <0.3) & (woe_iv_df.IV >= 0.1), 'comment'] = 'medium_predictor_0.1_0.3'
        woe_iv_df.loc[(woe_iv_df.IV <0.5) & (woe_iv_df.IV >= 0.3), 'comment'] = 'strong_predictor_0.3_0.5'
        woe_iv_df.loc[(woe_iv_df.IV >= 0.5), 'comment'] = 'strong_predictor_0.5_plus'
        woe_iv_df = woe_iv_df.rename(columns = {
                                        'var_code': '指标英文',
                                        'bin': '分箱',
                                        'PercentDist': '分布占比',
                                        'PercentBad': 'Bad分布占比',
                                        'PercentGood': 'Good分布占比',
                                        'EventRate': '逾期率',
                                        'NBad': '坏样本数量',
                                        'cumBad': 'Cumulative Bad Rate',
                                        'cumGood': 'Cumulative Good Rate',
                                        'cum_diff': 'Cumulative Rate Difference'
                                    })
        if var_dict is None:
            var_dict_flag = False
            woe_result = woe_iv_df
        else:
            var_dict_flag = True
            woe_result = var_dict.loc[:, ['数据源', '指标英文', '指标中文', '数据类型', '指标类型']]\
                        .merge(woe_iv_df, on='指标英文', how='right')

        if spec_dict_flag:
            # 加上分类变量的数据在用tree分类后返回的数字标签和相应的原始分类对应关系。
            woe_result.loc[:, '分箱对应原始分类'] = None
            for col, col_spec in list(all_spec_dict.items()):
                for new_label, original_x in list(col_spec.items()):
                    col_spec[str(new_label)] = ', '.join([str(i) for i in original_x])
                woe_result.loc[woe_result['指标英文'] == col, '分箱对应原始分类'] = woe_result.loc[woe_result['指标英文'] == col, '分箱'].astype('str')
                woe_result.loc[woe_result['指标英文'] == col, '分箱对应原始分类'] = woe_result.loc[woe_result['指标英文'] == col, '分箱对应原始分类'].replace(col_spec)
            if var_dict_flag:
                reorder_cols = ['数据源', '指标英文', '指标中文', '数据类型', '指标类型',\
                            '分箱', '分箱对应原始分类', 'N', '分布占比', '坏样本数量', '逾期率', 'WOE', \
                            'Bad分布占比', 'Good分布占比', 'Cumulative Bad Rate',\
                            'Cumulative Good Rate', 'Cumulative Rate Difference',\
                            'IV', 'KS', 'comment', 'sort_val']
            else:
                reorder_cols = ['指标英文','分箱', '分箱对应原始分类', 'N', '分布占比', '坏样本数量', '逾期率', 'WOE', \
                            'Bad分布占比', 'Good分布占比', 'Cumulative Bad Rate',\
                            'Cumulative Good Rate', 'Cumulative Rate Difference',\
                            'IV', 'KS', 'comment', 'sort_val']
        else:
            if var_dict_flag:
                reorder_cols = ['数据源', '指标英文', '指标中文', '数据类型', '指标类型',\
                            '分箱', 'N', '分布占比', '坏样本数量', '逾期率', 'WOE', \
                            'Bad分布占比', 'Good分布占比', 'Cumulative Bad Rate',\
                            'Cumulative Good Rate', 'Cumulative Rate Difference',\
                            'IV', 'KS', 'comment', 'sort_val']
            else:
                reorder_cols = ['指标英文','分箱', 'N', '分布占比', '坏样本数量', '逾期率', 'WOE', \
                            'Bad分布占比', 'Good分布占比', 'Cumulative Bad Rate',\
                            'Cumulative Good Rate', 'Cumulative Rate Difference',\
                            'IV', 'KS', 'comment', 'sort_val']
        result = woe_result[reorder_cols]
        return result

    def create_rebin_spec(self, woe_iv_df, cat_spec_dict, original_cat_spec, missing_alone=True):
        """
        把分类变量的 level:grouping:label_number 或 level:label_number综合到一起以及
        连续变量的分箱

        Args:
        woe_iv_df (pd.DataFrame): woe_iv_df data frame, 标准输出
        cat_spec_dict (dict): categorical variable binning 的输出
        original_cat_spec (dict): categorical variable binning 的输出
        missing_alone: 是否将-8887等缺失值单独处理为一箱，默认单独处理，传入 obtain_boundaries
        # woe_iv = pd.read_excel(os.path.join(RESULT_PATH, 'WOE_bin/%s_woe_iv_df.xlsx' % model_label), encoding='utf-8')
        # cat_spec_dict = load_data_from_pickle(RESULT_PATH, 'spec_dict/%s_spec_dict.pkl' % model_label)
        # original_cat_spec = load_data_from_pickle(RESULT_PATH, 'spec_dict/%s_encoding_map.pkl' % model_label)

        Returns:
        cat_spec_dict (dict): 合并好的dictionary
        """
        cat_spec_dict_copy = deepcopy(cat_spec_dict)
        for col, col_spec in list(cat_spec_dict_copy.items()):
            for cat_name, cat_spec in list(col_spec.items()):
                if isinstance(cat_name, str) and isinstance(cat_spec, str):
                    del col_spec[cat_name]
        cols = woe_iv_df.loc[woe_iv_df['数据类型']!='varchar', '指标英文'].tolist()
        cols = set(cols) - set(cat_spec_dict_copy.keys())
        #处理数值型变量
        for col in cols:
            var_bin = woe_iv_df.loc[woe_iv_df['指标英文'] == col, '分箱'].copy()
            cat_spec_dict_copy[col] = self.obtain_boundaries(var_bin, missing_alone=missing_alone)
        original_cat_cols = list(set(original_cat_spec.keys()) - set(cat_spec_dict_copy.keys()))
        if original_cat_cols:
            for col in original_cat_cols:
                cat_spec_dict_copy[col] = {v:k for k, v in list(original_cat_spec[col].items())}

        return cat_spec_dict_copy


    def generate_bin_map(self, bin_boundaries):
        """
        For numerical variables, 分箱转换成对应的数值.
        Args:
        bin_boundaries (dict): 分箱spec
            {
                'other_categories': [-8888.0, 0.0],
                'cut_boundaries': [0.0, 550.0, 2110.0, 5191.0, 9800.0, 16000.0, 28049.0, 54700.0, np.inf]
            }

        Returns:
        bin_to_label (dict): key为分箱，value为对应的数值
            {'(0.0, 1.0]': 0, '(1.0, inf]': 1}
        """
        a = pd.Series(0)
        # 这样拿到的是最全的分箱值，不依赖数据里是否有这一分箱。且顺序是排好的
        complete_bins = [str(i) for i in pd.cut(a, bin_boundaries['cut_boundaries']).cat.categories]
        complete_bins_list = sorted(bin_boundaries['other_categories']) + list(complete_bins)
        complete_bins_df = pd.Series(complete_bins_list)
        reversed_map = complete_bins_df.to_dict()
        bin_to_label = {v:k for k,v in reversed_map.items()}
        return bin_to_label


class Performance(object):
    def __init__(self):
        pass

    def p_to_score(self, p, PDO=20.0, Base=600, Ratio=1.0):
        """
        逾期概率转换分数

        Args:
        p (float): 逾期概率
        PDO (float): points double odds. default = 75
        Base (int): base points. default = 660
        Ratio (float): bad:good ratio. default = 1.0/15.0

        Returns:
        化整后的模型分数
        """
        B = 1.0*PDO/log(2)
        A = Base+B*log(Ratio)
        score=A-B*log(p/(1-p))
        return round(score,0)

    def score_to_p(self, score, PDO=75.0, Base=660, Ratio=1.0/15.0):
        """
        分数转换逾期概率

        Args:
        score (float): 模型分数
        PDO (float): points double odds. default = 75
        Base (int): base points. default = 660
        Ratio (float): bad:good ratio. default = 1.0/15.0

        Returns:
        转化后的逾期概率
        """
        B = 1.0*PDO/log(2)
        A = Base+B*log(Ratio)
        alpha = (A - score) / B
        p = np.exp(alpha) / (1+np.exp(alpha))
        return p


    def calculate_ks_by_decile(self, score, y, job, q=10, score_bin_size = 25, manual_cut_bounds=[], score_type='raw'):
        """
        可同时计算decile analysis和Runbook analysis

        Args:
        score (pd.Series): 计算好���模型分
        y (pd.Series): 逾期事件label
        job (str): ['decile', 'runbook'], decile时，将会把score平分成q份， runkbook时，
        将会把平分分成25分一档的区间。有一种runbook是decile时，q=20
        q (int): default = 10, 将score分成等分的数量
        manual_cut_bounds (list): default = [], 当需要手动切分箱的时候，可以将分箱的bounds
            传入。
        score_type (str): ['raw', 'binned']. default='raw'. if 'raw', 传入的 score 是原始分数, if 'binned',传入的是分箱好的数据

        Returns:
        r (pd.DataFrame): 按照score分箱计算的EventRate, CumBadPct, CumGoodPct等，用来
        放置于model evaluation结果里面。
        """
        if score_type == 'raw':
            if job == 'decile':
                if len(manual_cut_bounds) == 0:
                    decile_score = pd.qcut(score, q=q, duplicates='drop', precision=0).astype(str) #, labels=range(1,11))
                else:
                    decile_score = pd.cut(score, manual_cut_bounds, precision=0).astype(str)
            if job == 'runbook':
                if len(manual_cut_bounds) == 0:
                    min_score = int(np.round(min(score)))
                    max_score = int(np.round(max(score)))
                    score_bin_bounardies = list(range(min_score, max_score, score_bin_size))
                    score_bin_bounardies[0] = min_score - 0.001
                    score_bin_bounardies[-1] = max_score
                    decile_score = pd.cut(score, score_bin_bounardies, precision=0).astype(str)
                else:
                    decile_score = pd.cut(score, manual_cut_bounds, precision=0).astype(str)
        else:
            decile_score = score.astype(str)
        r = pd.crosstab(decile_score, y).rename(columns={0: 'N_nonEvent', 1: 'N_Event'})
        if 'N_Event' not in r.columns:
            r.loc[:, 'N_Event'] = 0
        if 'N_nonEvent' not in r.columns:
            r.loc[:, 'N_nonEvent'] = 0
        r.index.name = None
        r.loc[:, 'N_sample'] = decile_score.value_counts()
        r.loc[:, 'EventRate'] = r.N_Event * 1.0 / r.N_sample
        r.loc[:, 'BadPct'] = r.N_Event * 1.0 / sum(r.N_Event)
        r.loc[:, 'GoodPct'] = r.N_nonEvent * 1.0 / sum(r.N_nonEvent)
        r.loc[:, 'Pct'] = r.N_sample * 1.0 / sum(r.N_sample)
        r.loc[:, 'CumPct'] = r.Pct.cumsum()
        r.loc[:, 'CumBadPct'] = r.BadPct.cumsum()
        r.loc[:, 'CumGoodPct'] = r.GoodPct.cumsum()
        r.loc[:, 'KS'] = np.round(r.CumBadPct - r.CumGoodPct, 4)
        r.loc[:, 'odds(good:bad)'] = np.round(r.N_nonEvent*1.0 / r.N_Event, 1)
        r = r.reset_index().rename(columns={'index': '分箱',
                                            'N_sample': '样本数',
                                            'N_nonEvent': '好样本数',
                                            'N_Event': '坏样本数',
                                            'EventRate': '逾期率',
                                            'Pct': '样本分布占比',
                                            'BadPct': 'Bad分布占比',
                                            'GoodPct': 'Good分布占比',
                                            'CumBadPct': '累积Bad占比',
                                            'CumGoodPct': '累积Good占比'
                                            })
        reorder_cols = ['分箱', '样本数', '样本分布占比','好样本数', '坏样本数', '逾期率',\
                        'Bad分布占比', 'Good分布占比','累积Bad占比', '累积Good占比',\
                        'KS', 'odds(good:bad)']
        result = r[reorder_cols]
        result.loc[:, '分箱'] = result.loc[:, '分箱'].astype(str)
        return result

    def calculate_ks_by_decile_proba(self, score, y, job, q=10, score_bin_size = 25, manual_cut_bounds=[], score_type='raw'):
        """
        可同时计算decile analysis和Runbook analysis

        Args:
        score (pd.Series): 计算好的模型分
        y (pd.Series): 逾期事件label
        job (str): ['decile', 'runbook'], decile时，将会把score平分成q份， runkbook时，
        将会把平分分成25分一档的区间。有一种runbook是decile时，q=20
        q (int): default = 10, 将score分成等分的数量
        manual_cut_bounds (list): default = [], 当需要手动切分箱的时候，可以将分箱的bounds
            传入。
        score_type (str): ['raw', 'binned']. default='raw'. if 'raw', 传入的 score 是原始分数, if 'binned',传入的是分箱好的数据

        Returns:
        r (pd.DataFrame): 按照score分箱计算的EventRate, CumBadPct, CumGoodPct等，用来
        放置于model evaluation结果里面。
        """
        if score_type == 'raw':
            if job == 'decile':
                if len(manual_cut_bounds) == 0:
                    decile_score = pd.qcut(score, q=q, duplicates='drop', precision=10).astype(str) #, labels=range(1,11))
                else:
                    decile_score = pd.cut(score, manual_cut_bounds, precision=10).astype(str)
            if job == 'runbook':
                if len(manual_cut_bounds) == 0:
                    min_score = int(np.round(min(score)))
                    max_score = int(np.round(max(score)))
                    score_bin_bounardies = list(range(min_score, max_score, score_bin_size))
                    score_bin_bounardies[0] = min_score - 0.001
                    score_bin_bounardies[-1] = max_score
                    decile_score = pd.cut(score, score_bin_bounardies, precision=10).astype(str)
                else:
                    decile_score = pd.cut(score, manual_cut_bounds, precision=10).astype(str)
        else:
            decile_score = score.astype(str)
        r = pd.crosstab(decile_score, y).rename(columns={0: 'N_nonEvent', 1: 'N_Event'})
        if 'N_Event' not in r.columns:
            r.loc[:, 'N_Event'] = 0
        if 'N_nonEvent' not in r.columns:
            r.loc[:, 'N_nonEvent'] = 0
        r.index.name = None
        r.loc[:, 'N_sample'] = decile_score.value_counts()
        r.loc[:, 'EventRate'] = r.N_Event * 1.0 / r.N_sample
        r.loc[:, 'BadPct'] = r.N_Event * 1.0 / sum(r.N_Event)
        r.loc[:, 'GoodPct'] = r.N_nonEvent * 1.0 / sum(r.N_nonEvent)
        r.loc[:, 'Pct'] = r.N_sample * 1.0 / sum(r.N_sample)
        r.loc[:, 'CumPct'] = r.Pct.cumsum()
        r.loc[:, 'CumBadPct'] = r.BadPct.cumsum()
        r.loc[:, 'CumGoodPct'] = r.GoodPct.cumsum()
        r.loc[:, 'KS'] = np.round(r.CumBadPct - r.CumGoodPct, 4)
        r.loc[:, 'odds(good:bad)'] = np.round(r.N_nonEvent*1.0 / r.N_Event, 1)
        r = r.reset_index().rename(columns={'index': '分箱',
                                            'N_sample': '样本数',
                                            'N_nonEvent': '好样本数',
                                            'N_Event': '坏样本数',
                                            'EventRate': '逾期率',
                                            'Pct': '样本分布占比',
                                            'BadPct': 'Bad分布占比',
                                            'GoodPct': 'Good分布占比',
                                            'CumBadPct': '累积Bad占比',
                                            'CumGoodPct': '累积Good占比'
                                            })
        reorder_cols = ['分箱', '样本数', '样本分布占比','好样本数', '坏样本数', '逾期率',\
                        'Bad分布占比', 'Good分布占比','累积Bad占比', '累积Good占比',\
                        'KS', 'odds(good:bad)']
        result = r[reorder_cols]
        result.loc[:, '分箱'] = result.loc[:, '分箱'].astype(str)
        return result


    def psi(self, before, after, raw_cat=True):
        """
        计算PSI

        Args:
        before (pd.Series): 基准时点分箱好的数据
        after (pd.Series): 比较时点的分箱好的数据
        raw_cat (bool): default=True. 传入的数据为分箱好的数据。if False, 传入的数据是
            value_counts 好的，比如decile表格的现成的

        Returns:
        combined (pd.DataFrame): 对齐好的before and after 占比数据
        psi_value (float): 计算的PSI值
        """
        #object在.sort_index()会报错，修改为str
        if before.dtype=='object':
            before=before.astype(str)
        if after.dtype=='object':
            after=after.astype(str)

        if raw_cat:
            before_ct = before.value_counts().sort_index()
            after_ct = after.value_counts().sort_index()
        else:
            before_ct = before
            after_ct = after

        before_pct = before_ct * 1.0 / len(before)
        after_pct = after_ct * 1.0 / len(after)

        if type(before_pct) == pd.Series:
            before_pct = before_pct.to_frame('before_pct')
        if type(after_pct) == pd.Series:
            after_pct = after_pct.to_frame('after_pct')

        if type(before_ct) == pd.Series:
            before_ct = before_ct.to_frame('before_ct')
        if type(after_ct) == pd.Series:
            after_ct = after_ct.to_frame('after_ct')

        df_list = [before_ct, after_ct, before_pct, after_pct]
        # outer join是因为可能出现before， after数据各自分出的箱不全
        combined = reduce(lambda b, a: b.merge(a, left_index=True, right_index=True, how='outer'), \
                          df_list)
        # 保留两位小数，以防止在比较小占比的箱上，一点点的波动导致PSI大
        combined.loc[:, 'before_pct'] = combined.before_pct.round(2)
        combined.loc[:, 'after_pct'] = combined.after_pct.round(2)

        #处理某些分箱无数据造成psi inf
        psi_index_value = (np.log(combined.after_pct/combined.before_pct) * (combined.after_pct - combined.before_pct))
        psi_index_value = psi_index_value.replace(np.inf, 0)
        combined.loc[:, 'PSI'] = np.nansum(psi_index_value)
        combined = combined.reset_index().rename(columns={'index': '分箱'})
        # 添加排序列并排序
        combined = BinWoe().order_bin_output(combined, '分箱')
        combined = combined.drop('sort_val', 1)

        return combined, psi_index_value.sum()


    def variable_psi(self, X_cat_train, X_cat_test, var_dict):
        """
        批量计算变量的PSI

        Args:
        X_cat_train (pd.DataFrame): 分箱好的train数据，或者基准时点的数据
        X_cat_test (pd.DataFrame): 分箱好的test数据，或者比较时点的数据

        Returns
        result (pd.DataFrame): 包含指标英文，PSI两列。
        """
        before_columns = X_cat_train.columns
        after_columns = X_cat_test.columns
        common_columns = set(before_columns).intersection(set(after_columns))
        psi_result = []
        for col in common_columns:
            logging.log(18, 'variable PSI for %s' % col)
            psi_df, _ = self.psi(X_cat_train[col], X_cat_test[col])
            psi_df.loc[:, '指标英文'] = col
            psi_result.append(psi_df)

        result = pd.concat(psi_result)
        var_dict = var_dict.loc[:, ['数据源', '指标英文', '指标中文', '数据类型', '指标类型']]

        result = result.merge(var_dict, on='指标英文')

        result = result.rename(columns={'before_ct': '基准时点_N', \
                                       'before_pct': '基准时点占比',\
                                       'after_ct': '比较时点_N',\
                                       'after_pct': '比较时点占比'})

        ordered_cols = ['指标英文', 'PSI']
        psi_df = pd.DataFrame(result.groupby(['指标英文'])['PSI'].agg('sum').reset_index())
        return psi_df


    def data_score_KS(self, train, test, pre_label):
        #### TRAIN ####
        y_train = train.label.values
        y_train_pred = train[pre_label]
        # Score
        score_train = [round(self.p_to_score(value)) for value in y_train_pred]
        # 分数分箱
        decile_score = pd.qcut(pd.DataFrame(score_train)[0], q=10, duplicates='drop', precision=0).astype(str)
        decile_score_20 = pd.qcut(pd.DataFrame(score_train)[0], q=20,duplicates ='drop', precision=0).astype(str)
        # 概率分箱
        decile_proba = pd.qcut(y_train_pred, q=10, duplicates='drop', precision=6).astype(str)
        decile_proba_20 = pd.qcut(y_train_pred, q=20,duplicates ='drop', precision=6).astype(str)
        # data_scored_train
        data_scored_train = pd.DataFrame([train.index, y_train, y_train_pred.T, score_train, decile_score, decile_score_20,\
                                        decile_proba, decile_proba_20]).T
        data_scored_train = data_scored_train.rename(columns = {0:'order_no', 1:'Y',2:'y_pred',3:'score',4:'score_bin', 5:'score_bin_20',\
                                                6:'proba_bin', 7:'proba_bin_20'})
        data_scored_train['score'] = data_scored_train['score'].astype(float)
        data_scored_train['Y'] = data_scored_train['Y'].astype(float)
        data_scored_train['y_pred'] = data_scored_train['y_pred'].astype(float)

        # train_ks
        train_ks = self.calculate_ks_by_decile_proba(data_scored_train['y_pred'], data_scored_train['Y'],'decile', q=10)
        train_ks['KS'] = abs(train_ks['KS'])
        train_proba_boundary = BinWoe().obtain_boundaries(train_ks.分箱, missing_alone=False)  #注意输出的是个dictionary
        train_proba_bin = train_proba_boundary['cut_boundaries']
        train_ks.loc[:, '累计占比'] = train_ks.样本分布占比.cumsum()
        train_ks.loc[:, '相应逾期率'] = train_ks.坏样本数.cumsum() * 1.0 / train_ks.样本数.cumsum()
        # train_ks_20
        train_ks_20 = self.calculate_ks_by_decile_proba(data_scored_train['y_pred'], data_scored_train['Y'],'decile', q=20)
        train_ks_20['KS'] = abs(train_ks_20['KS'])
        train_proba_boundary_20 = BinWoe().obtain_boundaries(train_ks_20.分箱, missing_alone=False)  #注意输出的是个dictionary
        train_proba_bin_20 = train_proba_boundary_20['cut_boundaries']
        train_ks_20.loc[:, '累计占比'] = train_ks_20.样本分布占比.cumsum()
        train_ks_20.loc[:, '相应逾期率'] = train_ks_20.坏样本数.cumsum() * 1.0 / train_ks_20.样本数.cumsum()
        # train_ks
        train_ks = self.calculate_ks_by_decile(data_scored_train['score'],data_scored_train['Y'],'decile', q=10)
        train_score_boundary = BinWoe().obtain_boundaries(train_ks.分箱, missing_alone=False)
        train_score_bin = train_score_boundary['cut_boundaries']
        train_ks.loc[:, '累计占比'] = train_ks.样本分布占比[::-1].cumsum()[::-1]
        train_ks.loc[:, '相应逾期率'] = train_ks.坏样本数[::-1].cumsum()[::-1]* 1.0 / train_ks.样本数[::-1].cumsum()[::-1]
        # train_ks_20
        train_ks_20 = self.calculate_ks_by_decile(data_scored_train['score'],data_scored_train['Y'],'decile', q=20)
        train_score_boundary_20 = BinWoe().obtain_boundaries(train_ks_20.分箱, missing_alone=False)
        train_score_bin_20 = train_score_boundary_20['cut_boundaries']
        train_ks_20.loc[:, '累计占比'] = train_ks_20.样本分布占比[::-1].cumsum()[::-1]
        train_ks_20.loc[:, '相应逾期率'] = train_ks_20.坏样本数[::-1].cumsum()[::-1]* 1.0 / train_ks_20.样本数[::-1].cumsum()[::-1]

        #### TEST ####
        y_test = test.label.values
        y_test_pred = test[pre_label]
        # Score
        score_test = [round(self.p_to_score(value)) for value in y_test_pred]
        # data_scored_test
        data_scored_test = pd.DataFrame([test.index, y_test, y_test_pred, score_test]).T
        data_scored_test = data_scored_test.rename(columns = {0:'order_no', 1:'Y', 2:'y_pred', 3:'score'})

        data_scored_test['score'] = data_scored_test['score'].astype(float)
        data_scored_test['Y'] = data_scored_test['Y'].astype(float)
        data_scored_test['y_pred'] = data_scored_test['y_pred'].astype(float)

        data_scored_test['score_bin'] = pd.cut(data_scored_test['score'], bins = train_score_bin, precision=0)
        data_scored_test['score_bin_20'] = pd.cut(data_scored_test['score'], bins = train_score_bin_20, precision=0)
        data_scored_test['proba_bin'] = pd.cut(data_scored_test['y_pred'], bins = train_proba_bin, precision=10)
        data_scored_test['proba_bin_20'] = pd.cut(data_scored_test['y_pred'], bins = train_proba_bin_20, precision=10)
        # test_ks
        test_ks = self.calculate_ks_by_decile_proba(data_scored_test['y_pred'],data_scored_test['Y'],'decile', manual_cut_bounds = train_proba_bin)
        test_ks['KS'] = abs(test_ks['KS'])
        test_ks.loc[:, '累计占比'] = test_ks.样本分布占比.cumsum()
        test_ks.loc[:, '相应逾期率'] = test_ks.坏样本数.cumsum() * 1.0 / test_ks.样本数.cumsum()
        # test_ks_20
        test_ks_20 = self.calculate_ks_by_decile_proba(data_scored_test['y_pred'],data_scored_test['Y'],'decile', manual_cut_bounds = train_proba_bin_20)
        test_ks_20['KS'] = abs(test_ks_20['KS'])
        test_ks_20.loc[:, '累计占比'] = test_ks_20.样本分布占比.cumsum()
        test_ks_20.loc[:, '相应逾期率'] = test_ks_20.坏样本数.cumsum() * 1.0 / test_ks_20.样本数.cumsum()
        # test_ks
        test_ks = self.calculate_ks_by_decile(data_scored_test['score'],data_scored_test['Y'],'decile', manual_cut_bounds = train_score_bin)
        test_ks.loc[:, '累计占比'] = test_ks.样本分布占比[::-1].cumsum()[::-1]
        test_ks.loc[:, '相应逾期率'] = test_ks.坏样本数[::-1].cumsum()[::-1]* 1.0 / test_ks.样本数[::-1].cumsum()[::-1]
        # test_ks_20
        test_ks_20 = self.calculate_ks_by_decile(data_scored_test['score'],data_scored_test['Y'],'decile', manual_cut_bounds = train_score_bin_20)
        test_ks_20.loc[:, '累计占比'] = test_ks_20.样本分布占比[::-1].cumsum()[::-1]
        test_ks_20.loc[:, '相应逾期率'] = test_ks_20.坏样本数[::-1].cumsum()[::-1]* 1.0 / test_ks_20.样本数[::-1].cumsum()[::-1]

        return data_scored_train, train_ks, train_ks_20, train_ks, train_ks_20, data_scored_test, test_ks, test_ks_20, test_ks, test_ks_20





    def data_KS(self, train, test, pre_label, method='proba'):
        #### TRAIN ####
        y_train = train.label.values
        y_train_pred = train[pre_label]
        # Score
        score_train = [round(self.p_to_score(value)) for value in y_train_pred]

        #### TEST ####
        y_test = test.label.values
        y_test_pred = test[pre_label]
        # Score
        score_test = [round(self.p_to_score(value)) for value in y_test_pred]
        # data_scored_test
        data_scored_test = pd.DataFrame([test.index, y_test, y_test_pred, score_test]).T
        data_scored_test = data_scored_test.rename(columns = {0:'order_no', 1:'Y', 2:'y_pred', 3:'score'})

        data_scored_test['score'] = data_scored_test['score'].astype(float)
        data_scored_test['Y'] = data_scored_test['Y'].astype(float)
        data_scored_test['y_pred'] = data_scored_test['y_pred'].astype(float)

        if method == 'proba':
            # train概率分箱
            decile_proba = pd.qcut(y_train_pred, q=10, duplicates='drop', precision=6).astype(str)
            decile_proba_20 = pd.qcut(y_train_pred, q=20,duplicates ='drop', precision=6).astype(str)
            # data_scored_train
            data_scored_train = pd.DataFrame([train.index, y_train, y_train_pred.T, score_train, decile_proba, decile_proba_20]).T
            data_scored_train = data_scored_train.rename(columns = {0:'order_no', 1:'Y',2:'y_pred',3:'score',4:'proba_bin', 5:'proba_bin_20'})
            data_scored_train['score'] = data_scored_train['score'].astype(float)
            data_scored_train['Y'] = data_scored_train['Y'].astype(float)
            data_scored_train['y_pred'] = data_scored_train['y_pred'].astype(float)

            # train_ks
            train_ks = self.calculate_ks_by_decile_proba(data_scored_train['y_pred'], data_scored_train['Y'],'decile', q=10)
            train_ks['KS'] = abs(train_ks['KS'])
            train_proba_boundary = BinWoe().obtain_boundaries(train_ks.分箱, missing_alone=False)  #注意输出的是个dictionary
            train_proba_bin = train_proba_boundary['cut_boundaries']
            train_ks.loc[:, '累计占比'] = train_ks.样本分布占比.cumsum()
            train_ks.loc[:, '相应逾期率'] = train_ks.坏样本数.cumsum() * 1.0 / train_ks.样本数.cumsum()
            # train_ks_20
            train_ks_20 = self.calculate_ks_by_decile_proba(data_scored_train['y_pred'], data_scored_train['Y'],'decile', q=20)
            train_ks_20['KS'] = abs(train_ks_20['KS'])
            train_proba_boundary_20 = BinWoe().obtain_boundaries(train_ks_20.分箱, missing_alone=False)  #注意输出的是个dictionary
            train_proba_bin_20 = train_proba_boundary_20['cut_boundaries']
            train_ks_20.loc[:, '累计占比'] = train_ks_20.样本分布占比.cumsum()
            train_ks_20.loc[:, '相应逾期率'] = train_ks_20.坏样本数.cumsum() * 1.0 / train_ks_20.样本数.cumsum()
            # test_ks
            data_scored_test['proba_bin'] = pd.cut(data_scored_test['y_pred'], bins = train_proba_bin, precision=6)
            data_scored_test['proba_bin_20'] = pd.cut(data_scored_test['y_pred'], bins = train_proba_bin_20, precision=6)
            test_ks = self.calculate_ks_by_decile_proba(data_scored_test['y_pred'],data_scored_test['Y'],'decile', manual_cut_bounds = train_proba_bin)
            test_ks['KS'] = abs(test_ks['KS'])
            test_ks.loc[:, '累计占比'] = test_ks.样本分布占比.cumsum()
            test_ks.loc[:, '相应逾期率'] = test_ks.坏样本数.cumsum() * 1.0 / test_ks.样本数.cumsum()
            # test_ks_20
            test_ks_20 = self.calculate_ks_by_decile_proba(data_scored_test['y_pred'],data_scored_test['Y'],'decile', manual_cut_bounds = train_proba_bin_20)
            test_ks_20['KS'] = abs(test_ks_20['KS'])
            test_ks_20.loc[:, '累计占比'] = test_ks_20.样本分布占比.cumsum()
            test_ks_20.loc[:, '相应逾期率'] = test_ks_20.坏样本数.cumsum() * 1.0 / test_ks_20.样本数.cumsum()

        elif method == 'score':
            # train分数分箱
            decile_score = pd.qcut(pd.DataFrame(score_train)[0], q=10, duplicates='drop', precision=0).astype(str)
            decile_score_20 = pd.qcut(pd.DataFrame(score_train)[0], q=20,duplicates ='drop', precision=0).astype(str)
            # data_scored_train
            data_scored_train = pd.DataFrame([train.index, y_train, y_train_pred.T, score_train, decile_score, decile_score_20]).T
            data_scored_train = data_scored_train.rename(columns = {0:'order_no', 1:'Y',2:'y_pred',3:'score',4:'score_bin', 5:'score_bin_20'})
            data_scored_train['score'] = data_scored_train['score'].astype(float)
            data_scored_train['Y'] = data_scored_train['Y'].astype(float)
            data_scored_train['y_pred'] = data_scored_train['y_pred'].astype(float)

            # train_ks
            train_ks = self.calculate_ks_by_decile(data_scored_train['score'],data_scored_train['Y'],'decile', q=10)
            train_score_boundary = BinWoe().obtain_boundaries(train_ks.分箱, missing_alone=False)
            train_score_bin = train_score_boundary['cut_boundaries']
            train_ks.loc[:, '累计占比'] = train_ks.样本分布占比[::-1].cumsum()[::-1]
            train_ks.loc[:, '相应逾期率'] = train_ks.坏样本数[::-1].cumsum()[::-1]* 1.0 / train_ks.样本数[::-1].cumsum()[::-1]
            # train_ks_20
            train_ks_20 = self.calculate_ks_by_decile(data_scored_train['score'],data_scored_train['Y'],'decile', q=20)
            train_score_boundary_20 = BinWoe().obtain_boundaries(train_ks_20.分箱, missing_alone=False)
            train_score_bin_20 = train_score_boundary_20['cut_boundaries']
            train_ks_20.loc[:, '累计占比'] = train_ks_20.样本分布占比[::-1].cumsum()[::-1]
            train_ks_20.loc[:, '相应逾期率'] = train_ks_20.坏样本数[::-1].cumsum()[::-1]* 1.0 / train_ks_20.样本数[::-1].cumsum()[::-1]
            # test_ks
            data_scored_test['score_bin'] = pd.cut(data_scored_test['score'], bins = train_score_bin, precision=0)
            data_scored_test['score_bin_20'] = pd.cut(data_scored_test['score'], bins = train_score_bin_20, precision=0)
            test_ks = self.calculate_ks_by_decile(data_scored_test['score'],data_scored_test['Y'],'decile', manual_cut_bounds = train_score_bin)
            test_ks.loc[:, '累计占比'] = test_ks.样本分布占比[::-1].cumsum()[::-1]
            test_ks.loc[:, '相应逾期率'] = test_ks.坏样本数[::-1].cumsum()[::-1]* 1.0 / test_ks.样本数[::-1].cumsum()[::-1]
            # test_ks_20
            test_ks_20 = self.calculate_ks_by_decile(data_scored_test['score'],data_scored_test['Y'],'decile', manual_cut_bounds = train_score_bin_20)
            test_ks_20.loc[:, '累计占比'] = test_ks_20.样本分布占比[::-1].cumsum()[::-1]
            test_ks_20.loc[:, '相应逾期率'] = test_ks_20.坏样本数[::-1].cumsum()[::-1]* 1.0 / test_ks_20.样本数[::-1].cumsum()[::-1]

        return data_scored_train, train_ks, train_ks_20, data_scored_test, test_ks, test_ks_20

    def auc_acc_table(self, df):
        from sklearn.metrics import roc_auc_score
        y = df.label.values
        y_pred = df.y_pred
        auc = roc_auc_score(y, y_pred)
        predictions = [round(value) for value in y_pred]
        accuracy = accuracy_score(y, predictions)

        return auc, accuracy

