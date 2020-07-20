# encoding=utf8
import os
import json
import hashlib
from datetime import datetime

import pandas as pd
import numpy as np
from sklearn import preprocessing

# convert to the right data type
def convert_right_data_type(data, var_dict, quick_mode=False):
    """
    Convert to the right data type, have to apply it after processing the missing value.
    Even though the data are converted to the correct type before saving, reading
    it back will change it. For example, some categorical variables are coded
    as integers, they should be string type for usage, but reading it back, it
    will be loaded in as integers.

    Args:
    data (pd.DataFrame): X数据, 已经处理过缺失值
    var_dict(pd.DataFrame): 变量字典
    quick_mode (bool): default=False. If set True, 将会批量转化数据类型，如果某一列
        有问题，则数据整体将会转化失败。

    Returns:
    failed_conversion (list): if quick_mode=False, 转换失败的变量名list

    """
    data = data.copy()

    cols = data.columns.values
    failed_conversion = []

    if quick_mode:
        col_types = var_dict.loc[var_dict['指标英文'].isin(cols), ['指标英文', '数据类型']]
        for col_type in ['varchar', 'float', 'integer']:
            onetype_cols = col_types.loc[col_types['数据类型']==col_type, '指标英文'].tolist()
            try:
                if col_type == 'varchar':
                    data[onetype_cols] = data[onetype_cols].astype(str)
                elif col_type == 'float':
                    data[onetype_cols] = data[onetype_cols].astype(float)
                elif col_type == 'integer':
                    data[onetype_cols] = data[onetype_cols].astype(int)
            except:
                print("one column may have problem, so conversion for type %s failed" % col_type)

        return data

    else:
        cols = list(set(cols).intersection(set(var_dict['指标英文'].tolist())))
        for col in cols:
            right_type = var_dict.loc[var_dict['指标英文']==col, '数据类型'].iloc[0]
            non_missing_data = data.loc[~data[col].isin([-8888, -9999, -8887]), col]
            if non_missing_data.empty:
                current_type = right_type
            else:
                current_type = str(type(data.loc[~data[col].isin([-8888, -9999, -8887]), col].iloc[0]))

            if right_type == 'varchar':
                if 'float' in current_type:
                    try:
                        data[col] = data[col].astype(int).astype(str)
                    except:
                        print(col + ' conversion failed')
                        failed_conversion.append(col)
                else:
                    try:
                        data[col] = data[col].astype(str)
                    except:
                        print(col + ' conversion failed')
                        failed_conversion.append(col)
                if 'object' in str(data[col].dtype):
                    pass
                else:
                    print(col + ' conversion failed. Type should be object, but is still %s' % str(data[col].dtype))
                    failed_conversion.append(col)

            if right_type == 'float':
                try:
                    data[col] = data[col].astype(float)
                except:
                    print(col + ' conversion failed')
                    failed_conversion.append(col)

                if 'float' in str(data[col].dtype):
                    pass
                else:
                    print(col + ' conversion failed. Type should be float, but is still %s' % str(data[col].dtype))
                    failed_conversion.append(col)

            if right_type == 'integer':
                try:
                    data[col] = data[col].astype(int)
                except:
                    print(col + ' conversion failed')
                    failed_conversion.append(col)

                if 'int' in str(data[col].dtype):
                    pass
                else:
                    print(col + ' conversion failed. Type should be int, but is still %s' % str(data[col].dtype))
                    failed_conversion.append(col)

        return data, np.unique(failed_conversion)



def label_encode(x):
    """
    将原始分类变量用数字编码

    Args:
    x (pd.Series): 原始数值的分类变量

    Returns:
    x_encoded (pd.Series): 数字编码后的变量
    """
    le = preprocessing.LabelEncoder()
    x_encoded = le.fit_transform(x)
    return pd.Series(x_encoded, index=x.index)


def process_missing(X, var_dict, known_missing={}, downflagmap={}, verbose=True):
    """
    处理缺失值。如果某个数据源整条数据都没有就算是完整缺失未查得，这些被标成-8888。
    有些是可查得，但是个别变量值缺失。这些缺失被label为-8887。

    Args:
    X (pd.DataFrame): 原始数值的分类变量
    var_dict (pd.DataFrame): 标准数据字典，需包含数据源，指标英文两列。
    known_missing (dict): 已知的代表缺失的值以及想要替换成的值。格式为：
        {-1: -9999, -9999999: -8887}
    downflagmap (dict): 中间层有些数据源有downflag字段用来标注是否宕机，是否查无此人等。
        格式为：
        {'Anrong_DownFlag': {1: -9999}, 'Tongdun_DownFlag': {1: -9999}}

    Returns:
    new_X (pd.DataFrame): 填补后的x
    """

    if '数据源' not in var_dict.columns and '指标英文' not in var_dict.columns:
        raise 'Check var_dict column names'

    X = X.replace('nan', np.nan)\
         .replace('None', np.nan)\
         .replace('NaN', np.nan)\
         .replace('null', np.nan)\
         .replace('Null', np.nan)\
         .replace('NULL', np.nan)\
         .replace('', np.nan)

    # 先把已知是缺失的值转化成NA，这样可以确保其不会干扰别的值得缺失赋值
    if len(known_missing) > 0:
        known_missing_values = list(known_missing.keys())
        known_missing_values_str = [str(i) for i in known_missing_values]
        known_missing_values = known_missing_values + known_missing_values_str

    unq_data_sources = var_dict['数据源'].unique()
    new_X_list = []
    for data_source in unq_data_sources:
        data_sources_vars = var_dict.loc[var_dict['数据源']==data_source, '指标英文'].unique()
        data_sources_vars = list(set(data_sources_vars).intersection(set(X.columns)))
        if verbose & (len(set(downflagmap.keys()).intersection(set(data_sources_vars))) == 0):
            #print("Warnings: downflag variable is not in downflagmap provided for %s" % data_source)
            pass
        else:
            the_downflag_var = list(set(downflagmap.keys()).intersection(set(data_sources_vars)))

        downflag_vars_x = [i for i in data_sources_vars if 'downflag' in i.lower()]
        data_sources_vars = [i for i in data_sources_vars if 'downflag' not in i.lower()]
        sub_X = X[data_sources_vars].copy()
        checker = len(data_sources_vars)

        if len(known_missing) > 0:
            missing_bool = (pd.isnull(sub_X) | sub_X.isin(known_missing_values))
        else:
            missing_bool = pd.isnull(sub_X)

        num_missing = missing_bool.sum(1)

        sub_X = sub_X.fillna(-9999)
        sub_x1 = sub_X.loc[num_missing==checker].copy()
        sub_x2 = sub_X.loc[num_missing!=checker].copy()
        sub_x1.replace(-9999, -8888, inplace=True)
        sub_x2.replace(-9999, -8887, inplace=True)
        new_sub_X = pd.concat([sub_x1, sub_x2]).sort_index()

        if len(downflagmap) > 0:
            for downflag_var in the_downflag_var:
                to_replace = downflagmap.get(downflag_var, {})
                if len(to_replace) > 0:
                    for flag_value, replace_value in list(to_replace.items()):
                        new_sub_X.loc[X[downflag_var].isin([flag_value, str(flag_value)]), :] = replace_value

        new_sub_X = new_sub_X.merge(X[downflag_vars_x], left_index=True, right_index=True)
        new_X_list.append(new_sub_X)

    new_X = pd.concat(new_X_list, axis=1)


    if len(known_missing) > 0:
        for known_missing_value, replace_value in list(known_missing.items()):
            new_X = new_X.replace(known_missing_value, replace_value)\
                         .replace(str(known_missing_value), replace_value)

    return new_X


def convert_rebin_spec2XGB_rebin_spec(rebin_spec):
    """
    将自动分箱的rebin_spec文件转换为xgboost使用的格式

    Args:
    rebin_spec (dict): 分箱文件

    Returns:
    rebin_spec (dict): xgboost分箱文件
    """
    for i in rebin_spec.keys():
        try:
            rebin_spec[i]['cut_boundaries'] = sorted(set(rebin_spec[i]['cut_boundaries'] \
                                                         + rebin_spec[i]['other_categories'] + [-np.inf, np.inf]))
            rebin_spec[i]['other_categories'] = []
        except:
            pass

    return rebin_spec
