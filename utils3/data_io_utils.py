#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : data_io_utils.py
@Time    : 2020-07-01 00:42
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


import os
import pickle as pickle
import simplejson as json
import pandas as pd
import numpy as np
# import psycopg2
import itertools
from sqlalchemy import create_engine
import time

__all__ = ['print_run_time',
           'DataBase',
           'DataSummary'
           ]

def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        result = func(*args, **kw)
        print('current Function [%s] run time is %.2f'%(func.__name__ ,time.time() - local_time))
        return result
    return wrapper


class DataBase():
    def __init__(self):
        self.cf = {
            "database": "rim",
            "user": "po",
            "password": "M2019",
            "host": "1.2.20",
            "port": "4"
        }

    @print_run_time
    def get_df_from_pg(self, SQL):
        try:
            conn = psycopg2.connect(**self.cf)
            print("Opened database successfully")
        except Exception as e:
            print(e)
        cur = conn.cursor()
        cur.execute(SQL)
        rows = cur.fetchall()
        df = pd.DataFrame(rows,columns=[i.name for i in cur.description])
        df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
        return df

    @print_run_time
    def upload_df_to_pg(self, sql):
        """
        If sql is table creation, then rows = [[True]] is succeed.
        If sql is insert query, then rows = [[4]] where 4 is changeable according to
        number of rows inserted
        """
        try:
            conn = psycopg2.connect(**self.cf)
        except Exception as e:
            print(e)
        cur = conn.cursor()
        cur.execute(sql)
        try:
            conn.commit()
            return 'success'
        except:
            print("upload failed, check query")
            return 'failure'

    @print_run_time
    def auto_upload_df_to_pg(self,df, tb = 'temp_table'):
        connect = create_engine('postgresql+psycopg2://' + self.cf['user'] + ':' + self.cf['password'] \
                                + '@'+ self.cf['host'] + ':' + self.cf['port'] + '/' + self.cf['database'])

        pd.io.sql.to_sql(df, tb, connect, schema='public')



class DataSummary():
    def __init__(self):
        pass

    @print_run_time
    def create_dict(self, df, is_available = 'Y', data_sorce=None, data_type=None, useless_vars = []):
        '''
        :param df: DataFrame, 原始数据集
        :param is_available: 指标是否可用(Y, N)
        :param data_sorce: 数据来源，三方数据还是自有数据
        :param data_type: 指标类型，原始变量还是加工的变量
        :param useless_vars: 字典中不需要用的列
        :return: var_dict
        '''
        if useless_vars:
            X_col = list(set(df.columns.tolist()).difference(set(useless_vars)))
        else:
            X_col = list(set(df.columns.tolist()))

        X = df[X_col].copy()
        var_dict = pd.DataFrame(columns=['数据源', '指标英文', '指标中文', '数据类型', '指标类型', '是否可用'])
        var_dict['指标英文'] = X.dtypes.index
        var_dict['指标中文'] = X.dtypes.index
        var_dict['数据类型'] = X.dtypes.values
        var_dict['数据类型'] = var_dict['数据类型'].apply(
            lambda x: 'varchar' if x == 'object' else 'float' if x == 'float64' else 'int')
        var_dict['数据源'] = data_sorce
        var_dict['指标类型'] = data_type
        var_dict['是否可用'] = is_available
        return var_dict


    @print_run_time
    def split_df(self, df, test_size=0.3, random_state=2, shuffle=True, stratify='flag'):
        '''
        :param df:数据集 DataFrame
        :param test_size: 测试集占比，默认30%
        :param random_state: 是否每次结果一致
        :param shuffle:
        :param stratify: 按某个字段分层抽样
        :return: df_all, 一个dict {'train': df_train, 'test': df_test}
        '''
        import sklearn
        from sklearn.model_selection import train_test_split
        from collections import OrderedDict

        df_train, df_test = train_test_split(df, test_size=test_size, random_state=random_state, shuffle=shuffle,
                                             stratify=df[stratify])

        df_all = OrderedDict()
        df_all['train'] = df_train
        df_all['test'] = df_test

        df_dict = {'df_all': df, 'df_train': df_train, 'df_test': df_test}
        for keys, values in df_dict.items():
            df_info = values
            print('\n***************** % s *****************' % keys)
            print('数据集行列：{}\n逾期率：{}\n{}样本占整体样本的比例：{}'.\
                  format(df_info.shape,
                         '%.2f%%' % (df_info[stratify].sum() / df_info.shape[0] * 100),
                         keys,
                         '%.2f%%' % (df_info.shape[0] / df.shape[0] * 100))
                        )

        return df_all


    @print_run_time
    def eda(self, X, useless_vars=[], special_value=[], var_dict=None, result_path=None, save_label=None, cutoff=0.97):
        """
        :param X: DataFrame,数据集
        :param useless_vars: 不需要分析的列
        :param special_value: 需单独统计的数值或者类型
        :param var_dict: 数据字典，默认为None，如果有 包含['数据源', '指标英文', '指标中文', '数据类型', '指标类型', '是否可用']
        :param result_path: 文件存储位置，默认为None
        :param save_label: 文件名，格式为'%s_EDA.xlsx' % save_label
        :param cutoff:  判断异常的阈值
        :return: DataFrame  EDA
        """
        vars_summary = X.count().to_frame('N_available')
        vars_summary.loc[:, 'N'] = X.shape[0]
        if special_value:
            for values in special_value:
                if (values == '') or (np.isnan(values)):
                    vars_summary.loc[:, 'N_NA'] = (X.isna()).sum()
                    vars_summary.loc[:, 'pct_NA'] = np.round(vars_summary['N_NA'] * 1.0 / vars_summary.N, 3)
                else:
                    vars_summary.loc[:, 'N_%s' % values] = (X.isin([values])).sum()
                    vars_summary.loc[:, 'pct_%s' % values] = np.round(
                        vars_summary['N_%s' % values] * 1.0 / vars_summary.N, 3)
        # 如果有字典就按照字典中的类型来，如果没有就系统自己判断
        try:
            no_vars = var_dict.loc[(var_dict['数据类型'] != 'varchar') & ~pd.isnull(var_dict['指标英文']), '指标英文'].tolist()
            cate_vars = var_dict.loc[(var_dict['数据类型'] == 'varchar') & ~pd.isnull(var_dict['指标英文']), '指标英文'].tolist()
            no_vars = list(set(no_vars).intersection(set(X.columns)))
            cate_vars = list(set(cate_vars).intersection(set(X.columns)))
        except:
            no_vars = X.select_dtypes(exclude=['object']).columns
            cate_vars = X.select_dtypes('O').columns

        # the following can only be done for continuous vars
        if len(no_vars) > 0:
            X_no = X[no_vars].apply(lambda x: x.astype(float), 0).replace(special_value, [np.nan] * len(special_value))

            no_vars_summary = X_no.mean().round(6).to_frame('mean')
            no_vars_summary.loc[:, 'std'] = X_no.std().round(6)
            no_vars_summary.loc[:, 'median'] = X_no.median().round(6)
            no_vars_summary.loc[:, 'min'] = X_no.min()
            no_vars_summary.loc[:, 'max'] = X_no.max()
            no_vars_summary.loc[:, 'p01'] = X_no.quantile(0.01)
            no_vars_summary.loc[:, 'p05'] = X_no.quantile(q=0.05)
            no_vars_summary.loc[:, 'p10'] = X_no.quantile(q=0.10)
            no_vars_summary.loc[:, 'p25'] = X_no.quantile(q=0.25)
            no_vars_summary.loc[:, 'p75'] = X_no.quantile(q=0.75)
            no_vars_summary.loc[:, 'p90'] = X_no.quantile(q=0.90)
            no_vars_summary.loc[:, 'p95'] = X_no.quantile(q=0.95)
            no_vars_summary.loc[:, 'p99'] = X_no.quantile(q=0.99)

        # the following are for cate_vars
        if len(cate_vars) > 0:
            X_cate = X[cate_vars].copy()
            X_cate = X_cate.replace(special_value, [''] * len(special_value))
            cate_vars_summary = X_cate.nunique().to_frame('N_categories')
            cat_list = []
            for col in cate_vars:
                if X_cate[col].count() == 0:
                    pass
                else:
                    cat_count = X_cate[col].value_counts().head(3)
                    if len(cat_count) == 3:
                        col_result = pd.Series({'1st类别': str(cat_count.index.values[0]) + ' #=' + str(cat_count.iloc[0]) \
                                                         + ', %=' + str(np.round(cat_count.iloc[0] * 1.0 / len(X), 2)), \
                                                '2nd类别': str(cat_count.index.values[1]) + ' #=' + str(cat_count.iloc[1]) \
                                                         + ', %=' + str(np.round(cat_count.iloc[1] * 1.0 / len(X), 2)), \
                                                '3rd类别': str(cat_count.index.values[2]) + ' #=' + str(cat_count.iloc[2]) \
                                                         + ', %=' + str(np.round(cat_count.iloc[2] * 1.0 / len(X), 2)) \
                                                }) \
                            .to_frame().transpose()
                    elif len(cat_count) == 2:
                        col_result = pd.Series({'1st类别': str(cat_count.index.values[0]) + ' #=' + str(cat_count.iloc[0]) \
                                                         + ', %=' + str(np.round(cat_count.iloc[0] * 1.0 / len(X), 2)), \
                                                '2nd类别': str(cat_count.index.values[1]) + ' #=' + str(cat_count.iloc[1]) \
                                                         + ', %=' + str(np.round(cat_count.iloc[1] * 1.0 / len(X), 2)), \
                                                }) \
                            .to_frame().transpose()
                    elif len(cat_count) == 1:
                        col_result = pd.Series({'1st类别': str(cat_count.index.values[0]) + ' #=' + str(cat_count.iloc[0]) \
                                                         + ', %=' + str(np.round(cat_count.iloc[0] * 1.0 / len(X), 2))}) \
                            .to_frame().transpose()
                    else:
                        pass
                    col_result.index = [col]
                    cat_list.append(col_result)
            cat_df = pd.concat(cat_list)

        # merge all summaries
        if len(no_vars) > 0 and len(cate_vars) > 0:
            all_vars_summary = vars_summary.merge(no_vars_summary, how='left', left_index=True, right_index=True)
            all_vars_summary = all_vars_summary.merge(cate_vars_summary, how='left', left_index=True, right_index=True)
            all_vars_summary = all_vars_summary.merge(cat_df, how='left', left_index=True, right_index=True)
        elif len(no_vars) > 0 and len(cate_vars) == 0:
            all_vars_summary = vars_summary.merge(no_vars_summary, how='left', left_index=True, right_index=True)
        elif len(cate_vars) > 0 and len(no_vars) == 0:
            all_vars_summary = vars_summary.merge(cate_vars_summary, how='left', left_index=True, right_index=True)
            all_vars_summary = all_vars_summary.merge(cat_df, how='left', left_index=True, right_index=True)
        else:
            return None

        all_vars_summary.loc[:, 'exclusion_reason'] = None
        all_vars_summary.loc[all_vars_summary.pct_NA > cutoff, 'exclusion_reason'] = '缺失NA比例大于{}'.format(cutoff)
        if 0 in special_value:
            all_vars_summary.loc[all_vars_summary.pct_0 > cutoff, 'exclusion_reason'] = '0值比例大于{}'.format(cutoff)
        if len(cate_vars) > 0:
            all_vars_summary.loc[all_vars_summary.N_categories == 1, 'exclusion_reason'] = '只有一个分类'
            all_vars_summary.loc[all_vars_summary.N_categories > 100, 'exclusion_reason'] = '类别变量的类别数过多'
        all_vars_summary.loc[useless_vars, 'exclusion_reason'] = '无用变量'

        final_output = all_vars_summary.reset_index().rename(columns={'index': '指标英文'})
        if var_dict.values.any():
            final_output = pd.merge(var_dict, final_output, on='指标英文')
        try:
            final_output.to_excel(os.path.join(result_path, '%s_EDA.xlsx' % save_label), encoding='utf-8', index=False)
        except:
            print('There is no storage path!')
        return final_output


    @print_run_time
    def load_data_from_pickle(selef, file_path, file_name):
        file_path_name = os.path.join(file_path, file_name)
        with open(file_path_name, 'rb') as infile:
            result = pickle.load(infile)
        return result


    @print_run_time
    def save_data_to_pickle(self, obj, file_path, file_name):
        file_path_name = os.path.join(file_path, file_name)
        with open(file_path_name, 'wb') as outfile:
            pickle.dump(obj, outfile)


    @print_run_time
    def from_json(self, data, var_name):
        """
        :param data: dataframe
        :param var_name: column name of json in dataframe, json object: dict or [dict]
        :return:
        """
        a1 = data.copy()
        a1 = a1.reset_index(drop=True)
        other_col_list = list(a1.columns)
        other_col_list.remove(var_name)
         # 判断是否有异常的json串
        temp1 = a1.copy()
        for index, rows in a1.iterrows():
            if isinstance(rows[var_name], str):
                try:
                    temp1.iloc[index][var_name] = json.loads(rows[var_name])
                except:
                    print('JSONDecodeError in  index {}'.format(index))
                    break
            else:
                temp1.iloc[index][var_name] = rows[var_name]

        a1[var_name] = a1[var_name].map(lambda x: json.loads(x) if isinstance(x, str) else x)
        if not isinstance(a1[var_name][0], dict) or not isinstance(a1[var_name][1], dict):
            a1[var_name] = a1[var_name].map(lambda x: [{'temp': None}] if len(x) == 0 else x)
            list_len = list(map(len, a1[var_name].values))
            newvalues = np.hstack((np.repeat(a1[other_col_list].values, list_len, axis=0),
                                   np.array([np.concatenate(a1[var_name].values)]).T))
            a1 = pd.DataFrame(data=newvalues, columns=other_col_list + [var_name])

        start = time.time()
        # 新增一列'columns'用于存储每一列的json串的字段名
        a1['columns'] = a1[str(var_name)].map(
            lambda x: list(x.keys()) if isinstance(x, dict) else list(json.loads(x).keys()))
        print('new columns done')
        # 获取json串中的所有字段名称
        add_columns_list = list(set(list(itertools.chain(*a1['columns']))))
        for columns in add_columns_list:
            # 将json串展开
            a1[str(columns)] = a1[str(var_name)].map(
                lambda x: x.get(str(columns)) if isinstance(x, dict) else json.loads(x).get(str(columns)))
            print(str(columns))
        if 'temp' in a1.columns:
            del a1['temp']
        del a1['columns'], a1[str(var_name)]
        end = time.time()
        print("run time = {}".format(end - start))
        return a1
