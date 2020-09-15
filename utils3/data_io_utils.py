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
import psycopg2
import itertools
from sqlalchemy import create_engine
import time


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



class DataCombine():
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
            lambda x: 'varchar' if x == 'object' else 'float' if x.find('float')>-1 else 'int')
        var_dict['数据源'] = data_sorce
        var_dict['指标类型'] = data_type
        var_dict['是否可用'] = is_available
        return var_dict


    def load_data_from_pickle(selef, file_path, file_name):
        file_path_name = os.path.join(file_path, file_name)
        with open(file_path_name, 'rb') as infile:
            result = pickle.load(infile)
        return result

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
