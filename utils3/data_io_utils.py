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
           'DataBase'
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


