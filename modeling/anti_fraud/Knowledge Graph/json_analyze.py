import json
import pandas as pd
from pandas.api.types import is_dict_like
import numpy as np
import itertools
import time
import os

"""
dc_favor_good = pd.read_csv('dc_favor_good.csv')
dc_favor_good = dc_favor_good.head(1)
dc_favor_good['favor_good'] = dc_favor_good['favor_good'].map(lambda x: json.loads(x))
dc_favor_good['favor_good'] = dc_favor_good['favor_good'].map\
    (lambda x: [{'temp': None}] if len(x) == 0 else x)
list_len = list(map(len, dc_favor_good['favor_good'].values))

newvalues = np.dstack((np.repeat(dc_favor_good['order_no'].values, list_len),
                       np.repeat(dc_favor_good['createtime'].values, list_len),
                       np.concatenate(dc_favor_good['favor_good'].values)))

newvalues = np.hstack((np.repeat(dc_favor_good[['order_no', 'createtime']].values, list_len, axis=0),
                       np.array([np.concatenate(dc_favor_good['favor_good'].values)]).T))

a1 = pd.DataFrame(data=newvalues, columns=dc_favor_good.columns)
a1 = from_json(a1, 'favor_good')

a = ['a', 'b']
is_list_like(dc_favor_good['favor_good'])
"""


def from_json(data, var_name):
    """

    :param data: dataframe
    :param var_name: column name of json in dataframe, json object: dict or [dict]
    :return:
    """

    a1 = data.copy()
    other_col_list = list(a1.columns)
    other_col_list.remove(var_name)

    a1[var_name] = a1[var_name].map(lambda x: json.loads(x) if isinstance(x, str) else x)

    if not is_dict_like(a1[var_name]):
        a1[var_name] = a1[var_name].map(lambda x: [{'temp': None}] if len(x) == 0 else x)
        list_len = list(map(len, a1[var_name].values))
        newvalues = np.hstack((np.repeat(a1[other_col_list].values, list_len, axis=0),
                               np.array([np.concatenate(a1[var_name].values)]).T))
        a1 = pd.DataFrame(data=newvalues, columns=other_col_list + [var_name])
        
    start = time.time()
    # 新增一列'columns'用于存储每一列的json串的字段名
    a1['columns'] = a1[str(var_name)].map(lambda x: list(x.keys()) if isinstance(x, dict) else list(json.loads(x).keys()))
    print('new columns done')
    # 获取json串中的所有字段名称
    add_columns_list = list(set(list(itertools.chain(*a1['columns']))))
    for columns in add_columns_list:
        # 将json串展开
        a1[str(columns)] = a1[str(var_name)].map(lambda x: x.get(str(columns)) if isinstance(x, dict) else json.loads(x).get(str(columns)))
        print(str(columns))
    if 'temp' in a1.columns:
        del a1['temp']
    del a1['columns'], a1[str(var_name)]
    end = time.time()
    print("run time = {}".format(end - start))

    return a1


def dict_generator(indict, pre=None):
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                if len(value) == 0:
                    yield pre+[key, '{}']
                else:
                    for d in dict_generator(value, pre + [key]):
                        yield d
            elif isinstance(value, list):
                if len(list(filter(None, value))) == 0:
                    yield pre+[key, '[]']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            elif isinstance(value, tuple):
                if len(value) == 0:
                    yield pre+[key, '()']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            else:
                yield pre + [key, value]
    else:
        yield indict


