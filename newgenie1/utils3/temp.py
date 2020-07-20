#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : temp.py
@Time    : 2020-06-30 23:31
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import pandas as pd
import numpy as np
k = np.arange(5,20,2)
v = np.arange(13,21)
frame1 = pd.DataFrame.from_dict(dict(zip(k,v)),orient='index',columns=['a'])
frame1
frame1.reset_index()
frame1.reset_index(drop=True)


