#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : WGZ.py
@Time    : 2020-09-21 16:39
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import pandas as pd
pd.set_option('display.max_columns', None)
import os
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn import model_selection
import statsmodels.api as sm

df_path = '/Users/yantingting/Documents/Code/WGZ/'
my_df = pd.read_excel(os.path.join(df_path, 'DATA.xlsx'), sheet_name='细菌')
my_df.head()
my_df.info()
#model = sm.formula.ols('SBacterai ~ TN + NH4N + NO3N + SOC + AP + SD + Ca + Mg + K', data = my_df).fit()
model = sm.formula.ols('SBacterai~ Ca + Mg + K', data = my_df).fit()
model.params

sm.formula.lm_