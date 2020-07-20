#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 01.py
@Time    : 2020-06-23 18:11
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""



import os
import pandas as pd
pd.set_option('display.max_columns', None)


file_path = '/Users/yantingting/Seafile/风控/模型/13 菲律宾/业务重启/'

df_trans = pd.read_csv(os.path.join(file_path,'data/transactions.csv'))
df_trans.head()
df_trans.shape
df_trans['merchant_name'].value_counts()

df_kyc = pd.read_csv(os.path.join(file_path,'data/users_kyc_bashed.csv'))
df_kyc.head()
df_kyc.shape
df_kyc['primary_income'].value_counts()
kyc_list = ['id', 'sex',  'date_of_birth', 'occupation','primary_income', 'education']
pd.crosstab(df_kyc['occupation'],df_kyc['primary_income'])





#%%
import pandas as pd
import sys
sys.path.append(r'E:\qiQuanWork\mainWork\PhlData\202004271800_总部资料\202005070900_印尼老客模型\20200513_更新\newgenie')
from utils3 import *
import utils3.summary_statistics as ss
import os
# os.chdir(r"D:\qiQuanWork\mainWork\PhlData\202005271130_白名单数据\20200529新版数据")
from datetime import datetime
#%%
start_time = datetime.now()


def cat_hour(hour):
    if hour in ["06", "07", "08", "09", "10", "11", "12"]:
        return "[06-12]"
    elif hour in ["13", "14", "15", "16", "17", "18"]:
        return "[13-18]"
    elif hour in ["19", "20", "21", "22"]:
        return "[19-22]"
    else:
        return "[23-05]"


def get_value(df, group_id, group_name):
    df_new = pd.pivot_table(df, index=group_id, columns=group_name, values="amount",
                            aggfunc=[len, np.mean, np.sum])
    if len(group_name) == 1:
        df_new.columns = [df_new.columns.map("{0[1]}_{0[0]}".format)]
    elif len(group_name) == 2:
        df_new.columns = [df_new.columns.map("{0[2]}_{0[1]}_{0[0]}".format)]
    elif len(group_name) == 3:
        df_new.columns = [df_new.columns.map("{0[3]}_{0[2]}_{0[1]}_{0[0]}".format)]
    df_new.columns = [i[0] for i in df_new.columns]
#    df_new.reset_index(inplace=True)
#    df_new.rename(columns={"index": group_id}, inplace=True)
    return df_new


def get_name(df, group_id, group_name):
    # TODO "transaction_Month_cat"
    df_1 = df[[group_id, group_name]]
    df_2 = df_1.groupby([group_id])[group_name].unique().reset_index()
    df_2["交易%s数" % group_name] = df_2[group_name].map(lambda s: int(len(s)))
    df_2.drop(columns=[group_name], inplace=True)
    df_2.set_index(group_id, inplace=True)
    return df_2


df = pd.read_csv("transactions.csv")
df["transaction_date"] = df["transaction_at"].map(lambda s: s[:10])
df["transaction_Month_cat"] = df["transaction_date"].map(lambda s: s[:7])
df["transaction_time"] = df["transaction_at"].map(lambda s: s[11:19])
df["transaction_hour"] = df["transaction_at"].map(lambda s: s[11:13])
df["transaction_hour_cat"] = df["transaction_hour"].map(lambda s: cat_hour(s))


df_merchant_len = get_name(df, "user_id", "merchant_name")
df_transaction_Month_cat_len = get_name(df, "user_id", "transaction_Month_cat")
df_zong = get_value(df, "user_id", [])
df_app = get_value(df, "user_id", ["merchant_name"])
df_month = get_value(df, "user_id", ["transaction_Month_cat"])
df_hour = get_value(df, "user_id", ["transaction_hour_cat"])
df_month_hour = get_value(df, "user_id", ["transaction_hour_cat", "transaction_Month_cat"])
df_month_app = get_value(df, "user_id", ["merchant_name", "transaction_Month_cat"])
df_hour_app = get_value(df, "user_id", ["merchant_name", "transaction_hour_cat"])
df_month_hour_app = get_value(df, "user_id", ["merchant_name", "transaction_hour_cat", "transaction_Month_cat"])
# print(df_month_hour_app.head())
df_all = pd.concat([df_zong, df_merchant_len, df_transaction_Month_cat_len, df_app, df_month, df_hour, df_month_hour, df_month_app, df_hour_app, df_month_hour_app], axis=1)
#%%
df_base = pd.read_csv("users_kyc_bashed.csv")
df_base["age"] = df_base["date_of_birth"].map(lambda s: (2020-int(s[-4:])))
df_base.rename(columns={"id": "user_id"}, inplace=True)
df_base.set_index("user_id", inplace=True)
#%%
df_result = pd.merge(df_all, df_base, left_index=True, right_index=True, how="left")
#df_all.set_index("user_id", inplace=True)
df_result.to_csv("Derived_data_%s.csv" % datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
columns_df = pd.DataFrame(df_result.columns)
columns_df.to_csv("columns_%s.csv" % datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))


df = pd.read_csv("Derived_data_2020_06_22_06_05_22.csv")
var_dict_df = pd.read_excel("白名单数据字典.xlsx", encoding="gbk")

file_name = "中间数据_%s.xlsx" % datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
writer = pd.ExcelWriter(file_name)
#%%
useless_vars = ['user_id', 'sex', 'name', 'email', 'phone', 'date_of_birth', 'occupation',
                'primary_income', 'education', 'months_in_address', ]
eda_result = ss.eda(df.fillna(-1), var_dict=var_dict_df, useless_vars=useless_vars, exempt_vars=[], uniqvalue_cutoff=0.97)
eda_result.to_excel(writer, sheet_name="all_derived_data_summary")
#%%

col_user_list = list(eda_result["指标英文"].loc[eda_result["NA占比"] <= 0.1])
col_user_list.append("user_id")
cat_list = ['sex', 'name', 'email', 'phone', 'date_of_birth', 'occupation', 'primary_income',
            'education', 'months_in_address']

df_cluster = df[col_user_list].drop(columns=cat_list)
for column in list(df_cluster.columns[df_cluster.isnull().sum() > 0]):
    mean_val = df_cluster[column].mean()
    df_cluster[column].fillna(mean_val, inplace=True)

(df_cluster.corr()).to_excel(writer, sheet_name="相关性检测")
from sklearn.cluster import KMeans

#
#
# import matplotlib.pyplot as plt
# # -- 肘部法则–Elbow Method
# SSE = []  # 存放每次结果的误差平方和
# for k in range(1, 10):
#     estimator = KMeans(n_clusters=k)  # 构造聚类器
#     estimator.fit(df_cluster.drop(columns="user_id"))
#     SSE.append(estimator.inertia_) # estimator.inertia_获取聚类准则的总和
# X = range(1, 10)
# plt.xlabel('k')
# plt.ylabel('SSE')
# plt.plot(X, SSE, 'o-')
# plt.savefig("Elbow_pic.jpg")
# plt.show()

km = KMeans(n_clusters=3).fit(df_cluster.drop(columns="user_id"))
df_zhixin = pd.DataFrame(km.cluster_centers_, columns=df_cluster.drop(columns="user_id").columns)
df_zhixin.to_excel(writer, sheet_name="质心")
df["label"] = km.labels_

df_final = df[list(set(cat_list) | set(["user_id", "label", "len", "mean", "sum"]))]
df_final.to_excel(writer, sheet_name="final")
writer.save()





