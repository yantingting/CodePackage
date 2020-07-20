import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/genie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *
import utils3.data_processing as dp

data_path = 'D:/Model/indn/202001_mvp_model/01_data/'
result_path = 'D:/Model/202001_mvp_model/02_result/'

# json解析函数
def from_json(data, var_name: str):
    """

    :param data: dataframe
    :param var_name: column name of json in dataframe, json object: dict or [dict]
    :return:
    """

    a1 = data.copy()
    a1 = a1[~a1[var_name].isna()].reset_index(drop=True)
    other_col_list = list(a1.columns)
    other_col_list.remove(var_name)

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

import psycopg2
def get_df_from_pg(SQL):
    usename = "postgres"
    password = "Mintq2019"
    db = "risk_dm"
    host = "192.168.2.19"
    port = "5432"
    try:
        conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)
        print("Opened database successfully")
    except Exception as e:
        print(e)
    cur = conn.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    df = pd.DataFrame(rows,columns=[i.name for i in cur.description])
    df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
    return df


os.getcwd()
os.chdir('C:\\Users\\Mint\\Documents\\Python Scripts\\02_模型\\06_印尼新客V6\\01_取数APP')
#------------------
#------ 取数 ------
#------------------

##############
## 申请样本 ##
##############

app_sql = """
WITH program as (
    SELECT customer_id, packages_cleaned, create_time 
    FROM gocash_loan_risk_program_packages
    WHERE create_time <= '2019-12-01 00:00:00'
), 
loan as (
    SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
    FROM dw_gocash_go_cash_loan_gocash_core_loan
    WHERE apply_time between '2019-09-15 00:00:00' and '2019-11-30 23:59:59' and return_flag = 'true'
)
SELECT *
        , date_part('minutes', apply_time :: timestamp - create_time:: timestamp) package_update_minutes
FROM (SELECT loan.*
            , program.create_time
            , program.packages_cleaned
            , row_number() over(partition by loan.loan_id order by program.create_time desc) rn
      FROM loan
      LEFT JOIN program 
      ON loan.customer_id :: varchar = program.customer_id
      WHERE loan.apply_time :: timestamp > program.create_time) t
where t.rn = 1
"""

app_data = get_df_from_pg(app_sql)

app_data.apply_time.min()
app_data.apply_time.max()

save_data_to_pickle(app_data, data_path, 'app_0917to1031_packagedclean.pkl')
app_data = load_data_from_pickle(data_path, 'app_0917to1031_packagedclean.pkl')

"""合并APP数据"""
app_data_0916to0925 = load_data_from_pickle(data_path, 'app_0916to0925.pkl')
app_data_0926to0930 = load_data_from_pickle(data_path, 'app_0926to0930.pkl')
app_data_1001to1016 = load_data_from_pickle(data_path, 'app_1001to1016.pkl')
app_data_1017to1031 = load_data_from_pickle(data_path, 'app_1017to1031.pkl')

all_app = pd.concat([app_data_0916to0925, app_data_0926to0930, app_data_1001to1016, app_data_1017to1031])
save_data_to_pickle(all_app, data_path, 'all_app_0916to1031.pkl')

app_data.shape  #(94019, 6)
app_data.loan_id.nunique()
app_data.apply_time.max()

#----------------------
#------ 解析json ------
#----------------------

# app list原始json
import json
import time
import itertools

all_app.columns

app_data2 = app_data[['loan_id','packages_cleaned']].reset_index(drop= True)
app_data2.head()

x = app_data2.copy()


for row in range(app_data2.shape[0]):
    #print(row)
    try:
        json.loads(app_data2.iloc[row, 1])
    except:
        x.drop(index=row, inplace=True)
        print(row)

x.shape   # 54225: 2817条解析json发生错误
x.reset_index(drop=True, inplace=True)
app_data_analysis = from_json(x, 'packages_cleaned')

app_data_analysis.shape  #8380542
app_data_analysis.columns

#合并时间
app_data.loan_id = app_data.loan_id.astype(str)

app_data_analysis.loan_id = app_data_analysis.loan_id.astype(str)

app_data_analysis2 = app_data_analysis.merge(app_data[['loan_id', 'apply_time']], on = 'loan_id', how = 'left')

app_data_analysis2.isnull().sum()


save_data_to_pickle(app_data_analysis2, data_path, 'app_data_analysis0916to1031_packagecleaned.pkl')
app_data_analysis2 = load_data_from_pickle(data_path, 'app_data_analysis0916to1031_packagecleaned.pkl')

app_data_analysis2.apply_time.max()

app_data_analysis2.apply_time = app_data_analysis2.apply_time.astype(str)

#------------------------
#------ 计算tf idf ------
#------------------------

# 1) 生成频数字典（安装率超过5% 的app作为未来tf-idf考虑的app)

app_train = app_data_analysis2.loc[(app_data_analysis2.apply_time >= '2019-09-17 00:00:00') & (app_data_analysis2.apply_time < '2019-10-05 00:00:00')]
app_train.loan_id.nunique()

app_freq = pd.DataFrame(app_train['appName'].value_counts()).reset_index()
print('训练集的不同app数量: ' , app_freq.shape[0])  #
app_freq.rename(columns={'index': 'app', 'appName': 'freq'}, inplace=True)

cnt_loan = app_train['loan_id'].nunique()
cnt_loan = 21009
app_freq['rate_freq'] = app_freq['freq'].apply(lambda x: x/cnt_loan)

app_tfidf = app_freq[app_freq['rate_freq']>=0.05]
print('可用于tf_idf的app总数是(安装率超过5%)： ', app_tfidf.shape[0]) #186
app_tfidf.to_csv(os.path.join(data_path, 'app_for_tfidf_0917_1007.csv'), index = False)

#训练集的不同app数量:  57229
#可用于tf_idf的app总数是(安装率超过5%)：  200

high = pd.Series(app_freq['freq'].unique()).quantile(0.98)
mid = pd.Series(app_freq['freq'].unique()).quantile(0.93)
high
mid

min(app_freq.rate_freq)
max(app_freq.rate_freq)


app_freq['high_freq_app'] = app_freq['rate_freq'].apply(lambda x: 1 if x >= 0.3 else 0)
print('高频APP个数', app_freq['high_freq_app'].sum())
app_freq['mid_freq_app'] = app_freq['rate_freq'].apply(lambda x: 1 if (0.2 <= x < 0.3) else 0)
print('中频APP个数', app_freq['mid_freq_app'].sum())
app_freq['low_freq_app'] = app_freq['freq'].apply(lambda x: 1 if x <= 1 else 0)
print('低频APP个数(仅一人安装)', app_freq['low_freq_app'].sum())
#高频APP个数 32
#中频APP个数 22
#低频APP个数(仅一人安装) 25012
app_freq.to_csv(os.path.join(data_path, 'app_freq_train_dict_0917_1007.csv'), index=False)

# 2) 生成idf 字典
time.sleep(0.5)
import math

app_idf_data = app_train[['loan_id','appName']].copy()
app_idf = pd.pivot_table(app_idf_data.drop_duplicates(), index = 'appName', values = 'loan_id', aggfunc = 'count').reset_index()
app_idf.rename(columns={'loan_id': 'cnt_idf'}, inplace=True)
cnt_document = app_idf_data['loan_id'].nunique()
app_idf['idf'] = app_idf['cnt_idf'].apply(lambda x: math.log10(cnt_document/(x+1)))

app_idf.shape
app_idf.head()

#app_idf[['appName', 'idf']].to_csv(os.path.join(data_path,'app_idf_train_dict_0917_1007.csv'), index=False)
app_idf[['appName', 'idf']].to_excel(os.path.join(data_path, 'app_idf_train_dict_0917_1007.xlsx'), index=False)

# 3) 生成频数变量
time.sleep(0.1)
#df_app = pd.read_csv('app_effect_0627_1031.csv')
df_app = app_data_analysis2.copy()

from collections import Counter
count_app = Counter(df_app['loan_id'])
#app_freq = pd.read_csv('app_freq_train_dict_0627_1106.csv')
train1 = pd.merge(df_app, app_freq, left_on='appName', right_on='app', how = 'left')
var_app = train1.groupby(['loan_id'])['high_freq_app', 'mid_freq_app', 'low_freq_app'].agg('sum').reset_index()
var_app['cnt_app'] = var_app['loan_id'].map(lambda x: count_app[x])
list1 = ['high_freq_app', 'mid_freq_app', 'low_freq_app']
for i in list1:
    var_app['rate_' + i] = var_app.apply(lambda x: x[i]/x['cnt_app'], axis=1)

var_app.loan_id = var_app.loan_id.astype(str)
var_app.to_csv(os.path.join(data_path,'var_app_freq_0917_1007.csv'), index=False)
var_dict_app_freq = dp.VarDict(var_app.drop('loan_id', axis = 1), data_sorce='app_freq')
var_dict_app_freq.to_csv(os.path.join(data_path,'var_dict_app_freq_0917_1007.csv'), index = False)
print('app频数变量: ', var_app.shape)
var_app.head()
var_app.shape



# 4) 生成TF-IDF相关的变量

time.sleep(0.5)
from collections import Counter
dict_tfidf = pd.read_excel(os.path.join(data_path, 'app_idf_train_dict_0917_1007.xlsx'))

app_tfidf_data = app_data_analysis2.copy()

# 计算每个loan的app个数
cnt_app = Counter(app_tfidf_data['loan_id'])

# 计算TF
app_tfidf =  pd.pivot_table(app_tfidf_data, index=['loan_id', 'appName'], values='packageName',aggfunc='count' ,fill_value=0).reset_index()
app_tfidf.rename(columns = {'packageName': 'cnt_tf'},  inplace=True)
app_tfidf['cnt_app'] = app_tfidf['loan_id'].map(lambda x: cnt_app[x])
app_tfidf['app_tf'] = app_tfidf.apply(lambda x: x['cnt_tf']/x['cnt_app'], axis = 1)
app_tfidf.head(10)

# 计算IDF
app_tfidf = pd.merge(app_tfidf, dict_tfidf, on='appName', how = 'left')
app_tfidf['tf_idf'] = app_tfidf.apply(lambda x: x['app_tf']*x['idf'], axis=1)

app_for_tfidf = pd.read_csv(os.path.join(data_path,'app_for_tfidf_0917_1007.csv'))

app_tfidf_list = app_for_tfidf['app'].values.tolist()
app_tfidf1=app_tfidf[app_tfidf['appName'].isin(app_tfidf_list)]
app_tfidf1.head()

var_app_tfidf = pd.pivot_table(app_tfidf1, index='loan_id', columns = 'appName',values='tf_idf', aggfunc='sum', fill_value=0).reset_index()

var_app_tfidf = pd.merge(app_data_analysis2[['loan_id']].drop_duplicates(),var_app_tfidf, on='loan_id' , how='left')

print(var_app_tfidf.shape)
var_app_tfidf.loan_id = var_app_tfidf.loan_id.astype(str)
var_app_tfidf.to_csv(os.path.join(data_path,'var_app_tfidf_0917_1007.csv'), index = False)

#var_app_tfidf = pd.read_csv('var_app_tfidf.csv')
var_dict_app_tfidf = dp.VarDict(var_app_tfidf.drop('loan_id', axis = 1), data_sorce='app_freq')

var_dict_app_tfidf.to_csv(os.path.join(data_path,'var_dict_app_tfidf_0917_1007.csv'), index = False)
var_app_tfidf.loan_id.nunique()



## 拼接变量
#var_app = pd.read_csv('var_app_freq_0627_1031.csv')
#var_app_tfidf = pd.read_csv('var_app_tfidf_0627_1031.csv')

#var_app.isnull().sum()
#var_app_tfidf.isnull().sum()

var_app.shape
var_app_tfidf.shape

var_app.columns
var_app_tfidf.columns

var_app.loan_id = var_app.loan_id.astype(str)
var_app_tfidf.loan_id = var_app_tfidf.loan_id.astype(str)

var_app_freq_w_tfidf = pd.merge(var_app, var_app_tfidf, on = 'loan_id', how = 'inner')
var_app_freq_w_tfidf.shape
var_app_freq_w_tfidf.isnull().sum()

var_app_freq_w_tfidf.loan_id = var_app_freq_w_tfidf.loan_id.astype(str)
var_app_freq_w_tfidf.to_csv(os.path.join(data_path,'var_app_freq_w_tfidf_0917_1007.csv'), index=False)

save_data_to_pickle(var_app_freq_w_tfidf, data_path, 'var_app_freq_w_tfidf_0917_1007.pkl')

var_app_freq_w_tfidf.head()





