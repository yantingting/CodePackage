"""
201908轻松分期建模数据清理
@author: yuexin, mingqing
"""
import datetime
import os
import pandas as pd
import numpy as np
import sys
from jinja2 import Template

sys.path.append('/Users/Mint/Desktop/repos/genie')

import matplotlib
matplotlib.use('TkAgg')
import utils3.plotting as pl
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *
from utils3.data_io_utils import *

data_path = 'D:/Model/201908_qsfq_model/01_data'

"""
取所有flag
"""

#新放款的用户的贷后表现

xjbk_tb_perf_sql = """
select order_no
, business_id
, effective_date
, fst_term_late_day
, fst_term_due_date
from t_loan_performance
where business_id in ('xjbk','tb') and dt = '20190915' and effective_date between '2019-07-04' and '2019-08-15'
"""

bk_tb_perf_new = get_df_from_pg(xjbk_tb_perf_sql)
bk_tb_perf_new.shape
bk_tb_perf_new.order_no.nunique()

bk_tb_perf_new.fst_term_due_date.value_counts().sort_index()

def true_perf(data, due_date_var,due_date,late_day):
    data_kept = data.loc[data[due_date_var] > due_date]
    data_kept['fid7'] = np.where(data_kept[fst_term_late_day]>=7,1,0)
    data_kept['fid15'] = np.where(data_kept[fst_term_late_day]>=15,1,0)
    data_kept['fid30'] = np.where(data_kept[fst_term_late_day]>=30,1,0)
    return(data_kept)

bk_tb_perf_new['fid7'] = np.where(bk_tb_perf_new.loc[bk_tb_perf_new.fst_term_due_date <= '2019-09-07'].fst_term_late_day >=7,1,0)
bk_tb_perf_new['fid15'] = np.where(bk_tb_perf_new.loc[bk_tb_perf_new.fst_term_due_date <= '2019-09-01'].fst_term_late_day >=15,1,0)
bk_tb_perf_new['fid30'] = np.where(bk_tb_perf_new.loc[bk_tb_perf_new.fst_term_due_date <= '2019-08-15'].fst_term_late_day >=30,1,0)

bk_tb_perf_new['fid7'] = np.where(bk_tb_perf_new.fst_term_late_day >=7,1,0)
bk_tb_perf_new['fid15'] = np.where(bk_tb_perf_new.fst_term_late_day >=15,1,0)
bk_tb_perf_new['fid30'] = np.where(bk_tb_perf_new.fst_term_late_day >=30,1,0)

#
bk_tb_perf_new.loc[bk_tb_perf_new.fst_term_due_date <= '2019-09-07']['fid7'].value_counts()
bk_tb_perf_new.loc[bk_tb_perf_new.fst_term_due_date <= '2019-09-01']['fid15'].value_counts()
bk_tb_perf_new.loc[bk_tb_perf_new.fst_term_due_date <= '2019-08-15']['fid30'].value_counts()

bk_tb_perf_new = bk_tb_perf_new.loc[bk_tb_perf_new.fst_term_due_date <= '2019-09-07']
bk_tb_perf_new.shape

"""
新增样本数
"""
sample_add_sql = """
select *, 'tb' as business_id
from dc_sample_add
union
select *, 'xjbk' as business_id
from bk_sample_add
"""
sample_add = get_df_from_pg(sample_add_sql)


"""
卡贷4326样本表现
"""
tb_perf_bef = pd.read_csv(os.path.join(data_path,'flag 20190514.csv')).drop('flag7',1).rename(columns = {'t1.order_no':'order_no'})
tb_perf_bef.shape

loan_num = pd.read_csv(os.path.join(data_path,'loan_num.csv')).rename(columns = {'t1.order_no':'order_no'})
loan_num.loc[loan_num.loan_num != 1]['order_no']
#CP2018121903105020757156 ok
#CP2018121905024907342713 ok
#CP2018122621505275396700 ok
#CP2019011017065552120284
#CP2019012905002864921867 ok

loan_num = loan_num.loc[loan_num.loan_num == 1]
loan_num.shape
loan_num.head()

tb_perf_bef = tb_perf_bef.merge(loan_num[['order_no']], on = 'order_no' )
tb_perf_bef.head()

#合并数据
tb_perf_bef.columns #4326
tb_perf_bef['business_id'] = 'tb'
sample_add.order_no.nunique() #13371

sample_add.business_id.value_counts()
#tb      12644
#xjbk      727
bk_tb_perf_new.order_no.nunique()
#xjbk    2850
#tb      1313

all_orderno = pd.concat([sample_add[['order_no','business_id']], tb_perf_bef[['order_no','business_id']], bk_tb_perf_new[['order_no','business_id']] ])
all_orderno.shape #21659
all_orderno.order_no.nunique()
all_orderno.order_no.value_counts()

#BK2019081210163868546185 现金白卡放款用户，且该用户在花不完有放款过
sample_add.loc[sample_add.order_no =='BK2019081210163868546185']
bk_tb_perf_new.loc[bk_tb_perf_new.order_no =='BK2019081210163868546185'

all_orderno = all_orderno.drop_duplicates()
all_orderno.business_id.value_counts()
#tb      18123
#xjbk     3536

"""
花不完保只保留用户的第一次借款时候的表现
"""
hbw_dd = pd.read_excel('D:/Model/201908_qsfq_model/00_sample_analysis/hbw_dd.xlsx')
hbw_dd.columns
hbw_dd.hbw_customer_id.value_counts()
hbw_dd[['hbw_customer_id']]

hbw_dd_onlynew2.loc[hbw_dd_onlynew2.hbw_customer_id == 30345171]
hbw_dd.loc[hbw_dd.hbw_customer_id == 400127887]

#只保留第一次的放款表现
hbw_dd_onlynew = hbw_dd[['hbw_customer_id','hbw_order_no','effective_date','max_late_day']]
hbw_dd_onlynew2.head()
hbw_dd_onlynew2 = hbw_dd_onlynew.sort_values(['hbw_customer_id','effective_date'], ascending= True).drop_duplicates(['hbw_customer_id'], keep = 'first')

#重新关联轻松分期的orderno

hbw_dd2 = hbw_dd[['order_no','created_time','business_id','customer_id','hbw_customer_id']].drop_duplicates()
#hbw_dd2_loan_num = hbw_dd2.customer_id.value_counts().reset_index()

hbw_dd2.business_id.value_counts()

hbw_dd2 = hbw_dd2.merge(hbw_dd_onlynew2, on = 'hbw_customer_id', how = 'left')
hbw_dd2.head()

hbw_dd2.effective_date.max()

hbw_dd2['ever30'] = np.where(hbw_dd2.max_late_day >= 30, 1,0)
hbw_dd2.ever30.value_counts()

#0    3867
#1    1368
hbw_dd2.to_csv('D:/Model/201908_qsfq_model/00_sample_analysis/hbw_dd_final0917.csv')

orderlist = list(hbw_dd2.hbw_order_no)

# hbw_perf_sql2 = """
# select order_no, business_id, max_late_day
# from t_loan_performance
# where business_id = 'hbw' and dt = '20190915' and order_no in {{order_list}}
# """
#
# hbw_perf_new = get_df_from_pg(Template(hbw_perf_sql2).render(order_list = tuple(orderlist)))
# hbw_perf_new.head()
#
# hbw_perf_new.loc[hbw_perf_new.max_late_day >= 30].shape
#
# hbw_perf_new.loc[hbw_perf_new.max_late_day >= 30].shape

"""
合并flag
"""
sample_add.columns
tb_perf_bef.columns
bk_tb_perf_new.columns

all_flag = pd.concat([sample_add, tb_perf_bef, bk_tb_perf_new])
all_flag.columns

all_flag.order_no.value_counts()
all_flag.loc[all_flag.order_no == 'BK2019081210163868546185']

all_flag = pd.concat([tb_perf_bef, bk_tb_perf_new]).merge(sample_add, on = 'order_no',how = 'outer').merge(hbw_dd2[['order_no','ever30']], on = 'order_no', how = 'outer')
all_flag.shape #21229
all_flag.to_excel(os.path.join(data_path,'all_flag.xlsx'))
all_flag.business_id_x.value_counts()
all_flag.business_id_y.value_counts()

"""
匹配所有的X
"""
dc_baseinfo_data = pd.read_excel(os.path.join(data_path, 'dc_baseinfo.xlsx'))
bk_baseinfo_data = pd.read_excel(os.path.join(data_path, 'bk_baseinfo.xlsx'))


all_baseinfo = pd.concat([dc_baseinfo_data,bk_baseinfo_data])
all_baseinfo.shape #(21670, 79)
set(all_baseinfo.order_no) - set(all_orderno.order_no) #CP2018092621145021249937 #11

#id_city_level & job_city_level 转换
all_baseinfo.job_city_level = all_baseinfo.job_city_level.replace(['一线城市','新一线城市','二线城市','三线城市','四线城市','五线城市'],['0','1','2','3','4','5'])
all_baseinfo.id_city_level = all_baseinfo.id_city_level.replace(['一线城市','新一线城市','二线城市','三线城市','四线城市','五线城市'],['0','1','2','3','4','5'])

all_baseinfo.id_city_level.value_counts()
all_baseinfo.idcity_jobcity.value_counts()
#性别转换
all_baseinfo.idcardgender = all_baseinfo.idcardgender.replace(['男','女'],['1','0'])

all_baseinfo.age_marital.value_counts()

dc_jxl_data7.shape #(18134, 193)
bk_jxl_data7.shape #(3536, 193)

set(bk_jxl_data7.columns) - set(dc_jxl_data7.columns)

all_jxl = pd.concat([dc_jxl_data7, bk_jxl_data7])
all_jxl.shape #(21670, 193)


#新颜
bk_xinyan_data = pd.read_csv(os.path.join(data_path,'bk_xinyan.csv'))
all_xy = pd.concat([dc_xinyan_data,bk_xinyan_data ])
all_xy = all_xy.drop(['Unnamed: 0'],1)
all_xy.shape
all_xy.isnull().sum()

#天启
dc_tianqi_data = pd.read_csv(os.path.join(data_path,'dc_tianqi.csv'))
bk_tianqi_data = pd.read_csv(os.path.join(data_path,'bk_tianqi.csv'))
all_tq = pd.concat([dc_tianqi_data,bk_tianqi_data ])

#天御
all_ty = pd.concat([dc_tianyu_data,bk_tianyu_data ])

#同盾
td = pd.read_csv(os.path.join(data_path, 'tongdun_x_0917.csv')).drop(['Unnamed: 0'],1)
td.shape

all_x = all_orderno.merge(all_baseinfo, on = 'order_no', how = 'left').merge(all_xy, on = 'order_no', how = 'left')\
    .merge(all_jxl, on = 'order_no', how = 'left').merge(all_tq, on = 'order_no', how = 'left').merge(all_ty, on = 'order_no', how = 'left').merge(td, on = 'order_no', how = 'left')

all_x.order_no.nunique()
all_x.shape
all_x.columns

#all_x.to_csv(os.path.join(data_path,'all_x_0917_v2.csv'))

x_with_y = all_flag.merge(all_x, on = 'order_no')
x_with_y = x_with_y.drop(['fid15','fid30','fst_term_due_date','fst_term_late_day','business_id_x','business_id_y'],1)
x_with_y.order_no.nunique()


#划分训练&验证
data = pd.read_csv(os.path.join(data_path,'data_clean_0917.csv'))
data.cp_is_test = data.cp_is_test.replace(-1,'')
data.cp_is_test.value_counts()

x_with_y = x_with_y.merge(data[['order_no','cp_is_test']], on = 'order_no', how = 'left')
x_with_y.cp_is_test.value_counts()

x_with_y.loc[x_with_y.cp_is_test == 1,'sample_set'] = 'test' #3076
x_with_y.loc[x_with_y.cp_is_test == 0,'sample_set'] = 'train' #1250

x_with_y.loc[(~x_with_y.cp_is_test.isin([0,1])) & (x_with_y.effective_date.notnull()),'sample_set'] = 'test_new'
x_with_y.loc[(~x_with_y.cp_is_test.isin([0,1])) & (x_with_y.effective_date.isnull()),'sample_set'] = 'train_new'

x_with_y.loc[x_with_y.effective_date.notnull()].shape

8049 - 1250 - 3076 #3723

x_with_y.sample_set.value_counts(dropna=False)
#train_new    13180 新增训练样本
#test_new      3723 新增验证样本
#train         3076 线上模型训练样本
#test       1250 线上模型建模样本

x_with_y.perf_flag.value_counts(dropna = False)

x_with_y = x_with_y.drop(['perf_flag'],1)
x_with_y.flag_credit.value_counts(dropna = False)


# for i in range(x_with_y.shape[0]):
#     print (x_with_y.loc[i, 'flag_credit'])
#     if x_with_y.loc[i, 'flag_credit'] is not None:
#         x_with_y.loc[i,'perf_flag'] = x_with_y.loc[i,'flag_credit']
#     else:
#         #x_with_y.loc[i, 'flag_credit'] is None:
#         x_with_y.loc[i, 'perf_flag'] = x_with_y.loc[i, 'fid7']
#     else:
#         None

x_with_y.loc[x_with_y.sample_set.isin(['train','test']), 'perf_flag'] = x_with_y.loc[x_with_y.sample_set.isin(['train','test']), 'flag_credit']
x_with_y.loc[x_with_y.sample_set.isin(['test_new']), 'perf_flag'] = x_with_y.loc[x_with_y.sample_set.isin(['test_new']), 'fid7']


x_with_y['perf_flag'].value_counts(dropna = False)

pd.crosstab(x_with_y['perf_flag'], x_with_y['fid7'])
pd.crosstab(x_with_y['perf_flag'], x_with_y['flag_credit'])



"""
变量字典整理
"""
var_dict = pd.read_excel('D:/Model/201908_qsfq_model/建模代码可用变量字典.xlsx')
var_dict.columns

set(x_with_y.columns) - set(var_dict.指标英文.unique())
set(var_dict.指标英文.unique()) - set(x_with_y.columns)
x_cols = list(set(x_with_y.columns).intersection(set(var_dict.指标英文.unique())))

len(x_cols)




"""
BK部分数据没有基本信息,补充
"""

x_with_y.loc[x_with_y.age == -1][['age','order_no','business_id']].to_csv(os.path.join(data_path,'age_data.csv'))
len(bk_base_list) #28

bk_baseinfo_add_sql = """
with bi_city_level as (
select distinct city_level, idcardcity from (
select a.*, substr(b.idcard, 1, 6) as idcardcity
from bi_city_level a
left join bi_idcard_location_v2 b on substr(a.city, 1, 6) = regexp_replace(substr(b.city, 1, 6), '市', '')) a
)
select t1.order_no
,education
,maritalStatus
,case when idCardGender='男' then 1 when idCardGender='女' then 0 else null end as idCardGender
,floor(((date_part('year',created_time)-date_part('year',date(idCardBirth)))*12+date_part('month',created_time)-date_part('month',date(idCardBirth)))/12) as age
--,companyCounty
--,companyCity
--,companyProvince
,industry
,monthlyIncome
,positionType
,case when substr(idCardNo, 1, 4)=companyCity then 1 else 0 end as idcity_jobcity
,case when b.city_level='一线城市' then 0
      when b.city_level='新一线城市' then 1
      when b.city_level='二线城市' then 2
      when b.city_level='三线城市' then 3
      when b.city_level='四线城市' then 4
      when b.city_level='五线城市' then 5 else null end as id_city_level
,case when c.city_level='一线城市' then 0
      when c.city_level='新一线城市' then 1
      when c.city_level='二线城市' then 2
      when c.city_level='三线城市' then 3
      when c.city_level='四线城市' then 4
      when c.city_level='五线城市' then 5 else null end as job_city_level
,case when b.city_level = c.city_level then 1 else 0 end as idcitylevel_jobcitylevel  
,case when floor(((date_part('year',created_time)-date_part('year',date(idCardBirth)))*12+date_part('month',created_time)-date_part('month',date(idCardBirth)))/12) >30
	  and (maritalstatus='1' or maritalstatus='2' or maritalstatus='4') then 1
	  when floor(((date_part('year',created_time)-date_part('year',date(idCardBirth)))*12+date_part('month',created_time)-date_part('month',date(idCardBirth)))/12) <=30
	  and maritalstatus='3' then 2
	  else '3' end as age_marital
from (
select order_no
,customer_history::json #>>'{t,customer,customerEducation,education}' as education
,customer_history::json #>>'{t,device}' as device
,customer_history::json #>>'{t,customer,customerFamily,maritalStatus}' as maritalStatus
,customer_history::json #>>'{t,customer,customerIdentity,idCardGender}' as idCardGender
,customer_history::json #>>'{t,customer,customerIdentity,idCardBirth}' as idCardBirth
,customer_history::json #>>'{t,customer,customerIdentity,idCardNo}' as idCardNo
,customer_history::json #>>'{t,customer,customerJob,companyCounty}' as companyCounty
,customer_history::json #>>'{t,customer,customerJob,companyCity}' as companyCity
,customer_history::json #>>'{t,customer,customerJob,companyProvince}' as companyProvince
,customer_history::json #>>'{t,customer,customerJob,companyCityName}' as companyCityName
,customer_history::json #>>'{t,customer,customerJob,entryTime}' as entryTime
,customer_history::json #>>'{t,customer,customerJob,industry}' as industry
,customer_history::json #>>'{t,customer,customerJob,monthlyIncome}' as monthlyIncome
,customer_history::json #>>'{t,customer,customerJob,positionType}' as positionType
from (select * from bakrt_customer_history_base_info 
      where order_no in ('BK2019072001071839471166',
'BK2019072517055507727649',
'BK2019072109414601155719',
'BK2019080113140982054433',
'BK2019080105304572458692',
'BK2019080313373549399934',
'BK2019080417552622570041',
'BK2019071206001561014523',
'BK2019070807371755356146',
'BK2019070815571367021266',
'BK2019080600130201821919',
'BK2019071007093461276370',
'BK2019070421045885067469',
'BK2019072411564776869234',
'BK2019070910110687420406',
'BK2019071007550244277749',
'BK2019072915575247480878',
'BK2019070814140131770437',
'BK2019071014245792688242',
'BK2019072221094127468145',
'BK2019070614473532679993',
'BK2019070513511288477423',
'BK2019070608244103226759',
'BK2019071715143566148244',
'BK2019070814104518068015',
'BK2019070711535899436099',
'BK2019070510414109843729',
'BK2019071009514696474779'))a
)t1
left join dw_bk_bk_core_bk_core_application t2 on t1.order_no = t2.order_no
left join bi_city_level b on b.idcardcity = substr(t1.idCardNo, 1, 6)
left join bi_city_level c on c.idcardcity = substr(t1.companyCounty, 1, 6)
"""
bk_base_add = get_df_from_pg(bk_baseinfo_add_sql)
bk_base_add.shape
bk_base_add = bk_base_add.drop_duplicates()
bk_base_add.order_no.nunique()
bk_base_add.columns

base_cols = list(var_dict.loc[var_dict.数据源== '基本信息','指标英文'])

bk_base_list = list(bk_base_add['order_no'])


for i in bk_base_list:
    for j in base_cols:
         print(i)
         print(j)
         x_with_y.loc[x_with_y.order_no== i,j] = bk_base_add.loc[bk_base_add.order_no == i,j].iloc[0]

bk_base_add.to_csv(os.path.join(data_path,'bk_base_add.csv'))

x_with_y.loc[x_with_y.order_no.isin(bk_base_list),base_cols]
= bk_base_add.loc[bk_base_add.order_no == 'BK2019071009514696474779'].education.iloc[0]


x_with_y.loc[x_with_y.order_no == 'BK2019071009514696474779','education'] = bk_base_add.loc[bk_base_add.order_no == 'BK2019071009514696474779'].education.iloc[0]

"""
缺失值填充
"""
x_with_y[x_cols] =x_with_y[x_cols].replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999],[-1,  -1, -1, -1, -1, -1, -1]).fillna(-1)
x_with_y[x_cols] =x_with_y[x_cols].replace(['-9995', '-9996', '-9997', '-9998', '-9999', '-99998', '-99999'],['-1',  '-1', '-1', '-1', '-1', '-1', '-1'])


x_with_y.head()
x_with_y.perf_flag.value_counts()
x_with_y.order_no = x_with_y.order_no.astype(str)

x_with_y.order_no

x_with_y = pd.read_csv(os.path.join(data_path,'x_with_y_v3.csv'))
x_with_y.loc[x_with_y.age == -1]['business_id'].value_counts()

x_with_y.to_csv(os.path.join(data_path, 'x_with_y_v3.csv'), index = False)
x_with_y.order_no.nunique()

"""
银联数据清理
"""
x_with_y = pd.read_excel(os.path.join(data_path, 'x_with_y_v4.xlsx'))
x_with_y = x_with_y.drop(var_dict.loc[var_dict.数据源 == '银联']['指标英文'],1)

br = pd.read_excel(os.path.join(data_path, 'bairong_0926.xlsx')).rename(columns = {'cus_num':'order_no'})
ly = pd.read_excel(os.path.join(data_path, 'data_yl_0925.xlsx')).drop(['apply_time','sm3_name','sm3_phone','sm3_idcard','sm3_bankcard'],1)


x_with_y2 = x_with_y.merge(br, on = 'order_no', how = 'left').merge(ly, on = 'order_no', how = 'left')

x_with_y2.to_excel(os.path.join(data_path,'x_with_y_v6.xlsx'))

