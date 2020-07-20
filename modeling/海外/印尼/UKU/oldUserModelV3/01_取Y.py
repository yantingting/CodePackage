import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/newgenie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *

#建模所用flag

perf_sql = """
with loan as (
select id as loan_id
,customer_id
,apply_time
,approved_period
,effective_date
,paid_off_time
,due_date
,loan_status
,extend_times
,return_flag
,current_date
,late_days
, case when late_days >= 3 then 1 else 0 end as ever3
, case when late_days >= 7 then 1 else 0 end as ever7
, case when late_days >= 15 then 1 else 0 end as ever15
, case when late_days >= 30 then 1 else 0 end as ever30
, case when current_date - due_date >= 3 then 1 else 0 end as due_flag3
, case when current_date - due_date >= 7 then 1 else 0 end as due_flag7
, case when current_date - due_date >= 15 then 1 else 0 end as due_flag15
, case when current_date - due_date >= 30 then 1 else 0 end as due_flag30
from dw_gocash_go_cash_loan_gocash_core_loan 
--where effective_date!='1970-01-01' and effective_date between '2020-01-24' and '2020-03-03' and return_flag = 'true' and product_id = 1
where effective_date!='1970-01-01' and effective_date between '2020-01-24' and '2020-04-02' and return_flag = 'true' and product_id = 1 --td从4-2开始重新调用
)
select loan.*
, case when (loan.effective_date between '2020-01-24' and '2020-03-03') and a.loan_id is not null then 1 
       when (loan.effective_date between '2020-01-24' and '2020-03-03') and a.loan_id is null then -1 
        else 0 end as hs_sample_flag
--, md5(concat('+628',substring(t1.cell_phone,3))) as md5_cellphone_izi
--, md5(t1.cell_phone) as md5_cellphone
--, md5(t1.id_card_no) as md5_id_card_no
--, md5(t1.id_card_name) as md5_id_card_name
--, id_card_name
from loan
--left join dw_gocash_go_cash_loan_gocash_core_customer t1 on loan.customer_id = t1.id
left join temp_uku_oldmodelv3_sample a on loan.loan_id :: text = a.loan_id
"""
perf_data = get_df_from_pg(perf_sql)

perf_data_td = get_df_from_pg(perf_sql)
perf_data_td.loan_id.nunique()#54156

perf_data.loan_id.nunique() #101259
perf_data.customer_id.nunique() #63460
101259/63460 #1.6次复贷



perf_data.groupby(['hs_sample_flag'])['effective_date'].max()

perf_data.hs_sample_flag.value_counts()

perf_data.due_flag15.value_counts()

#1/24-3/3时间段固定seed抽样
perf_data_sample = perf_data.sample(n = 20000, replace = False,random_state = 0)
perf_data_sample.shape

save_data_to_pickle(perf_data, 'D:/Model/indn/202004_uku_old_model/01_data/', 'perf_0124to0303_0506_origin.pkl')
save_data_to_pickle(perf_data_sample, 'D:/Model/indn/202004_uku_old_model/01_data/', 'perf_0124to0303_0506.pkl')

perf_data_sample.effective_date.value_counts().sort_index()


#3.4-4.2时间段固定seed抽样(因为td没有调用)
perf_data_td_sample = perf_data_td.sample(n = 20000, replace = False,random_state = 1)
perf_data_td_sample.shape
perf_data_td.loan_id.nunique()/perf_data_td.customer_id.nunique()
perf_data_td_sample.loan_id.nunique()/perf_data_td_sample.customer_id.nunique()

save_data_to_pickle(perf_data, 'D:/Model/indn/202004_uku_old_model/01_data/', 'perf_0304to0402_0506_origin.pkl')
save_data_to_pickle(perf_data_td_sample, 'D:/Model/indn/202004_uku_old_model/01_data/', 'perf_data_td_sample_0304to0402_0506.pkl')

perf_td = perf_data_td_sample[['loan_id', 'apply_time', 'md5_cellphone_izi','md5_id_card_no','md5_id_card_name','id_card_name','md5_cellphone']]
perf_td = pd.concat([perf_td, perf_izi])
perf_td.shape
perf_td.loan_id = perf_td.loan_id.astype(str)
perf_td['apply_date'] = perf_td.apply_time.apply(lambda x:str(x)[0:10])
perf_td.to_excel('D:/Model/indn/202004_uku_old_model/01_data/izi_td/td回溯样本.xlsx')
perf_td.apply_date.value_counts().sort_index()

#给izi的回溯样本
perf_data_sample.columns

perf_data_sample.loan_id = perf_data_sample.loan_id.astype(str)
perf_izi = perf_data_sample[['loan_id', 'apply_time', 'md5_cellphone_izi','md5_id_card_no','md5_id_card_name','id_card_name','md5_cellphone']]
perf_izi['apply_date'] = perf_izi.apply_time.apply(lambda x:str(x)[0:10])
perf_izi.apply_date

perf_izi.to_excel('D:/Model/indn/202004_uku_old_model/01_data/izi_td/izi回溯样本.xlsx')


"""查看同盾调用记录"""
td_online_sql = """
select effective_date
      , count(distinct a.customer_id)
      , count(distinct b.customer_id)
from (select a.*, b.apply_time, b.id as loanid, b.effective_date, b.return_flag
               , md5(c.cell_phone) as md5_cell_phone
               , md5(id_card_name) as md5_id_card_name
               , md5(id_card_no) as md5_id_card_no
               , row_number() over(partition by b.id order by a.createtime desc) as rn
       from (select * 
             from dw_gocash_go_cash_loan_gocash_core_loan
             where effective_date!='1970-01-01' and effective_date between '2020-01-24' and '2020-03-03' and return_flag = 'true' and product_id = 1
            ) a
     left join (select * from public.risk_mongo_gocash_tdaurora where businessid = 'uku') b on a.customerid = b.customer_id :: varchar
     left join dw_gocash_go_cash_loan_gocash_core_customer c on b.customer_id = c.id
     WHERE b.apply_time::timestamp + '8 hour' >= a.createtime::timestamp 
)
group by effective_date
"""
td_online_huisu = get_df_from_pg(td_online_sql)


#建模数据上传至pg

perf_data_sample.columns
perf_data_sample.dtypes

perf_data_sample.apply_time = perf_data_sample.apply_time.astype(str)
perf_data_sample.effective_date = perf_data_sample.effective_date.astype(str)
perf_data_sample.paid_off_time = perf_data_sample.paid_off_time.astype(str)
perf_data_sample.due_date = perf_data_sample.due_date.astype(str)
perf_data_sample.customer_id = perf_data_sample.customer_id.astype(str)

cols = ['loan_id', 'customer_id', 'apply_time', 'approved_period', 'effective_date', 'paid_off_time', 'due_date', 'loan_status', 'extend_times']
perf_data_sample[cols].dtypes


SQL_CREATE_TABLE = """
CREATE TABLE temp_uku_oldmodelv3_sample(
    loan_id VARCHAR,
    customer_id VARCHAR,
    apply_time VARCHAR,
    approved_period FLOAT,
    effective_date VARCHAR,
    paid_off_time VARCHAR,
    due_date VARCHAR,
    loan_status VARCHAR,
    extend_times FLOAT
    )
"""

upload_df_to_pg(SQL_CREATE_TABLE)

insert = """
INSERT INTO temp_uku_oldmodelv3_sample
 VALUES
{% for var in var_list %}
{{ var }},
{% endfor %}
"""

var_list = []
for cols, rows in perf_data_sample[cols].iterrows():
    c = tuple(rows)
    var_list.append(c)

insert_sql = Template(insert).render(var_list=var_list)[:-2]
insert_sql = insert_sql.replace('\n\n','')
insert_sql = insert_sql.replace('\n','')

upload_df_to_pg(insert_sql)

"""5/11 更新flag"""

from sklearn.model_selection import train_test_split
sklearn.model_selection.train_test_split


perf_data.groupby(['hs_sample_flag'])['effective_date'].max()
perf_data['hs_sample_flag'].value_counts()
perf_data_new = perf_data.loc[perf_data.hs_sample_flag != -1]


perf_data_new.shape

perf_data_new.columns

perf_data_new.effective_date = perf_data_new.effective_date.astype(str)


#方案一
train_id, test_id = train_test_split(perf_data_new.loc[perf_data_new.effective_date <= '2020-02-20', 'loan_id'], test_size = 0.3)
len(train_id)
len(test_id)

perf_data_new.loc[perf_data_new.loan_id.isin(train_id), 'sample_set'] = 'train'
perf_data_new.loc[perf_data_new.loan_id.isin(test_id), 'sample_set'] = 'test'

perf_data_new.loc[(perf_data_new.effective_date >= '2020-02-21') & (perf_data_new.effective_date <= '2020-03-03'), 'sample_set'] = 'oot1'
#3/4 修改内容较大, 去掉此天
perf_data_new.loc[(perf_data_new.effective_date >= '2020-03-05'), 'sample_set'] = 'oot2'

perf_data_new.sample_set.value_counts()
#oot2     51247
#train     9643
#oot1      6224
#test      4133

4133 + 9643

#方案二建模

perf_data_temp = perf_data_new.loc[perf_data_new.effective_date >= '2020-03-05'].sample(n = 6000, replace = False,random_state = 1)
perf_data_temp.shape

6000/(6000 + 4133 + 9643) #30%

train_id2, test_id2 = train_test_split(perf_data_temp['loan_id'], test_size = 0.3)

perf_data_new.loc[perf_data_new.loan_id.isin(train_id), 'sample_set2'] = 'train'
perf_data_new.loc[perf_data_new.loan_id.isin(test_id), 'sample_set2'] = 'test'

perf_data_new.loc[perf_data_new.loan_id.isin(train_id2), 'sample_set2'] = 'train2'
perf_data_new.loc[perf_data_new.loan_id.isin(test_id2), 'sample_set2'] = 'test2'

perf_data_new.loc[(perf_data_new.effective_date >= '2020-02-21') & (perf_data_new.effective_date <= '2020-03-03'), 'sample_set2'] = 'oot1'

perf_data_new.sample_set2.value_counts(dropna=False)

perf_data_new.loc[(~perf_data_new.loan_id.isin(perf_data_temp.loan_id)) & (perf_data_new.effective_date >= '2020-03-05'), 'sample_set2'] = 'oot2'


pd.crosstab(perf_data_new['sample_set'], perf_data_new['sample_set2'])

perf_data_new.due_flag15.value_counts()
perf_data_new.loc[perf_data_new.due_flag15 == 0].due_date.max()

perf_data_new.loc[(perf_data_new.ever15 == 1) & (perf_data_new.due_flag15 == 1), 'Y'] = 1
perf_data_new.loc[(perf_data_new.ever15 == 0) & (perf_data_new.due_flag15 == 1) & (perf_data_new.ever3 == 0), 'Y'] = 0
perf_data_new.loc[(perf_data_new.ever15 == 0) & (perf_data_new.due_flag15 == 1) & (perf_data_new.ever3 != 0), 'Y'] = -1
perf_data_new.loc[(perf_data_new.due_flag15 == 0), 'Y'] = -2

pd.crosstab(perf_data_new.due_flag15, perf_data_new.Y)

#Y标签含义
# Y = 1 : bad
# # Y = 0 : < ever3
# # Y = -1 : 逾期3-15 灰样本
# # Y = -2: 未到期

perf_data_new.loan_id = perf_data_new.loan_id.astype(str)
perf_data_new.customer_id = perf_data_new.customer_id.astype(str)
perf_data_new.to_excel('D:/Model/indn/202004_uku_old_model/01_data/flag_0511.xlsx')

perf_data_new = pd.read_excel('D:/Model/indn/202004_uku_old_model/01_data/flag_0511.xlsx')
perf_data_new = perf_data_new.drop('Unnamed: 0',1)

perf_data_new.loc[perf_data_new.sample_set.isnull(),'effective_date'].value_counts()

perf_data_new.loc[(perf_data_new.sample_set.notnull()) & (perf_data_new.sample_set2.isnull()), 'sample_set2'] = 'oot2'
perf_data_new.loan_id = perf_data_new.loan_id.astype(str)

perf_data_new.to_excel('D:/Model/indn/202004_uku_old_model/01_data/flag_0511_v2.xlsx')

pd.crosstab(perf_data_new.sample_set, perf_data_new.sample_set2)

perf_data_new.groupby('sample_set')['effective_date'].max()
perf_data_new.groupby('sample_set2')['effective_date'].max()

perf_data_new.loc[perf_data_new.sample_set2 == 'train2'].effective_date.value_counts().sort_index()
perf_data_new.loc[perf_data_new.sample_set2 == 'test2'].effective_date.value_counts().sort_index()

perf_data_new.effective_date.value_counts().sort_index() #主要在3/25之前