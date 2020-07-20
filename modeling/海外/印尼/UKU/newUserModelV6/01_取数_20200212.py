# -*- coding: utf-8 -*-
"""
Created on Sun Dec  8 10:57:47 2019

@author: yuexin
"""
import os
import time
import pandas as pd
import numpy as np
import psycopg2
import pickle as pickle

def load_data_from_pickle(file_path, file_name):
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'rb') as infile:
        result = pickle.load(infile)
    return result

path_rawdata = 'D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212/'
path = 'D:/Model Development/202001 IDN new v6/01 Data/'

usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)

''' **************************** FLAG **************************** '''
sql = '''
	select loan_id, effective_date, null as flag7_raw, flag7, 'traintest' as sample_set from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, effective_date, 
	case when extend_times>3 then 9
	when paid_off_time::Date-due_date>=3 then 1 
	when loan_status='COLLECTION' and current_date::Date-due_date<3 then -3 
	when loan_status='COLLECTION' and current_date::Date-due_date>=3 then 1
	when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
	when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
	else 0 end as flag7_raw, 
	case when extend_times>3 then 0
	when paid_off_time::Date-due_date>=3 then 1 
	when loan_status='COLLECTION' and current_date::Date-due_date<3 then 0
	when loan_status='COLLECTION' and current_date::Date-due_date>=3 then 1
	when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then 0
	when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then 0
	else 0 end as flag7,
	'oot' as sample_set from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql)
r_flag = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_flag.shape)
print(r_flag.loan_id.nunique())
print(r_flag.dtypes)

print(r_flag.flag7.value_counts(dropna=False))

r_flag.groupby('effective_date')['flag7'].sum()
r_flag_f.groupby('effective_date')['flag7'].sum()

r_flag_f = r_flag.copy()

# r_flag_f.to_csv(path_rawdata + 'r_flag.csv', index = False)
# r_flag_f = pd.read_csv(path_rawdata + 'r_flag_f.csv')


''' **************************** EXTEND **************************** '''
sql_extend = '''
select a.loan_id, b.extend_times, b.approved_principal, b.approved_period, b.product_id, 
a.apply_time::date - c.create_time::date as reg_date_diff
from (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
) a 
left join dw_gocash_go_cash_loan_gocash_core_loan b on a.loan_id = b.id::text
left join dw_gocash_go_cash_loan_gocash_core_customer c on a.customer_id = c.id::text 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_extend)
r_extend = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_extend.shape)
print(r_extend.loan_id.nunique())
print(r_extend.dtypes)

r_extend.product_id.value_counts()

# r_extend.loan_id = r_extend.loan_id.astype(str)
# r_extend_f = r_extend.copy()

r_extend.to_csv(path_rawdata + 'r_extend.csv', index = False)
# r_extend = pd.read_csv(path_rawdata + 'r_extend.csv', dtype={'loan_id': str})

''' **************************** GPS **************************** '''
sql_gps = '''
select loan_id, jailbreak_status, simulator_status, 
latitude, longitude, altitude, speed, accuracy
from (
select loan_id, a.effective_date, b.*,
row_number() over(partition by loan_id order by b.create_time desc) as rn 
from (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
) a 
left join ( 
	select customer_id, create_time, 
    case when jailbreak_status in ('True','true') then 1 when jailbreak_status in ('False','false') then 0 else -1 end as jailbreak_status, 
    case when simulator_status in ('True','true') then 1 when simulator_status in ('False','false') then 0 else -1 end as simulator_status, 
	case when gps <> '' then gps::json #>> '{latitude}' end as latitude, 
	case when gps <> '' then gps::json #>> '{longitude}' end as longitude, 
	case when gps <> '' then gps::json #>> '{direction}' end as direction, 
	case when gps <> '' then gps::json #>> '{speed}' end as speed, 
	case when gps <> '' then gps::json #>> '{altitude}' end as altitude, 
	case when gps <> '' then gps::json #>> '{accuracy}' end as accuracy
	from gocash_loan_risk_program_baseinfo) b on a.customer_id::varchar = b.customer_id and a.apply_time >= b.create_time
) t 
where rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_gps)
r_gps = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_gps.shape)
print(r_gps.loan_id.nunique())
print(r_gps.dtypes)

r_gps_bk = r_gps.copy()
r_gps = r_gps_bk.copy()

r_gps.loan_id = r_gps.loan_id.astype(str)
r_gps.latitude = r_gps.latitude.astype(str)
r_gps.longitude = r_gps.longitude.astype(str)
r_gps.speed = r_gps.speed.astype(float)
r_gps.altitude = r_gps.altitude.astype(float)
r_gps.accuracy = r_gps.accuracy.astype(float)

r_gps = r_gps.replace(['None', ''],[np.nan, np.nan])

print(r_gps.groupby('simulator_status').size())

r_gps['gps_0'] = '(' + round(pd.to_numeric(r_gps.latitude), 0).astype(str) + ', ' \
                 + round(pd.to_numeric(r_gps.longitude), 0).astype(str) + ')'
#r_gps['gps_1'] = '(' + round(pd.to_numeric(r_gps.latitude), 1).astype(str) + ', ' \
#                 + round(pd.to_numeric(r_gps.longitude), 1).astype(str) + ')'
r_gps = r_gps.replace(['(nan, nan)', '(None, None)'],[np.nan, np.nan])

# CREATE DUMMY VARIABLES FOR GPS_0
gps_0_size = pd.DataFrame(r_gps.groupby('gps_0').size())
gps_0_size = gps_0_size.reset_index().rename(columns={'index': 'gps_0', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
gps_0_size['cum_sum'] = gps_0_size['ct'].cumsum()
gps_0_size['cum_perc'] = 100 * gps_0_size['cum_sum']/gps_0_size['ct'].sum()
gps_0_list = list(gps_0_size.gps_0[gps_0_size.cum_perc <= 90])
gps_0_dummy = pd.get_dummies(r_gps['gps_0'])[gps_0_list]
gps_0_dummy.columns = ['gps_' + str(col) for col in gps_0_dummy.columns]

r_gps_f = r_gps.join(gps_0_dummy)

r_gps_f = r_gps_f.drop(['latitude', 'longitude', 'gps_0'], axis = 1)
print(r_gps_f.columns)
print(r_gps_f.dtypes)

# r_gps_f.to_csv(path_rawdata + 'r_gps_f.csv', index = False)

# '''检查分布 '''
# var = list(r_gps_f.iloc[:,2:].columns)
# test = (r_gps_f.groupby('effective_date')[var].agg(['mean','min', 'median', 'max'])).T
# test2 = (r_gps_f.groupby('effective_date')[var].quantile([0, 0.25, 0.5, 0.75, 0.95, 1])).T

# ''' 检查缺失比例'''
# check = r_gps_f.copy()

# check = check.fillna(-1)
# check = check.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])

# var = list(check.iloc[:,2:].columns)
# check_missingpct = pd.DataFrame(check.groupby(['effective_date']).size(), columns=['total'])
# for x in var:
#     check[x] = check[x].mask(check[x].ne(-1))
#     check_missing = (check.groupby(['effective_date'])[x].count())
#     check_total = (check.groupby(['effective_date']).size())
#     check_missingpct = check_missingpct.merge(pd.DataFrame(check_missing/check_total, columns=[x]), how='left',left_index=True, right_index=True)

# check_missingpct.to_csv(path + 'check_missing_gps.csv')
# test2.to_csv(path + 'test2.csv')
# test.to_csv(path + 'test.csv')



''' **************************** DEVICE **************************** '''
sql_device = '''
select loan_id, brand, model, manufacturer, heightpixels, widthpixels
from (
select loan_id, a.effective_date, b.*, row_number() over(partition by loan_id order by b.create_time desc) as rn 
from (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
) a 
left join ( 
	select customer_id, create_time, 
    case when device_info <>'' then device_info::json #>> '{brand}' end as brand, 
    case when device_info <>'' then device_info::json #>> '{Model}' end as model,
    case when device_info <>'' then device_info::json #>> '{manufacturer}' end as manufacturer,
    case when device_info <>'' then device_info::json #>> '{heightPixels}' end as heightpixels, 
    case when device_info <>'' then device_info::json #>> '{widthPixels}' end as widthpixels
	from (
		select customer_id, create_time, case when substring(device_info,1,1)='"' then substring(device_info,2,length(device_info)-2) else device_info end as device_info
		from gocash_loan_risk_program_baseinfo) t ) b on a.customer_id::varchar = b.customer_id and a.apply_time >= b.create_time
) t 
where rn = 1 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_device)
r_device = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_device.shape)
print(r_device.loan_id.nunique())
print(r_device.dtypes)

r_device_bk = r_device.copy()
r_device = r_device_bk.copy()

r_device.loan_id = r_device.loan_id.astype(str)
r_device.brand = r_device.brand.astype(str).str.upper()
r_device.model = r_device.model.astype(str).str.upper()
r_device.manufacturer = r_device.manufacturer.astype(str).str.upper()
r_device.heightpixels = r_device.heightpixels.astype(str)
r_device.widthpixels = r_device.widthpixels.astype(str)

#r_device = r_device.replace(['false', 'False', 'true', 'True'],[0, 0, 1, 1])
r_device = r_device.replace(['NONE', 'None', ''],[np.nan, np.nan, np.nan])

#test = r_device.groupby('screen').size()

r_device['screen'] = '(' + round(pd.to_numeric(r_device.heightpixels), 0).astype(str) + ', ' \
                 + round(pd.to_numeric(r_device.widthpixels), 0).astype(str) + ')'
r_device = r_device.replace(['(nan, nan)', '(NONE, NONE)'],[np.nan, np.nan])

# CREATE DUMMY VARIABLES FOR screen
screen_size = pd.DataFrame(r_device.groupby('screen').size())
screen_size = screen_size.reset_index().rename(columns={'index': 'screen', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
screen_size['cum_sum'] = screen_size['ct'].cumsum()
screen_size['cum_perc'] = 100 * screen_size['cum_sum']/screen_size['ct'].sum()
screen_list = list(screen_size.screen[screen_size.cum_perc <= 90])
screen_dummy = pd.get_dummies(r_device['screen'])[screen_list]
screen_dummy.columns = ['screen_' + str(col) for col in screen_dummy.columns]

# CREATE DUMMY VARIABLES FOR brand
brand_size = pd.DataFrame(r_device.groupby('brand').size())
brand_size = brand_size.reset_index().rename(columns={'index': 'brand', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
brand_size['cum_sum'] = brand_size['ct'].cumsum()
brand_size['cum_perc'] = 100 * brand_size['cum_sum']/brand_size['ct'].sum()
brand_list = list(brand_size.brand[brand_size.cum_perc <= 95])
brand_dummy = pd.get_dummies(r_device['brand'])[brand_list]
brand_dummy.columns = ['brand_' + str(col) for col in brand_dummy.columns]

# CREATE DUMMY VARIABLES FOR model
model_size = pd.DataFrame(r_device.groupby('model').size())
model_size = model_size.reset_index().rename(columns={'index': 'model', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
model_size['cum_sum'] = model_size['ct'].cumsum()
model_size['cum_perc'] = 100 * model_size['cum_sum']/model_size['ct'].sum()
model_list = list(model_size.model[model_size.cum_perc <= 75])
model_dummy = pd.get_dummies(r_device['model'])[model_list]
model_dummy.columns = ['model_' + str(col) for col in model_dummy.columns]

# CREATE DUMMY VARIABLES FOR manufacturer
manufacturer_size = pd.DataFrame(r_device.groupby('manufacturer').size())
manufacturer_size = manufacturer_size.reset_index().rename(columns={'index': 'manufacturer', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
manufacturer_size['cum_sum'] = manufacturer_size['ct'].cumsum()
manufacturer_size['cum_perc'] = 100 * manufacturer_size['cum_sum']/manufacturer_size['ct'].sum()
manufacturer_list = list(manufacturer_size.manufacturer[manufacturer_size.cum_perc <= 95])
manufacturer_dummy = pd.get_dummies(r_device['manufacturer'])[manufacturer_list]
manufacturer_dummy.columns = ['manufacturer_' + str(col) for col in manufacturer_dummy.columns]


r_device_f = r_device.join(screen_dummy).join(brand_dummy).join(model_dummy).join(manufacturer_dummy)

r_device_f = r_device_f.drop(['heightpixels', 'widthpixels', 'brand', 'model', 'manufacturer'], axis = 1)

# r_device_f.to_csv(path_rawdata + 'r_device_f.csv', index=False)

#'''检查分布 '''
#var = list(r_device_f.iloc[:,2:].columns)
#test = (r_device_f.groupby('effective_date')[var].agg(['mean','min', 'median', 'max'])).T
#test2 = (r_device_f.groupby('effective_date')[var].quantile([0, 0.25, 0.5, 0.75, 0.95, 1])).T

# ''' 检查缺失比例'''
# check = r_device.copy()

# check = check.fillna(-1)
# check = check.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])

# var = list(check.iloc[:,2:].columns)
# check_missingpct = pd.DataFrame(check.groupby(['effective_date']).size(), columns=['total'])
# for x in var:
#     check[x] = check[x].mask(check[x].ne(-1))
#     check_missing = (check.groupby(['effective_date'])[x].count())
#     check_total = (check.groupby(['effective_date']).size())
#     check_missingpct = check_missingpct.merge(pd.DataFrame(check_missing/check_total, columns=[x]), how='left',left_index=True, right_index=True)

# check_missingpct.to_csv(path + 'check_missing_device.csv')

# test2.to_csv(path + 'test2.csv')
# test.to_csv(path + 'test.csv')


''' **************************** BASE INFO **************************** '''
sql_baseinfo = '''
WITH loan as (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
),
baseinfo as 
(
SELECT id as customer_id
    , update_time
    , cell_phone
    , mail
    , id_card_address
    ,marital_status
    ,religion
    ,education
    ,channel
FROM dw_gocash_go_cash_loan_gocash_core_customer
UNION
SELECT customer_id
    , update_time
    , cell_phone
    , mail
    , id_card_address
    , marital_status
    , religion
    ,education
    ,channel
FROM dw_gocash_go_cash_loan_gocash_core_customer_history 
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , baseinfo.*
            , row_number() over(partition by loan.customer_id order by baseinfo.update_time desc) as rn
      FROM loan
      LEFT JOIN baseinfo  ON loan.customer_id = baseinfo.customer_id::text
      WHERE loan.apply_time >= baseinfo.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_baseinfo)
r_baseinfo = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_baseinfo.shape)
print(r_baseinfo.loan_id.nunique())
print(r_baseinfo.dtypes)

r_baseinfo.loan_id = r_baseinfo.loan_id.astype(str)

r_baseinfo['mail_address1'] = r_baseinfo.mail.apply(lambda x: x.split('@'))
r_baseinfo['mail_address2'] = r_baseinfo.mail_address1.apply(lambda x: x[-1])
r_baseinfo['mail_address3'] = r_baseinfo.mail_address2.apply(lambda x: x.lower().split('.')[0])
r_baseinfo['mail_address'] = r_baseinfo.mail_address3.apply(lambda x: 'gmail' if x in ['gmail'] else 'yahoo'  if x in ['yahoo','ymail','rocketmail'] else 'others')
r_baseinfo['mail_address'] = r_baseinfo.mail_address3.apply(lambda x: 'gmail' if x in ['gmail'] else 'yahoo'  if x in ['yahoo','ymail','rocketmail'] else 'others')
r_baseinfo['mail_address'] = r_baseinfo['mail_address'].replace('gmail','1').replace('yahoo','2').replace('others','3')
r_baseinfo = r_baseinfo.drop(['mail_address1','mail_address2','mail_address3'],1)

r_baseinfo = r_baseinfo[['loan_id', 'mail_address', 'marital_status', 'religion', 'education']]


sql_baseinfo2 = '''
WITH loan as 
(	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
),
baseinfo as (
SELECT customer_id
    , update_time
    , id_card_no
    --前6位为身份证地区信息
    , substring(id_card_no, 1,2) as provincecode
    , substring(id_card_no, 3,2) as citycode
    , substring(id_card_no, 5,2) as districtcode
    --7-12为出生信息，顺序为日/月/年
    --, substring(id_card_no, 7,2) as day
    --, substring(id_card_no, 9,2) as month
    --, substring(id_card_no, 11,2) as year
    --, case when substring(id_card_no, 7,2)::int > 40 then substring(id_card_no, 7,2)::int -40 else  substring(id_card_no, 7,2)::int end as birth_day
    --, case when substring(id_card_no, 11,2)::int <20 then '20' else '19'end as yearhead -判断是20年出生的还是19XX年出生
    --计算年龄和性别
    , case when substring(id_card_no, 7,2)::int > 40 then 'female' else 'male' end as gender --female生日 = 1-31 + 40; male生日=1-31
FROM public.dw_gocash_go_cash_loan_gocash_core_customer_history
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
                , case when substring(id_card_no, 11,2)::int >substring(cast(apply_time as varchar),3,2)::int 
        then 100 +substring(cast(apply_time as varchar),3,2)::int - substring(id_card_no, 11,2)::int
        else substring(cast(apply_time as varchar),3,2)::int - substring(id_card_no, 11,2)::int  end as age
            , baseinfo.*
            , row_number() over(partition by loan.customer_id order by baseinfo.update_time desc) as rn
      FROM loan
      LEFT join baseinfo ON loan.customer_id = baseinfo.customer_id::text
      WHERE loan.apply_time >= baseinfo.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_baseinfo2)
r_baseinfo2 = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_baseinfo2.shape)
print(r_baseinfo2.loan_id.nunique())
print(r_baseinfo2.dtypes)

r_baseinfo2.loan_id = r_baseinfo2.loan_id.astype(str)
r_baseinfo2 = r_baseinfo2[['loan_id', 'age', 'provincecode', 'citycode', 'districtcode', 'gender']]

r_baseinfo = r_baseinfo.merge(r_baseinfo2, on = 'loan_id', how = 'left')

r_baseinfo.gender = r_baseinfo.gender.replace('male', 1).replace('female', 0)

print(r_baseinfo.marital_status.value_counts(dropna = False))
print(r_baseinfo.religion.value_counts(dropna = False))
print(r_baseinfo.education.value_counts(dropna = False))
print(r_baseinfo.provincecode.value_counts(dropna = False))
print(r_baseinfo.citycode.value_counts(dropna = False))
print(r_baseinfo.districtcode.value_counts(dropna = False))
print(r_baseinfo.gender.value_counts(dropna = False))

# CREATE DUMMY VARIABLES FOR mail
mail_size = pd.DataFrame(r_baseinfo.groupby('mail_address').size())
mail_size = mail_size.reset_index().rename(columns={'index': 'mail_address', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
mail_size['cum_sum'] = mail_size['ct'].cumsum()
mail_size['cum_perc'] = 100 * mail_size['cum_sum']/mail_size['ct'].sum()
mail_list = list(mail_size.mail_address[mail_size.cum_perc <= 100])
mail_dummy = pd.get_dummies(r_baseinfo['mail_address'])[mail_list]
mail_dummy.columns = ['mail_' + str(col) for col in mail_dummy.columns]

# CREATE DUMMY VARIABLES FOR marital
marital_size = pd.DataFrame(r_baseinfo.groupby('marital_status').size())
marital_size = marital_size.reset_index().rename(columns={'index': 'marital', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
marital_size['cum_sum'] = marital_size['ct'].cumsum()
marital_size['cum_perc'] = 100 * marital_size['cum_sum']/marital_size['ct'].sum()
marital_list = list(marital_size.marital_status[marital_size.cum_perc <= 100])
marital_dummy = pd.get_dummies(r_baseinfo['marital_status'])[marital_list]
marital_dummy.columns = ['marital_' + str(col) for col in marital_dummy.columns]

# CREATE DUMMY VARIABLES FOR religion
religion_size = pd.DataFrame(r_baseinfo.groupby('religion').size())
religion_size = religion_size.reset_index().rename(columns={'index': 'religion', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
religion_size['cum_sum'] = religion_size['ct'].cumsum()
religion_size['cum_perc'] = 100 * religion_size['cum_sum']/religion_size['ct'].sum()
religion_list = list(religion_size.religion[religion_size.cum_perc <= 100])
religion_dummy = pd.get_dummies(r_baseinfo['religion'])[religion_list]
religion_dummy.columns = ['religion_' + str(col) for col in religion_dummy.columns]

# CREATE DUMMY VARIABLES FOR education
education_size = pd.DataFrame(r_baseinfo.groupby('education').size())
education_size = education_size.reset_index().rename(columns={'index': 'education', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
education_size['cum_sum'] = education_size['ct'].cumsum()
education_size['cum_perc'] = 100 * education_size['cum_sum']/education_size['ct'].sum()
education_list = list(education_size.education[education_size.cum_perc <= 100])
education_dummy = pd.get_dummies(r_baseinfo['education'])[education_list]
education_dummy.columns = ['education_' + str(col) for col in education_dummy.columns]

# CREATE DUMMY VARIABLES FOR provincecode
provincecode_size = pd.DataFrame(r_baseinfo.groupby('provincecode').size())
provincecode_size = provincecode_size.reset_index().rename(columns={'index': 'provincecode', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
provincecode_size['cum_sum'] = provincecode_size['ct'].cumsum()
provincecode_size['cum_perc'] = 100 * provincecode_size['cum_sum']/provincecode_size['ct'].sum()
provincecode_list = list(provincecode_size.provincecode[provincecode_size.cum_perc <= 95])
provincecode_dummy = pd.get_dummies(r_baseinfo['provincecode'])[provincecode_list]
provincecode_dummy.columns = ['provincecode_' + str(col) for col in provincecode_dummy.columns]

# CREATE DUMMY VARIABLES FOR citycode
citycode_size = pd.DataFrame(r_baseinfo.groupby('citycode').size())
citycode_size = citycode_size.reset_index().rename(columns={'index': 'citycode', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
citycode_size['cum_sum'] = citycode_size['ct'].cumsum()
citycode_size['cum_perc'] = 100 * citycode_size['cum_sum']/citycode_size['ct'].sum()
citycode_list = list(citycode_size.citycode[citycode_size.cum_perc <= 100])
citycode_dummy = pd.get_dummies(r_baseinfo['citycode'])[citycode_list]
citycode_dummy.columns = ['citycode_' + str(col) for col in citycode_dummy.columns]

# CREATE DUMMY VARIABLES FOR districtcode
districtcode_size = pd.DataFrame(r_baseinfo.groupby('districtcode').size())
districtcode_size = districtcode_size.reset_index().rename(columns={'index': 'districtcode', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
districtcode_size['cum_sum'] = districtcode_size['ct'].cumsum()
districtcode_size['cum_perc'] = 100 * districtcode_size['cum_sum']/districtcode_size['ct'].sum()
districtcode_list = list(districtcode_size.districtcode[districtcode_size.cum_perc <= 95])
districtcode_dummy = pd.get_dummies(r_baseinfo['districtcode'])[districtcode_list]
districtcode_dummy.columns = ['districtcode_' + str(col) for col in districtcode_dummy.columns]

r_baseinfo_f = r_baseinfo.join(mail_dummy).join(marital_dummy).join(religion_dummy).join(education_dummy).join(provincecode_dummy).join(citycode_dummy).join(districtcode_dummy)
print(r_baseinfo_f.columns)

del mail_dummy
del marital_dummy
del religion_dummy
del education_dummy
del provincecode_dummy
del citycode_dummy
del districtcode_dummy

r_baseinfo_f = r_baseinfo_f.drop(['mail_address', 'marital_status', 'religion', 'education', 'provincecode', 'citycode','districtcode'], axis = 1)

# r_baseinfo_f.to_csv(path_rawdata + 'r_baseinfo_f.csv', index=False)


"""职业信息"""
sql_prof = '''
WITH loan as 
(
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'),
prof as 
(
SELECT customer_id
    , update_time
    , occupation_type
    , job
    , industry_involved
    , monthly_salary
    , company_area
    , employee_number
    , jobless_time_income
    , monthly_income_resource
    , pre_work_industry
    , pre_work_income
FROM dw_gocash_go_cash_loan_gocash_core_cusomer_profession
UNION
SELECT customer_id
    , update_time
    , occupation_type
    , job
    , industry_involved
    , monthly_salary
    , company_area
    , employee_number
    , jobless_time_income
    , monthly_income_resource
    , pre_work_industry
    , pre_work_income
FROM dw_gocash_go_cash_loan_gocash_core_cusomer_profession_history 
)
SELECT loan_id
    , apply_time
    , effective_date
    , customer_id
    , occupation_type
    , case when occupation_type in ('UNEMPLOYED') then jobless_time_income end as jobless_time_income
    , case when occupation_type in ('UNEMPLOYED') then monthly_income_resource end as monthly_income_resource
    , case when occupation_type in ('UNEMPLOYED') then pre_work_industry end as pre_work_industry
    , case when occupation_type in ('UNEMPLOYED') then pre_work_income end as pre_work_income   
    , case when occupation_type in ('OFFICE') then job end as job
    , case when occupation_type in ('OFFICE','ENTREPRENEUR') then industry_involved end as industry_involved
    , case when occupation_type in ('OFFICE') then monthly_salary end as monthly_salary
    , case when occupation_type in ('OFFICE','ENTREPRENEUR') then company_area end as company_area
    , case when occupation_type in ('ENTREPRENEUR') then employee_number end as employee_number
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , prof.*
            , row_number() over(partition by loan.customer_id order by prof.update_time desc) as rn
      FROM loan
      LEFT JOIN prof ON loan.customer_id = prof.customer_id::text
      WHERE loan.apply_time >= prof.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_prof)
r_prof = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_prof.shape)
print(r_prof.loan_id.nunique())
print(r_prof.dtypes)

r_prof.loan_id = r_prof.loan_id.astype(str)

r_prof = r_prof[['loan_id', 'occupation_type', 'jobless_time_income', 'monthly_income_resource',
       'pre_work_industry', 'pre_work_income', 'job', 'industry_involved',
       'monthly_salary', 'company_area', 'employee_number']]

r_prof['company_area1'] = r_prof['company_area'].str.split(',').str[-1]
r_prof['company_area2'] = r_prof['company_area'].str.split(',').str[-2]
r_prof['company_area3'] = r_prof['company_area'].str.split(',').str[-3]

print(r_prof.occupation_type.value_counts(dropna = False))
print(r_prof.pre_work_industry.value_counts(dropna = False))
print(r_prof.job.value_counts(dropna = False))
print(r_prof.industry_involved.value_counts(dropna = False))
print(r_prof.company_area1.value_counts(dropna = False))
print(r_prof.company_area2.value_counts(dropna = False))
print(r_prof.company_area3.value_counts(dropna = False))
print(r_prof.employee_number.value_counts(dropna = False))

# CREATE DUMMY VARIABLES FOR occupation_type
occupation_type_size = pd.DataFrame(r_prof.groupby('occupation_type').size())
occupation_type_size = occupation_type_size.reset_index().rename(columns={'index': 'occupation_type', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
occupation_type_size['cum_sum'] = occupation_type_size['ct'].cumsum()
occupation_type_size['cum_perc'] = 100 * occupation_type_size['cum_sum']/occupation_type_size['ct'].sum()
occupation_type_list = list(occupation_type_size.occupation_type[occupation_type_size.cum_perc <= 100])
occupation_type_dummy = pd.get_dummies(r_prof['occupation_type'])[occupation_type_list]
occupation_type_dummy.columns = ['occupation_type_' + str(col) for col in occupation_type_dummy.columns]

# CREATE DUMMY VARIABLES FOR pre_work_industry
pre_work_industry_size = pd.DataFrame(r_prof.groupby('pre_work_industry').size())
pre_work_industry_size = pre_work_industry_size.reset_index().rename(columns={'index': 'pre_work_industry', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
pre_work_industry_size['cum_sum'] = pre_work_industry_size['ct'].cumsum()
pre_work_industry_size['cum_perc'] = 100 * pre_work_industry_size['cum_sum']/pre_work_industry_size['ct'].sum()
pre_work_industry_list = list(pre_work_industry_size.pre_work_industry[pre_work_industry_size.cum_perc <= 100])
pre_work_industry_dummy = pd.get_dummies(r_prof['pre_work_industry'])[pre_work_industry_list]
pre_work_industry_dummy.columns = ['pre_work_industry_' + str(col) for col in pre_work_industry_dummy.columns]

# CREATE DUMMY VARIABLES FOR industry_involved
industry_involved_size = pd.DataFrame(r_prof.groupby('industry_involved').size())
industry_involved_size = industry_involved_size.reset_index().rename(columns={'index': 'industry_involved', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
industry_involved_size['cum_sum'] = industry_involved_size['ct'].cumsum()
industry_involved_size['cum_perc'] = 100 * industry_involved_size['cum_sum']/industry_involved_size['ct'].sum()
industry_involved_list = list(industry_involved_size.industry_involved[industry_involved_size.cum_perc <= 100])
industry_involved_dummy = pd.get_dummies(r_prof['industry_involved'])[industry_involved_list]
industry_involved_dummy.columns = ['industry_involved_' + str(col) for col in industry_involved_dummy.columns]

# CREATE DUMMY VARIABLES FOR job
job_size = pd.DataFrame(r_prof.groupby('job').size())
job_size = job_size.reset_index().rename(columns={'index': 'job', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
job_size['cum_sum'] = job_size['ct'].cumsum()
job_size['cum_perc'] = 100 * job_size['cum_sum']/job_size['ct'].sum()
job_list = list(job_size.job[job_size.cum_perc <= 100])
job_dummy = pd.get_dummies(r_prof['job'])[job_list]
job_dummy.columns = ['job_' + str(col) for col in job_dummy.columns]

# CREATE DUMMY VARIABLES FOR employee_number
employee_number_size = pd.DataFrame(r_prof.groupby('employee_number').size())
employee_number_size = employee_number_size.reset_index().rename(columns={'index': 'employee_number', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
employee_number_size['cum_sum'] = employee_number_size['ct'].cumsum()
employee_number_size['cum_perc'] = 100 * employee_number_size['cum_sum']/employee_number_size['ct'].sum()
employee_number_list = list(employee_number_size.employee_number[employee_number_size.cum_perc <= 100])
employee_number_dummy = pd.get_dummies(r_prof['employee_number'])[employee_number_list]
employee_number_dummy.columns = ['employee_number_' + str(col) for col in employee_number_dummy.columns]

# CREATE DUMMY VARIABLES FOR company_area1
company_area1_size = pd.DataFrame(r_prof.groupby('company_area1').size())
company_area1_size = company_area1_size.reset_index().rename(columns={'index': 'company_area1', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
company_area1_size['cum_sum'] = company_area1_size['ct'].cumsum()
company_area1_size['cum_perc'] = 100 * company_area1_size['cum_sum']/company_area1_size['ct'].sum()
company_area1_list = list(company_area1_size.company_area1[company_area1_size.cum_perc <= 90])
company_area1_dummy = pd.get_dummies(r_prof['company_area1'])[company_area1_list]
company_area1_dummy.columns = ['company_area1_' + str(col) for col in company_area1_dummy.columns]

# CREATE DUMMY VARIABLES FOR company_area2
company_area2_size = pd.DataFrame(r_prof.groupby('company_area2').size())
company_area2_size = company_area2_size.reset_index().rename(columns={'index': 'company_area2', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
company_area2_size['cum_sum'] = company_area2_size['ct'].cumsum()
company_area2_size['cum_perc'] = 100 * company_area2_size['cum_sum']/company_area2_size['ct'].sum()
company_area2_list = list(company_area2_size.company_area2[company_area2_size.cum_perc <= 70])
company_area2_dummy = pd.get_dummies(r_prof['company_area2'])[company_area2_list]
company_area2_dummy.columns = ['company_area2_' + str(col) for col in company_area2_dummy.columns]

# CREATE DUMMY VARIABLES FOR company_area3
# company_area3_size = pd.DataFrame(r_prof.groupby('company_area3').size())
# company_area3_size = company_area3_size.reset_index().rename(columns={'index': 'company_area3', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
# company_area3_size['cum_sum'] = company_area3_size['ct'].cumsum()
# company_area3_size['cum_perc'] = 100 * company_area3_size['cum_sum']/company_area3_size['ct'].sum()
# company_area3_list = list(company_area3_size.company_area3[company_area3_size.cum_perc <= 50])
# company_area3_dummy = pd.get_dummies(r_prof['company_area3'])[company_area3_list]
# company_area3_dummy.columns = ['company_area3_' + str(col) for col in company_area3_dummy.columns]


r_prof_f = r_prof.join(occupation_type_dummy).join(pre_work_industry_dummy).join(industry_involved_dummy).join(job_dummy).join(employee_number_dummy).\
    join(company_area1_dummy).join(company_area2_dummy)
print(r_prof_f.columns)

del occupation_type_dummy
del pre_work_industry_dummy
del industry_involved_dummy
del job_dummy
del employee_number_dummy
del company_area1_dummy
del company_area2_dummy
# del company_area3_dummy

r_prof_f = r_prof_f.drop(['occupation_type', 'pre_work_industry', 'industry_involved', 'job', 'employee_number','company_area1',
                                  'company_area2', 'company_area3', 'company_area','monthly_income_resource'], axis = 1)

tmp = r_prof_f.columns

# r_prof_f.to_csv(path_rawdata + 'r_prof_f.csv', index=False)
# r_prof_f = pd.read_csv(path_rawdata + 'r_prof_f.csv', dtype = {'loan_id': str})



"""银行卡信息"""
sql_bank = '''
WITH loan as 
(
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'),
bank as (
SELECT customer_id
    , create_time as update_time
    , bank_code
FROM dw_gocash_go_cash_loan_gocash_core_customer_account
UNION
SELECT customer_id
    , update_time
    , bank_code
FROM dw_gocash_go_cash_loan_gocash_core_customer_account_history 
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , bank.*
            , row_number() over(partition by loan.customer_id order by bank.update_time desc) as rn
      FROM loan
      LEFT join bank ON loan.customer_id = bank.customer_id::text
      WHERE loan.apply_time >= bank.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_bank)
r_bank = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_bank.shape)
print(r_bank.loan_id.nunique())
print(r_bank.dtypes)

r_bank.loan_id = r_bank.loan_id.astype(str)

r_bank = r_bank[['loan_id', 'bank_code']]

# CREATE DUMMY VARIABLES FOR bankcode
bankcode_size = pd.DataFrame(r_bank.groupby('bank_code').size())
bankcode_size = bankcode_size.reset_index().rename(columns={'index': 'bankcode', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
bankcode_size['cum_sum'] = bankcode_size['ct'].cumsum()
bankcode_size['cum_perc'] = 100 * bankcode_size['cum_sum']/bankcode_size['ct'].sum()
bankcode_list = list(bankcode_size.bank_code[bankcode_size.cum_perc <= 100])
bankcode_dummy = pd.get_dummies(r_bank['bank_code'])[bankcode_list]
bankcode_dummy.columns = ['bankcode_' + str(col) for col in bankcode_dummy.columns]

r_bank_f = r_bank.join(bankcode_dummy)
print(r_bank_f.columns)

del bankcode_dummy

r_bank_f = r_bank_f.drop(['bank_code'], axis = 1)

# r_bank_f.to_csv(path_rawdata + 'r_bank_f.csv', index=False)
# r_bank_f = pd.read_csv(path_rawdata + 'r_bank_f.csv', dtype = {'loan_id': str})


"""紧急联系人"""
sql_refer = '''
WITH loan as 
(
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'),
refer as 
(
SELECT customer_id
    , create_time
    , refer_type
    , case when refer_type = 'BROTHERS_AND_SISTERS' then 1 else 0 end as refer_bro_sis 
    , case when refer_type = 'PARENTS' then 1 else 0 end as refer_parents
    , case when refer_type = 'SPOUSE' then 1 else 0 end as refer_spouse    
FROM dw_gocash_go_cash_loan_gocash_core_customer_refer
WHERE create_time != '1970-01-01' and refer_type not in ('SELF','KINSFOLK','FRIEND')
)
SELECT loan_id
    , max(refer_bro_sis) as refer_bro_sis
    , max(refer_parents) as refer_parents
    , max(refer_spouse) as refer_spouse
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , refer.*
            , dense_rank() over(partition by refer.customer_id order by refer.create_time desc) as rn
      FROM loan
      LEFT JOIN refer  ON loan.customer_id = refer.customer_id::text
      WHERE loan.apply_time >= refer.create_time) t
WHERE rn = 1
GROUP BY loan_id
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_refer)
r_refer = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_refer.shape)
print(r_refer.loan_id.nunique())
print(r_refer.dtypes)

r_refer.loan_id = r_refer.loan_id.astype(str)

r_refer_f = r_refer.copy()

# r_refer_f.to_csv(path_rawdata + 'r_refer_f.csv', index=False)
# r_refer_f = pd.read_csv(path_rawdata + 'r_refer_f.csv', dtype = {'loan_id': str})



"""izi"""
sql_izi1 = '''
select loan_id, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> 'age' as int) end, age) as izi_phoneage
from (
select 
*, row_number() over(partition by a.loan_id order by coalesce(b.create_time::timestamp, c.createtime::timestamp) desc) as rn
from (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
) a 
left join gocash_oss_phone_age b on a.customer_id = b.customer_id and a.effective_date >= b.create_time::date
left join risk_gocash_mongo_iziphoneage c on a.customer_id = c.customerid::text and a.effective_date >= c.createtime::date) t 
where rn = 1 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_izi1)
r_izi1 = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_izi1.shape)
print(r_izi1.loan_id.nunique())
print(r_izi1.dtypes)

r_izi1.loan_id = r_izi1.loan_id.astype(str)


sql_izi2 = '''
select loan_id
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> 'total' as int) end, total) as izi_total
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '07d' as int) end, "07d") as izi_07d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '14d' as int) end, "14d") as izi_14d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '21d' as int) end, "21d") as izi_21d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '30d' as int) end, "30d") as izi_30d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '60d' as int) end, "60d") as izi_60d
, coalesce(case when substring(message,1,1) = '{' then cast(message::json ->> '90d' as int) end, "90d") as izi_90d
from (
select 
*, row_number() over(partition by a.loan_id order by coalesce(b.create_time::timestamp, c.createtime::timestamp) desc) as rn
from (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
) a 
left join gocash_oss_inquiries_cell_phone b on a.customer_id = b.customer_id and a.effective_date >= b.create_time::date
left join risk_gocash_mongo_iziinquiriesbytype c on a.customer_id = c.customerid::text and a.effective_date >= c.createtime::date) t 
where rn = 1 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_izi2)
r_izi2 = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_izi2.shape)
print(r_izi2.loan_id.nunique())
print(r_izi2.dtypes)

r_izi2.loan_id = r_izi2.loan_id.astype(str)


sql_izi3 = '''
select loan_id,
case when coalesce(message, result)='MATCH' then 1 when coalesce(message, result) ='NOT_MATCH' then 0 else -1 end as izi_phoneverify
from (
select 
*, row_number() over(partition by a.loan_id order by coalesce(b.create_time::timestamp, c.createtime::timestamp) desc) as rn
from (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
) a 
left join gocash_oss_phone_verify b on a.customer_id = b.customer_id and a.effective_date >= b.create_time::date
left join risk_gocash_mongo_iziphoneverify c on a.customer_id = c.customerid::text and a.effective_date >= c.createtime::date) t 
where rn = 1 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_izi3)
r_izi3 = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_izi3.shape)
print(r_izi3.loan_id.nunique())
print(r_izi3.dtypes)



''' **************************** 同盾信贷保镖 **************************** '''
sql_td = '''
with td as (
select customer_id,create_time, 
json_array_elements(cast(result_desc::json #>>'{ANTIFRAUD,risk_items}' as json)) ->>'risk_name' as risk_name,
json_array_elements(cast(result_desc::json #>>'{ANTIFRAUD,risk_items}' as json)) ->>'rule_id' as rule_id,
json_array_elements(cast(result_desc::json #>>'{ANTIFRAUD,risk_items}' as json))->>'risk_detail' as risk_detail
from gocash_oss_credit_guard_go_no 
where substring(result_desc,1,1)='{'  
) 
select loan_id
, sum(multiplatform_apply_3day_count::int) multiplatform_apply_3day_count
, sum(multiplatform_apply_3day_total::int) multiplatform_apply_3day_total
, sum(multiplatform_apply_7day_count::int) multiplatform_apply_7day_count
, sum(multiplatform_apply_7day_total::int) multiplatform_apply_7day_total
, sum(multiplatform_apply_14day_count::int) multiplatform_apply_14day_count
, sum(multiplatform_apply_14day_total::int) multiplatform_apply_14day_total
, sum(multiplatform_apply_1month_count::int) multiplatform_apply_1month_count
, sum(multiplatform_apply_1month_total::int) multiplatform_apply_1month_total
, sum(multiplatform_apply_1month_nightapply_count::int) multiplatform_apply_1month_nightapply_count
, sum(multiplatform_apply_1month_nightapply_total::int) multiplatform_apply_1month_nightapply_total
, sum(multiplatform_apply_1month_applycycle::int) multiplatform_apply_1month_applycycle
, sum(multiplatform_apply_3month_count::int) multiplatform_apply_3month_count
, sum(multiplatform_apply_3month_total::int) multiplatform_apply_3month_total
, sum(multiplatform_apply_3month_nightapply_count::int) multiplatform_apply_3month_nightapply_count
, sum(multiplatform_apply_3month_nightapply_total::int) multiplatform_apply_3month_nightapply_total
, sum(multiplatform_apply_3month_applycycle::int) multiplatform_apply_3month_applycycle
, sum(multiplatform_apply_6month_count::int) multiplatform_apply_6month_count
, sum(multiplatform_apply_6month_total::int) multiplatform_apply_6month_total
, sum(multiplatform_apply_6month_nightapply_count::int) multiplatform_apply_6month_nightapply_count
, sum(multiplatform_apply_6month_nightapply_total::int) multiplatform_apply_6month_nightapply_total
, sum(multiplatform_apply_6month_applycycle::int) multiplatform_apply_6month_applycycle

, sum(multiplatform_grant_3day_count::int) multiplatform_grant_3day_count
, sum(multiplatform_grant_3day_total::int) multiplatform_grant_3day_total
, sum(multiplatform_grant_7day_count::int) multiplatform_grant_7day_count
, sum(multiplatform_grant_7day_total::int) multiplatform_grant_7day_total
, sum(multiplatform_grant_14day_count::int) multiplatform_grant_14day_count
, sum(multiplatform_grant_14day_total::int) multiplatform_grant_14day_total
, sum(multiplatform_grant_1month_count::int) multiplatform_grant_1month_count
, sum(multiplatform_grant_1month_total::int) multiplatform_grant_1month_total
, sum(multiplatform_grant_1month_nightgrant_count::int) multiplatform_grant_1month_nightgrant_count
, sum(multiplatform_grant_1month_nightgrant_total::int) multiplatform_grant_1month_nightgrant_total
, sum(multiplatform_grant_1month_grantcycle::int) multiplatform_grant_1month_grantcycle
, sum(multiplatform_grant_3month_count::int) multiplatform_grant_3month_count
, sum(multiplatform_grant_3month_total::int) multiplatform_grant_3month_total
, sum(multiplatform_grant_3month_nightgrant_count::int) multiplatform_grant_3month_nightgrant_count
, sum(multiplatform_grant_3month_nightgrant_total::int) multiplatform_grant_3month_nightgrant_total
, sum(multiplatform_grant_3month_grantcycle::int) multiplatform_grant_3month_grantcycle
, sum(multiplatform_grant_6month_count::int) multiplatform_grant_6month_count
, sum(multiplatform_grant_6month_total::int) multiplatform_grant_6month_total
, sum(multiplatform_grant_6month_nightgrant_count::int) multiplatform_grant_6month_nightgrant_count
, sum(multiplatform_grant_6month_nightgrant_total::int) multiplatform_grant_6month_nightgrant_total
, sum(multiplatform_grant_6month_grantcycle::int) multiplatform_grant_6month_grantcycle
from (
select loan_id ,risk_detail, risk_name
, case when risk_name ='Borrower_applied_multiplatform_in3day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_3day_count
, case when risk_name ='Borrower_applied_times_in3day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_3day_total

, case when risk_name ='Borrower_applied_multiplatform_in7day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_7day_count
, case when risk_name ='Borrower_applied_times_in7day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_7day_total

, case when risk_name ='Borrower_applied_multiplatform_in14day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_14day_count
, case when risk_name ='Borrower_applied_times_in14day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_14day_total

, case when risk_name ='Borrower_applied_multiplatform_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_1month_count
, case when risk_name ='Borrower_applied_times_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_1month_total
, case when risk_name ='Borrower_nightapplied_multiplatform_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_1month_nightapply_count
, case when risk_name ='Borrower_nightapplied_times_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_1month_nightapply_total
, case when risk_name ='Borrower_apply_cycle_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_1month_applycycle

, case when risk_name ='Borrower_applied_multiplatform_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_3month_count
, case when risk_name ='Borrower_applied_times_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_3month_total
, case when risk_name ='Borrower_nightapplied_multiplatform_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_3month_nightapply_count
, case when risk_name ='Borrower_nightapplied_times_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_3month_nightapply_total
, case when risk_name ='Borrower_apply_cycle_in900day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_3month_applycycle

, case when risk_name ='Borrower_applied_multiplatform_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_6month_count
, case when risk_name ='Borrower_applied_times_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_6month_total
, case when risk_name ='Borrower_nightapplied_multiplatform_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_6month_nightapply_count
, case when risk_name ='Borrower_nightapplied_times_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_6month_nightapply_total
, case when risk_name ='Borrower_apply_cycle_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_apply_6month_applycycle

, case when risk_name ='Borrower_granted_multiplatform_in3day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_3day_count
, case when risk_name ='Borrower_granted_times_in3day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_3day_total

, case when risk_name ='Borrower_granted_multiplatform_in7day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_7day_count
, case when risk_name ='Borrower_granted_times_in7day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_7day_total

, case when risk_name ='Borrower_granted_multiplatform_in14day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_14day_count
, case when risk_name ='Borrower_granted_times_in14day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_14day_total

, case when risk_name ='Borrower_granted_multiplatform_in30ay' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_1month_count
, case when risk_name ='Borrower_granted_times_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_1month_total
, case when risk_name ='Borrower_nightgranted_multiplatform_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_1month_nightgrant_count
, case when risk_name ='Borrower_nightgranted_times_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_1month_nightgrant_total
, case when risk_name ='Borrower_grant_cycle_in30day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_1month_grantcycle

, case when risk_name ='Borrower_granted_multiplatform_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_3month_count
, case when risk_name ='Borrower_granted_times_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_3month_total
, case when risk_name ='Borrower_nightgranted_multiplatform_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_3month_nightgrant_count
, case when risk_name ='Borrower_nightgranted_times_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_3month_nightgrant_total
, case when risk_name ='Borrower_grant_cycle_in90day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_3month_grantcycle

, case when risk_name ='Borrower_granted_multiplatform_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_6month_count
, case when risk_name ='Borrower_granted_times_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_6month_total
, case when risk_name ='Borrower_nightgranted_multiplatform_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_6month_nightgrant_count
, case when risk_name ='Borrower_nightgranted_times_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_6month_nightgrant_total
, case when risk_name ='Borrower_grant_cycle_in180day' then cast(risk_detail::json->0 ->>'risk_details' as json)->0 ->>'mh_value' else '0' end as multiplatform_grant_6month_grantcycle

from (
	select loan_id, customer_id, apply_time::timestamp, effective_date from tmp_uku_v6_flag_traintest 
	union 
	select id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date from dw_gocash_go_cash_loan_gocash_core_loan 
	where return_flag = 'false' and device_approval <> 'IOS' and grouping like '%Test%' and effective_date between '2020-01-22' and '2020-01-26'
) a 
inner join td  b on a.customer_id = b.customer_id and a.effective_date >= b.create_time::date
--where risk_name in ('Borrower_applied_multiplatform_in180day','Borrower_applied_times_in180day','Borrower_nightapplied_multiplatform_in180day','Borrower_nightapplied_times_in180day' ) --limit 100
--where loan_id ='420164437025587200'
) t 
group by loan_id
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_td)
r_td = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_td.shape)
print(r_td.loan_id.nunique())
print(r_td.dtypes)


''' **************************** IZI NEW **************************** '''
import json
import pandas as pd
from pandas.api.types import is_dict_like
import numpy as np
import itertools
import time
import os

r_izinew3 = pd.read_excel('D:\\Model Development\\000001 IDN External Data\\01 izidata\\02 返回数据\\20200213\\新izi回溯返回_v6样本 20200213.xlsx', 
                        dtype={'loan_id': str})

izi = r_izinew3[['loan_id','apply_time', '身份证多头v4_result']]
izi['身份证多头v4_result'] = izi['身份证多头v4_result'].str.replace("\'","\"")

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

#解析一层
try1= from_json(izi, '身份证多头v4_result')
try1.head()

#解析两层
try2= from_json(try1, 'detail')
try2.head()

# 从json/dict形式提取出所有日期
def extract_date(dict_ori):
    s=''
    for i in dict_ori.values():
        s = s+","+','.join(i)
    string = s.lstrip(',')
    lst = string.split(',')
    return lst

try3 = try2.copy()
try3['AA'] = try3['A'].apply(lambda x: extract_date(x))
try3['BB'] = try3['B'].apply(lambda x: extract_date(x))
try3['CC'] = try3['C'].apply(lambda x: extract_date(x))
try3.head()

#将日期与申请日期相减计算相差的天数
import datetime

def date_to_daysdiff(row, length, col):
    if row[col] == ['']:
        date_diff=[]
    elif row[col] == []:
        date_diff=[]
    else:
        date_diff=[]
        for i in range(length):
            start = row[col][i]
            end = row['apply_time']
            d1 = datetime.datetime.strptime(start, '%Y%m%d')
            d2 = datetime.datetime.strptime(str(end), '%Y%m%d')
            diff = (d2-d1).days
            #print(diff)
            date_diff.append(str(diff))
            #print(date_diff)
            #date_d = date_diff.astype(str)
    return date_diff

try4 = try3.copy()
for row in range(try4.shape[0]):
    #print(row)
    a=try4.iloc[row]
    try4.loc[row,'A_daysdiff']= ','.join(date_to_daysdiff(a, len(a['AA']), 'AA'))
    try4.loc[row,'B_daysdiff']= ','.join(date_to_daysdiff(a, len(a['BB']), 'BB'))
    try4.loc[row,'C_daysdiff']= ','.join(date_to_daysdiff(a, len(a['CC']), 'CC'))


# 将相差日期衍生成变量
try4.A_daysdiff = try4.A_daysdiff.replace('', '-9999')
try4.B_daysdiff = try4.B_daysdiff.replace('', '-9999')
try4.C_daysdiff = try4.C_daysdiff.replace('', '-9999')

try5 = try4.copy()
try5['A_3d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['A_7d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['A_14d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['A_21d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['A_30d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['A_60d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['A_90d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['A_180d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['A_360d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

try5['B_3d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['B_7d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['B_14d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['B_21d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['B_30d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['B_60d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['B_90d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['B_180d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['B_360d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

try5['C_3d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['C_7d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['C_14d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['C_21d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['C_30d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['C_60d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['C_90d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['C_180d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['C_360d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))


try5.loan_id = try5.loan_id.astype(str)

# try5.to_csv('izi_origin_ys.csv', index=False)
# try5.to_excel('izi_origin_ys.xlsx')


izi_ys = try5.drop(['A','B','C','AA','BB','CC','A_daysdiff','B_daysdiff','C_daysdiff'],1)
izi_ys.head()

izi_ys.loan_id = izi_ys.loan_id.astype(str)

# izi_ys.to_csv('izi_ys.csv', index=False)
# izi_ys.to_excel('izi_ys.xlsx')

# 拼接izi其他数据和衍生后的多头

izi_all_new = r_izinew3.merge(izi_ys.drop(['apply_time'],1), on='loan_id',how='left')
izi_all_new.shape


izi_all_new.head()

# izi_all_new = izi_all_new.iloc[:,1:]
izi_all_new.head()
izi_all_new.shape
izi_all_new.columns
izi_all3 = izi_all_new[['loan_id', 'telcom','preference_bank_60d',
'preference_bank_90d',
'preference_bank_180d',
'preference_bank_270d',
'preference_ecommerce_60d',
'preference_ecommerce_90d',
'preference_ecommerce_180d',
'preference_ecommerce_270d',
'preference_game_60d',
'preference_game_90d',
'preference_game_180d',
'preference_game_270d',
'preference_lifestyle_60d',
'preference_lifestyle_90d',
'preference_lifestyle_180d',
'preference_lifestyle_270d',
'phonescore',
'topup_0_30_avg',
'topup_0_30_times',
'topup_0_30_max',
'topup_0_30_min',
'topup_0_60_avg',
'topup_0_60_times',
'topup_0_60_max',
'topup_0_60_min',
'topup_0_90_avg',
'topup_0_90_times',
'topup_0_90_max',
'topup_0_90_min',
'topup_0_180_avg',
'topup_0_180_times',
'topup_0_180_max',
'topup_0_180_min',
'topup_0_360_avg',
'topup_0_360_times',
'topup_0_360_max',
'topup_0_360_min',
'topup_30_60_avg',
'topup_30_60_times',
'topup_30_60_max',
'topup_30_60_min',
'topup_60_90_avg',
'topup_60_90_times',
'topup_60_90_max',
'topup_60_90_min',
'topup_90_180_avg',
'topup_90_180_times',
'topup_90_180_max',
'topup_90_180_min',
'topup_180_360_avg',
'topup_180_360_times',
'topup_180_360_max',
'topup_180_360_min',
'topup_360_720_avg',
'topup_360_720_times',
'topup_360_720_max',
'topup_360_720_min',
'A_3d',
'A_7d',
'A_14d',
'A_21d',
'A_30d',
'A_60d',
'A_90d',
'A_180d',
'A_360d',
'B_3d',
'B_7d',
'B_14d',
'B_21d',
'B_30d',
'B_60d',
'B_90d',
'B_180d',
'B_360d',
'C_3d',
'C_7d',
'C_14d',
'C_21d',
'C_30d',
'C_60d',
'C_90d',
'C_180d',
'C_360d']]

izi_all3.telcom.value_counts()
izi_all3['telcom_Telkomsel'] = izi_all3['telcom'].apply(lambda x: 1 if x == 'Telkomsel' else 0)
izi_all3['telcom_IM3'] = izi_all3['telcom'].apply(lambda x: 1 if x == 'IM3' else 0)
izi_all3['telcom_XL'] = izi_all3['telcom'].apply(lambda x: 1 if x == 'XL' else 0)
izi_all3['telcom_3'] = izi_all3['telcom'].apply(lambda x: 1 if x == '3' else 0)
izi_all3['telcom_AXIS'] = izi_all3['telcom'].apply(lambda x: 1 if x == 'AXIS' else 0)
izi_all3['telcom_Hutchison'] = izi_all3['telcom'].apply(lambda x: 1 if x == 'Hutchison' else 0)
izi_all3['telcom_Smartfren'] = izi_all3['telcom'].apply(lambda x: 1 if x == 'Smartfren' else 0)
izi_all3 = izi_all3.drop('telcom', 1)

print(izi_all3.shape)
print(izi_all3.loan_id.nunique())
print(izi_all3.columns)
print(izi_all3.dtypes)

izi_all3.to_csv(path_rawdata + 'r_izinew.csv', index=False)


''' **************************** APP **************************** '''

r_app_f = pd.read_csv(path_rawdata + 'var_app_freq_w_tfidf_1220_0126.csv', dtype={'loan_id': str})

r_app_f = pd.read_csv(path_rawdata + 'var_app_freq_w_tfidf2_1220_0126.csv', dtype={'loan_id': str})

print(r_app_f.shape)


'''################## 合并准备 ####################'''
# r_flag_f = pd.read_csv(path_rawdata + 'r_flag_f.csv', dtype = {'loan_id': str, 'customer_id': str})
# r_gps_f = pd.read_csv(path_rawdata + 'r_gps_f.csv', dtype={'loan_id': str})
# r_device_f = pd.read_csv(path_rawdata + 'r_device_f.csv', dtype={'loan_id': str})
# r_baseinfo_f = pd.read_csv(path_rawdata + 'r_baseinfo_f.csv', dtype = {'loan_id': str})
# r_prof_f = pd.read_csv(path_rawdata + 'r_prof_f.csv', dtype={'loan_id': str})
# r_refer_f = pd.read_csv(path_rawdata + 'r_refer_f.csv', dtype = {'loan_id': str})
# r_bank_f = pd.read_csv(path_rawdata + 'r_bank_f.csv', dtype = {'loan_id': str})

''' MERGE ALL '''
r_all = r_flag_f.merge(r_gps_f, how='left', on='loan_id')
r_all = r_all.merge(r_device_f.drop(['screen'], axis=1), how='left', on='loan_id')
r_all = r_all.merge(r_baseinfo_f, how='left', on='loan_id')
r_all = r_all.merge(r_prof_f, how='left', on='loan_id')
r_all = r_all.merge(r_refer_f, how='left', on='loan_id')
r_all = r_all.merge(r_bank_f, how='left', on='loan_id')
r_all = r_all.merge(r_izi1, how='left', on='loan_id')
r_all = r_all.merge(r_izi2, how='left', on='loan_id')
r_all = r_all.merge(r_izi3, how='left', on='loan_id')
r_all = r_all.merge(izi_all3, how='left', on='loan_id')
r_all = r_all.merge(r_td, how='left', on='loan_id')
r_all = r_all.merge(r_app_f, how='left', on='loan_id')

print(r_all.shape)
print(r_all.loan_id.nunique())


r_all.to_csv(path_rawdata + 'r_all_app.csv', index = False)
r_all.to_csv(path_rawdata + 'r_all_app2.csv', index = False)

r_all = pd.read_csv(path_rawdata + 'r_all_app2.csv')

r_all_td_date = r_all[(pd.to_datetime(r_all.effective_date) == pd.to_datetime('2019-12-20'))
                       | (pd.to_datetime(r_all.effective_date) >= pd.to_datetime('2019-12-27'))]

r_all_td_date.to_csv(path_rawdata + 'r_all_app_tddate.csv', index = False)
r_all_td_date.to_csv(path_rawdata + 'r_all_app2_tddate.csv', index = False)


'''******************* + advanceAI *******************'''

r_adv_tel = pd.read_excel('D:/Model Development/000001 IDN External Data/07 AdvanceAI/02 回溯数据/返回数据_带loanid 20200218.xlsx', 
                          dtype = {'loan_id': str}, sheet_name = 'extendedtelscore')
r_adv_multi = pd.read_excel('D:/Model Development/000001 IDN External Data/07 AdvanceAI/02 回溯数据/返回数据_带loanid 20200218.xlsx', 
                          dtype = {'loan_id': str}, sheet_name = 'multiplatformscore')
r_adv_credit = pd.read_excel('D:/Model Development/000001 IDN External Data/07 AdvanceAI/02 回溯数据/返回数据_带loanid 20200218.xlsx', 
                          dtype = {'loan_id': str}, sheet_name = 'creditscore')
r_adv_fraud = pd.read_excel('D:/Model Development/000001 IDN External Data/07 AdvanceAI/02 回溯数据/返回数据_带loanid 20200218.xlsx', 
                          dtype = {'loan_id': str}, sheet_name = 'fraudscore')

r_adv_multi.rename(columns = {'score': 'multiscore'}, inplace=True)
r_adv_credit.rename(columns = {'score': 'creditscore'}, inplace=True)
r_adv_fraud.rename(columns = {'score': 'fraudscore'}, inplace=True)

r_adv_tel = r_adv_tel.drop(['phoneNumber','createTimestamp'],1)
r_adv_multi = r_adv_multi.drop(['name','idNumber','phoneNumber','createTimestamp'],1)
r_adv_credit = r_adv_credit.drop(['name','idNumber','phoneNumber','createTimestamp'],1)
r_adv_fraud = r_adv_fraud.drop(['name','idNumber','phoneNumber','createTimestamp'],1)

# print(r_adv_tel.dtypes)
# print(r_adv_multi.dtypes)
# print(r_adv_credit.dtypes)
# print(r_adv_fraud.dtypes)

var_adv_tel = list(set(r_adv_tel.columns) - set(['loan_id']))
var_adv_multi = list(set(r_adv_multi.columns) - set(['loan_id']))
var_adv_credit = list(set(r_adv_credit.columns) - set(['loan_id']))
var_adv_fraud = list(set(r_adv_fraud.columns) - set(['loan_id']))

import pickle
with open(path_rawdata + "var_adv_tel.txt", "wb") as fp:   #Pickling
    pickle.dump(var_adv_tel, fp)
with open(path_rawdata + "var_adv_multi.txt", "wb") as fp:   #Pickling
    pickle.dump(var_adv_multi, fp)
with open(path_rawdata + "var_adv_credit.txt", "wb") as fp:   #Pickling
    pickle.dump(var_adv_credit, fp)
with open(path_rawdata + "var_adv_fraud.txt", "wb") as fp:   #Pickling
    pickle.dump(var_adv_fraud, fp)
    

# merge advanceAI data
r_all_app_tddate = pd.read_csv('D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212/r_all_app_tddate.csv', dtype = {'loan_id': str})
r_all_app2_tddate = pd.read_csv('D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212/r_all_app2_tddate.csv', dtype = {'loan_id': str})

r_all_app_tddate_adv = r_all_app_tddate.merge(r_adv_tel, on='loan_id', how='left')
r_all_app_tddate_adv = r_all_app_tddate_adv.merge(r_adv_multi, on='loan_id', how='left')
r_all_app_tddate_adv = r_all_app_tddate_adv.merge(r_adv_credit, on='loan_id', how='left')
r_all_app_tddate_adv = r_all_app_tddate_adv.merge(r_adv_fraud, on='loan_id', how='left')
print(r_all_app_tddate.shape)
print(r_all_app_tddate_adv.shape)

r_all_app2_tddate_adv = r_all_app2_tddate.merge(r_adv_tel, on='loan_id', how='left')
r_all_app2_tddate_adv = r_all_app2_tddate_adv.merge(r_adv_multi, on='loan_id', how='left')
r_all_app2_tddate_adv = r_all_app2_tddate_adv.merge(r_adv_credit, on='loan_id', how='left')
r_all_app2_tddate_adv = r_all_app2_tddate_adv.merge(r_adv_fraud, on='loan_id', how='left')
print(r_all_app2_tddate.shape)
print(r_all_app2_tddate_adv.shape)

r_all_app_tddate_adv.to_csv(path_rawdata + 'r_all_app_tddate_adv.csv', index = False)
r_all_app2_tddate_adv.to_csv(path_rawdata + 'r_all_app2_tddate_adv.csv', index = False)





''' CHECK MISSING '''
check = r_all.copy()

check = check.fillna(-1)
check = check.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999, -1111],[-1,  -1, -1, -1, -1, -1, -1, -1, -1])

# var = list(check.iloc[:,3:].columns).remove('sample_set')

var = list(check.columns)
var = [x for x in var if x not in {'loan_id', 'flag7', 'effective_date', 'sample_set','customer_id', 'apply_time', 'curr_date', 'paid_off_time', 'due_date',
 'loan_status'}]

check_missingpct = pd.DataFrame(check.groupby(['effective_date']).size(), columns=['total'])
for x in var:
    check[x] = check[x].mask(check[x].ne(-1))
    check_missing = (check.groupby(['effective_date'])[x].count())
    check_total = (check.groupby(['effective_date']).size())
    check_missingpct = check_missingpct.merge(pd.DataFrame(check_missing/check_total, columns=[x]), how='left',left_index=True, right_index=True)

check_missingpct.to_excel(path_rawdata + 'check_missing_all.xlsx')




