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

path_rawdata = 'D:/Model Development/202001 IDN new v6/01 Data/raw data w600-618/'
path = 'D:/Model Development/202001 IDN new v6/01 Data/'

usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)

''' **************************** FLAG **************************** '''
sql = '''
select distinct * from (
    select a.id::text as loan_id, a.customer_id::text, a.apply_time, a.effective_date, 
    a.grouping, newusermodelscorev5, 
    case when substring(b.newusermodelscorev5,1,1)='{' then b.newusermodelscorev5::json ->> 'newUserModelScoreV5' else b.newusermodelscorev5 end as scorev5,
	a.extend_times, a.approved_principal, a.approved_period, a.product_id, 
	case when extend_times>3 then 0
	when paid_off_time::Date-due_date>=7 then 1 
	when loan_status='COLLECTION' and current_date::Date-due_date<7 then 0
	when loan_status='COLLECTION' and current_date::Date-due_date>=7 then 1
	when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then 0
	when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then 0
	else 0 end as flag7
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26') t
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql)
r_flag = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_flag.shape)
print(r_flag.loan_id.nunique())
print(r_flag.dtypes)

print(r_flag.flag7.value_counts(dropna=False))

r_flag.groupby('effective_date')['flag7'].mean()
r_flag.groupby('grouping')['flag7'].mean()

print(r_flag.scorev5.value_counts(dropna=False))

''' **************************** GPS **************************** '''
sql_gps = '''
select  
loan_id, jailbreak_status, simulator_status, 
	case when gps <> '' then gps::json #>> '{latitude}' end as latitude, 
	case when gps <> '' then gps::json #>> '{longitude}' end as longitude, 
	case when gps <> '' then gps::json #>> '{direction}' end as direction, 
	case when gps <> '' then gps::json #>> '{speed}' end as speed, 
	case when gps <> '' then gps::json #>> '{altitude}' end as altitude, 
	case when gps <> '' then gps::json #>> '{accuracy}' end as accuracy
from (
select loan_id, a.effective_date, b.*,
row_number() over(partition by loan_id order by b.create_time desc) as rn 
from (
	select a.id::text as loan_id, customer_id, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26') a 
left join ( 
	select customer_id, create_time, 
    case when jailbreak_status in ('True','true') then 1 when jailbreak_status in ('False','false') then 0 else -1 end as jailbreak_status, 
    case when simulator_status in ('True','true') then 1 when simulator_status in ('False','false') then 0 else -1 end as simulator_status,gps
	from gocash_loan_risk_program_baseinfo) b on a.customer_id::varchar = b.customer_id and a.apply_time::timestamp >= b.create_time::timestamp
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
select loan_id, 
    case when device_info <>'' then device_info::json #>> '{brand}' end as brand, 
    case when device_info <>'' then device_info::json #>> '{Model}' end as model,
    case when device_info <>'' then device_info::json #>> '{manufacturer}' end as manufacturer,
    case when device_info <>'' then device_info::json #>> '{heightPixels}' end as heightpixels, 
    case when device_info <>'' then device_info::json #>> '{widthPixels}' end as widthpixels
from (
select loan_id, a.effective_date, b.*, row_number() over(partition by loan_id order by b.create_time desc) as rn 
from (
	select a.id::text as loan_id, customer_id, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
) a 
left join ( 
	select customer_id, create_time, device_info
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
	select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
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
(		select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
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
	select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
    ),
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
		select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
    ),
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
		select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
    ),
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
	select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
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
	select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
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
	select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
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
	select a.id::text as loan_id, customer_id::text, apply_time::timestamp, effective_date 
    from dw_gocash_go_cash_loan_gocash_core_loan a
	inner join risk_mongo_gocash_installmentriskcontrolparams b on a.id::text = b.loanid and rctype<>'rc2' and newusermodelscorev5 is not null
	where return_flag = 'false' and effective_date between '2019-12-31' and '2020-01-26'
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



''' **************************** APP **************************** '''



'''################## 合并准备 ####################'''

''' MERGE ALL '''
r_all = r_flag.merge(r_gps_f, how='left', on='loan_id')
r_all = r_all.merge(r_device_f.drop(['screen'], axis=1), how='left', on='loan_id')
r_all = r_all.merge(r_baseinfo_f, how='left', on='loan_id')
r_all = r_all.merge(r_prof_f, how='left', on='loan_id')
r_all = r_all.merge(r_refer_f, how='left', on='loan_id')
r_all = r_all.merge(r_bank_f, how='left', on='loan_id')
r_all = r_all.merge(r_izi1, how='left', on='loan_id')
r_all = r_all.merge(r_izi2, how='left', on='loan_id')
r_all = r_all.merge(r_izi3, how='left', on='loan_id')
r_all = r_all.merge(r_td, how='left', on='loan_id')

print(r_all.shape)
print(r_all.loan_id.nunique())

r_all.scorev5 = r_all.scorev5.astype(float)

r_all.to_csv(path_rawdata + 'r_all.csv', index = False)

print(r_all[r_all.scorev5>618].shape)
print(r_all[r_all.scorev5<=618].shape)

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




