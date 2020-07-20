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

path_rawdata = 'D:/Model Development/201912 IDN new v5/01 Data/raw data/'
path = 'D:/Model Development/201912 IDN new v5/01 Data/'

usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)

''' **************************** FLAG **************************** '''
sql_flag = '''
select 
a.id as loan_id, customer_id, apply_time, effective_date, current_date as curr_date, paid_off_time, due_date, loan_status,
case when extend_times>3 then 0 
when paid_off_time::Date-due_date>7 then 1 
when loan_status='COLLECTION' and current_date::Date-due_date<=7 then -3 
when loan_status='COLLECTION' and current_date::Date-due_date>7 then 1
when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
else 0 end as flag7
from dw_gocash_go_cash_loan_gocash_core_loan a 
where return_flag = 'false' and effective_date between '2019-05-01' and '2019-11-18' 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_flag)
r_flag = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_flag.shape)
print(r_flag.loan_id.nunique())
print(r_flag.dtypes)

r_flag.loan_id = r_flag.loan_id.astype(str)
r_flag.customer_id = r_flag.customer_id.astype(str)

r_flag['sample_set'] = pd.to_datetime(r_flag['effective_date']).apply(lambda x: 'train' if x <= pd.to_datetime('2019-07-31') else 
        ('test' if x <= pd.to_datetime('2019-08-12') else 
         ('' if x <= pd.to_datetime('2019-08-22') else 
          ('test' if x <= pd.to_datetime('2019-08-28') else 
           ('' if x <= pd.to_datetime('2019-08-29') else 'oot')))))

r_flag_f = r_flag.copy()

r_flag_f.to_csv(path_rawdata + 'r_flag_f.csv', index = False)

r_flag_f = pd.read_csv(path_rawdata + 'r_flag 20191218.csv')
print(r_flag_f.shape)
print(r_flag_f.loan_id.nunique())
r_flag_f.groupby('flag7').size()


''' **************************** EXTEND **************************** '''
sql_extend = '''
select a.id as loan_id, extend_times, approved_principal
from dw_gocash_go_cash_loan_gocash_core_loan a 
where return_flag = 'false' and effective_date between '2019-05-01' and '2019-12-11' 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_extend)
r_extend = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_extend.shape)
print(r_extend.loan_id.nunique())
print(r_extend.dtypes)

r_extend.loan_id = r_extend.loan_id.astype(str)
r_extend_f = r_extend.copy()

r_extend_f.to_csv(path_rawdata + 'r_extend_f.csv', index = False)
r_extend_f = pd.read_csv(path_rawdata + 'r_extend_f.csv', dtype={'loan_id': str})

''' **************************** GPS **************************** '''
sql_gps = '''
select loan_id, effective_date, jailbreak_status, simulator_status, 
latitude, longitude, altitude, speed, accuracy
from (
select a.id as loan_id, a.customer_id, a.apply_time, a.effective_date, b.*,
row_number() over(partition by a.id order by b.create_time desc) as rn 
from dw_gocash_go_cash_loan_gocash_core_loan a 
left join ( 
	select customer_id, create_time, jailbreak_status, simulator_status, 
	gps::json #>> '{latitude}' as latitude, gps::json #>> '{longitude}' as longitude, 
	gps::json #>> '{direction}' as direction, gps::json #>> '{speed}' as speed, gps::json #>> '{altitude}' as altitude, gps::json #>> '{accuracy}' as accuracy
	from gocash_loan_risk_program_baseinfo 
	where gps <>'' ) b on a.customer_id::varchar = b.customer_id and a.apply_time >= b.create_time
where a.return_flag = 'false' and a.effective_date between '2019-05-01' and '2019-12-08'
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
r_gps.simulator_status = r_gps.simulator_status.astype(str)
r_gps.latitude = r_gps.latitude.astype(str)
r_gps.longitude = r_gps.longitude.astype(str)
r_gps.speed = r_gps.speed.astype(float)
r_gps.altitude = r_gps.altitude.astype(float)
r_gps.accuracy = r_gps.accuracy.astype(float)

r_gps = r_gps.replace(['false', 'False', 'true', 'True'],[0, 0, 1, 1])
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

r_gps_f.to_csv(path_rawdata + 'r_gps_f.csv', index = False)

r_gps_f = pd.read_csv(path_rawdata + 'r_gps_f.csv', dtype={'loan_id': str})

'''检查分布 '''
var = list(r_gps_f.iloc[:,2:].columns)
test = (r_gps_f.groupby('effective_date')[var].agg(['mean','min', 'median', 'max'])).T
test2 = (r_gps_f.groupby('effective_date')[var].quantile([0, 0.25, 0.5, 0.75, 0.95, 1])).T

''' 检查缺失比例'''
check = r_gps_f.copy()

check = check.fillna(-1)
check = check.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])

var = list(check.iloc[:,2:].columns)
check_missingpct = pd.DataFrame(check.groupby(['effective_date']).size(), columns=['total'])
for x in var:
    check[x] = check[x].mask(check[x].ne(-1))
    check_missing = (check.groupby(['effective_date'])[x].count())
    check_total = (check.groupby(['effective_date']).size())
    check_missingpct = check_missingpct.merge(pd.DataFrame(check_missing/check_total, columns=[x]), how='left',left_index=True, right_index=True)

check_missingpct.to_csv(path + 'check_missing_gps.csv')
test2.to_csv(path + 'test2.csv')
test.to_csv(path + 'test.csv')



''' **************************** DEVICE **************************** '''
sql_device = '''
select loan_id, effective_date, brand, model, manufacturer, version, heightpixels, widthpixels
from (
select a.id as loan_id, a.customer_id, a.apply_time, a.effective_date, b.*,
row_number() over(partition by a.id order by b.create_time desc) as rn 
from dw_gocash_go_cash_loan_gocash_core_loan a 
left join ( 
	select customer_id, create_time, 
    device_info::json #>> '{brand}' as brand, device_info::json #>> '{Model}' as model,
    device_info::json #>> '{manufacturer}' as manufacturer, device_info::json #>> '{version}' as version, 
    device_info::json #>> '{heightPixels}' as heightpixels, device_info::json #>> '{widthPixels}' as widthpixels
	from gocash_loan_risk_program_baseinfo 
	where device_info <>'' ) b on a.customer_id::varchar = b.customer_id and a.apply_time >= b.create_time
where a.return_flag = 'false' and a.effective_date between '2019-05-01' and '2019-12-08'
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
r_device.version = r_device.version.astype(str)
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

# CREATE DUMMY VARIABLES FOR version
version_size = pd.DataFrame(r_device.groupby('version').size())
version_size = version_size.reset_index().rename(columns={'index': 'version', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
version_size['cum_sum'] = version_size['ct'].cumsum()
version_size['cum_perc'] = 100 * version_size['cum_sum']/version_size['ct'].sum()
version_list = list(version_size.version[version_size.cum_perc <= 100])
version_dummy = pd.get_dummies(r_device['version'])[version_list]
version_dummy.columns = ['version_' + str(col) for col in version_dummy.columns]


r_device_f = r_device.join(screen_dummy).join(brand_dummy).join(model_dummy).join(manufacturer_dummy).join(version_dummy)

r_device_f = r_device_f.drop(['heightpixels', 'widthpixels', 'brand', 'model', 'manufacturer', 'version'], axis = 1)

r_device_f.to_csv(path_rawdata + 'r_device_f.csv', index=False)

r_device_f = pd.read_csv(path_rawdata + 'r_device_f.csv', dtype={'loan_id': str})

#'''检查分布 '''
#var = list(r_device_f.iloc[:,2:].columns)
#test = (r_device_f.groupby('effective_date')[var].agg(['mean','min', 'median', 'max'])).T
#test2 = (r_device_f.groupby('effective_date')[var].quantile([0, 0.25, 0.5, 0.75, 0.95, 1])).T

''' 检查缺失比例'''
check = r_device.copy()

check = check.fillna(-1)
check = check.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])

var = list(check.iloc[:,2:].columns)
check_missingpct = pd.DataFrame(check.groupby(['effective_date']).size(), columns=['total'])
for x in var:
    check[x] = check[x].mask(check[x].ne(-1))
    check_missing = (check.groupby(['effective_date'])[x].count())
    check_total = (check.groupby(['effective_date']).size())
    check_missingpct = check_missingpct.merge(pd.DataFrame(check_missing/check_total, columns=[x]), how='left',left_index=True, right_index=True)

check_missingpct.to_csv(path + 'check_missing_device.csv')

test2.to_csv(path + 'test2.csv')
test.to_csv(path + 'test.csv')


''' **************************** borrowing_purposes **************************** '''
sql_borrow = '''
SELECT loan_id, effective_date, 
case when upper(borrowing_purposes) like '%KEPERLUAN_SEHARI-HARI%' then 'KEPERLUAN_SEHARI-HARI'
when upper(borrowing_purposes) like '%EDUKASI%' then 'EDUKASI'
when upper(borrowing_purposes) like '%DEKORASI%' then 'DEKORASI'
when upper(borrowing_purposes) like '%MODAL USAHA%' then 'MODAL USAHA'
when upper(borrowing_purposes) like '%HIBURAN%' then 'HIBURAN' else 'OTHER' end as borrowing_purposes
FROM (SELECT loan.id as loan_id
            , loan.apply_time
            , loan.effective_date
            , baseinfo.*
            , row_number() over(partition by loan.customer_id order by baseinfo.update_time desc) as rn
      FROM dw_gocash_go_cash_loan_gocash_core_loan loan
      LEFT JOIN dw_gocash_go_cash_loan_gocash_core_customer baseinfo  ON loan.customer_id = baseinfo.id
      WHERE loan.apply_time >= baseinfo.update_time and loan.return_flag = 'false' and loan.effective_date between '2019-05-01' and '2019-12-08'
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_borrow)
r_borrow = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_borrow.shape)
print(r_borrow.loan_id.nunique())
print(r_borrow.dtypes)

r_borrow.loan_id = r_borrow.loan_id.astype(str)

r_borrow_bk = r_borrow.copy()
r_borrow = r_borrow_bk.copy()

# CREATE DUMMY VARIABLES FOR borrowing_purposes
borrowing_purposes_size = pd.DataFrame(r_borrow.groupby('borrowing_purposes').size())
borrowing_purposes_size = borrowing_purposes_size.reset_index().rename(columns={'index': 'borrowing_purposes', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
borrowing_purposes_size['cum_sum'] = borrowing_purposes_size['ct'].cumsum()
borrowing_purposes_size['cum_perc'] = 100 * borrowing_purposes_size['cum_sum']/borrowing_purposes_size['ct'].sum()
borrowing_purposes_list = list(borrowing_purposes_size.borrowing_purposes[borrowing_purposes_size.cum_perc <= 100])
borrowing_purposes_dummy = pd.get_dummies(r_borrow['borrowing_purposes'])[borrowing_purposes_list]
borrowing_purposes_dummy.columns = ['borrowing_purposes_' + str(col) for col in borrowing_purposes_dummy.columns]

r_borrow_f = r_borrow.join(borrowing_purposes_dummy).drop(['borrowing_purposes', 'effective_date'], axis=1)

r_borrow_f.to_csv(path_rawdata + 'r_borrow_f.csv')

r_borrow_f = pd.read_csv(path_rawdata + 'r_borrow_f.csv', dtype={'loan_id': str})
r_borrow_f = r_borrow_f.drop('Unnamed: 0', axis=1)


''' **************************** LINKAJA DATA **************************** '''
r_data = load_data_from_pickle(path, 'all_x_20191203_v2.pkl')

r_data.loan_id = r_data.loan_id.astype(str)

#r_data['sample_set'] = pd.to_datetime(r_data['effective_date']).apply(lambda x: '1' if x <= pd.to_datetime('2019-06-26') else 
#    ('train' if x <= pd.to_datetime('2019-07-31') else 
#        ('test' if x <= pd.to_datetime('2019-08-12') else 
#         ('2' if x <= pd.to_datetime('2019-08-22') else 
#          ('test' if x <= pd.to_datetime('2019-08-28') else 
#           ('3' if x <= pd.to_datetime('2019-08-29') else 
#            ('oot' if x <= pd.to_datetime('2019-11-06') else '4')))))))

r_data['sample_set'] = pd.to_datetime(r_data['effective_date']).apply(lambda x: '1' if x <= pd.to_datetime('2019-09-16') else 
    ('train' if x <= pd.to_datetime('2019-10-15') else 
        ('test' if x <= pd.to_datetime('2019-10-28') else 
         ('oot' if x <= pd.to_datetime('2019-11-06') else 2))))

r_use = r_data[(r_data.sample_set == 'oot') | (r_data.sample_set == 'train') | (r_data.sample_set == 'test')].reset_index(drop=True)

r_use.result = r_use.result.replace('MATCH', 1).replace('NOT_MATCH', 0).astype(float)
r_use.whatsapp = r_use.whatsapp.replace('yes', 1).replace('no', 0).astype(float)

# CREATE DUMMY VARIABLES FOR marital
marital_size = pd.DataFrame(r_use.groupby('marital_status').size())
marital_size = marital_size.reset_index().rename(columns={'index': 'marital', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
marital_size['cum_sum'] = marital_size['ct'].cumsum()
marital_size['cum_perc'] = 100 * marital_size['cum_sum']/marital_size['ct'].sum()
marital_list = list(marital_size.marital_status[marital_size.cum_perc <= 100])
marital_dummy = pd.get_dummies(r_use['marital_status'])[marital_list]
marital_dummy.columns = ['marital_' + str(col) for col in marital_dummy.columns]

# CREATE DUMMY VARIABLES FOR religion
religion_size = pd.DataFrame(r_use.groupby('religion').size())
religion_size = religion_size.reset_index().rename(columns={'index': 'religion', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
religion_size['cum_sum'] = religion_size['ct'].cumsum()
religion_size['cum_perc'] = 100 * religion_size['cum_sum']/religion_size['ct'].sum()
religion_list = list(religion_size.religion[religion_size.cum_perc <= 100])
religion_dummy = pd.get_dummies(r_use['religion'])[religion_list]
religion_dummy.columns = ['religion_' + str(col) for col in religion_dummy.columns]

# CREATE DUMMY VARIABLES FOR education
education_size = pd.DataFrame(r_use.groupby('education').size())
education_size = education_size.reset_index().rename(columns={'index': 'education', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
education_size['cum_sum'] = education_size['ct'].cumsum()
education_size['cum_perc'] = 100 * education_size['cum_sum']/education_size['ct'].sum()
education_list = list(education_size.education[education_size.cum_perc <= 100])
education_dummy = pd.get_dummies(r_use['education'])[education_list]
education_dummy.columns = ['education_' + str(col) for col in education_dummy.columns]

# CREATE DUMMY VARIABLES FOR mail
mail_size = pd.DataFrame(r_use.groupby('mail_address').size())
mail_size = mail_size.reset_index().rename(columns={'index': 'mail_address', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
mail_size['cum_sum'] = mail_size['ct'].cumsum()
mail_size['cum_perc'] = 100 * mail_size['cum_sum']/mail_size['ct'].sum()
mail_list = list(mail_size.mail_address[mail_size.cum_perc <= 100])
mail_dummy = pd.get_dummies(r_use['mail_address'])[mail_list]
mail_dummy.columns = ['mail_' + str(col) for col in mail_dummy.columns]

# CREATE DUMMY VARIABLES FOR provincecode
provincecode_size = pd.DataFrame(r_use.groupby('provincecode').size())
provincecode_size = provincecode_size.reset_index().rename(columns={'index': 'provincecode', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
provincecode_size['cum_sum'] = provincecode_size['ct'].cumsum()
provincecode_size['cum_perc'] = 100 * provincecode_size['cum_sum']/provincecode_size['ct'].sum()
provincecode_list = list(provincecode_size.provincecode[provincecode_size.cum_perc <= 95])
provincecode_dummy = pd.get_dummies(r_use['provincecode'])[provincecode_list]
provincecode_dummy.columns = ['provincecode_' + str(col) for col in provincecode_dummy.columns]

# CREATE DUMMY VARIABLES FOR citycode
citycode_size = pd.DataFrame(r_use.groupby('citycode').size())
citycode_size = citycode_size.reset_index().rename(columns={'index': 'citycode', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
citycode_size['cum_sum'] = citycode_size['ct'].cumsum()
citycode_size['cum_perc'] = 100 * citycode_size['cum_sum']/citycode_size['ct'].sum()
citycode_list = list(citycode_size.citycode[citycode_size.cum_perc <= 100])
citycode_dummy = pd.get_dummies(r_use['citycode'])[citycode_list]
citycode_dummy.columns = ['citycode_' + str(col) for col in citycode_dummy.columns]

# CREATE DUMMY VARIABLES FOR districtcode
districtcode_size = pd.DataFrame(r_use.groupby('districtcode').size())
districtcode_size = districtcode_size.reset_index().rename(columns={'index': 'districtcode', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
districtcode_size['cum_sum'] = districtcode_size['ct'].cumsum()
districtcode_size['cum_perc'] = 100 * districtcode_size['cum_sum']/districtcode_size['ct'].sum()
districtcode_list = list(districtcode_size.districtcode[districtcode_size.cum_perc <= 95])
districtcode_dummy = pd.get_dummies(r_use['districtcode'])[districtcode_list]
districtcode_dummy.columns = ['districtcode_' + str(col) for col in districtcode_dummy.columns]

# CREATE DUMMY VARIABLES FOR bankcode
bankcode_size = pd.DataFrame(r_use.groupby('bank_code').size())
bankcode_size = bankcode_size.reset_index().rename(columns={'index': 'bankcode', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
bankcode_size['cum_sum'] = bankcode_size['ct'].cumsum()
bankcode_size['cum_perc'] = 100 * bankcode_size['cum_sum']/bankcode_size['ct'].sum()
bankcode_list = list(bankcode_size.bank_code[bankcode_size.cum_perc <= 100])
bankcode_dummy = pd.get_dummies(r_use['bank_code'])[bankcode_list]
bankcode_dummy.columns = ['bankcode_' + str(col) for col in bankcode_dummy.columns]

# CREATE DUMMY VARIABLES FOR occupation_type
occupation_type_size = pd.DataFrame(r_use.groupby('occupation_type').size())
occupation_type_size = occupation_type_size.reset_index().rename(columns={'index': 'occupation_type', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
occupation_type_size['cum_sum'] = occupation_type_size['ct'].cumsum()
occupation_type_size['cum_perc'] = 100 * occupation_type_size['cum_sum']/occupation_type_size['ct'].sum()
occupation_type_list = list(occupation_type_size.occupation_type[occupation_type_size.cum_perc <= 100])
occupation_type_dummy = pd.get_dummies(r_use['occupation_type'])[occupation_type_list]
occupation_type_dummy.columns = ['occupation_type_' + str(col) for col in occupation_type_dummy.columns]

# CREATE DUMMY VARIABLES FOR pre_work_industry
pre_work_industry_size = pd.DataFrame(r_use.groupby('pre_work_industry').size())
pre_work_industry_size = pre_work_industry_size.reset_index().rename(columns={'index': 'pre_work_industry', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
pre_work_industry_size['cum_sum'] = pre_work_industry_size['ct'].cumsum()
pre_work_industry_size['cum_perc'] = 100 * pre_work_industry_size['cum_sum']/pre_work_industry_size['ct'].sum()
pre_work_industry_list = list(pre_work_industry_size.pre_work_industry[pre_work_industry_size.cum_perc <= 100])
pre_work_industry_dummy = pd.get_dummies(r_use['pre_work_industry'])[pre_work_industry_list]
pre_work_industry_dummy.columns = ['pre_work_industry_' + str(col) for col in pre_work_industry_dummy.columns]

# CREATE DUMMY VARIABLES FOR industry_involved
industry_involved_size = pd.DataFrame(r_use.groupby('industry_involved').size())
industry_involved_size = industry_involved_size.reset_index().rename(columns={'index': 'industry_involved', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
industry_involved_size['cum_sum'] = industry_involved_size['ct'].cumsum()
industry_involved_size['cum_perc'] = 100 * industry_involved_size['cum_sum']/industry_involved_size['ct'].sum()
industry_involved_list = list(industry_involved_size.industry_involved[industry_involved_size.cum_perc <= 100])
industry_involved_dummy = pd.get_dummies(r_use['industry_involved'])[industry_involved_list]
industry_involved_dummy.columns = ['industry_involved_' + str(col) for col in industry_involved_dummy.columns]

# CREATE DUMMY VARIABLES FOR job
job_size = pd.DataFrame(r_use.groupby('job').size())
job_size = job_size.reset_index().rename(columns={'index': 'job', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
job_size['cum_sum'] = job_size['ct'].cumsum()
job_size['cum_perc'] = 100 * job_size['cum_sum']/job_size['ct'].sum()
job_list = list(job_size.job[job_size.cum_perc <= 100])
job_dummy = pd.get_dummies(r_use['job'])[job_list]
job_dummy.columns = ['job_' + str(col) for col in job_dummy.columns]

r_use_f = r_use.join(marital_dummy).join(religion_dummy).join(education_dummy).join(mail_dummy).\
    join(provincecode_dummy).join(citycode_dummy).join(districtcode_dummy).join(bankcode_dummy).\
    join(occupation_type_dummy).join(pre_work_industry_dummy).join(industry_involved_dummy).join(job_dummy)
r_use_f = r_use_f.drop([
        'apply_time', 'effective_date', 'customer_id', 'cell_phone',
       'mail', 'marital_status', 'religion', 'education', 'mail_address',
       'provincecode', 'citycode', 'districtcode', 'bank_code', 'job', 'industry_involved',
       'pre_work_industry', 'occupation_type'], axis=1)
    
r_use_f.to_csv(path_rawdata + 'r_use_f2.csv', index = False)

r_use_f = pd.read_csv(path_rawdata + 'r_use_f.csv', dtype={'loan_id': str})

''' **************************** ANTIFRAUD RULE **************************** '''
#r_rule = pd.read_excel('D:/Model Development/201911 IDN Anti-fraud/Graph Analysis/03 Result/规则合集详情.xlsx', 
#                       sheet_name = 'Dedup', dtype={'order_no': str})
#
#r_rule_f = r_rule.rename(columns={'order_no': 'loan_id'})[['loan_id', 'R1_FLAG', 'R2_FLAG', 'R3_FLAG', 'R4_FLAG', 'R5_FLAG',
#       'R6_FLAG', 'R7_FLAG', 'R8_FLAG', 'R9_FLAG']]
#print(r_rule_f.dtypes)

''' **************************** APP **************************** '''
#r_app = pd.read_csv(path_rawdata + 'var_app_freq_w_tfidf_0627_1106.csv', dtype={'loan_id': str})

r_app = pd.read_csv(path_rawdata + 'var_app_freq_w_tfidf_0917_1106.csv', dtype={'loan_id': str})

r_app_f = r_app.copy()

var_app = list(set(list(r_app_f.columns)) - set(['loan_id']))
print(len(var_app))

print(r_app_f.shape)


''' MERGE ALL '''
r_all = r_use_f.merge(r_flag[['loan_id', 'effective_date', 'flag7']], how='left', on='loan_id')
r_all = r_all.merge(r_device_f.drop(['effective_date', 'screen'], axis=1), how='left', on='loan_id')
r_all = r_all.merge(r_borrow_f, how='left', on='loan_id')
r_all = r_all.merge(r_app_f, how='left', on='loan_id')
#r_all = r_all.merge(r_rule_f, how='left', on='loan_id')

r_all.to_csv(path_rawdata + 'r_all2.csv', index = False)

r_all = pd.read_csv(path_rawdata + 'r_all2.csv', dtype={'loan_id': str})

print(r_all.shape)



''' CHECK MISSING '''
check = r_all.copy()

check = check.fillna(-1)
check = check.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])

var = list(check.iloc[:,3:].columns).remove('sample_set')

var = list(check.columns)
var = [x for x in var if x not in {'loan_id', 'flag7', 'effective_date', 'sample_set'}]

check_missingpct = pd.DataFrame(check.groupby(['sample_set']).size(), columns=['total'])
for x in var:
    check[x] = check[x].mask(check[x].ne(-1))
    check_missing = (check.groupby(['sample_set'])[x].count())
    check_total = (check.groupby(['sample_set']).size())
    check_missingpct = check_missingpct.merge(pd.DataFrame(check_missing/check_total, columns=[x]), how='left',left_index=True, right_index=True)

check_missingpct.to_csv(path + 'check_missing_all.csv')




