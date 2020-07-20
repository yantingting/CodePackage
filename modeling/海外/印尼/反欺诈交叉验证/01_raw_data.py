# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 15:18:24 2019

@author: yuexin
"""

import os
import time
import pandas as pd
import numpy as np
import psycopg2

work_dir = 'D:/Model Development/201911 IDN Anti-fraud/Graph Analysis/02 Data/raw data/'

usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)

''' **************************** flag **************************** '''
sql_flag = '''
select id as loan_id, customer_id,effective_date,paid_off_time,due_date, current_date::date as curr_date, loan_status, extend_times,
case when extend_times>3 then 0 
when paid_off_time::Date-due_date>30 then 1 
when loan_status='COLLECTION' and current_date::Date-due_date<=30 then -3 
when loan_status='COLLECTION' and current_date::Date-due_date>30 then 1
when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
else 0 end as flag30,
case when extend_times>3 then 0 
when paid_off_time::Date-due_date>7 then 1 
when loan_status='COLLECTION' and current_date::Date-due_date<=7 then -3 
when loan_status='COLLECTION' and current_date::Date-due_date>7 then 1
when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
else 0 end as flag7,
case when extend_times>3 then 0 
when paid_off_time::Date-due_date>3 then 1 
when loan_status='COLLECTION' and current_date::Date-due_date<=3 then -3 
when loan_status='COLLECTION' and current_date::Date-due_date>3 then 1
when extend_times<=3 and extend_times>0 and loan_status='FUNDED' then -2
when current_date-effective_date < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
else 0 end as flag3
from dw_gocash_go_cash_loan_gocash_core_loan
where effective_date!='1970-01-01' and return_flag='false' and effective_date between '2019-02-01' and '2019-10-10' 
'''

'''
select a.id as loan_id, a.customer_id, effective_date, return_flag, extend_times
, case when extend_times=0 and loan_status='COLLECTION' and due_date <= '2019-10-20' then 1 
	   when extend_times in (1,2) and loan_status='COLLECTION' and due_date <= '2019-10-20' then 9 else 0 end as flag
from dw_gocash_go_cash_loan_gocash_core_loan a
where effective_date between '2019-02-01' and '2019-10-05' 
'''

conn.rollback()
cur = conn.cursor()
cur.execute(sql_flag)
r_flag = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_flag.shape)
print(r_flag.dtypes)

r_flag.loan_id = r_flag.loan_id.astype(str)
r_flag.customer_id = r_flag.customer_id.astype(str)

print(r_flag[pd.to_datetime(r_flag.effective_date)>=pd.to_datetime('2019-10-01')].groupby('flag30').size())
print(r_flag[pd.to_datetime(r_flag.effective_date)>=pd.to_datetime('2019-10-01')].groupby('flag7').size())
print(r_flag[pd.to_datetime(r_flag.effective_date)>=pd.to_datetime('2019-10-01')].groupby('flag3').size())

r_flag.to_csv(work_dir + 'r_flag.csv', index = False)
r_flag = pd.read_csv(work_dir + 'r_flag.csv', dtype = {'loan_id': str, 'customer_id': str})


''' **************************** base **************************** '''
sql_base = '''
select a.id as loan_id, a.customer_id, apply_time, effective_date, return_flag
from dw_gocash_go_cash_loan_gocash_core_loan a
where date(a.apply_time) <= '2019-11-18' 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_base)
r_base = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])

r_base.loan_id = r_base.loan_id.astype(str)
r_base.customer_id = r_base.customer_id.astype(str)

print(r_base.shape)
print(r_base.dtypes)

r_base.to_csv(work_dir + 'r_base.csv', index = False)
r_base = pd.read_csv(work_dir + 'r_base.csv', dtype = {'loan_id': str, 'customer_id': str})



''' **************************** profession **************************** '''
sql_profession = """
select id as loan_id
,case when occupation_type in ('ENTREPRENEUR', 'OFFICE') then company_name end as company_name
,case when occupation_type in ('ENTREPRENEUR', 'OFFICE') then company_phone end as company_phone
,case when occupation_type in ('ENTREPRENEUR', 'OFFICE') then company_address end as company_address
,case when occupation_type in ('ENTREPRENEUR', 'OFFICE') then company_area end as company_area
from (
	select a.id, a.apply_time, a.effective_date, b.*, row_number() over(partition by a.id order by b.update_time desc) as rn
	from dw_gocash_go_cash_loan_gocash_core_loan a
	left join (
	select customer_id, update_time, occupation_type, company_name, company_phone, company_address, company_area from dw_gocash_go_cash_loan_gocash_core_cusomer_profession
	union all 
	select customer_id, update_time, occupation_type, company_name, company_phone, company_address, company_area from dw_gocash_go_cash_loan_gocash_core_cusomer_profession_history
	) b on a.customer_id = b.customer_id and a.apply_time >= b.update_time
	where date(a.apply_time) <= '2019-11-18' 
) t 
where rn=1
"""
conn.rollback()
cur = conn.cursor()
cur.execute(sql_profession)
r_profession = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
print(r_profession.shape)

r_profession.loan_id = r_profession.loan_id.astype(str)
r_profession.company_name = r_profession.company_name.astype(str)
r_profession.company_phone = r_profession.company_phone.astype(str)
r_profession.company_address = r_profession.company_address.astype(str)
#r_profession_bk = r_profession.copy()
#r_profession = r_profession_bk.copy()

r_profession.company_name = r_profession.company_name.str.strip()
r_profession.company_address = r_profession.company_address.str.strip()
r_profession.company_phone = r_refer.refer_phone.str.replace(' ','')

r_profession['company_name'].replace(['', np.nan], [None, None], inplace=True)
r_profession['company_phone'].replace(['', np.nan], [None, None], inplace=True)
r_profession['company_area'].replace(['', np.nan], [None, None], inplace=True)
r_profession['company_address'].replace(['', np.nan], [None, None], inplace=True)

r_profession.company_name = r_profession.company_name.str.upper()
r_profession.company_area = r_profession.company_area.str.upper()
r_profession.company_address = r_profession.company_address.str.upper()

r_profession.loc[r_profession.company_name.str.len() <= 2, 'company_name'] = None
r_profession.loc[r_profession.company_address.str.len() <= 5, 'company_address'] = None

rm_list = ['00000000','000000000','0000000000','00000000000','000000000000',
           '11111111','111111111','1111111111','11111111111','111111111111',
           '22222222','222222222','2222222222','22222222222','222222222222',
           '33333333','333333333','3333333333','33333333333','333333333333',
           '44444444','444444444','4444444444','44444444444','444444444444',
           '55555555','555555555','5555555555','55555555555','555555555555',
           '66666666','666666666','6666666666','66666666666','666666666666',
           '77777777','777777777','7777777777','77777777777','777777777777',
           '88888888','888888888','8888888888','88888888888','888888888888',
           '99999999','999999999','9999999999','99999999999','999999999999']
r_profession.loc[r_profession.company_phone.isin(rm_list), 'company_phone'] = None

r_profession.loc[~r_profession.company_phone.astype(str).str.isdigit(), 'company_phone'] = None  # remove non numeric company_phone

r_profession['company_fulladdr'] = r_profession['company_address'] + r_profession['company_area']
r_profession['company_fulladdr'].replace([np.nan], [None], inplace=True) # if addr or area is None, then None

r_profession['company_dist1'] = r_profession['company_area'].str.split(',').str[-1]
r_profession['company_dist2'] = r_profession['company_area'].str.split(',').str[-2]
r_profession['company_dist3'] = r_profession['company_area'].str.split(',').str[-3]

r_profession.to_csv(work_dir + 'r_profession.csv', index = False)
r_profession = pd.read_csv(work_dir + 'r_profession.csv', dtype = str)


''' 数据检查
check_profession = r_profession.copy()

check_profession['len_name'] = r_profession.company_name.str.len()
check_profession['len_phone'] = r_profession.company_phone.str.len()
check_profession['len_address'] = r_profession.company_address.str.len()
check_profession['len_area'] = r_profession.company_area.str.len()

check_profession.groupby('len_phone').size()
test = check_profession[check_profession['len_phone']==13]

check_profession['ct_area'] = r_profession.company_area.str.count(',')

print(sum(r_profession.company_name.isnull()))
print(sum(r_profession.company_phone.isnull()))
print(sum(r_profession.company_address.isnull()))
print(sum(r_profession.company_area.isnull()))

# 缺失率1.7%

check_profession.groupby('company_phone').size()
test = check_profession[check_profession['len_name']>=100]

check_profession.groupby('len_phone').size()
test = check_profession[check_profession['len_phone']<=8]

check_profession.groupby('len_address').size()
test = check_profession[check_profession['len_address']>200]

check_profession.groupby('ct_area').size()
test = check_profession[check_profession['ct_area']==3]
'''


''' **************************** customer_history **************************** '''
sql_cust = '''
select id as loan_id
,cell_phone, mail, id_card_no, id_card_name, id_card_address
from (
	select a.id, a.apply_time, a.effective_date, b.*, row_number() over(partition by a.id order by b.update_time desc) as rn
	from dw_gocash_go_cash_loan_gocash_core_loan a
	left join (
	select id as customer_id, update_time, cell_phone, mail, id_card_no, id_card_name, id_card_address from dw_gocash_go_cash_loan_gocash_core_customer where idcard_pass_flag='true'
	union
	select customer_id, update_time, cell_phone, mail, md5(id_card_no) as id_card_no, id_card_name, id_card_address from dw_gocash_go_cash_loan_gocash_core_customer_history 
	) b on a.customer_id = b.customer_id and a.apply_time >= b.update_time
	where date(a.apply_time) <= '2019-11-18' 
) t 
where rn=1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_cust)
r_cust = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )

r_cust.loan_id = r_cust.loan_id.astype(str)
r_cust.cell_phone = r_cust.cell_phone.astype(str)
r_cust.mail = r_cust.mail.astype(str)
r_cust.id_card_name = r_cust.id_card_name.astype(str)
r_cust.id_card_address = r_cust.id_card_address.astype(str)

#r_cust_bk = r_cust.copy()
#r_cust = r_cust_bk.copy()

r_cust.cell_phone = r_cust.cell_phone.str.replace(' ','')
r_cust.mail = r_cust.mail.str.strip()
r_cust.id_card_name = r_cust.id_card_name.str.strip()
r_cust.id_card_address = r_cust.id_card_address.str.strip()

r_cust['cell_phone'].replace(['', np.nan], [None, None], inplace=True)
r_cust['mail'].replace(['', np.nan], [None, None], inplace=True)
r_cust['id_card_no'].replace(['', np.nan], [None, None], inplace=True)
r_cust['id_card_name'].replace(['', np.nan], [None, None], inplace=True)
r_cust['id_card_address'].replace(['', np.nan], [None, None], inplace=True)

r_cust.mail = r_cust.mail.str.upper()
r_cust.id_card_name = r_cust.id_card_name.str.upper()
r_cust.id_card_address = r_cust.id_card_address.str.upper()

r_cust.loc[r_cust.mail.str.len() <= 12, 'mail'] = None
r_cust.loc[r_cust.id_card_name.str.len() <= 2, 'id_card_name'] = None

r_cust['id_card_dist1'] = r_cust['id_card_address'].str.split(',').str[-1]
r_cust['id_card_dist2'] = r_cust['id_card_address'].str.split(',').str[-2]
r_cust['id_card_dist3'] = r_cust['id_card_address'].str.split(',').str[-3]


r_cust.to_csv(work_dir + 'r_cust.csv', index = False)
r_cust = pd.read_csv(work_dir + 'r_cust.csv', dtype = str)

'''
check_cust = r_cust.copy()

check_cust['len_cell'] = r_cust.cell_phone.str.len()
check_cust['len_mail'] = r_cust.mail.str.len()
check_cust['len_idno'] = r_cust.id_card_no.str.len()
check_cust['len_idname'] = r_cust.id_card_name.str.len()
check_cust['len_idaddr'] = r_cust.id_card_address.str.len()

check_cust.groupby('len_cell').size()
test = check_cust[check_cust['len_cell']>=100]

check_cust.groupby('len_mail').size()
test = check_cust[check_cust['len_mail']==12]

check_cust.groupby('len_idno').size()

check_cust.groupby('len_idname').size()
test = check_cust.id_card_name[check_cust['len_idname']==3]

check_cust.groupby('len_idaddr').size()
test = check_cust[check_cust['len_idaddr']==0]

print(sum(r_cust.cell_phone.isnull()))  # less than 1% missing
print(sum(r_cust.mail.isnull()))
print(sum(r_cust.id_card_no.isnull()))
print(sum(r_cust.id_card_name.isnull()))
print(sum(r_cust.id_card_address.isnull()))

set(r_cust['id_card_dist1'])
test2 = (r_cust.groupby('id_card_dist1').size())

'''


''' **************************** refer **************************** '''
sql_refer = '''
select tt.id as loan_id, tt.refer_name, tt.refer_phone, tt.refer_type
from (
	select t1.id, t1.apply_time, t1.customer_id, t1.effective_date, t1.loan_status, t1.risk_rank, t1.return_flag, 
	t2.refer_name, t2.refer_phone, t2.refer_type, t2.refer_status, t2.append_flag, t2.create_time refer_create_time, t2.update_time refer_update_time,
	dense_rank() over(partition by t1.id order by t2.create_time desc) as rn
	from dw_gocash_go_cash_loan_gocash_core_loan t1
	left join (select * from dw_gocash_go_cash_loan_gocash_core_customer_refer where create_time != '1970-01-01' and refer_type!='SELF') t2 on t1.customer_id = t2.customer_id and t1.apply_time >= t2.create_time
	where date(t1.apply_time) <= '2019-11-18' ) tt
where rn=1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_refer)
r_refer = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
print(r_refer.shape)

r_refer.loan_id = r_refer.loan_id.astype(str)
r_refer.refer_name = r_refer.refer_name.astype(str)
r_refer.refer_phone = r_refer.refer_phone.astype(str)
r_refer.refer_type = r_refer.refer_type.astype(str)

#r_refer_bk = r_refer.copy()
#r_refer = r_refer_bk.copy()

r_refer.refer_name = r_refer.refer_name.str.strip()
r_refer.refer_type = r_refer.refer_type.str.strip()
r_refer.refer_phone = r_refer.refer_phone.str.replace(' ','')
r_refer.refer_phone = r_refer.refer_phone.str.replace('-','')
r_refer.refer_phone = r_refer.refer_phone.str.replace(',','')
r_refer.refer_phone = r_refer.refer_phone.str.replace('.','')
r_refer.refer_phone = r_refer.refer_phone.str.replace('*','')
r_refer.refer_phone = r_refer.refer_phone.str.replace('#','')
r_refer.refer_phone = r_refer.refer_phone.str.replace('?','')
r_refer.refer_phone = r_refer.refer_phone.str.replace(r"\\n","")
r_refer.refer_phone = r_refer.refer_phone.str.replace('/','')
r_refer.refer_phone = r_refer.refer_phone.str.replace('62','0')  # remove country area code
r_refer['refer_phone'].replace(['85621887', '8501887'], ['085621887', '08501887',], inplace=True)

r_refer.refer_name = r_refer.refer_name.str.upper()

r_refer['refer_name'].replace(['', np.nan], [None, None], inplace=True)
r_refer['refer_phone'].replace(['', np.nan], [None, None], inplace=True)
r_refer['refer_type'].replace(['', np.nan], [None, None], inplace=True)

rm_list = ['00000000','000000000','0000000000','00000000000','000000000000',
           '11111111','111111111','1111111111','11111111111','111111111111',
           '22222222','222222222','2222222222','22222222222','222222222222',
           '33333333','333333333','3333333333','33333333333','333333333333',
           '44444444','444444444','4444444444','44444444444','444444444444',
           '55555555','555555555','5555555555','55555555555','555555555555',
           '66666666','666666666','6666666666','66666666666','666666666666',
           '77777777','777777777','7777777777','77777777777','777777777777',
           '88888888','888888888','8888888888','88888888888','888888888888',
           '99999999','999999999','9999999999','99999999999','999999999999']
r_refer.loc[r_refer.refer_phone.isin(rm_list), 'refer_phone'] = None

r_refer.loc[~r_refer.refer_phone.astype(str).str.isdigit(), 'refer_phone'] = None  # remove non numeric company_phone

r_refer.loc[r_refer.refer_name.str.len() <= 2, 'refer_name'] = None
r_refer.loc[r_refer.refer_phone.str.len() <= 3, 'refer_phone'] = None

r_refer['refer_namephone'] = r_refer['refer_name'] + r_refer['refer_phone']
r_refer['refer_namephone'].replace(['', np.nan], [None, None], inplace=True)

r_refer.to_csv(work_dir + 'r_refer.csv', index = False)
r_refer = pd.read_csv(work_dir + 'r_refer.csv', dtype = str)

''' 数据检查
print(sum(r_refer.refer_name.isnull())) # 15
print(sum(r_refer.refer_phone.isnull())) # 428
print(sum(r_refer.refer_type.isnull()))   # 6 missing
print(sum(r_refer.refer_namephone.isnull()))

check_refer = r_refer.copy()

check_refer['len_name'] = check_refer.refer_name.str.len()
check_refer['len_phone'] = check_refer.refer_phone.str.len()

print(check_refer.groupby('len_name').size())
print(check_refer.groupby('len_phone').size())

test = check_refer[check_refer.len_name==4]
check_refer.groupby('refer_type').size()
'''


''' **************************** gps **************************** '''
sql_gps = '''
select loan_id, latitude, longitude 
from (
select a.id as loan_id, a.apply_time, gps::json ->> 'latitude' as latitude, gps::json ->> 'longitude'  as longitude, 
row_number() over(partition by a.id order by c.create_time desc) as rn 
from dw_gocash_go_cash_loan_gocash_core_loan a 
left join gocash_loan_risk_program_baseinfo c on a.customer_id::varchar = c.customer_id and a.apply_time >= c.create_time and c. gps<>''
where date(a.apply_time) <= '2019-11-18'
) t 
where rn=1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_gps)
r_gps = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
print(r_gps.shape)

r_gps.loan_id = r_gps.loan_id.astype(str)

r_gps['latitude'].replace(' ', np.nan, inplace=True)
r_gps['longitude'].replace(' ', np.nan, inplace=True)

r_gps['gps'] = '(' + r_gps.latitude + ', ' + r_gps.longitude + ')'
r_gps.gps.replace('(nan, nan)', np.nan, inplace=True)
r_gps.gps.replace('(None, None)', np.nan, inplace=True)

r_gps['gps_0'] = '(' + round(pd.to_numeric(r_gps.latitude), 0).astype(str) + ', ' \
                 + round(pd.to_numeric(r_gps.longitude), 0).astype(str) + ')'
r_gps.gps_0.replace('(nan, nan)', np.nan, inplace=True)
r_gps.gps_0.replace('(None, None)', np.nan, inplace=True)

r_gps['gps_1'] = '(' + round(pd.to_numeric(r_gps.latitude), 1).astype(str) + ', ' \
                 + round(pd.to_numeric(r_gps.longitude), 1).astype(str) + ')'
r_gps.gps_1.replace('(nan, nan)', np.nan, inplace=True)
r_gps.gps_1.replace('(None, None)', np.nan, inplace=True)

r_gps['gps_2'] = '(' + round(pd.to_numeric(r_gps.latitude), 2).astype(str) + ', ' \
                 + round(pd.to_numeric(r_gps.longitude), 2).astype(str) + ')'
r_gps.gps_2.replace('(nan, nan)', np.nan, inplace=True)
r_gps.gps_2.replace('(None, None)', np.nan, inplace=True)

r_gps['gps_3'] = '(' + round(pd.to_numeric(r_gps.latitude), 3).astype(str) + ', ' \
                 + round(pd.to_numeric(r_gps.longitude), 3).astype(str) + ')'
r_gps.gps_3.replace('(nan, nan)', np.nan, inplace=True)
r_gps.gps_3.replace('(None, None)', np.nan, inplace=True)

r_gps['gps_4'] = '(' + round(pd.to_numeric(r_gps.latitude), 4).astype(str) + ', ' \
                 + round(pd.to_numeric(r_gps.longitude), 4).astype(str) + ')'
r_gps.gps_4.replace('(nan, nan)', np.nan, inplace=True)
r_gps.gps_4.replace('(None, None)', np.nan, inplace=True)

r_gps.drop(['apply_time', 'latitude', 'longitude'], axis=1, inplace=True)

r_gps.to_csv(work_dir + 'r_gps.csv', index = False)
r_gps = pd.read_csv(work_dir + 'r_gps.csv', dtype = str)


''' 数据检查 
print(sum(r_gps.latitude.isnull()))
print(sum(r_gps.longitude.isnull()))  # 13% missing

check_gps = r_gps.copy()

check_gps['apply_date']=check_gps['apply_time'].dt.date

test = (check_gps.groupby('apply_date').size())
test = test.reset_index()

test2 = (check_gps['latitude'].isnull().groupby(check_gps['apply_date']).sum())
test2 = test2.reset_index()

test = test.merge(test2, how='left', on = 'apply_date')
check_gps2 = test.copy()
check_gps2['missing_by_date'] = check_gps2['latitude']/check_gps2.iloc[:,1]
'''


