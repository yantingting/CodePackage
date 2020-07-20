import numpy as np
import pandas as pd
import sys
sys.path.append('/Users/Mint/Desktop/repos/genie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *

data_path = 'D:/Model/202001_mvp_model/01_data/'
result_path = 'D:/Model/202001_mvp_model/02_result/'

"""基本信息"""

baseinfo_sql = """
WITH baseinfo as 
(
SELECT id as customer_id
    , update_time
    , cell_phone
    , mail
    , id_card_address
    ,marital_status
    ,religion
    ,education
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
FROM dw_gocash_go_cash_loan_gocash_core_customer_history 
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , baseinfo.*
            , date_part('minutes', apply_time :: timestamp - update_time:: timestamp) baseinfo_update_minutes
            , row_number() over(partition by loan.loan_id order by baseinfo.update_time desc) as rn
      FROM  (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM rt_t_gocash_core_loan
            --WHERE effective_date between '2019-08-08' and '2019-11-30' and return_flag = 'true'
            WHERE date(apply_time) >= '2020-02-24 15:10:00' and return_flag = 'true' 
            ) loan
      LEFT JOIN baseinfo  ON loan.customer_id :: varchar = baseinfo.customer_id :: varchar
      WHERE loan.apply_time::timestamp >= baseinfo.update_time
) t
WHERE rn = 1
"""
base_data = get_df_from_pg(baseinfo_sql)


base_data.loc[base_data.loan_id == 365956210519744512].T
base_data.shape

base_data.columns


base_data['mail_address1'] = base_data.mail.apply(lambda x: x.split('@'))
base_data['mail_address2'] = base_data.mail_address1.apply(lambda x: x[-1])
base_data['mail_address3'] = base_data.mail_address2.apply(lambda x: x.lower().split('.')[0])
base_data['mail_address'] = base_data.mail_address3.apply(lambda x: 'gmail' if x in ['gmail'] else 'yahoo'  if x in ['yahoo','ymail','rocketmail'] else 'others')
base_data['mail_address'].value_counts()
base_data.index = base_data.loan_id
base_data.columns #无缺失

mail_dummy = pd.get_dummies(base_data['mail_address'])
mail_dummy.head()
mail_dummy.columns = ['mail_' + str(col) for col in mail_dummy.columns]

marital_dummy = pd.get_dummies(base_data['marital_status'])
marital_dummy.head()
marital_dummy.columns = ['marital_' + str(col) for col in marital_dummy.columns]

religion_dummy = pd.get_dummies(base_data['religion'])
religion_dummy.columns = ['religion_' + str(col) for col in religion_dummy.columns]

education_dummy = pd.get_dummies(base_data['education'])
education_dummy.columns = ['education_' + str(col) for col in education_dummy.columns]

base_data_f = base_data.merge(mail_dummy,  left_index = True, right_index = True).merge(marital_dummy, left_index = True, right_index = True)\
            .merge(religion_dummy, left_index = True, right_index = True).merge(education_dummy,left_index = True, right_index = True)

base_data_f = base_data_f.reset_index(drop = True)

save_data_to_pickle(base_data_f, data_path, 'base_0808to1130.pkl')
base_data = load_data_from_pickle(data_path, 'base_0808to1130.pkl')


baseinfo_sql2 = """
WITH baseinfo as (
SELECT customer_id
    , update_time
    --三要素
    , cell_phone
    , id_card_name
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
    , case when substring(id_card_no, 11,2)::int >19 --说明是19XX年出生
        then 119 - substring(id_card_no, 11,2)::int else 19 - substring(id_card_no, 11,2)::int  end as age
    --, case when substring(id_card_no, 11,2)::int >substring(cast(update_time as varchar),3,2)::int
    --    then 100 +substring(cast(update_time as varchar),3,2)::int - substring(id_card_no, 11,2)::int
    --    else substring(cast(update_time as varchar),3,2)::int - substring(id_card_no, 11,2)::int  end as age        
FROM public.dw_gocash_go_cash_loan_gocash_core_customer_history
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , baseinfo.*
            , row_number() over(partition by loan.loan_id order by baseinfo.update_time desc) as rn
      FROM (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            WHERE effective_date between '2019-08-08' and '2019-11-30' and return_flag = 'true'
            --WHERE date(apply_time) >= '2020-02-24' and return_flag = 'true'
            ) loan
      LEFT join baseinfo ON loan.customer_id :: varchar = baseinfo.customer_id :: varchar
      WHERE loan.apply_time :: timestamp >= baseinfo.update_time
) t
WHERE rn = 1
"""
base_data2 = get_df_from_pg(baseinfo_sql2)

temp = base_data2[['loan_id','age']]
temp.loan_id = temp.loan_id.astype(str)
temp1 = x_with_y[['loan_id','age']]
temp1 = temp1.reset_index(drop = True)

temp2 = temp.merge(temp1, on = 'loan_id')
temp2['age_diff'] = temp2['age_x'] - temp2['age_y']

base_data2.shape
base_data2.gender = base_data2.gender.replace('male', 1).replace('female', 0)

save_data_to_pickle(base_data2, data_path, 'base2_0808to1130.pkl')

"""职业信息"""
prof_sql = """
WITH prof as 
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
    , date_part('minutes', apply_time :: timestamp - update_time:: timestamp) occupation_update_minutes
    , case when occupation_type in ('UNEMPLOYED') then jobless_time_income end as jobless_time_income
    , case when occupation_type in ('UNEMPLOYED') then monthly_income_resource end as monthly_income_resource
    , case when occupation_type in ('UNEMPLOYED') then pre_work_industry end as pre_work_industry
    , case when occupation_type in ('UNEMPLOYED') then pre_work_income end as pre_work_income   
    , case when occupation_type in ('OFFICE') then job end as job
    , case when occupation_type in ('OFFICE','ENTREPRENEUR') then industry_involved end as industry_involved
    --, case when occupation_type in ('OFFICE') then monthly_salary end as monthly_salary
    , monthly_salary 
    , case when occupation_type in ('OFFICE','ENTREPRENEUR') then company_area end as company_area
    , case when occupation_type in ('ENTREPRENEUR') then employee_number end as employee_number
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , prof.*
            , row_number() over(partition by loan.loan_id order by prof.update_time desc) as rn
      FROM (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            --WHERE effective_date between '2019-08-08' and '2019-11-30' and return_flag = 'true'
            --WHERE date(apply_time) >= '2020-02-24' and return_flag = 'true'
            WHERE date(apply_time) between '2020-01-10' and '2020-02-09' and return_flag = 'true'
            ) loan
      LEFT JOIN prof ON loan.customer_id = prof.customer_id
      WHERE loan.apply_time :: timestamp >= prof.update_time
) t
WHERE rn = 1
"""
prof_data = get_df_from_pg(prof_sql)

prof_data.shape
prof_data.columns

set(flag_0113.loan_id) - set(prof_data.loan_id) #少15条数据

flag_0113.loc[flag_0113.loan_id == '374142337395818496']
prof_data.loc[prof_data.customer_id == '360831736761851904']

prof_data.head()

save_data_to_pickle(prof_data, data_path, 'prof_0808to1130.pkl')
save_data_to_pickle(prof_data, data_path, 'prof_0808to1130_v2.pkl')

prof_data.loan_id = prof_data.loan_id.astype(str)
prof_data.to_excel('D:/Model/indn/202001_mvp_model/03_策略/prof_data.xlsx')

"""银行卡信息"""
bank_sql = """
WITH bank as (
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
            , date_part('minutes', apply_time :: timestamp - update_time:: timestamp) bankcode_update_minutes
            , row_number() over(partition by loan.loan_id order by bank.update_time desc) as rn
      FROM (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            --WHERE effective_date between '2019-08-08' and '2019-11-30' and return_flag = 'true'
            WHERE date(apply_time) >= '2020-02-24' and return_flag = 'true'
            ) loan
      LEFT join bank ON loan.customer_id = bank.customer_id 
      WHERE loan.apply_time :: timestamp >= bank.update_time
) t
WHERE rn = 1
"""
bank_data = get_df_from_pg(bank_sql)

bank_data.shape
bank_data.index = bank_data.loan_id

bank_dummy = pd.get_dummies(bank_data['bank_code'])

bank_dummy.columns = ['bankcode_' + str(col) for col in bank_dummy.columns]
bank_dummy.head()
bank_data_f = bank_data.merge(bank_dummy, left_index = True, right_index = True)
bank_data_f.shape

bank_data_f = bank_data_f.reset_index(drop=True)
bank_data_f.head()

save_data_to_pickle(bank_data_f, data_path, 'bank_0808to1130.pkl')
bank_data = load_data_from_pickle(data_path, 'bank_0501to1128.pkl')

"""紧急联系人"""

refer_sql = """
WITH refer as 
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
    , customer_id
    , max(date_part('day', interval_value) * 24 * 60 + date_part('hour', interval_value) * 60 +  date_part('minute', interval_value)) as refer_update_minutes
    , max(refer_bro_sis) as refer_bro_sis
    , max(refer_parents) as refer_parents
    , max(refer_spouse) as refer_spouse
FROM (SELECT loan.loan_id
            , refer.*
            , (apply_time :: timestamp - create_time:: timestamp) as interval_value
            , dense_rank() over(partition by loan.loan_id order by refer.create_time desc) as rn
      FROM (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            --WHERE effective_date between '2019-08-08' and '2019-11-30' and return_flag = 'true'
            WHERE date(apply_time) between '2020-01-10' and '2020-02-09' and return_flag = 'true'
            ) loan
      LEFT JOIN refer  ON loan.customer_id = refer.customer_id
      WHERE loan.apply_time :: timestamp >= refer.create_time) t
WHERE rn = 1
GROUP BY loan_id, customer_id
"""

refer_data = get_df_from_pg(refer_sql)
refer_data.to_excel('D:/Model/indn/202001_mvp_model/03_策略/refer.xlsx')
refer_data.shape
refer_data.customer_id.nunique()
save_data_to_pickle(refer_data, data_path, 'refer_0808to1130_v2.pkl')



"""device"""

device_sql = '''
SELECT loan_id, customer_id, device_update_minutes, brand, model, manufacturer, version, heightpixels, widthpixels
FROM ( SELECT a.loan_id
            , b.*
            , date_part('minutes', apply_time :: timestamp - create_time:: timestamp) device_update_minutes
            , row_number() over(partition by a.loan_id order by b.create_time desc) as rn 
        FROM (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            WHERE effective_date between '2019-08-08' and '2019-11-30' and return_flag = 'true') a 
        LEFT JOIN ( SELECT customer_id
                        , create_time
                        , case when device_info <>'' then device_info::json #>> '{brand}' end as brand
                        , case when device_info <>'' then device_info::json #>> '{Model}' end as model
                        , case when device_info <>'' then device_info::json #>> '{manufacturer}' end as manufacturer
                        , case when device_info <>'' then device_info::json #>> '{version}' end as version
                        , case when device_info <>'' then device_info::json #>> '{heightPixels}' end as heightpixels
                        , case when device_info <>'' then device_info::json #>> '{widthPixels}' end as widthpixels
	                FROM gocash_loan_risk_program_baseinfo
	                WHERE create_time::date <= '2019-12-01') b 
	    ON a.customer_id::varchar = b.customer_id and a.apply_time :: timestamp >= b.create_time
) t 
WHERE rn = 1
'''
device_data = get_df_from_pg(device_sql)
device_data.columns

device_data.loan_id = device_data.loan_id.astype(str)
device_data.brand = device_data.brand.astype(str).str.upper()
device_data.model = device_data.model.astype(str).str.upper()
device_data.manufacturer = device_data.manufacturer.astype(str).str.upper()
device_data.version = device_data.version.astype(str)
device_data.heightpixels = device_data.heightpixels.astype(str)
device_data.widthpixels = device_data.widthpixels.astype(str)

#r_device = r_device.replace(['false', 'False', 'true', 'True'],[0, 0, 1, 1])
device_data = device_data.replace(['NONE', 'None', ''],[np.nan, np.nan, np.nan])

#test = r_device.groupby('screen').size()

device_data['screen'] = '(' + round(pd.to_numeric(device_data.heightpixels), 0).astype(str) + ', ' + round(pd.to_numeric(device_data.widthpixels), 0).astype(str) + ')'
device_data = device_data.replace(['(nan, nan)', '(NONE, NONE)'],[np.nan, np.nan])

# CREATE DUMMY VARIABLES FOR screen
screen_size = pd.DataFrame(device_data.groupby('screen').size())
screen_size = screen_size.reset_index().rename(columns={'index': 'screen', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
screen_size['cum_sum'] = screen_size['ct'].cumsum()
screen_size['cum_perc'] = 100 * screen_size['cum_sum']/screen_size['ct'].sum()
screen_list = list(screen_size.screen[screen_size.cum_perc <= 90])
screen_dummy = pd.get_dummies(device_data['screen'])[screen_list]
screen_dummy.columns = ['screen_' + str(col) for col in screen_dummy.columns]

# CREATE DUMMY VARIABLES FOR brand
brand_size = pd.DataFrame(device_data.groupby('brand').size())
brand_size = brand_size.reset_index().rename(columns={'index': 'brand', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
brand_size['cum_sum'] = brand_size['ct'].cumsum()
brand_size['cum_perc'] = 100 * brand_size['cum_sum']/brand_size['ct'].sum()
brand_list = list(brand_size.brand[brand_size.cum_perc <= 95])
brand_dummy = pd.get_dummies(device_data['brand'])[brand_list]
brand_dummy.columns = ['brand_' + str(col) for col in brand_dummy.columns]

# CREATE DUMMY VARIABLES FOR model
model_size = pd.DataFrame(device_data.groupby('model').size())
model_size = model_size.reset_index().rename(columns={'index': 'model', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
model_size['cum_sum'] = model_size['ct'].cumsum()
model_size['cum_perc'] = 100 * model_size['cum_sum']/model_size['ct'].sum()
model_list = list(model_size.model[model_size.cum_perc <= 75])
model_dummy = pd.get_dummies(device_data['model'])[model_list]
model_dummy.columns = ['model_' + str(col) for col in model_dummy.columns]

# CREATE DUMMY VARIABLES FOR manufacturer
manufacturer_size = pd.DataFrame(device_data.groupby('manufacturer').size())
manufacturer_size = manufacturer_size.reset_index().rename(columns={'index': 'manufacturer', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
manufacturer_size['cum_sum'] = manufacturer_size['ct'].cumsum()
manufacturer_size['cum_perc'] = 100 * manufacturer_size['cum_sum']/manufacturer_size['ct'].sum()
manufacturer_list = list(manufacturer_size.manufacturer[manufacturer_size.cum_perc <= 95])
manufacturer_dummy = pd.get_dummies(device_data['manufacturer'])[manufacturer_list]
manufacturer_dummy.columns = ['manufacturer_' + str(col) for col in manufacturer_dummy.columns]

# CREATE DUMMY VARIABLES FOR version
version_size = pd.DataFrame(device_data.groupby('version').size())
version_size = version_size.reset_index().rename(columns={'index': 'version', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
version_size['cum_sum'] = version_size['ct'].cumsum()
version_size['cum_perc'] = 100 * version_size['cum_sum']/version_size['ct'].sum()
version_list = list(version_size.version[version_size.cum_perc <= 100])
version_dummy = pd.get_dummies(device_data['version'])[version_list]
version_dummy.columns = ['version_' + str(col) for col in version_dummy.columns]


device_data_f = device_data.join(screen_dummy).join(brand_dummy).join(model_dummy).join(manufacturer_dummy).join(version_dummy)

device_data_f = device_data_f.drop(['heightpixels', 'widthpixels', 'brand', 'model', 'manufacturer', 'version'], axis = 1)
device_data_f.head()
save_data_to_pickle(device_data_f, data_path, 'device_0808to1130.pkl')

"""gps"""
gps_sql = """
SELECT loan_id
    , gps_update_minutes
    , jailbreak_status
    , simulator_status
    , latitude
    , longitude
    , altitude
    , speed
    , accuracy
FROM ( SELECT a.loan_id
            , b.*
            , date_part('minutes', apply_time :: timestamp - create_time:: timestamp) gps_update_minutes
            , row_number() over(partition by a.loan_id order by b.create_time desc) as rn 
        FROM (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            WHERE effective_date between '2019-08-08' and '2019-11-30' and return_flag = 'true') a 
        LEFT JOIN ( SELECT customer_id
                        , create_time
                        , jailbreak_status
                        , simulator_status
                        , gps::json #>> '{latitude}' as latitude, gps::json #>> '{longitude}' as longitude
                        , gps::json #>> '{direction}' as direction, gps::json #>> '{speed}' as speed
                        , gps::json #>> '{altitude}' as altitude
                        , gps::json #>> '{accuracy}' as accuracy
	                FROM gocash_loan_risk_program_baseinfo 
	                WHERE gps <>'' and create_time::date <= '2019-12-01') b 
	    ON a.customer_id::varchar = b.customer_id :: varchar and a.apply_time :: timestamp >= b.create_time
) t 
WHERE rn = 1
"""

gps_data = get_df_from_pg(gps_sql)
gps_data.shape

#gps变量衍生
gps_data.columns

gps_data.loan_id = gps_data.loan_id.astype(str)
gps_data.simulator_status = gps_data.simulator_status.astype(str)
gps_data.latitude = gps_data.latitude.astype(str)
gps_data.longitude = gps_data.longitude.astype(str)
gps_data.speed = gps_data.speed.astype(float)
gps_data.altitude = gps_data.altitude.astype(float)
gps_data.accuracy = gps_data.accuracy.astype(float)

gps_data = gps_data.replace(['false', 'False', 'true', 'True'],[0, 0, 1, 1])
gps_data = gps_data.replace(['None', ''],[np.nan, np.nan])

print(gps_data.groupby('simulator_status').size())

gps_data['gps_0'] = '(' + round(pd.to_numeric(gps_data.latitude), 0).astype(str) + ', ' + round(pd.to_numeric(gps_data.longitude), 0).astype(str) + ')'
#gps_data['gps_1'] = '(' + round(pd.to_numeric(gps_data.latitude), 1).astype(str) + ', ' + round(pd.to_numeric(gps_data.longitude), 1).astype(str) + ')'
gps_data = gps_data.replace(['(nan, nan)', '(None, None)'],[np.nan, np.nan])

# CREATE DUMMY VARIABLES FOR GPS_0
gps_0_size = pd.DataFrame(gps_data.groupby('gps_0').size())
gps_0_size = gps_0_size.reset_index().rename(columns={'index': 'gps_0', 0: 'ct'}).sort_values(by = 'ct', ascending=False)
gps_0_size['cum_sum'] = gps_0_size['ct'].cumsum()
gps_0_size['cum_perc'] = 100 * gps_0_size['cum_sum']/gps_0_size['ct'].sum()
gps_0_list = list(gps_0_size.gps_0[gps_0_size.cum_perc <= 90])
gps_0_dummy = pd.get_dummies(gps_data['gps_0'])[gps_0_list]
gps_0_dummy.columns = ['gps_' + str(col) for col in gps_0_dummy.columns]

gps_data_f = gps_data.join(gps_0_dummy)

gps_data_f = gps_data_f.drop(['latitude', 'longitude', 'gps_0', 'gps_1'], axis = 1)

save_data_to_pickle(gps_data_f, data_path, 'gps_0808to1130.pkl')


######变量检查

base_data.head()
base_data.groupby('customer_id')['mail','marital_status','religion','education'].nunique()

base_data_bycustomer = base_data[['customer_id','mail','marital_status','religion','education']].drop_duplicates()
base_data_bycustomer.shape #37985, 6

a = base_data_bycustomer.customer_id.value_counts().reset_index() #37951 34条不一致记录，17人修改过基本信息, 包括婚姻，教育
a = a.loc[a.customer_id >1]

base_data_bycustomer.loc[base_data_bycustomer.customer_id.isin(list(a['index']))].sort_values('customer_id').to_excel(os.path.join(data_path, 'base_data_diff.xlsx'))

base_data_bycustomer.loc[base_data_bycustomer.mail == 'listy.rachel@gmail.com']

#bankcode
bank_data_bycustomer = bank_data[['customer_id','bank_code']].drop_duplicates() #一致
bank_data_bycustomer.shape

#prof
prof_data_bycustomer = prof_data.drop(['loan_id','apply_time','effective_date'], 1).drop_duplicates()
a = prof_data_bycustomer.customer_id.value_counts().reset_index()
a.loc[a.customer_id >1].shape  #3159

prof_data_bycustomer.loc[prof_data_bycustomer.customer_id == 357961647788175360].T
prof_data_bycustomer.loc[prof_data_bycustomer.customer_id == 365846232756695040].T

prof_data_bycustomer.shape #41156 3205条职业发生过变化(主要有月收入引起）,3159申请人的职业信息发生变化, 1%
prof_data_bycustomer.shape #38105 仅有154条职业信息发生过变量（排除月收入), 基本都是2个loanid, 即77人的职业信息变化过

#refer
refer_data_bycustomer = refer_data.drop(['loan_id'], 1).drop_duplicates()
refer_data_bycustomer.shape #38062
refer_data_bycustomer.customer_id.value_counts().sort_values()

(38062 - 37951)/2  #55人修改过紧急联系人

refer_data.loc[refer_data.customer_id.isin([309728691982737408])].T


#device
device_data_bycustomer = device_data.drop(['loan_id'],1 ).drop_duplicates()
device_data_bycustomer.shape
a = device_data_bycustomer.customer_id.value_counts().reset_index()
a.loc[a.customer_id >1].shape
#3547多申请人修改过device


#样本计算
flag_0113.loc[flag_0113.effective_date >= '2019-09-17'].shape

#0808到0916时间段的样本数
flag_0113.loc[flag_0113.effective_date < '2019-09-17'].shape #46165
flag_0113.loc[flag_0113.effective_date < '2019-09-17'].customer_id.nunique() #26829
46165/26829 = 1.72


#0917到1016时间段的样本数
flag_0113.loc[(flag_0113.effective_date >= '2019-09-17') & (flag_0113.effective_date <= '2019-10-16')].shape #36543
flag_0113.loc[(flag_0113.effective_date >= '2019-09-17') & (flag_0113.effective_date <= '2019-10-16')].customer_id.nunique() #24576
36543/24576 = 1.48

flag_0113.loc[flag_0113.effective_date >= '2019-09-17'].customer_id.nunique()
flag_0113.head()


##回溯样本
flag_0113.shape
flag_0113.loc[flag_0113.effective_date>='2019-09-17'].shape
flag_0113.loc[flag_0113.effective_date>='2019-09-17'].effective_date.max()

td_sql = '''
SELECT  a.id as loan_id
        , customer_id
        , apply_time
        , effective_date
        , cell_phone
        , md5(cell_phone) as md5_cell_phone
        , id_card_name
        , md5(id_card_name) as md5_id_card_name
        , id_card_no
        , md5(id_card_no) as md5_id_card_no
FROM (select id, apply_time, effective_date, customer_id
      from dw_gocash_go_cash_loan_gocash_core_loan
      where return_flag = 'true' and effective_date between '2019-09-17' and '2019-10-30') a 
LEFT JOIN dw_gocash_go_cash_loan_gocash_core_customer b on a.customer_id = b.id and b.idcard_pass_flag = 'true'
'''

td_huisu = get_df_from_pg(td_sql)

td_huisu.shape
td_huisu.effective_date = td_huisu.effective_date.astype(str)
td_huisu.loan_id = td_huisu.loan_id.astype(str)
td_huisu.customer_id = td_huisu.customer_id.astype(str)

td_huisu.to_excel('D:/Model/202001_mvp_model/01_data/同盾回溯样本_0119.xlsx')
td_huisu.apply_time.min()

generate_md5('3174046811890001')
generate_md5('Nopia Nurdiani')
generate_md5('081219212308')

"""izi回溯样本"""
izi_sql = """
SELECT  t1.id as loanid
      , t1.customer_id
      , t1.apply_time
      --, t1.effective_date
      --, t1.return_flag
      , t2.cell_phone 
      --, substring(t2.cell_phone,3) as cell_phone2
      --, concat('+628',substring(t2.cell_phone,3)) as cell_phone3
      , md5(concat('+628',substring(t2.cell_phone,3))) as md5_cellphone_izi
      , t2.id_card_no
      , t2.id_card_name 
      , md5(t2.id_card_name) as md5name
FROM  (
        SELECT *
        FROM public.dw_gocash_go_cash_loan_gocash_core_loan
        WHERE (effective_date between '2019-09-17' and '2019-10-30') and return_flag = 'true'
        ) t1 
LEFT join public.dw_gocash_go_cash_loan_gocash_core_customer t2
ON t1.customer_id= t2.id
"""

izi_huisu = get_df_from_pg(izi_sql)

izi_huisu.shape
izi_huisu.loanid = izi_huisu.loanid.astype(str)
izi_huisu.customer_id = izi_huisu.customer_id.astype(str)
izi_huisu.to_excel('D:/Model/202001_mvp_model/01_data/izi回溯样本.xlsx')

td_online_sql = """
select a.*, b.apply_time, b.id as loanid, b.effective_date, b.return_flag
               , md5(c.cell_phone) as md5_cell_phone
               , md5(id_card_name) as md5_id_card_name
               , md5(id_card_no) as md5_id_card_no
               , row_number() over(partition by b.id order by a.createtime desc) as rn
from (select * 
      from public.risk_mongo_gocash_tdaurora
      where businessid = 'uku'
      limit 200) a
left join dw_gocash_go_cash_loan_gocash_core_loan b on a.customerid = b.customer_id :: varchar
left join dw_gocash_go_cash_loan_gocash_core_customer c on b.customer_id = c.id
WHERE b.apply_time::timestamp + '8 hour' >= a.createtime::timestamp 
"""
td_online_huisu = get_df_from_pg(td_online_sql)

td_online_huisu.loc[td_online_huisu.return_flag == 'true'].customerid

td_online_huisu.loc[td_online_huisu.customerid == '182929085853376512'][['createtime','apply_time','rn','loanid']]
td_online_huisu.loanid =td_online_huisu.loanid.astype(str)

td_online_huisu.to_excel(os.path.join(data_path, 'td_online.xlsx'))


"""izi回溯检查数据"""
izi_sql1 = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-09-17' and '2019-10-16'  and return_flag = 'true'
),
izi as 
(
SELECT customerid as customer_id
    , createtime
    , age as phone_age
FROM risk_gocash_mongo_iziphoneage
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , izi.*
            , row_number() over(partition by loan.loan_id order by izi.createtime desc) as rn
      FROM loan
      LEFT join izi ON loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
izi_data1 = get_df_from_pg(izi_sql1)
izi_data1.loan_id.nunique()
izi_data1.customer_id.nunique()

izi_sql2 = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-09-17' and '2019-10-16'  and return_flag = 'true'
),
izi as (
SELECT customerid as customer_id
    , createtime
    , "07d"
    , "14d"
    , "21d"
    , "30d"
    , "60d"
    , "90d"
    , total
FROM risk_gocash_mongo_iziinquiriesbytype
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , izi.*
            , row_number() over(partition by loan.loan_id order by izi.createtime desc) as rn
      FROM loan
      LEFT join izi on loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
izi_data2 = get_df_from_pg(izi_sql2)

izi_sql3 = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-09-17' and '2019-10-16'  and return_flag = 'true'
),
izi as 
(
SELECT customerid as customer_id
    , createtime
    , result
FROM risk_gocash_mongo_iziphoneverify
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , izi.*
        , row_number() over(partition by loan.loan_id order by izi.createtime desc) as rn
      FROM loan
      LEFT JOIN izi on loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
izi_data3 = get_df_from_pg(izi_sql3)

izi_online = izi_data1.merge(izi_data2, on = 'loan_id', how = 'outer').merge(izi_data3, on = 'loan_id', how = 'outer')
izi_online.shape
izi_online.isnull().sum()

izi_huisu1 = pd.read_excel('D:/Model/202001_mvp_model/01_data/izi_td/result_izi.xlsx')
izi_huisu2 = pd.read_excel('D:/Model/202001_mvp_model/01_data/izi_td/result_MinTech_phoneage.xlsx')
izi_huisu1 = izi_huisu1.drop('phoneage',1)
izi_huisu = izi_huisu1.merge(izi_huisu2, on = 'loanid', how = 'outer')
izi_compare = izi_online.merge(izi_huisu, left_on = 'loan_id', right_on = 'loanid')
izi_compare.shape
izi_compare.to_excel('D:/Model/202001_mvp_model/01_data/izi_td/izi_compare.xlsx')
izi_compare = pd.read_excel('D:/Model/202001_mvp_model/01_data/izi_td/izi_compare.xlsx')
izi_compare.columns
izi_compare['apply_day'] = izi_compare['apply_time_x_x'].apply(lambda x:str(x)[0:10])
izi_compare['createday'] = izi_compare['createtime_x'].apply(lambda x:str(x)[0:10])

"""izi回溯数据解析"""
izi_data = pd.read_excel('D:/Model/202001_mvp_model/01_data/izi_td/result_izi.xlsx')
izi_data = pd.read_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/iziData_test0210.xlsx')
izi_data.loanid = izi_data.loanid.astype(str)

izi = izi_data[['loanid','apply_time', '身份证多头v4_result']]
# json只认“”不认‘’
izi['身份证多头v4_result'] = izi['身份证多头v4_result'].str.replace("\'","\"")
izi.head()

import json
import itertools
import time


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

try4.head()
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

try5.head()

try5.loanid = try5.loanid.astype(str)
try5.shape

izi_phoneage = pd.read_excel('D:/Model/202001_mvp_model/01_data/izi_td/result_MinTech_phoneage.xlsx')

izi_data.loanid =izi_data.loanid.astype(str)
izi_phoneage.loanid =izi_phoneage.loanid.astype(str)

izi_data_all = izi_data.merge(try5, on = 'loanid', how = 'left').merge(izi_phoneage, on = 'loanid', how = 'left')
save_data_to_pickle(izi_data_all, data_path , 'izi_var.pkl')

#0212合并数据
try5 = try5.drop_duplicates()
try5 = try5.drop(['B','C','A','AA','BB','CC','A_daysdiff','B_daysdiff','C_daysdiff'],1)
#try5.loc[try5.loanid == '433054476088549376'].to_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/temp.xlsx')
try5 = try5.drop_duplicates()
try5.loanid.nunique()

izi_data.loanid = izi_data.loanid.astype(str)
izi_data2 = izi_data.drop(['身份证多头v4_status','身份证多头v4_result','号码多头v4_status','号码多头v4_result'],1).merge(try5, on = 'loanid', how = 'left')
izi_data2.shape
save_data_to_pickle(izi_data2, 'D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/' , 'izi_var_0212.pkl')
izi_data2 = load_data_from_pickle('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/','izi_var_0212.pkl')