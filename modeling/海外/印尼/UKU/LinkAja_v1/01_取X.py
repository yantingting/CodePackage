import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos_v1/genie')
import matplotlib
matplotlib.use('TkAgg')

from utils3.data_io_utils import *

data_path = 'D:/Model/201911_uku_ios_model/01_data/'
result_path = 'D:/Model/201911_uku_ios_model/02_result/'

""""基本信息"""

baseinfo_sql = """
WITH loan as 
(
SELECT id as loan_id
        , apply_time
        , effective_date
        , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
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
      LEFT JOIN baseinfo  ON loan.customer_id = baseinfo.customer_id
      WHERE loan.apply_time >= baseinfo.update_time
) t
WHERE rn = 1
"""
base_data = get_df_from_pg(baseinfo_sql)
base_data =base_data.drop_duplicates()

base_data.loc[base_data.customer_id == 382829403868397568]

#数据检查
set(perf_data.customer_id) - set(base_data.customer_id)


save_data_to_pickle(base_data, data_path, 'base_0501to1128.pkl')
base_data = load_data_from_pickle(data_path, 'base_0501to1128.pkl')

baseinfo_sql2 = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
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
    , case when substring(id_card_no, 11,2)::int >19 --说明是19XX年出生
        then 119 - substring(id_card_no, 11,2)::int else 19 - substring(id_card_no, 11,2)::int  end as age
FROM public.dw_gocash_go_cash_loan_gocash_core_customer_history
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , baseinfo.*
            , row_number() over(partition by loan.customer_id order by baseinfo.update_time desc) as rn
      FROM loan
      LEFT join baseinfo ON loan.customer_id = baseinfo.customer_id
      WHERE loan.apply_time >= baseinfo.update_time
) t
WHERE rn = 1
"""
base_data2 = get_df_from_pg(baseinfo_sql2)
base_data2 =base_data2.drop_duplicates()
base_data2 = base_data2.drop(['day','year'], 1)
save_data_to_pickle(base_data2, data_path, 'base2_0501to1128.pkl')


"""职业信息"""
prof_sql = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
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
      LEFT JOIN prof ON loan.customer_id = prof.customer_id
      WHERE loan.apply_time >= prof.update_time
) t
WHERE rn = 1
"""
prof_data = get_df_from_pg(prof_sql)

prof_data.loan_id = prof_data.loan_id.astype(str)
prof_data.loan_id = prof_data.loan_id.astype(float)

len(set(perf_data.loan_id) - set(prof_data.loan_id))/len(perf_data) #231缺失, 0.0020881167175889935缺失
set(perf_data.customer_id) - set(prof_data.customer_id)
save_data_to_pickle(prof_data, data_path, 'prof_0501to1128.pkl')
prof_data = load_data_from_pickle(data_path, 'prof_0501to1128.pkl')
prof_data.occupation_type.value_counts() #11个学生
prof_data.loc[prof_data.occupation_type == 'PELAJAR']
prof_data.jobless_time_income.value_counts()


"""银行卡信息"""
bank_sql = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
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
      LEFT join bank ON loan.customer_id = bank.customer_id
      WHERE loan.apply_time >= bank.update_time
) t
WHERE rn = 1
"""
bank_data = get_df_from_pg(bank_sql)
bank_data.shape

len(set(perf_data.customer_id) - set(bank_data.customer_id))/len(perf_data)
set(perf_data.customer_id) - set(bank_data.customer_id)
save_data_to_pickle(bank_data, data_path, 'bank_0501to1128.pkl')
bank_data = load_data_from_pickle(data_path, 'bank_0501to1128.pkl')

# """gps信息"""
# gps_sql = """
#WITH loan as
#(
#SELECT id as loan_id
#    , apply_time
#    , effective_date
#    , customer_id
#FROM dw_gocash_go_cash_loan_gocash_core_loan
#WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
#),
# gps as (
# select customer_id
# , create_time
# , gps::json ->> 'latitude' as latitude
# , gps::json ->> 'longitude'  as longitude
# from gocash_loan_risk_program_baseinfo
# where gps<>''
# )
# select *
# from (select loan.loan_id, loan.apply_time, loan.effective_date, gps.*
#       , row_number() over(partition by loan.customer_id order by gps.create_time desc) as rn
#       from loan
#       left join gps on loan.customer_id::varchar = gps.customer_id
#       where loan.apply_time >= gps.create_time
# ) t
# where rn = 1
# """
# gps_data = get_df_from_pg(gps_sql)


"""device"""
device_sql = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
),
device as 
(
SELECT customer_id
    , create_time
    , device_info::json ->> 'brand' as brand
    , device_info::json ->> 'heightPixels'  as heightPixels
    , device_info::json ->> 'widthPixels'  as widthPixels
    , device_info::json ->> 'manufacturer'  as manufacturer
    , device_info::json ->> 'model'  as model
    , device_info::json ->> 'version'  as version
FROM gocash_loan_risk_program_baseinfo
WHERE device_info<>''
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , device.*
            , row_number() over(partition by loan.customer_id order by device.create_time desc) as rn
      FROM loan
      LEFT JOIN device ON loan.customer_id::varchar = device.customer_id
      WHERE loan.apply_time >= device.create_time
) t
WHERE rn = 1
"""
device_data = get_df_from_pg(device_sql)
device_data.loc[device_data.loan_id == 347089652712251392]
base_data.loc[base_data.loan_id == 347089652712251392]
save_data_to_pickle(device_data, data_path, 'device_0501to1128.pkl')
1 - (device_data.shape[0]/len(base_data)) #14%

base_data.loc[(base_data.loan_id.isin(list(set(base_data.loan_id) - set(device_data.loan_id)))) & (base_data.effective_date == '2019-07-10')]
#a_missing = base_data.loc[base_data.loan_id.isin(list(set(base_data.loan_id) - set(device_data.loan_id)))].effective_date.value_counts().sort_index().reset_index()
# b = base_data.effective_date.value_counts().sort_index().reset_index()
#
# c = a_missing.merge(b, on = 'index')
# c.to_excel(os.path.join(data_path, 'device_check.xlsx'))

"""紧急联系人"""
refer_sql = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
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
    , apply_time
    , effective_date
    , max(refer_bro_sis) as refer_bro_sis
    , max(refer_parents) as refer_parents
    , max(refer_spouse) as refer_spouse
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , refer.*
            , dense_rank() over(partition by refer.customer_id order by refer.create_time desc) as rn
      FROM loan
      LEFT JOIN refer  ON loan.customer_id = refer.customer_id
      WHERE loan.apply_time >= refer.create_time) t
WHERE rn = 1
GROUP BY loan_id, apply_time, effective_date
"""
refer_data = get_df_from_pg(refer_sql)
refer_data = refer_data.fillna(0)
refer_data.isnull().sum()
refer_data.refer_type.value_counts()
save_data_to_pickle(refer_data, data_path, 'refer_0501to1128.pkl')


"""izi"""
izi_sql1 = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
),
izi as 
(
SELECT customerid as customer_id
    , createtime
    , age as phone_age
FROM risk_gocash_mongo_iziphoneage
--WHERE age is not null
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , izi.*
            , row_number() over(partition by loan.customer_id order by izi.createtime desc) as rn
      FROM loan
      LEFT join izi ON loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
izi_data1 = get_df_from_pg(izi_sql1)

izi_data1.effective_date.min()
izi_data1.effective_date = izi_data1.effective_date.astype(str)
izi_data1.head()
izi_data1.loc[izi_data1.customer_id  == 338691645822242816]


# def convert_to_date(data, time_cols):
#     for i in time_cols:
#         print(i)
#         data[i] = data[i].apply(lambda x:str(x)[0:10])
#     return (data)
#
# izi_data1 = convert_to_date(izi_data1, ['createtime', 'apply_time'])
save_data_to_pickle(izi_data1, data_path, 'izi1_0621to1128.pkl')
izi_data1 = load_data_from_pickle(data_path, 'izi1_0621to1128.pkl')
izi_data1.effective_date.min()

temp = flag.merge(izi_data1.drop(['customer_id','effective_date'],1), on = 'loan_id', how = 'left')
temp.isnull().sum()


izi_sql2 = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
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
            , row_number() over(partition by loan.customer_id order by izi.createtime desc) as rn
      FROM loan
      LEFT join izi on loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""

izi_data2 = get_df_from_pg(izi_sql2)

set(izi_data2.customer_id) - set(izi_data1.customer_id)
izi_data2.effective_date.value_counts().sort_index()
izi_data1.effective_date.value_counts().sort_index()

save_data_to_pickle(izi_data2, data_path, 'izi2_0621to1128_v2.pkl')
# izi_data2 = load_data_from_pickle(data_path, 'izi2_0621to1128.pkl')
# izi_data2.head()
# izi_data3.shape


izi_sql3 = """
WITH loan as 
(
SELECT id as loan_id
    , apply_time
    , effective_date
    , customer_id
FROM dw_gocash_go_cash_loan_gocash_core_loan
WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false'
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
        , row_number() over(partition by loan.customer_id order by izi.createtime desc) as rn
      FROM loan
      LEFT JOIN izi on loan.customer_id = izi.customer_id
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
izi_data3 = get_df_from_pg(izi_sql3)

izi_data3.shape
save_data_to_pickle(izi_data3, data_path, 'izi3_0621to1128_v2.pkl') #更新了取数逻辑
izi_data3 = load_data_from_pickle(data_path, 'izi3_0621to1128.pkl')

izi_sql4 = """
WITH loan as 
(
SELECT t1.id as loan_id
    , t1.apply_time
    , t1.effective_date
    , t1.customer_id
    , t2.cell_phone
FROM (SELECT * 
      FROM dw_gocash_go_cash_loan_gocash_core_loan
      WHERE effective_date between '2019-05-01' and '2019-11-28'  and return_flag = 'false') t1
LEFT JOIN (SELECT id as customer_id, cell_phone 
           FROM dw_gocash_go_cash_loan_gocash_core_customer) t2 
ON t1.customer_id = t2.customer_id
),
izi as 
(
SELECT number
    , createtime
    , whatsapp
FROM risk_gocash_mongo_iziwhatsapp
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , loan.cell_phone
            , izi.*
            , row_number() over(partition by loan.customer_id order by izi.createtime desc) as rn
      FROM loan
      LEFT JOIN izi ON loan.cell_phone = izi.number
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
izi_data4 = get_df_from_pg(izi_sql4)
izi_data4.shape
save_data_to_pickle(izi_data4, data_path, 'izi4_0621to1128_v2.pkl')
izi_data4 = load_data_from_pickle(data_path, 'izi4_0621to1128.pkl')

izi_data1.age.value_counts()
izi_data2.head()
izi_data3.effective_date.max()
izi_data4.effective_date.min()

temp = flag.merge(izi_data4.drop(['effective_date'],1), on = 'loan_id', how = 'left')
temp.to_excel(os.path.join(data_path, 'temp_whatsapp.xlsx'))

izi_data1.loc[izi_data1.effective_date>='2019-08-23'].groupby(['effective_date','age'])['age'].count().unstack()
izi_data2.loc[izi_data2.effective_date>='2019-08-23'].groupby(['effective_date','total'])['total'].count().unstack()


# writer = pd.ExcelWriter(os.path.join(data_path, 'izi_check.xlsx'))
# izi_data1.loc[izi_data1.effective_date>='2019-08-23'].groupby(['effective_date','age'])['age'].count().unstack().to_excel(writer, 'izi1', index=True)
# izi_data2.loc[izi_data2.effective_date>='2019-08-23'].groupby(['effective_date','total'])['total'].count().unstack().to_excel(writer, 'izi2', index=True)
# writer.save()
#9.9规则上线

#6.27-9.9放款用户
#变量检查和衍生
base_data.loc[base_data.marital_status == 'BEREFT'].effective_date.value_counts().sort_index()
#MARRIED         70774
#SPINSTERHOOD    34211 #单身
#DIVORCED         4480
#BEREFT           1161  #丧偶
#
base_data.marital_status.value_counts()
base_data.religion.value_counts() #ok
base_data.education.value_counts().sort_index()
base_data.mail.value_counts()

base_data['mail_address1'] = base_data.mail.apply(lambda x: x.split('@'))
base_data['mail_address2'] = base_data.mail_address1.apply(lambda x: x[-1])
base_data['mail_address3'] = base_data.mail_address2.apply(lambda x: x.lower().split('.')[0])
base_data['mail_address'] = base_data.mail_address3.apply(lambda x: 'gmail' if x in ['gmail'] else 'yahoo'  if x in ['yahoo','ymail','rocketmail'] else 'others')
base_data['mail_address'] = base_data.mail_address3.apply(lambda x: 'gmail' if x in ['gmail'] else 'yahoo'  if x in ['yahoo','ymail','rocketmail'] else 'others')
base_data['mail_address'] = base_data['mail_address'].replace('gmail','1').replace('yahoo','2').replace('others','3')
base_data = base_data.drop(['mail_address1','mail_address2','mail_address3'],1)

base_data.mail_address.value_counts()
#1    100805
#2      9024
#3       797

# a = pd.DataFrame(base_data.mail_address3.value_counts())
# a.to_excel(os.path.join(data_path, 'mail_address_v2.xlsx'))
base_data2.loc[base_data2.customer_id == 152700826998407168]
base_data2.loc[base_data2.customer_id == 353168418525982720]

base_data2.gender = base_data2.gender.replace('male', 1).replace('female', 0)

#职业信息
prof_data.job.value_counts()
prof_data.monthly_income_resource.value_counts() #linkaja非枚举项
prof_data.pre_work_industry.value_counts()
prof_data.industry_involved.value_counts()
prof_data.company_area.value_counts() #数据太脏
prof_data.employee_number.value_counts() #linkaja无此字段

#银行卡
bank_data.effective_date = bank_data.effective_date.astype(str)
bank_data.loc[(bank_data.effective_date >= '2019-06-27') & (bank_data.effective_date <= '2019-09-09')].bank_code.value_counts()

izi_data1 = izi_data1.rename(columns = {'age':'phone_age'})


#合并所有X
all_x = base_data.drop(['rn','update_time','id_card_address','channel'],1).merge(base_data2.drop(['apply_time','effective_date','customer_id','update_time','id_card_no','rn'],1), on = 'loan_id', how = 'left')\
                .merge(prof_data.drop(['apply_time','effective_date','customer_id'],1), on = 'loan_id', how = 'left')\
                .merge(bank_data.drop(['apply_time','effective_date','customer_id','update_time','rn'],1), on = 'loan_id', how = 'left')\
                .merge(izi_data1.drop(['apply_time','effective_date','customer_id','createtime','rn'],1), on = 'loan_id', how = 'left') \
                .merge(izi_data2.drop(['apply_time', 'effective_date', 'customer_id', 'createtime', 'rn'], 1), on='loan_id',how='left') \
                .merge(izi_data3.drop(['apply_time', 'effective_date', 'customer_id', 'createtime', 'rn'], 1), on='loan_id',how='left') \
                .merge(izi_data4.drop(['apply_time', 'effective_date','cell_phone','number', 'createtime', 'rn'], 1), on='loan_id',how='left')\
                .merge(refer_data.drop(['apply_time','effective_date'],1), on = 'loan_id', how = 'left')

all_x.isnull().sum()
all_x = all_x.drop(['occupation_type','jobless_time_income','pre_work_industry','pre_work_income','job','industry_involved','monthly_salary'],1)

prof_data.loan_id = prof_data.loan_id.astype(float)
all_x_2 = all_x.merge(prof_data.drop(['apply_time','effective_date','customer_id'],1), on = 'loan_id', how = 'left')
all_x_2.head()
all_x_2.isnull().sum()

all_x_2 = all_x_2.fillna(-1)
all_x_2.index  = all_x_2.loan_id

save_data_to_pickle(all_x_2, data_path, 'all_x_20191203_v2.pkl')
all_x_2 = load_data_from_pickle(data_path, 'all_x_20191203_v2.pkl')
all_x_2 = all_x_2.drop_duplicates('loan_id')
#all_x_20191203 0501-1128所有X
#all_x_20191203_v2 0501-1128所有X 更新Izi数据

#字典
pd.DataFrame(all_x_3.columns).to_excel('D:/Model/201911_uku_ios_model/字典.xlsx')

#合并X和Y
flag = pd.read_excel('D:/Model/201911_uku_ios_model/01_data/flag_20191203.xlsx', sheet_name= 'Sheet1')
flag.shape

x_with_y = flag.merge(all_x_2.drop(['effective_date','customer_id'],1), on = 'loan_id', how = 'left')

#分类变量映射
#izi
x_with_y.whatsapp.value_counts()
x_with_y.result = x_with_y.result.replace('MATCH', 1).replace('NOT_MATCH', 0)
x_with_y.whatsapp = x_with_y.whatsapp.replace('yes', 1).replace('no', 0)

dat_types = x_with_y.dtypes.reset_index().rename(columns = {'index':'var_name', 0:'types'})
x_varchar = [i for i in list(dat_types.loc[dat_types['types'] == 'object'].var_name) if i not in ['effective_date','result','whatsapp','loan_status','cell_phone','mail']]

x_with_y.index = x_with_y.loan_id
# temp_dummy = pd.get_dummies(x_with_y)
# temp_dummy.columns

var_map = {}
for i in x_with_y.columns:
    if i in x_varchar:
        print(i)
        mapping = {label: idx for idx, label in enumerate(set(x_with_y.loc[x_with_y[i] != -1 ][i]))}
        x_with_y[i] = x_with_y[i].map(mapping)
        print (mapping)
        var_map[i] = mapping

#industry_involved
industry_mapping = {label:idx for idx,label in enumerate(set(x_with_y.loc[x_with_y['industry_involved'] != -1 ]['industry_involved']))}
industry_mapping

var_map['industry'] = industry_mapping

x_with_y['industry_involved'] = x_with_y['industry_involved'].map(industry_mapping)
x_with_y['pre_work_industry'] = x_with_y['pre_work_industry'].map(industry_mapping)

x_with_y = x_with_y.loc[x_with_y.flag_7.isin([0,1])]

#save_data_to_pickle(x_with_y, data_path, 'x_with_y_20191203.pkl') #0627-0829所有X

save_data_to_json(var_map, data_path, 'var_map_v2.json')

#划分样本
from sklearn.model_selection import train_test_split
x_with_y.loc[x_with_y.effective_date <= '2019-08-07','sample_set'] = 'train'
x_with_y.loc[x_with_y.effective_date > '2019-08-07','sample_set'] = 'test'
x_with_y.sample_set.value_counts()

x_with_y = x_with_y.fillna(-1)

x_with_y.loc[x_with_y.industry_involved.isnull()].customer_id

# prof_data.loc[prof_data.customer_id == 373020133962457088].T
# all_x_2.loc[all_x_2.customer_id == 373020133962457088].T

x_with_y.isnull().sum()
save_data_to_pickle(x_with_y, data_path, 'x_with_y_20191203_v3.pkl') #对分类变量做了替换和去掉坏样本
#x_with_y_20191203_v2 #对分类变量做了替换和去掉坏样本
#x_with_y_20191203_v3 #更新izi数据
#x_with_y_20191203_v4 #最终建模所用数据，把provincecode按照原始值进行处理，不重新进行编码

#准备变量字段
var_dict = pd.read_excel('D:/Model/201911_uku_ios_model/建模代码可用变量字典.xlsx', sheet_name= '字典')
y_cols = set(x_with_y.columns) - set(var_dict.指标英文)

# x_with_y.customer_id = x_with_y.customer_id.astype(str)
# x_with_y.loan_id = x_with_y.loan_id.astype(str)
#
# x_with_y.to_excel(os.path.join(data_path, 'x_with_y.xlsx'))


""""
从原始表中取izi的数据, 但是发现原始表中whatsapp的数据中没有记录checking后回调的结果，所以不能使用原始数据
""""

izi_sql4_2 = """
WITH loan as 
(
SELECT t1.id as loan_id
    , t1.apply_time
    , t1.effective_date
    , t1.customer_id
    , t2.cell_phone
FROM (SELECT * 
      FROM dw_gocash_go_cash_loan_gocash_core_loan
      WHERE effective_date between '2019-06-20' and '2019-11-28'  and return_flag = 'false') t1
LEFT JOIN (SELECT id as customer_id, cell_phone 
           FROM dw_gocash_go_cash_loan_gocash_core_customer) t2 
ON t1.customer_id = t2.customer_id
),
izi as 
(
select *
	, concat('0',trim(leading '+62' from numbers)) as numbers2
	from (
		select create_time 
		--request_param::json ->>'number' as md5_numb
		, cast(trim(trailing '\\n' from response_data)::json ->> 'message' as json) ->> 'number' as numbers
		, cast(trim(trailing '\\n' from response_data)::json ->> 'message' as json) ->> 'whatsapp' as results
	from public.dw_gocash_go_cash_loan_t_gocash_risk_request_flow
	where request_url = 'https://api.izi.credit/v1/iswhatsapp' and response_code = 'OK'
	--and date(create_time) between '2019-0-18' and '2019-08-20'
	)t
)
SELECT *
FROM (SELECT loan.loan_id
            , loan.apply_time
            , loan.effective_date
            , loan.cell_phone
            , izi.*
            , row_number() over(partition by loan.customer_id order by izi.create_time desc) as rn
      FROM loan
      LEFT JOIN izi ON loan.cell_phone = izi.numbers2
      WHERE loan.apply_time::timestamp + '8 hour' >= izi.create_time::timestamp
) t
WHERE rn = 1
"""
izi_data4_2 = get_df_from_pg(izi_sql4_2)
izi_data4_2.shape
izi_data4_2.loan_id.nunique()

a = list(izi_data4_2.loc[(izi_data4_2.results == 'checking')].cell_phone)

izi_data4.loc[izi_data4.cell_phone.isin(a)]

set(izi_data4.cell_phone) - set(izi_data4_2.cell_phone)
set(izi_data4_2.cell_phone) - set(izi_data4.cell_phone)

temp = flag.merge(izi_data4_2.drop(['effective_date'],1), on = 'loan_id', how = 'left')
temp.to_excel(os.path.join(data_path, 'temp_whatsapp.xlsx'))

