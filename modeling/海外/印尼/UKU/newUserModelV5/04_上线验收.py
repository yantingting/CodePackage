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


usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)


import xgboost as xgb
import pandas as pd
import numpy as np
from pandas import read_csv
from pandas import read_excel
from xgboost import DMatrix

path_applydata = 'D:/Model Development/201912 IDN new v5/01 Data/apply data/'


'''###################### 取数 ######################'''
sql_all = '''

'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_all)
r_all = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_all.shape)
print(r_all.loan_id.nunique())
print(r_all.dtypes)


sql_cust = '''
WITH loan as 
(select loanid as loan_id, createtime as create_time, customerid::bigint as customer_id, religionIslam,
educationSeniorHighSchool,
educationRegularCollegeCourse
from risk_mongo_gocash_installmentriskcontrolparams a 
where newUserModelscoreV5 <>'' and createtime::date between '2019-12-23' and '2019-12-24'
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
SELECT loan_id, case religion when 'ISLAM' then 1 else 0 end as religion_ISLAM,
case education when 'SENIOR_HIGH_SCHOOL' then 1 else 0 end as education_SENIOR_HIGH_SCHOOL,
case education when 'REGULAR_COLLEGE_COURSE' then 1 else 0 end as education_REGULAR_COLLEGE_COURSE,
religionIslam,
educationSeniorHighSchool,
educationRegularCollegeCourse
FROM (SELECT loan.*
            , baseinfo.*
            , row_number() over(partition by loan.loan_id order by baseinfo.update_time desc) as rn
      FROM loan
      LEFT JOIN baseinfo  ON loan.customer_id = baseinfo.customer_id
      WHERE loan.create_time::timestamp >= baseinfo.update_time
) t
WHERE rn = 1
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_cust)
r_cust = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_cust.shape)
print(r_cust.loan_id.nunique())
print(r_cust.dtypes)

r_cust.religionislam = r_cust.religionislam.astype(float)
r_cust.educationseniorhighschool = r_cust.educationseniorhighschool.astype(float)
r_cust.educationregularcollegecourse = r_cust.educationregularcollegecourse.astype(float)

tmp = r_cust[r_cust.religion_islam != r_cust.religionislam]
tmp = r_cust[r_cust.education_senior_high_school != r_cust.educationseniorhighschool]
tmp = r_cust[r_cust.education_regular_college_course != r_cust.educationregularcollegecourse]


sql_prof = '''
WITH loan as 
(select loanid as loan_id, createtime as create_time, customerid::bigint as customer_id, jobManajer
from risk_mongo_gocash_installmentriskcontrolparams a 
where newUserModelscoreV5 <>'' and createtime::date between '2019-12-23' and '2019-12-24'
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
SELECT loan_id, case when occupation_type in ('OFFICE') and job='MANAJER' then 1 else 0 end as job_MANAJER, jobManajer
FROM (SELECT loan.*
            , prof.*
            , row_number() over(partition by loan.loan_id order by prof.update_time desc) as rn
      FROM loan
      LEFT JOIN prof ON loan.customer_id = prof.customer_id
      WHERE loan.create_time::timestamp >= prof.update_time
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

r_prof.jobmanajer = r_prof.jobmanajer.astype(float)

tmp = r_prof[r_prof.job_manajer != r_prof.jobmanajer]


sql_device = '''
select loan_id, customerid,
case when round(heightpixels::float)=1424 and round(widthpixels::float)=720 then 1 else 0 end as screen_1424_720,
case when upper(brand) = 'SAMSUNG' then 1 else 0 end as brand_SAMSUNG,
case when upper(model) = 'SH-04H' then 1 else 0 end as model_SH04H,
heightPixels1424widthPixels720,
brandSamsung,
ModelSh04h
from (
select a.loanid as loan_id, customerid,
heightPixels1424widthPixels720,
brandSamsung,
ModelSh04h, b.*, row_number() over(partition by a.loanid order by b.create_time desc) as rn 
from risk_mongo_gocash_installmentriskcontrolparams a 
left join ( 
	select customer_id, create_time, 
    case when device_info = '' then '' else device_info::json #>> '{brand}' end as brand, 
    case when device_info = '' then '' else device_info::json #>> '{Model}' end as model,
    case when device_info = '' then '' else device_info::json #>> '{heightPixels}' end as heightpixels, 
    case when device_info = '' then '' else device_info::json #>> '{widthPixels}' end as widthpixels
	from gocash_loan_risk_program_baseinfo ) b on a.customerid = b.customer_id and a.createtime::timestamp >= b.create_time
where newUserModelscoreV5 <>'' and createtime::date between '2019-12-26' and '2019-12-26'
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

r_device.heightpixels1424widthpixels720 = r_device.heightpixels1424widthpixels720.astype(float)
r_device.brandsamsung = r_device.brandsamsung.astype(float)
r_device.modelsh04h = r_device.modelsh04h.astype(float)

tmp = r_device[r_device.screen_1424_720 != r_device.heightpixels1424widthpixels720]
tmp = r_device[r_device.brand_samsung != r_device.brandsamsung]
tmp = r_device[r_device.model_sh04h != r_device.modelsh04h]

r_device = r_device.rename(columns={'screen_1424_720': 'screen_(1424.0, 720.0)', 'brand_samsung': 'brand_SAMSUNG', 'model_sh04h': 'model_SH-04H'})


sql_model = '''
select loanid as loan_id, customerid, createtime , newUserModelscoreV5,
newUserModelScoreV5InputModelParams, newUserModelScoreV5ModelParams
from risk_mongo_gocash_installmentriskcontrolparams par
where newUserModelscoreV5 <>'' and createtime::date between '2019-12-24' and '2019-12-24'
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_model)
r_model = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description])
print(r_model.shape)
print(r_model.loan_id.nunique())
print(r_model.dtypes)

r_model[r_model.newusermodelscorev5inputmodelparams]

'''###################### 打分 ######################'''
# LOAD FUNCTION
def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))
#.map(lambda x: int(x))
    
''' 读取数据 '''
# mydata = pd.read_csv('D:\\Model Development\\201912 IDN new v5\\01 Data\\raw data\\r_all3.csv')
# mydata.loan_id = mydata.loan_id.astype(str)

mydata = r_model.copy()

# mydata = mydata.fillna(-1)
# mydata = mydata.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])
# print(mydata.shape)
# print(mydata.dtypes)


# LOAD MODEL
mymodel = xgb.Booster() 
mymodel.load_model("D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800 python3.6.8\\grid_64_191219_145432.model")  

# LOAD VARIABLES 
# myvar = pd.read_excel('D:\\Model Development\\201912 IDN new v5\\03 Result\\py_output 20191213 0800\\grid_64_191213_120304.xlsx',\
#                       sheet_name='05_model_importance') 
# print(myvar.dtypes)

# PREDICT SCORES
# data_features = mydata[list(myvar['varName'])]


mydata.newusermodelscorev5inputmodelparams = mydata.newusermodelscorev5inputmodelparams.str.replace('[','') 
mydata.newusermodelscorev5inputmodelparams = mydata.newusermodelscorev5inputmodelparams.str.replace(']','') 
data_features = mydata.newusermodelscorev5inputmodelparams.str.split(',', expand=True)
print(data_features.dtypes)

var_to_float = list(range(0, 64, 1))
for x in var_to_float:
    data_features[x] = data_features[x].astype(float)
print(data_features.dtypes)

data_lean = DMatrix(data_features)
ypred = mymodel.predict(data_lean)


score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([mydata['loan_id'].values, 
                            mydata['createtime'].values, 
                            mydata['newusermodelscorev5'],
                            score, ypred]).T
data_scored.columns= ['loan_id', 'create_time', 'newusermodelscorev5', 
                      'score', 'prob']

print(data_scored.dtypes)
print(data_scored.shape)

writer = pd.ExcelWriter('D:\\Model Development\\201912 IDN new v5\\03 Result\\score_qc.xlsx')
data_scored.to_excel(writer, 'data_scored')
writer.save()


