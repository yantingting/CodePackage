#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : 01_取X.py
@Time    : 2020-05-27 11:20
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""




import sys
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
import os
from collections import Counter
sys.path.append('/Users/yantingting/Documents/MintechModel/newgenie/utils3/')
from data_io_utils import *

file_path = '/Users/yantingting/Seafile/风控/模型/15 印度/V4模型/01_Data'


'''------------------------------------ experian ------------------------------------'''

query0 = '''
with level1 as (
select customerid,"BureauScore","BureauScoreConfidLevel","ReportDate","ReportTime",
"TotalCAPSLast7Days","TotalCAPSLast30Days","TotalCAPSLast90Days","TotalCAPSLast180Days"
from ind_oss_creditreport_level 
),
level2 as  (
select customerid,"NonCreditCAPSLast7Days","NonCreditCAPSLast30Days","NonCreditCAPSLast90Days","NonCreditCAPSLast180Days",
"Outstanding_Balance_All","Outstanding_Balance_Secured","Outstanding_Balance_Secured_Percentage","Outstanding_Balance_UnSecured","Outstanding_Balance_UnSecured_Percentage",
"CreditAccountActive","CreditAccountClosed","CreditAccountDefault","CreditAccountTotal","CADSuitFiledCurrentBalance",
"CAPSLast7Days","CAPSLast30Days","CAPSLast90Days","CAPSLast180Days","State","Date_Of_Birth_Applicant","Gender_Code","MobilePhoneNumber","Amount_Financed"
from ind_oss_creditreport_level2 
),
caps as (select customerid,
count(distinct "ReportNumber") as cnt_report,
count(distinct case when "Amount_Financed" !='' then "ReportNumber" else null end ) as cnt_amount,
count(distinct case when "Duration_Of_Agreement" != '' then "ReportNumber" else null end) as cnt_duration,
count(distinct case when "Amount_Financed" !='0' then "ReportNumber" else null end ) as cnt_amountover0,
count(distinct case when "Duration_Of_Agreement" != '0' then "ReportNumber" else null end) as cnt_durationover0,
count(distinct case when "Finance_Purpose" = '99' then "ReportNumber" else null end ) as cnt_purpose99,
count(distinct case when "Finance_Purpose" = '55' then "ReportNumber" else null end ) as cnt_purpose55,
count(distinct case when "Finance_Purpose" = '35' then "ReportNumber" else null end ) as cnt_purpose35,
count(distinct case when "Finance_Purpose" = '22' then "ReportNumber" else null end ) as cnt_purpose22,
count(distinct case when "Finance_Purpose" = '1' then "ReportNumber" else null end ) as cnt_purpose1,
count(distinct case when "Finance_Purpose" = '23' then "ReportNumber" else null end ) as cnt_purpose23,
count(distinct case when "Finance_Purpose" = '11' then "ReportNumber" else null end ) as cnt_purpose11,
count(distinct case when "Finance_Purpose" = '40' then "ReportNumber" else null end ) as cnt_purpose40,
count(distinct case when "Enquiry_Reason" = '13' then "ReportNumber" else null end ) as cnt_enquiry13,
count(distinct case when "Enquiry_Reason" = '99' then "ReportNumber" else null end ) as cnt_enquiry99,
count(distinct case when "Enquiry_Reason" = '5' then "ReportNumber" else null end ) as cnt_enquiry5,
count(distinct case when "Enquiry_Reason" = '16' then "ReportNumber" else null end ) as cnt_enquiry16,
count(distinct case when "Enquiry_Reason" = '2' then "ReportNumber" else null end ) as cnt_enquiry2,
count(distinct case when "Enquiry_Reason" = '11' then "ReportNumber" else null end ) as cnt_enquiry11,
count(distinct case when "Enquiry_Reason" = '6' then "ReportNumber" else null end ) as cnt_enquiry6,
count(distinct case when "Enquiry_Reason" = '7' then "ReportNumber" else null end ) as cnt_enquiry7,
count(distinct case when "Enquiry_Reason" = '1' then "ReportNumber" else null end ) as cnt_enquiry1,
count(distinct case when "Enquiry_Reason" = '3' then "ReportNumber" else null end ) as cnt_enquiry3,
count(distinct case when "Enquiry_Reason" = '02' then "ReportNumber" else null end ) as cnt_enquiry02,
count(distinct case when "Enquiry_Reason" = '10' then "ReportNumber" else null end ) as cnt_enquiry10,
count(distinct case when "Enquiry_Reason" = '14' then "ReportNumber" else null end ) as cnt_enquiry14,
max(case when "Amount_Financed" !='' then "Amount_Financed"::float else 0 end ) as max_amount,
min(case when "Amount_Financed" !='' then "Amount_Financed"::float else 0 end) as min_amount,
min(case when "Amount_Financed" != '0' and "Amount_Financed" !='' then "Amount_Financed"::float else null end ) min_amountover0,
max(case when "Duration_Of_Agreement" !='' then "Duration_Of_Agreement":: float else 0 end ) as max_duration,
min(case when "Duration_Of_Agreement" !='' then "Duration_Of_Agreement":: float else 0 end ) as min_duration,
min(case when "Duration_Of_Agreement" != '0'  and "Duration_Of_Agreement" != '' then "Duration_Of_Agreement"::float else null end ) min_durationover0,
min("Date_of_Request") as first_report,
max("Date_of_Request") as last_report
from ind_oss_creditreport_caps_application_details
group by 1),
level3 as (select customerid,
--count(1) as cnt_account,
count(distinct accountholdertypecode) as cnt_accountholdertypecode,
count(open_date) as cnt_account,
count(case when accountholdertypecode = '1' then open_date else null end ) as cnt_account1,
count(case when accountholdertypecode = '2' then open_date else null end ) as cnt_account2,
count(case when accountholdertypecode = '3' then open_date else null end ) as cnt_account3,
count(case when accountholdertypecode = '7' then open_date else null end ) as cnt_account7,
min(open_date) as first_account,
max(open_date) as last_account,
count(distinct account_type) as cnt_account_type,
count(case when account_type = '05' then open_date end ) as cnt_accounttype05,
count(case when account_type = '06' then open_date end ) as cnt_accounttype06,
count(case when account_type = '07' then open_date end ) as cnt_accounttype07,
count(case when account_type = '10' then open_date end ) as cnt_accounttype10,
count(case when account_type = '00' then open_date end ) as cnt_accounttype00,
count(case when account_type = '13' then open_date end ) as cnt_accounttype13,
count(case when account_type = '01' then open_date end ) as cnt_accounttype01,
count(case when account_type = '53' then open_date end ) as cnt_accounttype53,
count(case when account_type = '02' then open_date end ) as cnt_accounttype02,
count(case when account_type = '41' then open_date end ) as cnt_accounttype41,
count(case when account_type = '12' then open_date end ) as cnt_accounttype12,
count(case when account_type = '51' then open_date end ) as cnt_accounttype51,
count(case when account_type = '17' then open_date end ) as cnt_accounttype17,
count(case when account_type = '0' then open_date end ) as cnt_accounttype0,
count(case when amount_past_due != '' and  amount_past_due != '0' then open_date end) as cnt_dueover0,
max(case when highest_credit_or_original_loan_amount !='' then highest_credit_or_original_loan_amount::float else 0 end ) as max_highest_credit_or_original_loan_amount,
min(case when highest_credit_or_original_loan_amount !='' then highest_credit_or_original_loan_amount::float else 0 end ) as min_highest_credit_or_original_loan_amount,
max(case when current_balance != '' then current_balance::float else 0 end) as max_balance,
min(case when current_balance != '' then current_balance::float else 0 end) as min_balance,
min(case when current_balance != '' and current_balance !='0' then current_balance::float else null end) as min_balanceover0,
count(case when status1 = 'active' then open_date else null end ) as cnt_active,
count(case when status1 = 'closed' then open_date else null end ) as cnt_closed,
count(case when status1 = 'other' then open_date else null end ) as cnt_other
from 
(select *,
case when account_status in ('11', '21', '22', '23', '24', '25', '71', '78', '80', '82', '83', '84') then 'active' 
     when account_status in ('13','14','15','16','17') then 'closed'
     else 'other' end as status1
from ind_oss_creditreport_level3)t
group by 1)
select level1.*
,level2.*
,caps.*
,level3.*
from level1 
left join level2 on level1.customerid = level2.customerid
left join caps on level1.customerid= caps.customerid
left join level3 on level1.customerid = level3.customerid
'''

# 不要强制转成int，会报错，可以转成float
df0 = DataBase().get_df_from_pg(query0)
df0.shape
# np.where(np.array(df0.columns.tolist())=='customerid')[0]  # [ 0,  9, 33, 68]
df = pd.DataFrame(df0.iloc[:,0]).merge(df0.drop('customerid',axis = 1),left_index = True,right_index = True,how = 'inner')
df.shape
save_data_to_pickle(df,file_path,'experian.pkl')









