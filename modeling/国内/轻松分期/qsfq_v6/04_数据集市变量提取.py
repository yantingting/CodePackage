"""
201908轻松分期建模三方数据源解析
@author: yuexin, mingqing
"""
import datetime
import os
import pandas as pd
import numpy as np
import sys

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

-----------------------------------基本信息-------------------------------
dc_baseinfo_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('tb') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no
from dc_sample_add
),
var as (
select order_no
, education
, maritalstatus
,idcardgender
,age
,industry
,monthlyincome
,positiontype
,idcity_jobcity
,id_city_level
,job_city_level
,idcitylevel_jobcitylevel
,case when age = '' then '-9999'
      when cast(age as integer)  >30 and (maritalstatus= '1' or maritalstatus='2' or maritalstatus='4') then 1
	  when cast(age as integer)  <=30 and maritalstatus='3' then 2
	  else 3 end as age_marital
from dc_customer_history_result 
)
select *
from sample t1
left join var t2 on t1.order_no = t2.order_no
"""

dc_baseinfo_data = get_df_from_pg(dc_baseinfo_sql)
dc_baseinfo_data.index = dc_baseinfo_data.iloc[:,0]
dc_baseinfo_data = dc_baseinfo_data.drop('order_no',1)
dc_baseinfo_data.columns
dc_baseinfo_data = dc_baseinfo_data.reset_index()
dc_baseinfo_data.to_excel(os.path.join(data_path,'dc_baseinfo.xlsx'))
dc_baseinfo_data.head()


bk_baseinfo_sql = """
with sample as ( 
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no
from bk_sample_add
),
var as (
select order_no
, education
, maritalstatus
,idcardgender
,age
,industry
,monthlyincome
,positiontype
,idcity_jobcity
,id_city_level
,job_city_level
,idcitylevel_jobcitylevel
,case when cast(age as integer) >30
	  and (maritalstatus = '1' or maritalstatus= '2' or maritalstatus= '4') then 1
	  when cast(age as integer)  <=30
	  and maritalstatus= '3' then 2
	  else 3 end as age_marital
from bk_customer_history_result
)
select *
from sample t1
left join var t2 on t1.order_no = t2.order_no
"""

bk_baseinfo_data = get_df_from_pg(bk_baseinfo_sql)

bk_baseinfo_data.order_no.nunique()
bk_baseinfo_data.order_no.value_counts()
bk_baseinfo_data.loc[bk_baseinfo_data.order_no == 'BK2019070319512691540594'].T #确认数据是否ok


bk_baseinfo_data = bk_baseinfo_data.drop_duplicates('order_no')

bk_baseinfo_data.index = bk_baseinfo_data.iloc[:,0]
bk_baseinfo_data = bk_baseinfo_data.drop('order_no',1)
bk_baseinfo_data = bk_baseinfo_data.reset_index()
bk_baseinfo_data.to_excel(os.path.join(data_path,'bk_baseinfo.xlsx'))


-------------------------------------信用卡网银账单-----------------------------

bk_cc_base_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no 
from bk_sample_add
)
select *
from sample t1
left join bk_cc_base_info_result t2 on t1.order_no = t2.order_no
left join bk_cc_bills_result t3 on t1.order_no = t3.order_no
left join bk_cc_installment1_result t4 on t1.order_no = t4.order_no
left join bk_cc_installment2_result t5 on t1.order_no = t5.order_no
left join bk_shoppingsheets_shoppingsheetone_result t6 on t1.order_no = t6.order_no
left join bk_shoppingsheets_shoppingsheettwo_result t7 on t1.order_no = t7.order_no
"""

bk_cc_data = get_df_from_pg(bk_cc_base_sql)
bk_cc_data.order_no.nunique()


for i in bk_cc_data.columns:
    #if i in ['order_no','customer_id','dt']:
    print (i)


bk_cc_bills_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no 
from bk_sample_add
)
select *
from sample t1
left join bk_cc_bills_result t2 on t1.order_no = t2.order_no
"""


bk_cc_inst_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no 
from bk_sample_add
)
select *
from sample t1
left join bk_cc_installment1_result t2 on t1.order_no = t2.order_no
left join bk_cc_installment2_result t3 on t1.order_no = t3.order_no
"""

bk_cc_inst_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no 
from bk_sample_add
)
select *
from sample t1
left join bk_cc_installment1_result t2 on t1.order_no = t2.order_no
left join bk_cc_installment2_result t3 on t1.order_no = t3.order_no
"""

bk_cc_shop_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no 
from bk_sample_add
)
select *
from sample t1
left join bk_shoppingsheets_shoppingsheetone_result t6 on t1.order_no = t6.order_no
left join bk_shoppingsheets_shoppingsheettwo_result t7 on t1.order_no = t7.order_no
"""

------------------------------淘宝-----------------------------------