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

------------------------------------- 同盾-----------------------------------------------

----多头------

td_duotou_loan_sql = """
select order_no
,max(case when rule_id = '47580344' then platform_count else null end) as r47580344_platform_ct
,sum(case when rule_id = '47580344' and industry_display_name='大数据金融' then count else null end) as r47580344_industry_1_ct
,sum(case when rule_id = '47580344' and industry_display_name='银行小微贷款' then count else null end) as r47580344_industry_2_ct
,sum(case when rule_id = '47580344' and industry_display_name='直销银行' then count else null end) as r47580344_industry_3_ct
,sum(case when rule_id = '47580344' and industry_display_name='网上银行' then count else null end) as r47580344_industry_4_ct
,sum(case when rule_id = '47580344' and industry_display_name='银行消费金融公司' then count else null end) as r47580344_industry_5_ct
,sum(case when rule_id = '47580344' and industry_display_name='大型消费金融公司' then count else null end) as r47580344_industry_6_ct
,sum(case when rule_id = '47580344' and industry_display_name='综合类电商平台' then count else null end) as r47580344_industry_7_ct
,sum(case when rule_id = '47580344' and industry_display_name='汽车租赁' then count else null end) as r47580344_industry_8_ct
,sum(case when rule_id = '47580344' and industry_display_name='P2P网贷' then count else null end) as r47580344_industry_9_ct
,sum(case when rule_id = '47580344' and industry_display_name='信息中介' then count else null end) as r47580344_industry_10_ct
,sum(case when rule_id = '47580344' and industry_display_name='设备租赁' then count else null end) as r47580344_industry_11_ct
,sum(case when rule_id = '47580344' and industry_display_name='财产保险' then count else null end) as r47580344_industry_12_ct
,sum(case when rule_id = '47580344' and industry_display_name='互联网金融门户' then count else null end) as r47580344_industry_13_ct
,sum(case when rule_id = '47580344' and industry_display_name='第三方服务商' then count else null end) as r47580344_industry_14_ct
,sum(case when rule_id = '47580344' and industry_display_name='一般消费分期平台' then count else null end) as r47580344_industry_15_ct
,sum(case when rule_id = '47580344' and industry_display_name='厂商汽车金融' then count else null end) as r47580344_industry_16_ct
,sum(case when rule_id = '47580344' and industry_display_name='银行个人业务' then count else null end) as r47580344_industry_17_ct
,sum(case when rule_id = '47580344' and industry_display_name='小额贷款公司' then count else null end) as r47580344_industry_18_ct
,sum(case when rule_id = '47580344' and industry_display_name='O2O' then count else null end) as r47580344_industry_19_ct
,sum(case when rule_id = '47580344' and industry_display_name='房地产金融' then count else null end) as r47580344_industry_20_ct
,sum(case when rule_id = '47580344' and industry_display_name='融资租赁' then count else null end) as r47580344_industry_21_ct
,sum(case when rule_id = '47580344' and industry_display_name='理财机构' then count else null end) as r47580344_industry_22_ct
,sum(case when rule_id = '47580344' and industry_display_name='信用卡中心' then count else null end) as r47580344_industry_23_ct
,sum(case when rule_id = '47580344' and industry_display_name='第三方支付' then count else null end) as r47580344_industry_24_ct
,max(case when rule_id = '47580334' then platform_count else null end) as r47580334_platform_ct
,sum(case when rule_id = '47580334' and industry_display_name='大数据金融' then count else null end) as r47580334_industry_1_ct
,sum(case when rule_id = '47580334' and industry_display_name='银行小微贷款' then count else null end) as r47580334_industry_2_ct
,sum(case when rule_id = '47580334' and industry_display_name='直销银行' then count else null end) as r47580334_industry_3_ct
,sum(case when rule_id = '47580334' and industry_display_name='网上银行' then count else null end) as r47580334_industry_4_ct
,sum(case when rule_id = '47580334' and industry_display_name='银行消费金融公司' then count else null end) as r47580334_industry_5_ct
,sum(case when rule_id = '47580334' and industry_display_name='大型消费金融公司' then count else null end) as r47580334_industry_6_ct
,sum(case when rule_id = '47580334' and industry_display_name='综合类电商平台' then count else null end) as r47580334_industry_7_ct
,sum(case when rule_id = '47580334' and industry_display_name='汽车租赁' then count else null end) as r47580334_industry_8_ct
,sum(case when rule_id = '47580334' and industry_display_name='P2P网贷' then count else null end) as r47580334_industry_9_ct
,sum(case when rule_id = '47580334' and industry_display_name='信息中介' then count else null end) as r47580334_industry_10_ct
,sum(case when rule_id = '47580334' and industry_display_name='设备租赁' then count else null end) as r47580334_industry_11_ct
,sum(case when rule_id = '47580334' and industry_display_name='财产保险' then count else null end) as r47580334_industry_12_ct
,sum(case when rule_id = '47580334' and industry_display_name='互联网金融门户' then count else null end) as r47580334_industry_13_ct
,sum(case when rule_id = '47580334' and industry_display_name='第三方服务商' then count else null end) as r47580334_industry_14_ct
,sum(case when rule_id = '47580334' and industry_display_name='一般消费分期平台' then count else null end) as r47580334_industry_15_ct
,sum(case when rule_id = '47580334' and industry_display_name='厂商汽车金融' then count else null end) as r47580334_industry_16_ct
,sum(case when rule_id = '47580334' and industry_display_name='银行个人业务' then count else null end) as r47580334_industry_17_ct
,sum(case when rule_id = '47580334' and industry_display_name='小额贷款公司' then count else null end) as r47580334_industry_18_ct
,sum(case when rule_id = '47580334' and industry_display_name='O2O' then count else null end) as r47580334_industry_19_ct
,sum(case when rule_id = '47580334' and industry_display_name='房地产金融' then count else null end) as r47580334_industry_20_ct
,sum(case when rule_id = '47580334' and industry_display_name='融资租赁' then count else null end) as r47580334_industry_21_ct
,sum(case when rule_id = '47580334' and industry_display_name='理财机构' then count else null end) as r47580334_industry_22_ct
,sum(case when rule_id = '47580334' and industry_display_name='信用卡中心' then count else null end) as r47580334_industry_23_ct
,sum(case when rule_id = '47580334' and industry_display_name='第三方支付' then count else null end) as r47580334_industry_24_ct
,max(case when rule_id = '47580324' then platform_count else null end) as r47580324_platform_ct
,sum(case when rule_id = '47580324' and industry_display_name='大数据金融' then count else null end) as r47580324_industry_1_ct
,sum(case when rule_id = '47580324' and industry_display_name='银行小微贷款' then count else null end) as r47580324_industry_2_ct
,sum(case when rule_id = '47580324' and industry_display_name='直销银行' then count else null end) as r47580324_industry_3_ct
,sum(case when rule_id = '47580324' and industry_display_name='网上银行' then count else null end) as r47580324_industry_4_ct
,sum(case when rule_id = '47580324' and industry_display_name='银行消费金融公司' then count else null end) as r47580324_industry_5_ct
,sum(case when rule_id = '47580324' and industry_display_name='大型消费金融公司' then count else null end) as r47580324_industry_6_ct
,sum(case when rule_id = '47580324' and industry_display_name='综合类电商平台' then count else null end) as r47580324_industry_7_ct
,sum(case when rule_id = '47580324' and industry_display_name='汽车租赁' then count else null end) as r47580324_industry_8_ct
,sum(case when rule_id = '47580324' and industry_display_name='P2P网贷' then count else null end) as r47580324_industry_9_ct
,sum(case when rule_id = '47580324' and industry_display_name='信息中介' then count else null end) as r47580324_industry_10_ct
,sum(case when rule_id = '47580324' and industry_display_name='设备租赁' then count else null end) as r47580324_industry_11_ct
,sum(case when rule_id = '47580324' and industry_display_name='财产保险' then count else null end) as r47580324_industry_12_ct
,sum(case when rule_id = '47580324' and industry_display_name='互联网金融门户' then count else null end) as r47580324_industry_13_ct
,sum(case when rule_id = '47580324' and industry_display_name='第三方服务商' then count else null end) as r47580324_industry_14_ct
,sum(case when rule_id = '47580324' and industry_display_name='一般消费分期平台' then count else null end) as r47580324_industry_15_ct
,sum(case when rule_id = '47580324' and industry_display_name='厂商汽车金融' then count else null end) as r47580324_industry_16_ct
,sum(case when rule_id = '47580324' and industry_display_name='银行个人业务' then count else null end) as r47580324_industry_17_ct
,sum(case when rule_id = '47580324' and industry_display_name='小额贷款公司' then count else null end) as r47580324_industry_18_ct
,sum(case when rule_id = '47580324' and industry_display_name='O2O' then count else null end) as r47580324_industry_19_ct
,sum(case when rule_id = '47580324' and industry_display_name='房地产金融' then count else null end) as r47580324_industry_20_ct
,sum(case when rule_id = '47580324' and industry_display_name='融资租赁' then count else null end) as r47580324_industry_21_ct
,sum(case when rule_id = '47580324' and industry_display_name='理财机构' then count else null end) as r47580324_industry_22_ct
,sum(case when rule_id = '47580324' and industry_display_name='信用卡中心' then count else null end) as r47580324_industry_23_ct
,sum(case when rule_id = '47580324' and industry_display_name='第三方支付' then count else null end) as r47580324_industry_24_ct
,max(case when rule_id = '47580354' then platform_count else null end) as r47580354_platform_ct
,sum(case when rule_id = '47580354' and industry_display_name='大数据金融' then count else null end) as r47580354_industry_1_ct
,sum(case when rule_id = '47580354' and industry_display_name='银行小微贷款' then count else null end) as r47580354_industry_2_ct
,sum(case when rule_id = '47580354' and industry_display_name='直销银行' then count else null end) as r47580354_industry_3_ct
,sum(case when rule_id = '47580354' and industry_display_name='网上银行' then count else null end) as r47580354_industry_4_ct
,sum(case when rule_id = '47580354' and industry_display_name='银行消费金融公司' then count else null end) as r47580354_industry_5_ct
,sum(case when rule_id = '47580354' and industry_display_name='大型消费金融公司' then count else null end) as r47580354_industry_6_ct
,sum(case when rule_id = '47580354' and industry_display_name='综合类电商平台' then count else null end) as r47580354_industry_7_ct
,sum(case when rule_id = '47580354' and industry_display_name='汽车租赁' then count else null end) as r47580354_industry_8_ct
,sum(case when rule_id = '47580354' and industry_display_name='P2P网贷' then count else null end) as r47580354_industry_9_ct
,sum(case when rule_id = '47580354' and industry_display_name='信息中介' then count else null end) as r47580354_industry_10_ct
,sum(case when rule_id = '47580354' and industry_display_name='设备租赁' then count else null end) as r47580354_industry_11_ct
,sum(case when rule_id = '47580354' and industry_display_name='财产保险' then count else null end) as r47580354_industry_12_ct
,sum(case when rule_id = '47580354' and industry_display_name='互联网金融门户' then count else null end) as r47580354_industry_13_ct
,sum(case when rule_id = '47580354' and industry_display_name='第三方服务商' then count else null end) as r47580354_industry_14_ct
,sum(case when rule_id = '47580354' and industry_display_name='一般消费分期平台' then count else null end) as r47580354_industry_15_ct
,sum(case when rule_id = '47580354' and industry_display_name='厂商汽车金融' then count else null end) as r47580354_industry_16_ct
,sum(case when rule_id = '47580354' and industry_display_name='银行个人业务' then count else null end) as r47580354_industry_17_ct
,sum(case when rule_id = '47580354' and industry_display_name='小额贷款公司' then count else null end) as r47580354_industry_18_ct
,sum(case when rule_id = '47580354' and industry_display_name='O2O' then count else null end) as r47580354_industry_19_ct
,sum(case when rule_id = '47580354' and industry_display_name='房地产金融' then count else null end) as r47580354_industry_20_ct
,sum(case when rule_id = '47580354' and industry_display_name='融资租赁' then count else null end) as r47580354_industry_21_ct
,sum(case when rule_id = '47580354' and industry_display_name='理财机构' then count else null end) as r47580354_industry_22_ct
,sum(case when rule_id = '47580354' and industry_display_name='信用卡中心' then count else null end) as r47580354_industry_23_ct
,sum(case when rule_id = '47580354' and industry_display_name='第三方支付' then count else null end) as r47580354_industry_24_ct
from (
	select t1.order_no, rule_id
	,json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_count' as platform_count
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail' as json)) ::json ->> 'industry_display_name' as industry_display_name
	,cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail' as json)) ::json ->> 'count' as int) as count
	from (select * from t_loan_performance 
		  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15')t1 
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580344', '47580334', '47580324', '47580354')
) tmp
group by order_no
"""
td_score_data = get_df_from_pg(td_duotou_loan_sql)
td_duotou_data = td_score_data.copy()
td_duotou_data.shape
td_duotou_data.order_no.nunique()

---命中黑名单---

td_blacklist_loan_sql = """
select order_no
,sum(case when rule_id='47579484' then 1 else null end) as r47579484_flag
,sum(case when rule_id='47579284' then 1 else null end) as r47579284_flag
,sum(case when rule_id='47579464' then 1 else null end) as r47579464_flag
,sum(case when rule_id='47579544' then 1 else null end) as r47579544_flag
,sum(case when rule_id='47580474' and fraud_type='agency' then 1 else null end) as r47580474_agency
,sum(case when rule_id='47580474' and fraud_type='consumercreditRisk' then 1 else null end) as r47580474_consumercreditRisk
,sum(case when rule_id='47580474' and fraud_type='creditSuspicious' then 1 else null end) as r47580474_creditSuspicious
,sum(case when rule_id='47580474' and fraud_type='suspiciousLoan' then 1 else null end) as r47580474_suspiciousLoan
,sum(case when rule_id='47580484' and fraud_type='agency' then 1 else null end) as r47580484_agency
,sum(case when rule_id='47580484' and fraud_type='consumercreditRisk' then 1 else null end) as r47580484_consumercreditRisk
,sum(case when rule_id='47580484' and fraud_type='creditSuspicious' then 1 else null end) as r47580484_creditSuspicious
,sum(case when rule_id='47580484' and fraud_type='suspiciousLoan' then 1 else null end) as r47580484_suspiciousLoan
--rule_id, fraud_type, risk_level, count(*)
from (
	select t1.order_no, rule_id, risk_name
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'grey_list_details' as json)) ::json ->> 'fraud_type' as fraud_type
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'grey_list_details' as json)) ::json ->> 'risk_level' as risk_level
	,json_array_elements(cast(risk_detail as json)) ::json ->> 'fraud_type_display_name' as fraud_type_display_name
	from (select * from t_loan_performance 
		  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15')t1
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580474','47580484','47579484','47579284','47579464','47579544')
) tmp
group by order_no
"""
td_blacklist_data = get_df_from_pg(td_blacklist_loan_sql)
td_blacklist_data = td_blacklist_data.drop_duplicates()
td_blacklist_data.shape
td_blacklist_data.order_no.nunique()



-- 关联亲属---
td_relation_loan_sql = """
 select order_no
 , sum(case when rule_id = '47580234' then result else null end) as r47580234_result
 , sum(case when rule_id = '47580254' then result else null end) as r47580254_result
 , sum(case when rule_id = '47580274' then result else null end) as r47580274_result
 from (
 select t1.order_no, rule_id, risk_detail
 ,cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'result' as int) as result
 from (select * from t_loan_performance 
		  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15')t1
 inner join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
 inner join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580234','47580254','47580274')
   ) tmp
 group by order_no
"""
td_relation_data = get_df_from_pg(td_relation_loan_sql)
td_relation_data = td_relation_data.drop_duplicates()
td_relation_data.shape
td_relation_data.order_no.nunique()

set(td_blacklist_data.order_no) - set(td_relation_data.order_no)

-- 模糊名单---4var
td_list_loan_sql = """
select order_no
,sum(case when rule_id = '47579864' then 1 else null end) as r47579864_flag
,sum(case when rule_id = '47579884' then 1 else null end) as r47579884_flag
,sum(case when rule_id = '47579834' then 1 else null end) as r47579834_flag
,sum(case when rule_id = '47579804' then 1 else null end) as r47579804_flag
from (
	select t1.order_no, rule_id, risk_detail
	from (select * from t_loan_performance 
		  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t1
	inner join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	inner join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47579864','47579884','47579834','47579804')
) tmp
group by order_no
"""
td_list_data = get_df_from_pg(td_list_loan_sql)

td_list_data = td_list_data.drop_duplicates()
td_list_data.shape
td_list_data.order_no.nunique()


----多个关联---6 var
td_connect_loan_sql = """
select order_no
,sum(case when rule_id = '47580034' and sub_dim_type='accountMobile' then count else null end) as r47580034_ct
,sum(case when rule_id = '47580054' and sub_dim_type='idNumber' then count else null end) as r47580054_ct
,sum(case when rule_id = '47580154' then count else null end) as r47580154_ct
,sum(case when rule_id = '47580194' and note='1天内身份证关联设备数' then count else null end) as r47580194_ct
,sum(case when rule_id = '47580204' and note='7天内身份证关联设备数' then count else null end) as r47580204_ct
,sum(case when rule_id = '47580214' and note='1个月内身份证关联设备数' then count else null end) as r47580214_ct
from (
	select t1.order_no, rule_id, risk_detail
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'frequency_detail_list' as json)) ::json ->> 'note' as note
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'frequency_detail_list' as json)) ::json ->> 'dim_type' as dim_type
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'frequency_detail_list' as json)) ::json ->> 'sub_dim_type' as sub_dim_type
	,cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'frequency_detail_list' as json)) ::json ->> 'count' as int) as count
	from (select * from t_loan_performance 
		  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t1
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580194','47580204','47580214','47580054','47580034','47580154')
) tmp
group by order_no
"""
td_connect_data = get_df_from_pg(td_connect_loan_sql)

td_connect_data = td_connect_data.drop_duplicates()
td_connect_data.shape
td_connect_data.order_no.nunique()

---高风险地区---1
td_highriskarea_loan_sql = """
select order_no
,sum(case when rule_id is not null then 1 else null end) as r47579154_flag
from (
	select t1.order_no, rule_id, risk_detail
	from (select * from t_loan_performance 
		  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t1 
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47579154')
) tmp
group by order_no
"""

td_highr_data = get_df_from_pg(td_highriskarea_loan_sql)

td_highr_data = td_highr_data.drop_duplicates()
td_highr_data.shape
td_highr_data.order_no.nunique()
td_highr_data.isnull().sum()

---一二阶联系人---
td_contact_loan_sql = """
select order_no
,sum(case when rule_id='47580084' and note='3个月内申请人身份证作为第一联系人身份证出现的次数' then count else null end) as r47580084_1
,sum(case when rule_id='47580084' and note='3个月内申请人身份证作为第二联系人身份证出现的次数' then count else null end) as r47580084_2
,sum(case when rule_id='47580094' and note='3个月内申请人手机号作为第一联系人手机号出现的次数' then count else null end) as r47580094_1
,sum(case when rule_id='47580094' and note='3个月内申请人手机号作为第二联系人手机号出现的次数' then count else null end) as r47580094_2
from (
	select t1.order_no, rule_id, risk_detail,
	json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'cross_frequency_detail_list' as json)) ::json ->>'note' as note
	,cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'cross_frequency_detail_list' as json)) ::json ->>'count' as int) as count
	from (select * from t_loan_performance 
		  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t1 
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580094','47580084')
) tmp
group by order_no
"""

td_contact_data = get_df_from_pg(td_contact_loan_sql)

td_contact_data = td_contact_data.drop_duplicates()
td_contact_data.shape
td_contact_data.order_no.nunique()
td_contact_data.isnull().sum()


---分数---
td_score_loan_sql = """
select t1.order_no
,max(final_score) final_score
,max(case when rule_id = '47580344' then score end) r47580344_score
,max(case when rule_id = '47580334' then score end) r47580334_score
,max(case when rule_id = '47580324' then score end) r47580324_score
,max(case when rule_id = '47580474' then score end) r47580474_score
,max(case when rule_id = '47580354' then score end) r47580354_score
,max(case when rule_id = '47580484' then score end) r47580484_score
,max(case when rule_id = '47580034' then score end) r47580034_score
,max(case when rule_id = '47580094' then score end) r47580094_score
,max(case when rule_id = '47580044' then score end) r47580044_score
,max(case when rule_id = '47580234' then score end) r47580234_score
,max(case when rule_id = '47580054' then score end) r47580054_score
,max(case when rule_id = '47580254' then score end) r47580254_score
,max(case when rule_id = '47579154' then score end) r47579154_score
,max(case when rule_id = '47579484' then score end) r47579484_score
,max(case when rule_id = '47580194' then score end) r47580194_score
,max(case when rule_id = '47579864' then score end) r47579864_score
,max(case when rule_id = '47579734' then score end) r47579734_score
,max(case when rule_id = '47579524' then score end) r47579524_score
,max(case when rule_id = '47579544' then score end) r47579544_score
,max(case when rule_id = '47580204' then score end) r47580204_score
,max(case when rule_id = '47580274' then score end) r47580274_score
,max(case when rule_id = '47579464' then score end) r47579464_score
,max(case when rule_id = '47579564' then score end) r47579564_score
,max(case when rule_id = '47579884' then score end) r47579884_score
,max(case when rule_id = '47580214' then score end) r47580214_score
,max(case when rule_id = '47579284' then score end) r47579284_score
,max(case when rule_id = '47580014' then score end) r47580014_score
,max(case when rule_id = '47579834' then score end) r47579834_score
,max(case when rule_id = '47580084' then score end) r47580084_score
,max(case when rule_id = '47579804' then score end) r47579804_score
,max(case when rule_id = '47579994' then score end) r47579994_score
,max(case when rule_id = '47580224' then score end) r47580224_score
,max(case when rule_id = '47580024' then score end) r47580024_score
,max(case when rule_id = '47579764' then score end) r47579764_score
,max(case when rule_id = '47580154' then score end) r47580154_score
,max(case when rule_id = '47579714' then score end) r47579714_score
--,max(case when rule_id = '47580344' then policy_score end) r47580344_policy_score
--,max(case when rule_id = '47580334' then policy_score end) r47580334_policy_score
--,max(case when rule_id = '47580324' then policy_score end) r47580324_policy_score
--,max(case when rule_id = '47580474' then policy_score end) r47580474_policy_score
--,max(case when rule_id = '47580354' then policy_score end) r47580354_policy_score
--,max(case when rule_id = '47580484' then policy_score end) r47580484_policy_score
--,max(case when rule_id = '47580034' then policy_score end) r47580034_policy_score
--,max(case when rule_id = '47580094' then policy_score end) r47580094_policy_score
--,max(case when rule_id = '47580044' then policy_score end) r47580044_policy_score
--,max(case when rule_id = '47580234' then policy_score end) r47580234_policy_score
--,max(case when rule_id = '47580054' then policy_score end) r47580054_policy_score
--,max(case when rule_id = '47580254' then policy_score end) r47580254_policy_score
--,max(case when rule_id = '47579154' then policy_score end) r47579154_policy_score
--,max(case when rule_id = '47579484' then policy_score end) r47579484_policy_score
--,max(case when rule_id = '47580194' then policy_score end) r47580194_policy_score
--,max(case when rule_id = '47579864' then policy_score end) r47579864_policy_score
--,max(case when rule_id = '47579734' then policy_score end) r47579734_policy_score
--,max(case when rule_id = '47579524' then policy_score end) r47579524_policy_score
--,max(case when rule_id = '47579544' then policy_score end) r47579544_policy_score
--,max(case when rule_id = '47580204' then policy_score end) r47580204_policy_score
--,max(case when rule_id = '47580274' then policy_score end) r47580274_policy_score
--,max(case when rule_id = '47579464' then policy_score end) r47579464_policy_score
--,max(case when rule_id = '47579564' then policy_score end) r47579564_policy_score
--,max(case when rule_id = '47579884' then policy_score end) r47579884_policy_score
--,max(case when rule_id = '47580214' then policy_score end) r47580214_policy_score
--,max(case when rule_id = '47579284' then policy_score end) r47579284_policy_score
--,max(case when rule_id = '47580014' then policy_score end) r47580014_policy_score
--,max(case when rule_id = '47579834' then policy_score end) r47579834_policy_score
--,max(case when rule_id = '47580084' then policy_score end) r47580084_policy_score
--,max(case when rule_id = '47579804' then policy_score end) r47579804_policy_score
--,max(case when rule_id = '47579994' then policy_score end) r47579994_policy_score
--,max(case when rule_id = '47580224' then policy_score end) r47580224_policy_score
--,max(case when rule_id = '47580024' then policy_score end) r47580024_policy_score
--,max(case when rule_id = '47579764' then policy_score end) r47579764_policy_score
--,max(case when rule_id = '47580154' then policy_score end) r47580154_policy_score
--,max(case when rule_id = '47579714' then policy_score end) r47579714_policy_score
from (select * from t_loan_performance 
	  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t1
left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid
group by t1.order_no
"""
td_score_data = get_df_from_pg(td_score_loan_sql)

td_score_data = td_score_data.drop_duplicates()
td_score_data.shape
td_score_data.order_no.nunique()
td_score_data.isnull().sum()


---同盾小额现金贷分---

# td_score_sql = """
# select t0.order_no,t2.micro_cash_score
# from (select * from t_loan_performance
# 	  where dt = '20190829' and business_id in ('xjbk','tb')
# 	        and effective_date between '2018-08-26' and '2019-08-15') t0
# inner join (select distinct orderno, messageno
# 		   from risk_mongo_installmentmessagerelated
# 		   where topicname in ('Application_thirdPart','Application_thirdPart_tongduncash') and databasename = 'installmentTongdunCash'
# 		   	     and businessid in ('bk','xjbk','tb')
#            ) t1 on t0.order_no = t1.orderno
# left join (select * from bakrt_tongdun_cashscore_result
# 		   where businessid in ('bk','xjbk','tb','xjm')) t2 on t1.messageno = t2.taskid
# """
td_microscore_loan_sql = """
with related as (
select distinct orderno, messageno
from risk_mongo_installmentmessagerelated 
where topicname in ('Application_thirdPart','Application_thirdPart_tongduncash') and databasename = 'installmentTongdunCash'
and businessid in ('bk','xjbk','tb')
),
score as (select * from bakrt_tongdun_cashscore_result
		   where businessid in ('bk','xjbk','tb','xjm'))
select t0.order_no,t0.effective_date
, t2.micro_cash_score
from (select * from t_loan_performance 
	  where dt = '20190829' and business_id in ('xjbk','tb')
	  and effective_date between '2018-08-26' and '2019-08-15') t0
left join related t1 on t0.order_no = t1.orderno
left join score t2 on t1.messageno = t2.taskid
"""
td_micscore_data = get_df_from_pg(td_score_sql)
td_micscore_data = td_micscore_data.drop_duplicates('order_no')

td_micscore_data.shape
td_micscore_data.order_no.nunique()
td_micscore_data.order_no.value_counts()
td_micscore_data.loc[td_micscore_data.order_no == '155607537012442114']

td_micscore_data.to_csv(os.path.join(data_path,'td_microscore.csv'))
td_micscore_data.isnull().sum()

####合并同盾分&同盾变量##########
td_microscore_data = pd.read_csv(os.path.join(data_path,'td_microscore.csv'))
td_microscore_data.shape
td_microscore_data.columns

#变量
td_duotou_data.shape
td_blacklist_data.shape
td_relation_data.shape
td_list_data.shape
td_connect_data.shape
td_highr_data.shape
td_contact_data.shape
td_score_data.shape

td_var = td_microscore_data.merge(td_duotou_data, on = 'order_no', how = 'left').merge(td_blacklist_data, on = 'order_no', how = 'left').merge(td_relation_data, on = 'order_no', how = 'left')\
							  .merge(td_list_data, on = 'order_no', how = 'left').merge(td_connect_data, on = 'order_no', how = 'left').merge(td_highr_data, on = 'order_no', how = 'left')\
							  .merge(td_contact_data, on = 'order_no', how = 'left').merge(td_score_data, on = 'order_no', how = 'left')
td_var.shape
td_var.to_csv(os.path.join(data_path,'td_var_micscore.csv'))



------------------------------------信德黑名单--------------------------------------------------
xd_loan_sql = """
with ad_result as (
select customerid
,taskid
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{advanceResult}' as json))::json ->>'overDue90ContactsCount' as overDue90ContactsCount
, date(createtime) as createtime
from bakrt_xinde
where businessid in ('tb','xjbk')
),
nor_result as (
select customerid
,taskid
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'firstLoanTime' as firstLoanTime
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'totalLoanCount' as totalLoanCount
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'totalOverDueCount' as totalOverDueCount
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'longestOverDuePaid' as longestOverDuePaid
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'currentOverDueCount' as currentOverDueCount
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'currentOverDueAmount' as currentOverDueAmount
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'longestOverDueUnpaid' as longestOverDueUnpaid
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'isLastLoanRefused' as isLastLoanRefused
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'lastLoanRefuseReason' as lastLoanRefuseReason
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'lastLoanRefuseTime' as lastLoanRefuseTime
, json_array_elements(cast(cast(oss::json #>>'{result}' as json):: json #>>'{result}' as json))::json ->>'hitFlag' as hitFlag
, date(createtime) as createtime
from bakrt_xinde 
where businessid in ('tb','xjbk')
),
apply as (
select order_no, date(created_time) as apply_time from dw_bk_bk_core_bk_core_application
union 
select order_no, date(created_time) as apply_time from  dw_dc_compensatory_cp_core_application
)
select distinct(t0.order_no)
, customer_id
, a.apply_time
,overDue90ContactsCount
,case when firstLoanTime = '' then -9999
	  else a.apply_time - date(firstLoanTime) 
	  end as first_loan_diff
,totalLoanCount
,totalOverDueCount
,longestOverDuePaid
,currentOverDueCount
,currentOverDueAmount
,longestOverDueUnpaid
,isLastLoanRefused
--,lastLoanRefuseTime
,case when isLastLoanRefused is null then null
	  when isLastLoanRefused = 'true' then 1
	  else 0
	  end as isLastLoanRefused_flag
,lastLoanRefuseReason
,case when lastLoanRefuseTime = '' or lastLoanRefuseTime is null then -9999
      else round((date(apply_time) - date(concat(substring(lastLoanRefuseTime,1,4),'-',substring(lastLoanRefuseTime,5,7),'-','01')))/30,1)
      end as lastloan_refuse_difff
,hitFlag
,t3.createtime
from (select * from t_loan_performance 
		  where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15'
	    ) t0
left join apply a on t0.order_no = a.order_no
left join (select * 
		   from risk_mongo_installmentmessagerelated 
		   where businessid in ('xjbk','bk','tb') and topicname in ('Application_thirdPart_xdblacklist','Application_thirdPart_xinde_blacklist')
		   ) t1 on t0.order_no = t1.orderno
left join nor_result t2 on t1.messageno = t2.taskid
left join ad_result t3 on t1.messageno = t3.taskid
"""
xd_data = get_df_from_pg(xd_loan_sql)
xd_data = xd_data.drop_duplicates()


xd_data.order_no.nunique()

a = xd_data.order_no.value_counts().reset_index()
muti_list = list(a.loc[a.order_no >1]['index'].unique())

#54条数据有多条数据记录

xd_data2 = xd_data.loc[~xd_data.order_no.isin(muti_list)]
xd_data2.to_csv(os.path.join(data_path, 'xd_data_rm_multipler.csv'))
xd_data.to_csv(os.path.join(data_path, 'xd_data_original.csv'))



----------------------------------富数------------------------------------
loan_fuju_sql = """
with fuju as (
select taskid
, cast(oss::json #>>'{result}' as json) #>>'{data,score}' as fuju_score
from public.risk_oss_fuju
where businessid in ('bk','xjbk','tb','xjm')
)
select t0.order_no
, t0.effective_date
, t1.orderno as related_orderno
, t1.messageno as related_ms
, t1.taskid as related_taskid
, t2.taskid
, fuju_score
--from dw_bk_bk_core_bk_core_application t0
from (select * from t_loan_performance 
	  where dt = '20190829' and business_id in ('xjbk','tb')
	        and effective_date between '2018-08-26' and '2019-08-15') t0
left join (select * 
		   from risk_mongo_installmentmessagerelated 
		   where businessid in ('xjbk','bk','tb') and topicname in ('Application_thirdPart_fushufuju')
		   ) t1 on t0.order_no = t1.orderno
left join fuju t2 on t1.messageno = t2.taskid
--where date(t0.created_time) between '2018-09-26' and '2019-08-15'
"""
loan_fuju_data = get_df_from_pg(loan_fuju_sql)
loan_fuju_data = loan_fuju_data.drop_duplicates()

loan_fuju_data.order_no.nunique(); loan_fuju_data.shape
loan_fuju_data.isnull().sum()

loan_fuju_data.loc[loan_fuju_data.related_orderno.isnull()].effective_date.min()
loan_fuju_data.loc[loan_fuju_data.related_orderno.isnull()].effective_date.max()

loan_fuju_data.loc[loan_fuju_data.effective_date == '2019-08-12']
loan_fuju_data.to_csv(os.path.join())
loan_fuju_data.groupby(['effective_date'])['fuju_score'].count()

dc_fuju_sql = """
with fuju as (
select taskid
, cast(oss::json #>>'{result}' as json) #>>'{data,score}' as fuju_score
from public.risk_oss_fuju
where businessid in ('tb','xjm')
)
select t0.order_no
, date(t0.created_time) as created_time
, t1.orderno
, t1.messagono
, t1.taskid
, t2.taskid
, fuju_score
from dw_dc_compensatory_cp_core_application t0
left join (select * 
		   from risk_mongo_installmentmessagerelated 
		   where businessid in ('xjm','tb') and topicname in ('Application_thirdPart_fushufuju')
		   ) t1 on t0.order_no = t1.orderno
left join fuju t2 on t1.messageno = t2.taskid
where date(t0.created_time) between '2018-09-26' and '2019-08-15'
"""

-------------------------------------魔蝎魔杖----------------------------
mx_loan_sql = """
with mx as (
select * 
from bakrt_mozhang_result 
where businessid in ('tb','xjbk'))
, 
related as (
select orderno, messageno
from risk_mongo_installmentmessagerelated
where businessid in ('xjbk','bk','tb') and topicname in ('Application_thirdPart_mozhang') and databasename = 'installmentMozhang'
)
select t0.order_no, t0.effective_date, t2.*
from  (select * from t_loan_performance 
	   where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t0
left join related t1 on t0.order_no = t1.orderno
left join mx t2 on t1.messageno = t2.taskid
"""
mx_data = get_df_from_pg(mx_loan_sql)
mx_data = mx_data.drop_duplicates()
mx_data.order_no.value_counts()
mx_data.loc[mx_data.order_no.isin(['155607537012442114','155170737764698115'])]
mx_data = mx_data.drop([3638,3771])
mx_data.order_no.nunique()
mx_data = mx_data.drop(['appid','bucket','businessid','createtime','customerid','ossid','taskid','thirdtype','updatetime','success','code','msg',
			  'fee'],1)
mx_data.loc[mx_data.mz_score.isnull()].effective_date.value_counts().sort_index() #7/10之后有8条缺失，其他都为没有调用该数据源
mx_data.to_csv(os.path.join(data_path,'mx.csv'))

var_dict = pd.read_excel('D:/Model/201908_qsfq_model/00_sample_analysis/可用数据源统计/轻松分期数据整理draft 20190823.xlsx', sheet_name = '变量字典')
var_dict.columns
mx_cols = var_dict.loc[var_dict.数据源.isin(['魔蝎_关注名单','魔蝎_借记卡','魔蝎_信用卡','魔蝎_芝麻分','魔蝎_淘宝','魔蝎_多头','魔蝎_朋友圈','魔杖分']),'指标英文']
len(mx_cols)

set(mx_cols) - set(mx_data.columns)
set(mx_data.columns) - set(mx_cols)


-------------------------------------宜信---------------------------------------------
# bk_yx_sql = """
# select
# --count(t0.order_no), count(t1.orderno),count(t1.messageno), count(t3.taskid)
# --t0.order_no
# --,date(t0.created_time) as created_time
# --,compositescore
# date(t0.created_time) as created_time
# , count(t3.taskid)
# from public.dw_bk_bk_core_bk_core_application t0
# left join (select *
# 		   from public.risk_mongo_installmentmessagerelated
# 		   where topicname in ('Application_thirdPart','Application_thirdPart_yxaf') and businessid in ('xjbk','bk')
# 		   ) t1 on t0.order_no = t1.orderno
# left join bakrt_yixin t3 on t1.messageno = t3.taskid
# group by date(t0.created_time)
# order by date(t0.created_time) desc
# """

yx_loan_sql = """
with related as (
select distinct orderno, messageno
from risk_mongo_installmentmessagerelated 
where topicname in ('Application_thirdPart') and databasename = 'installmentYXComposite'
and businessid in ('bk','xjbk','tb')
),
score as (select distinct taskid,compositescore from bakrt_yixin
		   where businessid in ('bk','xjbk','tb','xjm'))
select t0.order_no,t0.effective_date
, t2.compositescore
from (select * from t_loan_performance 
	  where dt = '20190829' and business_id in ('xjbk','tb')
	  and effective_date between '2018-08-26' and '2019-08-15') t0
left join related t1 on t0.order_no = t1.orderno
left join score t2 on t1.messageno = t2.taskid
"""
data_path = 'D:/Model/201908_qsfq_model/01_data/'
yx_loan_data = get_df_from_pg(yx_loan_sql)
yx_loan_data = yx_loan_data.drop_duplicates()

yx_loan_data.shape
yx_loan_data.order_no.nunique()
yx_loan_data.order_no.value_counts()
yx_loan_data.loc[yx_loan_data.order_no == '155607537012442114']
yx_loan_data = yx_loan_data.drop(3281)
yx_loan_data.to_csv(os.path.join(data_path,'yixin_score.csv'))
yx_loan_data.isnull().sum()

----------------------------------百融------------------------------------
----百融压力偿债指数----
br_stress_loan_sql = """
select t0.order_no
, cast(oss::json #>> '{result}' as json)::json #>>'{DebtRepayStress,nodebtscore}' as br_nodebtscore
from (select * from t_loan_performance where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t0
left join (select *
			from risk_mongo_installmentmessagerelated
			where businessid in ('bk','xjbk') and topicname in ('Application_thirdPart_bairongdebtrepaystress')
			and databasename = 'installmentBairongDebtRepayStress'
			)t1 on t0.order_no = t1.orderno
left join risk_oss_bairong_debt_repay_stress t2 on t1.messageno = t2.taskid
"""
br_stress_data = get_df_from_pg(br_stress_loan_sql)
br_stress_data.isnull().sum()
br_stress_data.shape
br_stress_data.order_no.nunique()
8503-8127

br_fraud_loan_sql = """
select t0.order_no
, cast(oss::json #>> '{result}' as json)::json #>>'{FraudRelation_g,list_level}' as br_frg_list_level
from (select * from t_loan_performance where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t0
left join (select *
			from risk_mongo_installmentmessagerelated
			where businessid in ('bk','xjbk') and topicname in ('Application_thirdPart_bairongfraudrelation')
			and databasename = 'installmentBairongFraudRelation'
			)t1 on t0.order_no = t1.orderno
left join risk_oss_bairong_fraud_relation t2 on t1.messageno = t2.taskid
"""
br_fraud_data = get_df_from_pg(br_fraud_loan_sql)

br_fraud_data.isnull().sum()
br_fraud_data.shape
br_fraud_data.order_no.nunique()
8503-7578

#####合并百融数据, 百融loanbefore代码部分见03_数据解析_百融#########
br_data = br_fraud_data.merge(br_stress_data, on = 'order_no', how = 'left' ).merge(br_loanbf_df2, on = 'order_no', how = 'left')
# br_stress_data
# br_loanbf_df
br_data.to_csv(os.path.join(data_path,'br_var.csv'))
天御

----------------------------聚信立 & 新颜 & 数美 & 天启 & 腾讯 -----------------------------------
#该5个数据源调用时间从2018年开始

----------新颜-----------
bk_xy_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no 
from bk_sample_add
),
apply as (
select order_no, date(created_time) as apply_time
from dw_bk_bk_core_bk_core_application
where date(created_time) between '2018-08-26' and '2019-08-15'
)
select t0.order_no
, t1.apply_time
, latest_six_month_xinyan as latest_six_month
, history_fail_fee_xinyan as history_fail_fee
, latest_three_month_xinyan as latest_three_month
, latest_one_month_fail_xinyan as latest_one_month_fail
, latest_one_month_suc_xinyan as latest_one_month_suc
, history_suc_fee_xinyan as history_suc_fee
, loans_credibility_xinyan as loans_credibility
, loans_score_xinyan as loans_score
, loans_settle_count_xinyan as loans_settle_count
, loans_count_xinyan as loans_count
, loans_long_time_xinyan as loans_long_time
, consfin_org_count_xinyan as consfin_org_count
, loans_cash_count_xinyan as loans_cash_count
, latest_one_month_xinyan as latest_one_month
--, loans_latest_time_xinyan as loans_latest_timediff
, loans_org_count_xinyan as loans_org_count
, loans_overdue_count_xinyan as loans_overdue_count      
, case when loans_latest_time_xinyan in ('-9999','-9998') then -9999
	  else t1.apply_time - date(loans_latest_time_xinyan) 
	  end as loans_latest_timediff
from sample t0
left join  apply t1 on t0.order_no = t1.order_no
left join bk_xinyan_result t2 on t0.order_no = t2.order_no
"""
bk_xinyan_data = get_df_from_pg(bk_xy_sql)
bk_xinyan_data.shape #3536
bk_xinyan_data.order_no.nunique()
bk_xinyan_data.isnull().sum()
bk_xinyan_data.to_csv(os.path.join(data_path,'bk_xinyan.csv'))

#dc的新颜数据只取新增样本

dc_xy_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('tb') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no 
from dc_sample_add
),
apply as (
select order_no, date(created_time) as apply_time
from dw_dc_compensatory_cp_core_application
where date(created_time) between '2018-08-26' and '2019-08-15'
),
xy as (
select order_no
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_count}', oss::json #>> '{data,report_detail,loans_count}') as loans_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_long_time}', oss::json #>> '{data,report_detail,loans_long_time}') as loans_long_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,consfin_org_count}', oss::json #>> '{data,report_detail,consfin_org_count}') as consfin_org_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_cash_count}', oss::json #>> '{data,report_detail,loans_cash_count}') as loans_cash_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_six_month}', oss::json #>> '{data,report_detail,latest_six_month}') as latest_six_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,history_fail_fee}', oss::json #>> '{data,report_detail,history_fail_fee}') as history_fail_fee
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_three_month}', oss::json #>> '{data,report_detail,latest_three_month}') as latest_three_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month_fail}', oss::json #>> '{data,report_detail,latest_one_month_fail}') as latest_one_month_fail
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month}', oss::json #>> '{data,report_detail,latest_one_month}') as latest_one_month
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,latest_one_month_suc}', oss::json #>> '{data,report_detail,latest_one_month_suc}') as latest_one_month_suc
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_latest_time}', oss::json #>> '{data,report_detail,loans_latest_time}') as loans_latest_time
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_org_count}', oss::json #>> '{data,report_detail,loans_org_count}') as loans_org_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,history_suc_fee}', oss::json #>> '{data,report_detail,history_suc_fee}') as history_suc_fee
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_credibility}', oss::json #>> '{data,report_detail,loans_credibility}') as loans_credibility
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_score}', oss::json #>> '{data,report_detail,loans_score}') as loans_score
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_settle_count}', oss::json #>> '{data,report_detail,loans_settle_count}') as loans_settle_count
, coalesce(oss::json #>> '{t,behavior_data,data,report_detail,loans_overdue_count}', oss::json #>> '{data,report_detail,loans_overdue_count}') as loans_overdue_count
from dc_xinyan_application  
where oss <>'' 
)
select t0.order_no
, t1.apply_time
, loans_count
, loans_long_time
, consfin_org_count
, loans_cash_count
, latest_six_month
, history_fail_fee
, latest_three_month
, latest_one_month_fail
, latest_one_month
, latest_one_month_suc
, loans_org_count
, history_suc_fee
, loans_credibility
, loans_score
, loans_settle_count
, loans_overdue_count 
--, loans_latest_time
, case when loans_latest_time in ('-9999','-9998') then -9999
	  else t1.apply_time - date(loans_latest_time) 
	  end as loans_latest_timediff
from sample t0
left join  apply t1 on t0.order_no = t1.order_no
left join xy t2 on t0.order_no = t2.order_no
"""
dc_xinyan_data = get_df_from_pg(dc_xy_sql)

dc_xinyan_data.shape #12644
dc_xinyan_data.order_no.nunique()
dc_xinyan_data.isnull().sum()
18134-10949 #匹配率较低，需要double check
dc_xinyan_data.head()
dc_xinyan_data.to_csv(os.path.join(data_path,'dc_xinyan.csv'))


----------------------------------聚信立------------------------------------
bk_jxl_sql = """
select l.order_no
,phone_gray_score 
,social_liveness 
,case when recent_active_time='-9999' then -9999 else date(jxl.created_time) - date(recent_active_time) end as recent_active_time_diff
,social_influence
,most_familiar_to_all
,most_familiar_be_all 
,most_familiar_all
,most_familiar_to_applied 
,most_familiar_be_applied 
,most_familiar_applied 
,to_max
,to_mean
,to_min
,be_max
,be_mean
,be_min
,max
,mean
,min
,to_is_familiar
,to_median_familiar
,to_not_familiar
,be_is_familiar
,be_median_familiar
,be_not_familiar
,is_familiar
,median_familiar
,not_familiar
,case when recent_time_to_all='-9999' then -9999 else date(jxl.created_time) - date(recent_time_to_all) end as recent_time_to_all_diff
,case when recent_time_be_all='-9999' then -9999 else date(jxl.created_time) - date(recent_time_be_all) end as recent_time_be_all_diff
,case when recent_time_to_black='-9999' then -9999 else date(jxl.created_time) - date(recent_time_to_black) end as recent_time_to_black_diff
,case when recent_time_be_black='-9999' then -9999 else date(jxl.created_time) - date(recent_time_be_black) end as recent_time_be_black_diff
,case when recent_time_to_applied='-9999' then -9999 else date(jxl.created_time) - date(recent_time_to_applied) end as recent_time_to_applied_diff
,case when recent_time_be_applied='-9999' then -9999 else date(jxl.created_time) - date(recent_time_be_applied) end as recent_time_be_applied_diff
,call_cnt_to_all  
,call_cnt_be_all  
,call_cnt_to_black  
,call_cnt_be_black 
,call_cnt_to_applied
,call_cnt_be_applied
,time_spent_to_all
,time_spent_be_all
,time_spent_to_black 
,time_spent_be_black
,time_spent_to_applied 
,time_spent_be_applied 
,cnt_to_all
,cnt_be_all
,cnt_all 
,cnt_router
,router_ratio
,cnt_to_black
,cnt_be_black
,cnt_black
,black_ratio
,cnt_black2
,cnt_to_applied
,cnt_be_applied 
,cnt_applied as 
,pct_cnt_to_all 
,pct_cnt_be_all
,pct_cnt_all as 
,pct_cnt_router
,pct_router_ratio
,pct_cnt_to_black 
,pct_cnt_be_black
,pct_cnt_black
,pct_black_ratio
,pct_cnt_black2
,pct_cnt_to_applied 
,pct_cnt_be_applied
,pct_cnt_applied
,case when to_recent_query_time='-9999' then -9999 else date(jxl.created_time) - date(to_recent_query_time) end as to_recent_query_time_diff
,case when be_recent_query_time='-9999' then -9999 else date(jxl.created_time) - date(be_recent_query_time) end as be_recent_query_time_diff
,to_query_cnt_05 
,be_query_cnt_05 
,query_cnt_05 
,to_query_cnt_1 
,be_query_cnt_1 
,query_cnt_1
,to_query_cnt_2
,be_query_cnt_2
,query_cnt_2
,to_query_cnt_3
,be_query_cnt_3
,query_cnt_3
,to_query_cnt_6 
,be_query_cnt_6
,query_cnt_6 
,to_query_cnt_9
,be_query_cnt_9
,query_cnt_9
,to_query_cnt_12
,be_query_cnt_12
,query_cnt_12
,to_org_cnt_05
,be_org_cnt_05
,org_cnt_05 
,to_org_cnt_1
,be_org_cnt_1 
,org_cnt_1
,to_org_cnt_2
,be_org_cnt_2 
,org_cnt_2 
,to_org_cnt_3 
,be_org_cnt_3 
,org_cnt_3
,to_org_cnt_6 
,be_org_cnt_6 
,org_cnt_6 
,to_org_cnt_9 
,be_org_cnt_9 
,org_cnt_9 
,to_org_cnt_12
,be_org_cnt_12
,org_cnt_12
,weight_to_all
,weight_be_all
,weight_all
,weight_to_black 
,weight_be_black 
,weight_black
,weight_to_applied 
,weight_be_applied 
,weight_applied
,searched_org_cnt 
,register_cnt
,blacklist_update_time_name_idcard
,case when blacklist_name_with_idcard='true' then 1 when blacklist_name_with_idcard='false' then 0 else null end as blacklist_name_with_idcard
,case when blacklist_update_time_name_phone='true' then 1 when blacklist_update_time_name_phone='false' then 0 else null end as blacklist_update_time_name_phone
,case when blacklist_name_with_phone='true' then 1 when blacklist_name_with_phone='false' then 0 else null end as blacklist_name_with_phone
,d30_iou_platform_cnt 
,total_loan_amount
,overdue_amount
,d360_iou_query_times  
,in_repayment_interest  
,in_repayment_amount
,overdue_payment_times
,overdue_times
,overdue_interest
,in_repayment_times
,d360_iou_platform_cnt
,overdue_payment_interest
,overdue_payment_amount
,d90_iou_platform_cnt
,total_loan_times 
,d90_iou_query_times
,d30_iou_query_times 
,case when phone_with_other_idcards='[]' then 0 when phone_with_other_idcards='-9999' then -9999 else json_array_length(cast(phone_with_other_idcards as json)) end as phone_with_other_idcards_ct 
,case when phone_with_other_names='[]' then 0 when phone_with_other_names='-9999' then -9999 else json_array_length(cast(phone_with_other_names as json)) end as phone_with_other_names_ct 
--from dw_bk_bk_core_bk_core_loan l
from  (select order_no
       from t_loan_performance
       where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
       union
       select order_no 
       from bk_sample_add) l
inner join bk_juxinli_result jxl on l.order_no = jxl.order_no 
"""
bk_jxl_data1 = get_df_from_pg(bk_jxl_sql)
bk_jxl_data1.shape #3536
bk_jxl_data1.order_no.nunique()
bk_jxl_data1.isnull().sum()


------idcard_with_other_phones----
bk_jxl_sql2 = """
select order_no, coalesce(sum(times),0) as jxl_idcard_with_other_phones_times
from (
	select t2.order_no, cast(json_array_elements(
		case when idcard_with_other_phones='[]' then '[null]' 
		when idcard_with_other_phones='-9999' then '[-9999]'  
		else cast(idcard_with_other_phones as json) end
		) ::json ->> 'times'  as int) as times
	--from dw_bk_bk_core_bk_core_loan t1 
	from  (select order_no
       from t_loan_performance
       where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
       union
       select order_no 
       from bk_sample_add) t1
	inner join bk_juxinli_result t2 on t1.order_no = t2.order_no and idcard_with_other_phones<>'-9999'
	) tmp
group by order_no
"""
bk_jxl_data2 = get_df_from_pg(bk_jxl_sql2)
bk_jxl_data2.shape #3536
bk_jxl_data2.order_no.nunique()
bk_jxl_data2.isnull().sum()



---register_orgs_count---
bk_jxl_sql3 = """
select order_no, sum(case when register_orgs_label='贷款' then register_orgs_count else 0 end) as register_orgs_loan_ct,
sum(case when register_orgs_label='理财/贷款' then register_orgs_count else 0 end) as register_orgs_loan_finance_ct,
sum(case when register_orgs_label='理财' then register_orgs_count else 0 end) as register_orgs_finance_ct 
from (
	select t2.order_no, 
	cast(json_array_elements(
		case when register_orgs_statistics='[]' then '[null]' 
		when register_orgs_statistics='-9999' then '[-9999]'  
		else cast(register_orgs_statistics as json) end
		) ::json ->> 'count'  as int) as register_orgs_count,
	json_array_elements(
		case when register_orgs_statistics='[]' then '[null]' 
		when register_orgs_statistics='-9999' then '[-9999]'  
		else cast(register_orgs_statistics as json) end
		) ::json ->> 'label'   as register_orgs_label
	--from dw_bk_bk_core_bk_core_loan t1 
	from  (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from bk_sample_add) t1
	inner join bk_juxinli_result t2 on t1.order_no = t2.order_no and register_orgs_statistics<>'-9999'
	--where t1.effective_date between '2019-07-05' and '2019-08-15'
	) tmp
group by order_no 
"""
bk_jxl_data3 = get_df_from_pg(bk_jxl_sql3)
bk_jxl_data3.shape #3536
bk_jxl_data3.order_no.nunique()
bk_jxl_data3.isnull().sum()


-- user_searched_history_by_orgs---
bk_jxl_sql4 = """
select  order_no
,sum(case when searched_org like '线上%' then 1 else 0 end) as searched_org_online_ct
,sum(case when searched_org like '线下%' then 1 else 0 end) as searched_org_offline_ct
,sum(case when searched_org='其他' then 1 else 0 end) as searched_org_1_ct
,sum(case when searched_org='小额信用贷' then 1 else 0 end) as searched_org_2_ct
,sum(case when searched_org='线上信用卡代还' then 1 else 0 end) as searched_org_3_ct
,sum(case when searched_org='线上信用现金贷' then 1 else 0 end) as searched_org_4_ct
,sum(case when searched_org='线上微额快速贷' then 1 else 0 end) as searched_org_5_ct
,sum(case when searched_org='线上抵押贷款' then 1 else 0 end) as searched_org_6_ct
,sum(case when searched_org='线上消费分期' then 1 else 0 end) as searched_org_7_ct
,sum(case when searched_org='线上租房分期' then 1 else 0 end) as searched_org_8_ct
,sum(case when searched_org='线上经营贷' then 1 else 0 end) as searched_org_9_ct
,sum(case when searched_org='线下供应链贷款' then 1 else 0 end) as searched_org_10_ct
,sum(case when searched_org='线下信用现金贷' then 1 else 0 end) as searched_org_11_ct
,sum(case when searched_org='线下微额快速贷' then 1 else 0 end) as searched_org_12_ct
,sum(case when searched_org='线下抵押贷款' then 1 else 0 end) as searched_org_13_ct
,sum(case when searched_org='线下消费分期' then 1 else 0 end) as searched_org_14_ct
,sum(case when searched_org='线下经营贷' then 1 else 0 end) as searched_org_15_ct
,sum(case when searched_org='贷款中介' then 1 else 0 end) as searched_org_16_ct
from (
	select t2.order_no, json_array_elements(
	case when user_searched_history_by_orgs='[]' then '[null]' 
	when user_searched_history_by_orgs='-9999' then '[-9999]'  
	else cast(user_searched_history_by_orgs as json) end ) ::json ->>'searched_org' searched_org
	--from dw_bk_bk_core_bk_core_loan t1 
	from  (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from bk_sample_add) t1
	inner join bk_juxinli_result t2 on t1.order_no = t2.order_no and user_searched_history_by_orgs<>'-9999'
	) tmp
group by order_no
"""
bk_jxl_data4 = get_df_from_pg(bk_jxl_sql4)
bk_jxl_data4.shape
bk_jxl_data4.order_no.nunique()
bk_jxl_data4.isnull().sum()


-- phone_applied_in_orgs---

bk_jxl_sql5 = """
select  order_no
,count(case when susp_org_type like '线上%' then susp_org_type end) as phone_applied_in_orgs_online_ct
,count(case when susp_org_type like '线下%' then susp_org_type end) as phone_applied_in_orgs_offline_ct
,count(case when susp_org_type in ('其他' , '其它') then susp_org_type end) as phone_applied_in_orgs_1_ct
,count(case when susp_org_type='小额信用贷' then susp_org_type end) as phone_applied_in_orgs_2_ct
,count(case when susp_org_type='线上信用卡代还' then susp_org_type end) as phone_applied_in_orgs_3_ct
,count(case when susp_org_type='线上信用现金贷' then susp_org_type end) as phone_applied_in_orgs_4_ct
,count(case when susp_org_type='线上微额快速贷' then susp_org_type end) as phone_applied_in_orgs_5_ct
,count(case when susp_org_type='线上抵押贷款' then susp_org_type end) as phone_applied_in_orgs_6_ct
,count(case when susp_org_type='线上消费分期' then susp_org_type end) as phone_applied_in_orgs_7_ct
,count(case when susp_org_type='线上租房分期' then susp_org_type end) as phone_applied_in_orgs_8_ct
,count(case when susp_org_type='线上经营贷' then susp_org_type end) as phone_applied_in_orgs_9_ct
,count(case when susp_org_type='线下供应链贷款' then susp_org_type end) as phone_applied_in_orgs_10_ct
,count(case when susp_org_type='线下信用现金贷' then susp_org_type end) as phone_applied_in_orgs_11_ct
,count(case when susp_org_type='线下微额快速贷' then susp_org_type end) as phone_applied_in_orgs_12_ct
,count(case when susp_org_type='线下抵押贷款' then susp_org_type end) as phone_applied_in_orgs_13_ct
,count(case when susp_org_type='线下消费分期' then susp_org_type end) as phone_applied_in_orgs_14_ct
,count(case when susp_org_type='线下经营贷' then susp_org_type end) as phone_applied_in_orgs_15_ct
,count(case when susp_org_type='贷款中介' then susp_org_type end) as phone_applied_in_orgs_16_ct
from (
	select t2.order_no, json_array_elements(
		case when phone_applied_in_orgs='[]' then '[null]' 
		when phone_applied_in_orgs='-9999' then '[-9999]'  
		else cast(phone_applied_in_orgs as json) end
		) ::json ->> 'susp_org_type'  as susp_org_type
	--from dw_bk_bk_core_bk_core_loan t1 
	from  (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from bk_sample_add) t1
	inner join bk_juxinli_result t2 on t1.order_no = t2.order_no and phone_applied_in_orgs<>'-9999'
	) tmp
group by order_no
"""

bk_jxl_data5 = get_df_from_pg(bk_jxl_sql5)
bk_jxl_data5.shape
bk_jxl_data5.order_no.nunique()
bk_jxl_data5.isnull().sum()

---idcard_applied_in_orgs---
bk_jxl_sql6 = """
select  order_no
,count(case when susp_org_type like '线上%' then susp_org_type end) as idcard_applied_in_orgs_online_ct
,count(case when susp_org_type like '线下%' then susp_org_type end) as idcard_applied_in_orgs_offline_ct
,count(case when susp_org_type in ('其他' , '其它') then susp_org_type end) as idcard_applied_in_orgs_1_ct
,count(case when susp_org_type='小额信用贷' then susp_org_type end) as idcard_applied_in_orgs_2_ct
,count(case when susp_org_type='线上信用卡代还' then susp_org_type end) as idcard_applied_in_orgs_3_ct
,count(case when susp_org_type='线上信用现金贷' then susp_org_type end) as idcard_applied_in_orgs_4_ct
,count(case when susp_org_type='线上微额快速贷' then susp_org_type end) as idcard_applied_in_orgs_5_ct
,count(case when susp_org_type='线上抵押贷款' then susp_org_type end) as idcard_applied_in_orgs_6_ct
,count(case when susp_org_type='线上消费分期' then susp_org_type end) as idcard_applied_in_orgs_7_ct
,count(case when susp_org_type='线上租房分期' then susp_org_type end) as idcard_applied_in_orgs_8_ct
,count(case when susp_org_type='线上经营贷' then susp_org_type end) as idcard_applied_in_orgs_9_ct
,count(case when susp_org_type='线下供应链贷款' then susp_org_type end) as idcard_applied_in_orgs_10_ct
,count(case when susp_org_type='线下信用现金贷' then susp_org_type end) as idcard_applied_in_orgs_11_ct
,count(case when susp_org_type='线下微额快速贷' then susp_org_type end) as idcard_applied_in_orgs_12_ct
,count(case when susp_org_type='线下抵押贷款' then susp_org_type end) as idcard_applied_in_orgs_13_ct
,count(case when susp_org_type='线下消费分期' then susp_org_type end) as idcard_applied_in_orgs_14_ct
,count(case when susp_org_type='线下经营贷' then susp_org_type end) as idcard_applied_in_orgs_15_ct
,count(case when susp_org_type='贷款中介' then susp_org_type end) as idcard_applied_in_orgs_16_ct
from (
	select t2.order_no, json_array_elements(
		case when idcard_applied_in_orgs='[]' then '[null]' 
		when idcard_applied_in_orgs='-9999' then '[-9999]'  
		else cast(idcard_applied_in_orgs as json) end
		) ::json ->> 'susp_org_type'  as susp_org_type
	--from dw_bk_bk_core_bk_core_loan t1 
	from  (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from bk_sample_add) t1
	inner join bk_juxinli_result t2 on t1.order_no = t2.order_no and idcard_applied_in_orgs<>'-9999'
	--where t1.effective_date between '2019-07-05' and '2019-08-15'
	) tmp
group by order_no
"""
bk_jxl_data6 = get_df_from_pg(bk_jxl_sql6)
bk_jxl_data6.shape
bk_jxl_data6.order_no.nunique()
bk_jxl_data6.isnull().sum()



bk_jxl_sql7	= """
select t2.order_no
,cast(d_7 as json) ::json ->> 'pct_cnt_org_cash'  as d_7pct_cnt_org_cash
,cast(d_7 as json) ::json ->> 'cnt_cc'  as d_7cnt_cc
,cast(d_7 as json) ::json ->> 'cnt_org'  as d_7cnt_org
,cast(d_7 as json) ::json ->> 'pct_cnt_org_all'  as d_7pct_cnt_org_all
,cast(d_7 as json) ::json ->> 'cnt'  as d_7cnt
,cast(d_7 as json) ::json ->> 'pct_cnt_org_cf'  as d_7pct_cnt_org_cf
,cast(d_7 as json) ::json ->> 'cnt_cf'  as d_7cnt_cf
,cast(d_7 as json) ::json ->> 'pct_cnt_cc'  as d_7pct_cnt_cc
,cast(d_7 as json) ::json ->> 'cnt_org_cf'  as d_7cnt_org_cf
,cast(d_7 as json) ::json ->> 'pct_cnt_cf'  as d_7pct_cnt_cf
,cast(d_7 as json) ::json ->> 'cnt_cash'  as d_7cnt_cash
,cast(d_7 as json) ::json ->> 'pct_cnt_cash'  as d_7pct_cnt_cash
,cast(d_7 as json) ::json ->> 'cnt_org_cc'  as d_7cnt_org_cc
,cast(d_7 as json) ::json ->> 'pct_cnt_org_cc'  as d_7pct_cnt_org_cc
,cast(d_7 as json) ::json ->> 'pct_cnt_all'  as d_7pct_cnt_all
,cast(d_7 as json) ::json ->> 'cnt_org_cash'  as d_7cnt_org_cash
,cast(d_15 as json) ::json ->> 'pct_cnt_org_cash'  as d_15pct_cnt_org_cash
,cast(d_15 as json) ::json ->> 'cnt_cc'  as d_15cnt_cc
,cast(d_15 as json) ::json ->> 'cnt_org'  as d_15cnt_org
,cast(d_15 as json) ::json ->> 'pct_cnt_org_all'  as d_15pct_cnt_org_all
,cast(d_15 as json) ::json ->> 'cnt'  as d_15cnt
,cast(d_15 as json) ::json ->> 'pct_cnt_org_cf'  as d_15pct_cnt_org_cf
,cast(d_15 as json) ::json ->> 'cnt_cf'  as d_15cnt_cf
,cast(d_15 as json) ::json ->> 'pct_cnt_cc'  as d_15pct_cnt_cc
,cast(d_15 as json) ::json ->> 'cnt_org_cf'  as d_15cnt_org_cf
,cast(d_15 as json) ::json ->> 'pct_cnt_cf'  as d_15pct_cnt_cf
,cast(d_15 as json) ::json ->> 'cnt_cash'  as d_15cnt_cash
,cast(d_15 as json) ::json ->> 'pct_cnt_cash'  as d_15pct_cnt_cash
,cast(d_15 as json) ::json ->> 'cnt_org_cc'  as d_15cnt_org_cc
,cast(d_15 as json) ::json ->> 'pct_cnt_org_cc'  as d_15pct_cnt_org_cc
,cast(d_15 as json) ::json ->> 'pct_cnt_all'  as d_15pct_cnt_all
,cast(d_15 as json) ::json ->> 'cnt_org_cash'  as d_15cnt_org_cash
,cast(d_30 as json) ::json ->> 'pct_cnt_org_cash'  as d_30pct_cnt_org_cash
,cast(d_30 as json) ::json ->> 'cnt_cc'  as d_30cnt_cc
,cast(d_30 as json) ::json ->> 'cnt_org'  as d_30cnt_org
,cast(d_30 as json) ::json ->> 'pct_cnt_org_all'  as d_30pct_cnt_org_all
,cast(d_30 as json) ::json ->> 'cnt'  as d_30cnt
,cast(d_30 as json) ::json ->> 'pct_cnt_org_cf'  as d_30pct_cnt_org_cf
,cast(d_30 as json) ::json ->> 'cnt_cf'  as d_30cnt_cf
,cast(d_30 as json) ::json ->> 'pct_cnt_cc'  as d_30pct_cnt_cc
,cast(d_30 as json) ::json ->> 'cnt_org_cf'  as d_30cnt_org_cf
,cast(d_30 as json) ::json ->> 'pct_cnt_cf'  as d_30pct_cnt_cf
,cast(d_30 as json) ::json ->> 'cnt_cash'  as d_30cnt_cash
,cast(d_30 as json) ::json ->> 'pct_cnt_cash'  as d_30pct_cnt_cash
,cast(d_30 as json) ::json ->> 'cnt_org_cc'  as d_30cnt_org_cc
,cast(d_30 as json) ::json ->> 'pct_cnt_org_cc'  as d_30pct_cnt_org_cc
,cast(d_30 as json) ::json ->> 'pct_cnt_all'  as d_30pct_cnt_all
,cast(d_30 as json) ::json ->> 'cnt_org_cash'  as d_30cnt_org_cash
,cast(d_60 as json) ::json ->> 'pct_cnt_org_cash'  as d_60pct_cnt_org_cash
,cast(d_60 as json) ::json ->> 'cnt_cc'  as d_60cnt_cc
,cast(d_60 as json) ::json ->> 'cnt_org'  as d_60cnt_org
,cast(d_60 as json) ::json ->> 'pct_cnt_org_all'  as d_60pct_cnt_org_all
,cast(d_60 as json) ::json ->> 'cnt'  as d_60cnt
,cast(d_60 as json) ::json ->> 'pct_cnt_org_cf'  as d_60pct_cnt_org_cf
,cast(d_60 as json) ::json ->> 'cnt_cf'  as d_60cnt_cf
,cast(d_60 as json) ::json ->> 'pct_cnt_cc'  as d_60pct_cnt_cc
,cast(d_60 as json) ::json ->> 'cnt_org_cf'  as d_60cnt_org_cf
,cast(d_60 as json) ::json ->> 'pct_cnt_cf'  as d_60pct_cnt_cf
,cast(d_60 as json) ::json ->> 'cnt_cash'  as d_60cnt_cash
,cast(d_60 as json) ::json ->> 'pct_cnt_cash'  as d_60pct_cnt_cash
,cast(d_60 as json) ::json ->> 'cnt_org_cc'  as d_60cnt_org_cc
,cast(d_60 as json) ::json ->> 'pct_cnt_org_cc'  as d_60pct_cnt_org_cc
,cast(d_60 as json) ::json ->> 'pct_cnt_all'  as d_60pct_cnt_all
,cast(d_60 as json) ::json ->> 'cnt_org_cash'  as d_60cnt_org_cash
,cast(d_90 as json) ::json ->> 'pct_cnt_org_cash'  as d_90pct_cnt_org_cash
,cast(d_90 as json) ::json ->> 'cnt_cc'  as d_90cnt_cc
,cast(d_90 as json) ::json ->> 'cnt_org'  as d_90cnt_org
,cast(d_90 as json) ::json ->> 'pct_cnt_org_all'  as d_90pct_cnt_org_all
,cast(d_90 as json) ::json ->> 'cnt'  as d_90cnt
,cast(d_90 as json) ::json ->> 'pct_cnt_org_cf'  as d_90pct_cnt_org_cf
,cast(d_90 as json) ::json ->> 'cnt_cf'  as d_90cnt_cf
,cast(d_90 as json) ::json ->> 'pct_cnt_cc'  as d_90pct_cnt_cc
,cast(d_90 as json) ::json ->> 'cnt_org_cf'  as d_90cnt_org_cf
,cast(d_90 as json) ::json ->> 'pct_cnt_cf'  as d_90pct_cnt_cf
,cast(d_90 as json) ::json ->> 'cnt_cash'  as d_90cnt_cash
,cast(d_90 as json) ::json ->> 'pct_cnt_cash'  as d_90pct_cnt_cash
,cast(d_90 as json) ::json ->> 'cnt_org_cc'  as d_90cnt_org_cc
,cast(d_90 as json) ::json ->> 'pct_cnt_org_cc'  as d_90pct_cnt_org_cc
,cast(d_90 as json) ::json ->> 'pct_cnt_all'  as d_90pct_cnt_all
,cast(d_90 as json) ::json ->> 'cnt_org_cash'  as d_90cnt_org_cash
,cast(m_4 as json) ::json ->> 'pct_cnt_org_cash'  as m_4pct_cnt_org_cash
,cast(m_4 as json) ::json ->> 'cnt_cc'  as m_4cnt_cc
,cast(m_4 as json) ::json ->> 'cnt_org'  as m_4cnt_org
,cast(m_4 as json) ::json ->> 'pct_cnt_org_all'  as m_4pct_cnt_org_all
,cast(m_4 as json) ::json ->> 'cnt'  as m_4cnt
,cast(m_4 as json) ::json ->> 'pct_cnt_org_cf'  as m_4pct_cnt_org_cf
,cast(m_4 as json) ::json ->> 'cnt_cf'  as m_4cnt_cf
,cast(m_4 as json) ::json ->> 'pct_cnt_cc'  as m_4pct_cnt_cc
,cast(m_4 as json) ::json ->> 'cnt_org_cf'  as m_4cnt_org_cf
,cast(m_4 as json) ::json ->> 'pct_cnt_cf'  as m_4pct_cnt_cf
,cast(m_4 as json) ::json ->> 'cnt_cash'  as m_4cnt_cash
,cast(m_4 as json) ::json ->> 'pct_cnt_cash'  as m_4pct_cnt_cash
,cast(m_4 as json) ::json ->> 'cnt_org_cc'  as m_4cnt_org_cc
,cast(m_4 as json) ::json ->> 'pct_cnt_org_cc'  as m_4pct_cnt_org_cc
,cast(m_4 as json) ::json ->> 'pct_cnt_all'  as m_4pct_cnt_all
,cast(m_4 as json) ::json ->> 'cnt_org_cash'  as m_4cnt_org_cash
,cast(m_5 as json) ::json ->> 'pct_cnt_org_cash'  as m_5pct_cnt_org_cash
,cast(m_5 as json) ::json ->> 'cnt_cc'  as m_5cnt_cc
,cast(m_5 as json) ::json ->> 'cnt_org'  as m_5cnt_org
,cast(m_5 as json) ::json ->> 'pct_cnt_org_all'  as m_5pct_cnt_org_all
,cast(m_5 as json) ::json ->> 'cnt'  as m_5cnt
,cast(m_5 as json) ::json ->> 'pct_cnt_org_cf'  as m_5pct_cnt_org_cf
,cast(m_5 as json) ::json ->> 'cnt_cf'  as m_5cnt_cf
,cast(m_5 as json) ::json ->> 'pct_cnt_cc'  as m_5pct_cnt_cc
,cast(m_5 as json) ::json ->> 'cnt_org_cf'  as m_5cnt_org_cf
,cast(m_5 as json) ::json ->> 'pct_cnt_cf'  as m_5pct_cnt_cf
,cast(m_5 as json) ::json ->> 'cnt_cash'  as m_5cnt_cash
,cast(m_5 as json) ::json ->> 'pct_cnt_cash'  as m_5pct_cnt_cash
,cast(m_5 as json) ::json ->> 'cnt_org_cc'  as m_5cnt_org_cc
,cast(m_5 as json) ::json ->> 'pct_cnt_org_cc'  as m_5pct_cnt_org_cc
,cast(m_5 as json) ::json ->> 'pct_cnt_all'  as m_5pct_cnt_all
,cast(m_5 as json) ::json ->> 'cnt_org_cash'  as m_5cnt_org_cash
,cast(m_6 as json) ::json ->> 'pct_cnt_org_cash'  as m_6pct_cnt_org_cash
,cast(m_6 as json) ::json ->> 'cnt_cc'  as m_6cnt_cc
,cast(m_6 as json) ::json ->> 'cnt_org'  as m_6cnt_org
,cast(m_6 as json) ::json ->> 'pct_cnt_org_all'  as m_6pct_cnt_org_all
,cast(m_6 as json) ::json ->> 'cnt'  as m_6cnt
,cast(m_6 as json) ::json ->> 'pct_cnt_org_cf'  as m_6pct_cnt_org_cf
,cast(m_6 as json) ::json ->> 'cnt_cf'  as m_6cnt_cf
,cast(m_6 as json) ::json ->> 'pct_cnt_cc'  as m_6pct_cnt_cc
,cast(m_6 as json) ::json ->> 'cnt_org_cf'  as m_6cnt_org_cf
,cast(m_6 as json) ::json ->> 'pct_cnt_cf'  as m_6pct_cnt_cf
,cast(m_6 as json) ::json ->> 'cnt_cash'  as m_6cnt_cash
,cast(m_6 as json) ::json ->> 'pct_cnt_cash'  as m_6pct_cnt_cash
,cast(m_6 as json) ::json ->> 'cnt_org_cc'  as m_6cnt_org_cc
,cast(m_6 as json) ::json ->> 'pct_cnt_org_cc'  as m_6pct_cnt_org_cc
,cast(m_6 as json) ::json ->> 'pct_cnt_all'  as m_6pct_cnt_all
,cast(m_6 as json) ::json ->> 'cnt_org_cash'  as m_6cnt_org_cash
,cast(m_9 as json) ::json ->> 'pct_cnt_org_cash'  as m_9pct_cnt_org_cash
,cast(m_9 as json) ::json ->> 'cnt_cc'  as m_9cnt_cc
,cast(m_9 as json) ::json ->> 'cnt_org'  as m_9cnt_org
,cast(m_9 as json) ::json ->> 'pct_cnt_org_all'  as m_9pct_cnt_org_all
,cast(m_9 as json) ::json ->> 'cnt'  as m_9cnt
,cast(m_9 as json) ::json ->> 'pct_cnt_org_cf'  as m_9pct_cnt_org_cf
,cast(m_9 as json) ::json ->> 'cnt_cf'  as m_9cnt_cf
,cast(m_9 as json) ::json ->> 'pct_cnt_cc'  as m_9pct_cnt_cc
,cast(m_9 as json) ::json ->> 'cnt_org_cf'  as m_9cnt_org_cf
,cast(m_9 as json) ::json ->> 'pct_cnt_cf'  as m_9pct_cnt_cf
,cast(m_9 as json) ::json ->> 'cnt_cash'  as m_9cnt_cash
,cast(m_9 as json) ::json ->> 'pct_cnt_cash'  as m_9pct_cnt_cash
,cast(m_9 as json) ::json ->> 'cnt_org_cc'  as m_9cnt_org_cc
,cast(m_9 as json) ::json ->> 'pct_cnt_org_cc'  as m_9pct_cnt_org_cc
,cast(m_9 as json) ::json ->> 'pct_cnt_all'  as m_9pct_cnt_all
,cast(m_9 as json) ::json ->> 'cnt_org_cash'  as m_9cnt_org_cash
,cast(m_12 as json) ::json ->> 'pct_cnt_org_cash'  as m_12pct_cnt_org_cash
,cast(m_12 as json) ::json ->> 'cnt_cc'  as m_12cnt_cc
,cast(m_12 as json) ::json ->> 'cnt_org'  as m_12cnt_org
,cast(m_12 as json) ::json ->> 'pct_cnt_org_all'  as m_12pct_cnt_org_all
,cast(m_12 as json) ::json ->> 'cnt'  as m_12cnt
,cast(m_12 as json) ::json ->> 'pct_cnt_org_cf'  as m_12pct_cnt_org_cf
,cast(m_12 as json) ::json ->> 'cnt_cf'  as m_12cnt_cf
,cast(m_12 as json) ::json ->> 'pct_cnt_cc'  as m_12pct_cnt_cc
,cast(m_12 as json) ::json ->> 'cnt_org_cf'  as m_12cnt_org_cf
,cast(m_12 as json) ::json ->> 'pct_cnt_cf'  as m_12pct_cnt_cf
,cast(m_12 as json) ::json ->> 'cnt_cash'  as m_12cnt_cash
,cast(m_12 as json) ::json ->> 'pct_cnt_cash'  as m_12pct_cnt_cash
,cast(m_12 as json) ::json ->> 'cnt_org_cc'  as m_12cnt_org_cc
,cast(m_12 as json) ::json ->> 'pct_cnt_org_cc'  as m_12pct_cnt_org_cc
,cast(m_12 as json) ::json ->> 'pct_cnt_all'  as m_12pct_cnt_all
,cast(m_12 as json) ::json ->> 'cnt_org_cash'  as m_12cnt_org_cash
,cast(m_18 as json) ::json ->> 'pct_cnt_org_cash'  as m_18pct_cnt_org_cash
,cast(m_18 as json) ::json ->> 'cnt_cc'  as m_18cnt_cc
,cast(m_18 as json) ::json ->> 'cnt_org'  as m_18cnt_org
,cast(m_18 as json) ::json ->> 'pct_cnt_org_all'  as m_18pct_cnt_org_all
,cast(m_18 as json) ::json ->> 'cnt'  as m_18cnt
,cast(m_18 as json) ::json ->> 'pct_cnt_org_cf'  as m_18pct_cnt_org_cf
,cast(m_18 as json) ::json ->> 'cnt_cf'  as m_18cnt_cf
,cast(m_18 as json) ::json ->> 'pct_cnt_cc'  as m_18pct_cnt_cc
,cast(m_18 as json) ::json ->> 'cnt_org_cf'  as m_18cnt_org_cf
,cast(m_18 as json) ::json ->> 'pct_cnt_cf'  as m_18pct_cnt_cf
,cast(m_18 as json) ::json ->> 'cnt_cash'  as m_18cnt_cash
,cast(m_18 as json) ::json ->> 'pct_cnt_cash'  as m_18pct_cnt_cash
,cast(m_18 as json) ::json ->> 'cnt_org_cc'  as m_18cnt_org_cc
,cast(m_18 as json) ::json ->> 'pct_cnt_org_cc'  as m_18pct_cnt_org_cc
,cast(m_18 as json) ::json ->> 'pct_cnt_all'  as m_18pct_cnt_all
,cast(m_18 as json) ::json ->> 'cnt_org_cash'  as m_18cnt_org_cash
,cast(m_24 as json) ::json ->> 'pct_cnt_org_cash'  as m_24pct_cnt_org_cash
,cast(m_24 as json) ::json ->> 'cnt_cc'  as m_24cnt_cc
,cast(m_24 as json) ::json ->> 'cnt_org'  as m_24cnt_org
,cast(m_24 as json) ::json ->> 'pct_cnt_org_all'  as m_24pct_cnt_org_all
,cast(m_24 as json) ::json ->> 'cnt'  as m_24cnt
,cast(m_24 as json) ::json ->> 'pct_cnt_org_cf'  as m_24pct_cnt_org_cf
,cast(m_24 as json) ::json ->> 'cnt_cf'  as m_24cnt_cf
,cast(m_24 as json) ::json ->> 'pct_cnt_cc'  as m_24pct_cnt_cc
,cast(m_24 as json) ::json ->> 'cnt_org_cf'  as m_24cnt_org_cf
,cast(m_24 as json) ::json ->> 'pct_cnt_cf'  as m_24pct_cnt_cf
,cast(m_24 as json) ::json ->> 'cnt_cash'  as m_24cnt_cash
,cast(m_24 as json) ::json ->> 'pct_cnt_cash'  as m_24pct_cnt_cash
,cast(m_24 as json) ::json ->> 'cnt_org_cc'  as m_24cnt_org_cc
,cast(m_24 as json) ::json ->> 'pct_cnt_org_cc'  as m_24pct_cnt_org_cc
,cast(m_24 as json) ::json ->> 'pct_cnt_all'  as m_24pct_cnt_all
,cast(m_24 as json) ::json ->> 'cnt_org_cash'  as m_24cnt_org_cash
from  (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from bk_sample_add) t1
left join bk_juxinli_result t2 on t1.order_no = t2.order_no and d_7<>'-9999'
"""

bk_jxl_data7 = get_df_from_pg(bk_jxl_sql7)
bk_jxl_data7.shape
bk_jxl_data7.order_no.nunique()
bk_jxl_data7.isnull().sum()

#合并白卡聚信立变量
bk_jxl_data1.shape
bk_jxl_data2.shape
bk_jxl_data3.shape

bk_jxl_data4.shape
bk_jxl_data5.shape
bk_jxl_data6.shape
bk_jxl_data7.shape

bk_jxl_data = bk_jxl_data1.merge(bk_jxl_data2, on = 'order_no', how = 'left').merge(bk_jxl_data3, on = 'order_no', how = 'left').merge(bk_jxl_data4, on = 'order_no', how = 'left').merge(bk_jxl_data5, on = 'order_no', how = 'left')\
.merge(bk_jxl_data6, on = 'order_no', how = 'left').merge(bk_jxl_data7, on = 'order_no', how = 'left')

bk_jxl_data.shape
bk_jxl_data.isnull().sum()

bk_jxl_data.to_csv(os.path.join(data_path, 'bk_jxl_data.csv'))

#dc

dc_jxl_sql7	= """
select t1.order_no
,oss::json #>> '{data,user_searched_history_by_day,d_7,pct_cnt_org_cash}'  as d_7pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,d_7,cnt_cc}'  as d_7cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_7,cnt_org}'  as d_7cnt_org
,oss::json #>> '{data,user_searched_history_by_day,d_7,pct_cnt_org_all}'  as d_7pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,d_7,cnt}'  as d_7cnt
,oss::json #>> '{data,user_searched_history_by_day,d_7,pct_cnt_org_cf}'  as d_7pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_7,cnt_cf}'  as d_7cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_7,pct_cnt_cc}'  as d_7pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_7,cnt_org_cf}'  as d_7cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_7,pct_cnt_cf}'  as d_7pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_7,cnt_cash}'  as d_7cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_7,pct_cnt_cash}'  as d_7pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_7,cnt_org_cc}'  as d_7cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_7,pct_cnt_org_cc}'  as d_7pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_7,pct_cnt_all}'  as d_7pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,d_7,cnt_org_cash}'  as d_7cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,d_15,pct_cnt_org_cash}'  as d_15pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,d_15,cnt_cc}'  as d_15cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_15,cnt_org}'  as d_15cnt_org
,oss::json #>> '{data,user_searched_history_by_day,d_15,pct_cnt_org_all}'  as d_15pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,d_15,cnt}'  as d_15cnt
,oss::json #>> '{data,user_searched_history_by_day,d_15,pct_cnt_org_cf}'  as d_15pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_15,cnt_cf}'  as d_15cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_15,pct_cnt_cc}'  as d_15pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_15,cnt_org_cf}'  as d_15cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_15,pct_cnt_cf}'  as d_15pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_15,cnt_cash}'  as d_15cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_15,pct_cnt_cash}'  as d_15pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_15,cnt_org_cc}'  as d_15cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_15,pct_cnt_org_cc}'  as d_15pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_15,pct_cnt_all}'  as d_15pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,d_15,cnt_org_cash}'  as d_15cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_org_cash}'  as d_30pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_cc}'  as d_30cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_org}'  as d_30cnt_org
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_org_all}'  as d_30pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt}'  as d_30cnt
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_org_cf}'  as d_30pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_cf}'  as d_30cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_cc}'  as d_30pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_org_cf}'  as d_30cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_cf}'  as d_30pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_cash}'  as d_30cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_cash}'  as d_30pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_org_cc}'  as d_30cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_org_cc}'  as d_30pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_30,pct_cnt_all}'  as d_30pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,d_30,cnt_org_cash}'  as d_30cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_org_cash}'  as d_60pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_cc}'  as d_60cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_org}'  as d_60cnt_org
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_org_all}'  as d_60pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt}'  as d_60cnt
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_org_cf}'  as d_60pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_cf}'  as d_60cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_cc}'  as d_60pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_org_cf}'  as d_60cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_cf}'  as d_60pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_cash}'  as d_60cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_cash}'  as d_60pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_org_cc}'  as d_60cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_org_cc}'  as d_60pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_60,pct_cnt_all}'  as d_60pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,d_60,cnt_org_cash}'  as d_60cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_org_cash}'  as d_90pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_cc}'  as d_90cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_org}'  as d_90cnt_org
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_org_all}'  as d_90pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt}'  as d_90cnt
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_org_cf}'  as d_90pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_cf}'  as d_90cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_cc}'  as d_90pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_org_cf}'  as d_90cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_cf}'  as d_90pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_cash}'  as d_90cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_cash}'  as d_90pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_org_cc}'  as d_90cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_org_cc}'  as d_90pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,d_90,pct_cnt_all}'  as d_90pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,d_90,cnt_org_cash}'  as d_90cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_cash}'  as m_4pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_cc}'  as m_4cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_org}'  as m_4cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_all}'  as m_4pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt}'  as m_4cnt
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_cf}'  as m_4pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_cf}'  as m_4cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_cc}'  as m_4pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_org_cf}'  as m_4cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_cf}'  as m_4pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_cash}'  as m_4cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_cash}'  as m_4pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_org_cc}'  as m_4cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_org_cc}'  as m_4pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_4,pct_cnt_all}'  as m_4pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_4,cnt_org_cash}'  as m_4cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_org_cash}'  as m_5pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_cc}'  as m_5cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_org}'  as m_5cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_org_all}'  as m_5pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt}'  as m_5cnt
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_org_cf}'  as m_5pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_cf}'  as m_5cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_cc}'  as m_5pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_org_cf}'  as m_5cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_cf}'  as m_5pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_cash}'  as m_5cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_cash}'  as m_5pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_org_cc}'  as m_5cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_org_cc}'  as m_5pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_5,pct_cnt_all}'  as m_5pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_5,cnt_org_cash}'  as m_5cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_org_cash}'  as m_6pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_cc}'  as m_6cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_org}'  as m_6cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_org_all}'  as m_6pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt}'  as m_6cnt
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_org_cf}'  as m_6pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_cf}'  as m_6cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_cc}'  as m_6pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_org_cf}'  as m_6cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_cf}'  as m_6pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_cash}'  as m_6cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_cash}'  as m_6pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_org_cc}'  as m_6cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_org_cc}'  as m_6pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_6,pct_cnt_all}'  as m_6pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_6,cnt_org_cash}'  as m_6cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_org_cash}'  as m_9pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_cc}'  as m_9cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org}'  as m_9cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_org_all}'  as m_9pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt}'  as m_9cnt
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_org_cf}'  as m_9pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_cf}'  as m_9cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_cc}'  as m_9pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org_cf}'  as m_9cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_cf}'  as m_9pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_cash}'  as m_9cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_cash}'  as m_9pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org_cc}'  as m_9cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_org_cc}'  as m_9pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_9,pct_cnt_all}'  as m_9pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_9,cnt_org_cash}'  as m_9cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_org_cash}'  as m_12pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_cc}'  as m_12cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org}'  as m_12cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_org_all}'  as m_12pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt}'  as m_12cnt
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_org_cf}'  as m_12pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_cf}'  as m_12cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_cc}'  as m_12pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org_cf}'  as m_12cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_cf}'  as m_12pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_cash}'  as m_12cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_cash}'  as m_12pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org_cc}'  as m_12cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_org_cc}'  as m_12pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_12,pct_cnt_all}'  as m_12pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_12,cnt_org_cash}'  as m_12cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_org_cash}'  as m_18pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_cc}'  as m_18cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org}'  as m_18cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_org_all}'  as m_18pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt}'  as m_18cnt
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_org_cf}'  as m_18pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_cf}'  as m_18cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_cc}'  as m_18pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org_cf}'  as m_18cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_cf}'  as m_18pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_cash}'  as m_18cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_cash}'  as m_18pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org_cc}'  as m_18cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_org_cc}'  as m_18pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_18,pct_cnt_all}'  as m_18pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_18,cnt_org_cash}'  as m_18cnt_org_cash

,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_cash}'  as m_24pct_cnt_org_cash
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_cc}'  as m_24cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org}'  as m_24cnt_org
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_all}'  as m_24pct_cnt_org_all
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt}'  as m_24cnt
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_cf}'  as m_24pct_cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_cf}'  as m_24cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_cc}'  as m_24pct_cnt_cc
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org_cf}'  as m_24cnt_org_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_cf}'  as m_24pct_cnt_cf
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_cash}'  as m_24cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_cash}'  as m_24pct_cnt_cash
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org_cc}'  as m_24cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_org_cc}'  as m_24pct_cnt_org_cc
,oss::json #>> '{data,user_searched_history_by_day,m_24,pct_cnt_all}'  as m_24pct_cnt_all
,oss::json #>> '{data,user_searched_history_by_day,m_24,cnt_org_cash}'  as m_24cnt_org_cash
from  (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('tb') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from dc_sample_add) t1
left join  (select * from dc_juxinli_application where oss <> '') t2 on t1.order_no = t2.order_no
"""

dc_jxl_data7 = get_df_from_pg(dc_jxl_sql7)
dc_jxl_data7.shape
dc_jxl_data7.order_no.nunique()
dc_jxl_data7.isnull().sum()

dc_jxl_data7.to_csv(os.path.join(data_path, 'dc_jxl.csv'))

----------------------------------数美------------------------------------

dc_shumei_sql = """
with perf as (
select order_no
from t_loan_performance
where business_id = 'tb' and effective_date  between '2018-08-26' and '2019-07-31'
), 
related as (
select orderno, messageno
from risk_mongo_installmentmessagerelated
where topicname = 'Application_shumei' and databasename = 'installmentShumei' and businessid = 'tb'
),
select order_no
, shumei::json #>> '{detail,is_hit}' as is_hit
, shumei::json #>> '{detail,in_black}' as in_black 
, shumei::json #>> '{detail,in_grey}' as in_grey 
, shumei::json #>> '{detail,itfin_loan_overdues }' as itfin_loan_overdues  
, shumei::json #>> '{detail,itfin_loan_overdue_level}' as itfin_loan_overdue_level 
, shumei::json #>> '{detail,itfin_loan_overdue_duration}' as itfin_loan_overdue_duration 
, shumei::json #>> '{detail,itfin_loan_overdues_7d}' as itfin_loan_overdues_7d 
, shumei::json #>> '{detail,itfin_loan_overdue_level_7d}' as itfin_loan_overdue_level_7d 
, shumei::json #>> '{detail,itfin_loan_overdue_duration_7d}' as itfin_loan_overdue_duration_7d 
, shumei::json #>> '{detail,itfin_loan_overdues_30d}' as itfin_loan_overdues_30d 
, shumei::json #>> '{detail,itfin_loan_overdue_level_30d}' as itfin_loan_overdue_level_30d 
, shumei::json #>> '{detail,itfin_loan_overdue_duration_30d}' as itfin_loan_overdue_duration_30d 
, shumei::json #>> '{detail,itfin_loan_overdues_60d}' as itfin_loan_overdues_60d 
, shumei::json #>> '{detail,itfin_loan_overdue_level_60d}' as itfin_loan_overdue_level_60d 
, shumei::json #>> '{detail,itfin_loan_overdue_duration_60d}' as itfin_loan_overdue_duration_60d 
, shumei::json #>> '{detail,itfin_loan_overdues_90d}' as itfin_loan_overdues_90d 
, shumei::json #>> '{detail,itfin_loan_overdue_level_90d}' as itfin_loan_overdue_level_90d 
, shumei::json #>> '{detail,itfin_loan_overdue_duration_90d}' as itfin_loan_overdue_duration_90d 
, shumei::json #>> '{detail,itfin_loan_overdues_180d}' as itfin_loan_overdues_180d 
, shumei::json #>> '{detail,itfin_loan_overdue_level_180d}' as itfin_loan_overdue_level_180d  
, shumei::json #>> '{detail,itfin_loan_overdue_duration_180d}' as itfin_loan_overdue_duration_180d 
, shumei::json #>> '{detail,credit_loan_overdues}' as credit_loan_overdues 
, shumei::json #>> '{detail,credit_loan_overdue_level}' as credit_loan_overdue_level 
, shumei::json #>> '{detail,credit_loan_overdue_duration}' as credit_loan_overdue_duration 
, shumei::json #>> '{detail,credit_loan_overdues_7d}' as credit_loan_overdues_7d 
, shumei::json #>> '{detail,credit_loan_overdue_level_7d}' as credit_loan_overdue_level_7d 
, shumei::json #>> '{detail,credit_loan_overdue_duration_7d}' as credit_loan_overdue_duration_7d 
, shumei::json #>> '{detail,credit_loan_overdues_30d}' as credit_loan_overdues_30d 
, shumei::json #>> '{detail,credit_loan_overdue_level_30d}' as credit_loan_overdue_level_30d 
, shumei::json #>> '{detail,credit_loan_overdue_duration_30d}' as credit_loan_overdue_duration_30d 
, shumei::json #>> '{detail,credit_loan_overdues_60d}' as credit_loan_overdues_60d 
, shumei::json #>> '{detail,credit_loan_overdue_level_60d}' as credit_loan_overdue_level_60d 
, shumei::json #>> '{detail,credit_loan_overdue_duration_60d}' as credit_loan_overdue_duration_60d 
, shumei::json #>> '{detail,credit_loan_overdues_90d}' as credit_loan_overdues_90d 
, shumei::json #>> '{detail,credit_loan_overdue_level_90d}' as credit_loan_overdue_level_90d 
, shumei::json #>> '{detail,credit_loan_overdue_duration_90d}' as credit_loan_overdue_duration_90d 
, shumei::json #>> '{detail,credit_loan_overdues_180d}' as credit_loan_overdues_180d 
, shumei::json #>> '{detail,credit_loan_overdue_level_180d}' as credit_loan_overdue_level_180d 
, shumei::json #>> '{detail,credit_loan_overdue_duration_180d}' as credit_loan_overdue_duration_180d 
from perf t0
left join related  t1 on t0.orderno = t1.orderno
left join bakrt_shumei_base_info t2 on t1.messageno = t2.messageno
"""



bk_shumei_sql = """
select t0.order_no
, oss::json #>> '{detail,is_hit}' as is_hit
, oss::json #>> '{detail,in_black}' as in_black 
, oss::json #>> '{detail,in_grey}' as in_grey 
, oss::json #>> '{detail,itfin_loan_overdues }' as itfin_loan_overdues  
, oss::json #>> '{detail,itfin_loan_overdue_level}' as itfin_loan_overdue_level 
, oss::json #>> '{detail,itfin_loan_overdue_duration}' as itfin_loan_overdue_duration 
, oss::json #>> '{detail,itfin_loan_overdues_7d}' as itfin_loan_overdues_7d 
, oss::json #>> '{detail,itfin_loan_overdue_level_7d}' as itfin_loan_overdue_level_7d 
, oss::json #>> '{detail,itfin_loan_overdue_duration_7d}' as itfin_loan_overdue_duration_7d 
, oss::json #>> '{detail,itfin_loan_overdues_30d}' as itfin_loan_overdues_30d 
, oss::json #>> '{detail,itfin_loan_overdue_level_30d}' as itfin_loan_overdue_level_30d 
, oss::json #>> '{detail,itfin_loan_overdue_duration_30d}' as itfin_loan_overdue_duration_30d 
, oss::json #>> '{detail,itfin_loan_overdues_60d}' as itfin_loan_overdues_60d 
, oss::json #>> '{detail,itfin_loan_overdue_level_60d}' as itfin_loan_overdue_level_60d 
, oss::json #>> '{detail,itfin_loan_overdue_duration_60d}' as itfin_loan_overdue_duration_60d 
, oss::json #>> '{detail,itfin_loan_overdues_90d}' as itfin_loan_overdues_90d 
, oss::json #>> '{detail,itfin_loan_overdue_level_90d}' as itfin_loan_overdue_level_90d 
, oss::json #>> '{detail,itfin_loan_overdue_duration_90d}' as itfin_loan_overdue_duration_90d 
, oss::json #>> '{detail,itfin_loan_overdues_180d}' as itfin_loan_overdues_180d 
, oss::json #>> '{detail,itfin_loan_overdue_level_180d}' as itfin_loan_overdue_level_180d  
, oss::json #>> '{detail,itfin_loan_overdue_duration_180d}' as itfin_loan_overdue_duration_180d 
, oss::json #>> '{detail,credit_loan_overdues}' as credit_loan_overdues 
, oss::json #>> '{detail,credit_loan_overdue_level}' as credit_loan_overdue_level 
, oss::json #>> '{detail,credit_loan_overdue_duration}' as credit_loan_overdue_duration 
, oss::json #>> '{detail,credit_loan_overdues_7d}' as credit_loan_overdues_7d 
, oss::json #>> '{detail,credit_loan_overdue_level_7d}' as credit_loan_overdue_level_7d 
, oss::json #>> '{detail,credit_loan_overdue_duration_7d}' as credit_loan_overdue_duration_7d 
, oss::json #>> '{detail,credit_loan_overdues_30d}' as credit_loan_overdues_30d 
, oss::json #>> '{detail,credit_loan_overdue_level_30d}' as credit_loan_overdue_level_30d 
, oss::json #>> '{detail,credit_loan_overdue_duration_30d}' as credit_loan_overdue_duration_30d 
, oss::json #>> '{detail,credit_loan_overdues_60d}' as credit_loan_overdues_60d 
, oss::json #>> '{detail,credit_loan_overdue_level_60d}' as credit_loan_overdue_level_60d 
, oss::json #>> '{detail,credit_loan_overdue_duration_60d}' as credit_loan_overdue_duration_60d 
, oss::json #>> '{detail,credit_loan_overdues_90d}' as credit_loan_overdues_90d 
, oss::json #>> '{detail,credit_loan_overdue_level_90d}' as credit_loan_overdue_level_90d 
, oss::json #>> '{detail,credit_loan_overdue_duration_90d}' as credit_loan_overdue_duration_90d 
, oss::json #>> '{detail,credit_loan_overdues_180d}' as credit_loan_overdues_180d 
, oss::json #>> '{detail,credit_loan_overdue_level_180d}' as credit_loan_overdue_level_180d 
, oss::json #>> '{detail,credit_loan_overdue_duration_180d}' as credit_loan_overdue_duration_180d 
--from dw_bk_bk_core_bk_core_application t0
from (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from bk_sample_add) t0
inner join (select * from bk_shumei_application where oss <> '')t1 on t0.order_no = t1.order_no
"""
bk_shumei_data = get_df_from_pg(bk_shumei_sql)
bk_shumei_data.shape
bk_shumei_data.order_no.nunique()
bk_shumei_data.isnull().sum()
bk_shumei_data.to_csv(os.path.join(data_path,'bk_shumei.csv'))

----------------------------------------天启-------------------------------------------
bk_tq_sql = """
with tq_raw as (select order_no
, oss::json #>>'{data,score}' as tq_score
, oss::json #>>'{data,is_black}' as is_black
, oss::json #>>'{data,fraud_level}' as fraud_level
, cast(json_array_elements(case when oss :: json #>> '{data, fraud_type}'= '[]' then '[null]'
						   when oss :: json #>> '{data, fraud_type}'= '' then '[null]'
						   else cast(oss::json #>>'{data,fraud_type}' as json) end)::json as varchar) as fraud_type
from bk_tianqi_application where oss <> ''
--(select * from dw_bk_bk_core_bk_core_application where date(created_time) between '2018-09-26' and '2019-08-15') t0
--left join (select *
--			from risk_mongo_installmentmessagerelated
--			where businessid in ('bk','xjbk') and topicname in ('Application_thirdPart','Application_thirdPart_tianqi')
--			and databasename = 'installmentTianQi'
--			)t1 on t0.order_no = t1.orderno
--left join public.bakrt_thirdpart_tianqi_base_info t2 on t1.messageno = t2.messageno
)
select t0.order_no
--, max(tq_score) as tq_score
--, max(is_black) as is_black
, max(fraud_level) as fraud_level
, max(case when fraud_type = '"R01"' then 1 else 0 end) as fraud_type_R01
, max(case when fraud_type = '"R02"' then 1 else 0 end) as fraud_type_R02
, max(case when fraud_type = '"R03"' then 1 else 0 end) as fraud_type_R03
, max(case when fraud_type = '"R04"' then 1 else 0 end) as fraud_type_R04
, max(case when fraud_type = '"R05"' then 1 else 0 end) as fraud_type_R05
from  (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from bk_sample_add) t0
left join tq_raw t1 on t0.order_no = t1.order_no
group by t0.order_no
"""

bk_tianqi_data = get_df_from_pg(bk_tq_sql)
bk_tianqi_data.shape
bk_tianqi_data.order_no.nunique()
bk_tianqi_data.isnull().sum()
bk_tianqi_data.to_csv(os.path.join(data_path,'bk_tianqi.csv'))

bk_tianqi_data  = pd.read_csv(os.path.join(data_path,'bk_tianqi.csv'))
bk_tianqi_data = bk_tianqi_data.drop(['Unnamed: 0'],1)
bk_tianqi_data.shape
bk_tianqi_data.columns


dc_tq_sql = """
with sample as (
select order_no
from t_loan_performance
where dt = '20190829' and business_id in ('tb') and effective_date between '2018-08-26' and '2019-08-15'
union
select order_no 
from dc_sample_add
),
tq_raw as (select order_no
, oss::json #>>'{data,score}' as tq_score
, oss::json #>>'{data,is_black}' as is_black
, oss::json #>>'{data,fraud_level}' as fraud_level
, cast(json_array_elements(case when oss :: json #>> '{data, fraud_type}'= '[]' then '[null]'
						   when oss :: json #>> '{data, fraud_type}'= '' then '[null]'
						   else cast(oss::json #>>'{data,fraud_type}' as json) end)::json as varchar) as fraud_type
from dc_tianqi_application where oss <>''
)
select t0.order_no
, max(fraud_level) as fraud_level
, max(case when fraud_type = '"R01"' then 1 else 0 end) as fraud_type_R01
, max(case when fraud_type = '"R02"' then 1 else 0 end) as fraud_type_R02
, max(case when fraud_type = '"R03"' then 1 else 0 end) as fraud_type_R03
, max(case when fraud_type = '"R04"' then 1 else 0 end) as fraud_type_R04
, max(case when fraud_type = '"R05"' then 1 else 0 end) as fraud_type_R05
from sample t0
left join tq_raw t1 on t0.order_no = t1.order_no
group by t0.order_no
"""
dc_tianqi_data = get_df_from_pg(dc_tq_sql)

dc_tianqi_data = dc_tianqi_data.drop('created_time',1)
dc_tianqi_data.shape
dc_tianqi_data.columns
dc_tianqi_data.order_no.nunique()
dc_tianqi_data.isnull().sum()
dc_tianqi_data.to_csv(os.path.join(data_path,'dc_tianqi.csv'))

----------------------------------------天御反欺诈-------------------------------------------

bk_tianyu_sql = """
select distinct t0.order_no
, max(riskscore) as ty_riskscore
, max(case when riskcode = '1' then riskcodevalue else null end) as ty_riskcode_1
, max(case when riskcode = '2' then riskcodevalue else null end) as ty_riskcode_2
, max(case when riskcode = '3' then riskcodevalue else null end) as ty_riskcode_3
, max(case when riskcode = '4' then riskcodevalue else null end) as ty_riskcode_4
, max(case when riskcode = '5' then riskcodevalue else null end) as ty_riskcode_5
, max(case when riskcode = '6' then riskcodevalue else null end) as ty_riskcode_6
, max(case when riskcode = '7' then riskcodevalue else null end) as ty_riskcode_7
, max(case when riskcode = '8' then riskcodevalue else null end) as ty_riskcode_8
, max(case when riskcode = '301' then riskcodevalue else null end) as ty_riskcode_301
, max(case when riskcode = '503' then riskcodevalue else null end) as ty_riskcode_503
from (select order_no
           from t_loan_performance
    	   where dt = '20190829' and business_id in ('xjbk') and effective_date between '2018-08-26' and '2019-08-15'
    	   union
    	   select order_no 
    	   from bk_sample_add) t0
left join (select order_no
			, oss :: json #>> '{riskScore}' as riskscore
			, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCode' as riskcode
			, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCodeValue' as riskcodevalue
			from  bk_tianyu_application 
			where oss <> '') t1  on t0.order_no = t1.order_no
group by t0.order_no
"""

bk_tianyu_data = get_df_from_pg(bk_tianyu_sql)

bk_tianyu_data.shape
bk_tianyu_data.order_no.nunique()
bk_tianyu_data.isnull().sum()
bk_tianyu_data.to_csv(os.path.join(data_path,'bk_tianyu.csv'))
bk_tianyu_data.head()
bk_tianyu_data = pd.read_csv(os.path.join(data_path,'bk_tianyu.csv'))

dc_tianyu_sql = """
with sample as (
select order_no
from t_loan_performance
where business_id = 'tb' and effective_date  between '2018-08-26' and '2019-08-15'
union 
select order_no
from dc_sample_add
), 
tianyu as (
select order_no
, oss :: json #>> '{riskscore}' as riskscore_kfk
, json_array_elements(case when oss :: json #>> '{riskinfo}'= '[]' then '[null]' 
						   else cast(oss :: json #>> '{riskinfo}' as json) end)::json ->> 'riskcode' as riskcode_kfk
, json_array_elements(case when oss :: json #>> '{riskinfo}'= '[]' then '[null]' 
						   else cast(oss :: json #>> '{riskinfo}' as json) end)::json ->> 'riskcodevalue' as riskcodevalue_kfk
, oss :: json #>> '{riskScore}' as riskscore_oss
, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' 
						   else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCode' as riskcode_oss
, json_array_elements(case when oss :: json #>> '{riskInfo}'= '[]' then '[null]' 
						   else cast(oss :: json #>> '{riskInfo}' as json) end)::json ->> 'riskCodeValue' as riskcodevalue_oss
from dc_tianyu_application 
where oss <>''),
var as (
select  t0.order_no
--oss
, max(riskscore_oss) as ty_riskscore_oss
, max(case when riskcode_oss = '1' then riskcodevalue_oss else null end) as ty_riskcode_1_oss
, max(case when riskcode_oss = '2' then riskcodevalue_oss else null end) as ty_riskcode_2_oss
, max(case when riskcode_oss = '3' then riskcodevalue_oss else null end) as ty_riskcode_3_oss
, max(case when riskcode_oss = '4' then riskcodevalue_oss else null end) as ty_riskcode_4_oss
, max(case when riskcode_oss = '5' then riskcodevalue_oss else null end) as ty_riskcode_5_oss
, max(case when riskcode_oss = '6' then riskcodevalue_oss else null end) as ty_riskcode_6_oss
, max(case when riskcode_oss = '7' then riskcodevalue_oss else null end) as ty_riskcode_7_oss
, max(case when riskcode_oss = '8' then riskcodevalue_oss else null end) as ty_riskcode_8_oss
, max(case when riskcode_oss = '301' then riskcodevalue_oss else null end) as ty_riskcode_301_oss
, max(case when riskcode_oss = '503' then riskcodevalue_oss else null end) as ty_riskcode_503_oss
--kfk
, max(riskscore_kfk) as ty_riskscore_kfk
, max(case when riskcode_kfk = '1' then riskcodevalue_kfk else null end) as ty_riskcode_1_kfk
, max(case when riskcode_kfk = '2' then riskcodevalue_kfk else null end) as ty_riskcode_2_kfk
, max(case when riskcode_kfk = '3' then riskcodevalue_kfk else null end) as ty_riskcode_3_kfk
, max(case when riskcode_kfk = '4' then riskcodevalue_kfk else null end) as ty_riskcode_4_kfk
, max(case when riskcode_kfk = '5' then riskcodevalue_kfk else null end) as ty_riskcode_5_kfk
, max(case when riskcode_kfk = '6' then riskcodevalue_kfk else null end) as ty_riskcode_6_kfk
, max(case when riskcode_kfk = '7' then riskcodevalue_kfk else null end) as ty_riskcode_7_kfk
, max(case when riskcode_kfk = '8' then riskcodevalue_kfk else null end) as ty_riskcode_8_kfk
, max(case when riskcode_kfk = '301' then riskcodevalue_kfk else null end) as ty_riskcode_301_kfk
, max(case when riskcode_kfk = '503' then riskcodevalue_kfk else null end) as ty_riskcode_503_kfk
from sample t0
left join tianyu t1 on t1.order_no = t0.order_no
group by t0.order_no
)
select order_no
, case when ty_riskscore_kfk is null
	   then ty_riskscore_oss
	   else ty_riskscore_kfk
	   end as ty_riskscore
, case when ty_riskcode_1_kfk is null
	   then ty_riskcode_1_oss
	   else ty_riskcode_1_kfk
	   end as ty_riskcode_1
, case when ty_riskcode_2_kfk is null
	   then ty_riskcode_2_oss
	   else ty_riskcode_2_kfk
	   end as ty_riskcode_2
, case when ty_riskcode_3_kfk is null
	   then ty_riskcode_3_oss
	   else ty_riskcode_3_kfk
	   end as ty_riskcode_3
, case when ty_riskcode_4_kfk is null
	   then ty_riskcode_4_oss
	   else ty_riskcode_4_kfk
	   end as ty_riskcode_4
, case when ty_riskcode_5_kfk is null
	   then ty_riskcode_5_oss
	   else ty_riskcode_5_kfk
	   end as ty_riskcode_5
, case when ty_riskcode_6_kfk is null
	   then ty_riskcode_6_oss
	   else ty_riskcode_6_kfk
	   end as ty_riskcode_6
, case when ty_riskcode_7_kfk is null
	   then ty_riskcode_7_oss
	   else ty_riskcode_7_kfk
	   end as ty_riskcode_7
, case when ty_riskcode_8_kfk is null
	   then ty_riskcode_8_oss
	   else ty_riskcode_8_kfk
	   end as ty_riskcode_8
, case when ty_riskcode_301_kfk is null
	   then ty_riskcode_301_oss
	   else ty_riskcode_301_kfk
	   end as ty_riskcode_301
, case when ty_riskcode_503_kfk is null
	   then ty_riskcode_503_oss
	   else ty_riskcode_503_kfk
	   end as ty_riskcode_503
from var
"""

dc_tianyu_data = get_df_from_pg(dc_tianyu_sql)

dc_tianyu_data.shape
dc_tianyu_data.order_no.nunique()
dc_tianyu_data.isnull().sum()

dc_tianyu_data.to_csv(os.path.join(data_path,'dc_tianyu.csv'))
dc_tianyu_data.order_no = dc_tianyu_data.order_no.astype(str)


----------------------------同盾------------------------------

bk_td_var_sql = """
--多头 身份证手机
select order_no
,sum(case when rule_id = '47580344' and dimension='借款人身份证' then count else null end) as r47580344_id_platform_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='大数据金融' then count else null end) as r47580344_id_industry_1_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='大型消费金融公司' then count else null end) as r47580344_id_industry_6_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='P2P网贷' then count else null end) as r47580344_id_industry_9_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='互联网金融门户' then count else null end) as r47580344_id_industry_13_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='一般消费分期平台' then count else null end) as r47580344_id_industry_15_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='厂商汽车金融' then count else null end) as r47580344_id_industry_16_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='小额贷款公司' then count else null end) as r47580344_id_industry_18_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='房地产金融' then count else null end) as r47580344_id_industry_20_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='融资租赁' then count else null end) as r47580344_id_industry_21_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='理财机构' then count else null end) as r47580344_id_industry_22_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='第三方支付' then count else null end) as r47580344_id_industry_24_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' then count else null end) as r47580344_m_platform_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='大数据金融' then count else null end) as r47580344_m_industry_1_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='大型消费金融公司' then count else null end) as r47580344_m_industry_6_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='P2P网贷' then count else null end) as r47580344_m_industry_9_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='互联网金融门户' then count else null end) as r47580344_m_industry_13_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='一般消费分期平台' then count else null end) as r47580344_m_industry_15_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='厂商汽车金融' then count else null end) as r47580344_m_industry_16_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='小额贷款公司' then count else null end) as r47580344_m_industry_18_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='房地产金融' then count else null end) as r47580344_m_industry_20_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='融资租赁' then count else null end) as r47580344_m_industry_21_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='理财机构' then count else null end) as r47580344_m_industry_22_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='第三方支付' then count else null end) as r47580344_m_industry_24_ct

,sum(case when rule_id = '47580334' and dimension='借款人身份证' then count else null end) as r47580344_id_platform_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='大数据金融' then count else null end) as r47580334_id_industry_1_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='大型消费金融公司' then count else null end) as r47580334_id_industry_6_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='P2P网贷' then count else null end) as r47580334_id_industry_9_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='互联网金融门户' then count else null end) as r47580334_id_industry_13_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='一般消费分期平台' then count else null end) as r47580334_id_industry_15_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='厂商汽车金融' then count else null end) as r47580334_id_industry_16_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='小额贷款公司' then count else null end) as r47580334_id_industry_18_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='房地产金融' then count else null end) as r47580334_id_industry_20_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='融资租赁' then count else null end) as r47580334_id_industry_21_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='理财机构' then count else null end) as r47580334_id_industry_22_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='第三方支付' then count else null end) as r47580334_id_industry_24_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' then count else null end) as r47580344_m_platform_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='大数据金融' then count else null end) as r47580334_m_industry_1_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='大型消费金融公司' then count else null end) as r47580334_m_industry_6_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='P2P网贷' then count else null end) as r47580334_m_industry_9_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='互联网金融门户' then count else null end) as r47580334_m_industry_13_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='一般消费分期平台' then count else null end) as r47580334_m_industry_15_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='厂商汽车金融' then count else null end) as r47580334_m_industry_16_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='小额贷款公司' then count else null end) as r47580334_m_industry_18_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='房地产金融' then count else null end) as r47580334_m_industry_20_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='融资租赁' then count else null end) as r47580334_m_industry_21_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='理财机构' then count else null end) as r47580334_m_industry_22_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='第三方支付' then count else null end) as r47580334_m_industry_24_ct
from (
	select t1.order_no, rule_id
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'dimension'  as dimension
	,cast(json_array_elements(cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'detail' as json)) ::json ->> 'count' as int) as count
	,json_array_elements(cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'detail' as json)) ::json ->> 'industry_display_name' as industry_display_name
	from (select * from t_loan_performance
	      where business_id = 'xjbk' and effective_date  between '2018-08-26' and '2019-08-15') t1
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580344', '47580334')
) tmp
group by order_no
"""

bk_td_data = get_df_from_pg(bk_td_var_sql)

dc_td_var_sql = """
--多头 身份证手机
select order_no
,sum(case when rule_id = '47580344' and dimension='借款人身份证' then count else null end) as r47580344_id_platform_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='大数据金融' then count else null end) as r47580344_id_industry_1_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='大型消费金融公司' then count else null end) as r47580344_id_industry_6_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='P2P网贷' then count else null end) as r47580344_id_industry_9_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='互联网金融门户' then count else null end) as r47580344_id_industry_13_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='一般消费分期平台' then count else null end) as r47580344_id_industry_15_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='厂商汽车金融' then count else null end) as r47580344_id_industry_16_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='小额贷款公司' then count else null end) as r47580344_id_industry_18_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='房地产金融' then count else null end) as r47580344_id_industry_20_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='融资租赁' then count else null end) as r47580344_id_industry_21_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='理财机构' then count else null end) as r47580344_id_industry_22_ct
,sum(case when rule_id = '47580344' and dimension='借款人身份证' and industry_display_name='第三方支付' then count else null end) as r47580344_id_industry_24_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' then count else null end) as r47580344_m_platform_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='大数据金融' then count else null end) as r47580344_m_industry_1_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='大型消费金融公司' then count else null end) as r47580344_m_industry_6_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='P2P网贷' then count else null end) as r47580344_m_industry_9_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='互联网金融门户' then count else null end) as r47580344_m_industry_13_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='一般消费分期平台' then count else null end) as r47580344_m_industry_15_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='厂商汽车金融' then count else null end) as r47580344_m_industry_16_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='小额贷款公司' then count else null end) as r47580344_m_industry_18_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='房地产金融' then count else null end) as r47580344_m_industry_20_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='融资租赁' then count else null end) as r47580344_m_industry_21_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='理财机构' then count else null end) as r47580344_m_industry_22_ct
,sum(case when rule_id = '47580344' and dimension='借款人手机' and industry_display_name='第三方支付' then count else null end) as r47580344_m_industry_24_ct

,sum(case when rule_id = '47580334' and dimension='借款人身份证' then count else null end) as r47580344_id_platform_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='大数据金融' then count else null end) as r47580334_id_industry_1_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='大型消费金融公司' then count else null end) as r47580334_id_industry_6_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='P2P网贷' then count else null end) as r47580334_id_industry_9_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='互联网金融门户' then count else null end) as r47580334_id_industry_13_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='一般消费分期平台' then count else null end) as r47580334_id_industry_15_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='厂商汽车金融' then count else null end) as r47580334_id_industry_16_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='小额贷款公司' then count else null end) as r47580334_id_industry_18_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='房地产金融' then count else null end) as r47580334_id_industry_20_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='融资租赁' then count else null end) as r47580334_id_industry_21_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='理财机构' then count else null end) as r47580334_id_industry_22_ct
,sum(case when rule_id = '47580334' and dimension='借款人身份证' and industry_display_name='第三方支付' then count else null end) as r47580334_id_industry_24_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' then count else null end) as r47580344_m_platform_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='大数据金融' then count else null end) as r47580334_m_industry_1_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='大型消费金融公司' then count else null end) as r47580334_m_industry_6_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='P2P网贷' then count else null end) as r47580334_m_industry_9_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='互联网金融门户' then count else null end) as r47580334_m_industry_13_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='一般消费分期平台' then count else null end) as r47580334_m_industry_15_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='厂商汽车金融' then count else null end) as r47580334_m_industry_16_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='小额贷款公司' then count else null end) as r47580334_m_industry_18_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='房地产金融' then count else null end) as r47580334_m_industry_20_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='融资租赁' then count else null end) as r47580334_m_industry_21_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='理财机构' then count else null end) as r47580334_m_industry_22_ct
,sum(case when rule_id = '47580334' and dimension='借款人手机' and industry_display_name='第三方支付' then count else null end) as r47580334_m_industry_24_ct
from (
	select t1.order_no, rule_id
	,json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'dimension'  as dimension
	,cast(json_array_elements(cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'detail' as json)) ::json ->> 'count' as int) as count
	,json_array_elements(cast(json_array_elements(cast(json_array_elements(cast(risk_detail as json)) ::json ->> 'platform_detail_dimension' as json)) ::json ->> 'detail' as json)) ::json ->> 'industry_display_name' as industry_display_name
	from (select * from t_loan_performance
	      where business_id = 'tb' and effective_date  between '2018-08-26' and '2019-08-15') t1
	left join risk_mongo_installmentmessagerelated r on t1.order_no = r.orderno and r.topicname = 'Application_thirdPart_tongdunbefroeloan'
	left join bakrt_tongdun_loanreview_result t2 on r.messageno = t2.taskid and t2.rule_id in ('47580344', '47580334')
) tmp
group by order_no
"""
dc_td_data = get_df_from_pg(dc_td_var_sql)
