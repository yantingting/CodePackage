#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : UKU流程转化.py
@Time    : 2020-06-08 18:46
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import os
import sys
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.2f' % x)
import datetime
import numpy as np
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)
sys.path.append('/Users/yantingting/Documents/MintechJob/newgenie1/utils3/')
from data_io_utils import *

today_date = datetime.date.today().strftime('%m%d')
file_path = '//Users/yantingting/Seafile/风控/10 印尼/05_日常分析'

# 节点转化率（整体）
query1 = '''
SELECT
		substring(l.apply_time::varchar, 1, 10) as applytime,l.return_flag,r.pipelinename,r.pipelineid,
		r.freephotocard,l.human_trial,
		count(distinct l.id) as application ,
		count(distinct case when r.risklevel is not null and r.risklevel <> '' then l.id end) as RC ,
		count(distinct case when r.basicrulesnode = 'R' then l.id end) as basicrulesnode,
	    count(distinct case when r.modelnode = 'R' then l.id end) as modelnode,
	    count(distinct case when r.thirdpartrulenode = 'R' then l.id end) as thirdpartrulenode,
	    count(distinct case when r.thirdpartblacklist = 'R' then l.id end) as thirdpartblacklist,
		count(distinct case when r.risklevel in ('N', 'P') then l.id end) as RC_pass ,
		count(distinct case when photo_result is not null and photo_result <> '' and r.risklevel in ('N', 'P') then l.id end) as photo,
		count(distinct case when photo_result = 'ENABLE' and photo_result <> '' and r.risklevel in ('N', 'P') then l.id end) as photo_pass,
		count(distinct case when bank_card_result is not null and bank_card_result <> '' and r.risklevel in ('N', 'P') then l.id end) as bank ,
		count(distinct case when bank_card_result in('BIND', 'BINDING') and r.risklevel in ('N', 'P') and bank_card_result <> '' then l.id end) as bank_pass ,
		count(distinct case when bank_card_result in('BIND', 'BINDING') and photo_result = 'ENABLE' and r.risklevel in ('N', 'P') then l.id end) as bankandphoto_pass,
		count(distinct case when r.risklevel in ('N', 'P') and human_trial = 'HUMAN_TRIAL' then l.id end) as audit2,
		count(distinct case when r.risklevel in ('N', 'P') and human_trial = 'HUMAN_TRIAL' and loan_status not in ('DENIED', 'RESCIND', 'APPROVING', 'CREATED') then l.id end) as audit2pass,
		count(distinct case when loan_status not in ('DENIED', 'RESCIND', 'APPROVING', 'CREATED') then l.id else null end)as fangkuan
	from (select t1.*,
         case when t2.preloanid is not null then t2.preloanid else t1.id::varchar end as risk_loanid,t2.freerc
         from rt_t_gocash_core_loan t1 
         left join (select * from rt_risk_mongo_gocash_riskcontrolresult where freerc = 'true') t2 
         on t1.id::varchar = t2.loanid )l 
	left join rt_risk_mongo_gocash_installmentriskcontrolresult r on l.risk_loanid = r.loanid
	where l.effective_date>='2020-02-01' and device_approval !='ANDROID' and return_flag = 'false'
	--date(l.apply_time)>='2020-02-01' 
	group by 1,2,3,4,5,6;
'''



# 空值按照0处理
df1 = DataBase().get_df_from_pg(query1)
df1.shape
df1.head()
df1['application'].sum()
df1['rc'].sum()
df1[df1['application']!=df1['rc']]
# df1.to_excel(os.path.join(file_path,'转化率' + today_date + '.xlsx'),index=False)
df1.to_excel(os.path.join(file_path,'linkaja+ios_转化率' + today_date + '.xlsx'),index=False)


# freephotocard = '1' 表示免照片审核
query2 = '''
select t1.id as loan_id, t1.return_flag,date(t1.apply_time) as apply_day,t2.pipelinename,
case when t1.effective_date>'1970-01-01' then 1 else 0 end as fangkuan,
t2.basicrulesnode,t2.modelnode,t2.thirdpartrulenode,t2.thirdpartblacklist,t2.freephotocard,t1.human_trial
from 
(select t1.*,
case when t2.preloanid is not null then t2.preloanid else t1.id::varchar end as risk_loanid,t2.freerc
from rt_t_gocash_core_loan t1 
left join (select * from rt_risk_mongo_gocash_riskcontrolresult where freerc = 'true') t2 
on t1.id::varchar = t2.loanid ) t1 
left join rt_risk_mongo_gocash_installmentriskcontrolresult t2 
on t1.risk_loanid = t2.orderno
where date(t1.apply_time)>='2020-07-05'
'''
df2 = DataBase().get_df_from_pg(query2)
df2.shape
df2.head()
df2.to_excel(os.path.join(file_path,'temp' + today_date + '.xlsx'),index=False)

