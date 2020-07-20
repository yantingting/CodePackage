## trytry

"""
小橘猫和现金白卡在花不完用户交叉
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

data_path = 'D:/Model/201908_qsfq_model/00_sample_analysis/'



"""白卡在花不完的表现"""

bk_hbw_perf = """
select t4.order_no as bk_order_no
, date(t4.created_time) as created_time
, t4.business_id
, t4.customer_id as bk_customer_id
, t1.id
, t2.customerid as hbw_customer_id
, t2.orderno as hbw_order_no
, risklevel
, pipelinename
, existLoanWith4fieldsXqd
, denied30daysWith6fieldsHbw
, duplicateCustomerWith6fieldsHbw
, rejectByXqdIn30Day
, callWithAntifraud30dGuys
, lastoverdueDays
, auditBlack
, blacklist
, tianqiFraudLevelResult
, tianchuangBlacklist
, highRiskTotal
, t3.effective_date
, t3.max_late_day
, t3.late_day
, t3.order_num
from dw_bk_bk_core_bk_core_application t4
left join dw_bk_uc_bk_customer t0 on t0.id = t4.customer_id
left join (select * from dw_fq_uc_customer where business_id = 'hbw') t1 ON t0.cell_phone = t1.cell_phone
left join (select * from ods_rsk_installmentriskcontrolresult where businessid ='hbw' and pipelinename <> '花不完全拒绝')t2 on cast(t1.id as varchar) = t2.customerid
left join (select * from t_loan_performance where dt = '20190820' and business_id = 'hbw') t3 on t2.orderno = t3.order_no
where date(t4.created_time) between '2018-09-26' and '2019-08-15'
"""

hbw_data = get_df_from_pg(bk_hbw_perf)
hbw_data = hbw_data.drop_duplicates()
hbw_data.shape #(29246, 22)

hbw_data.columns

hbw_data.bk_order_no.value_counts()

1- hbw_data.isnull().sum()/hbw_data.shape[0]
hbw_data.bk_customer_id.nunique()  #25603
hbw_data.bk_order_no.nunique() #27870
hbw_data.id.nunique() #2751
hbw_data.hbw_customer_id.nunique() #2005
hbw_data.hbw_order_no.nunique() #3235
hbw_data.loc[hbw_data.effective_date.notnull()].shape
hbw_data.hbw_order_no_wd.nunique() #468

hbw_data.to_excel(os.path.join(data_path,'bk_hbw_perf.xlsx'))



"""小橘猫"""

hbw_perf_xjm = """ select t4.order_no as xjm_order_no
, date(t4.created_time)  as created_time
, t4.business_id
, t4.customer_id as xjm_customer_id
, t1.id AS id
, t2.customerid as hbw_customer_id
, t2.orderno as hbw_order_no
, risklevel
, pipelinename
, existLoanWith4fieldsXqd
, denied30daysWith6fieldsHbw
, duplicateCustomerWith6fieldsHbw
, rejectByXqdIn30Day
, callWithAntifraud30dGuys
, lastoverdueDays
, auditBlack
, blacklist
, tianqiFraudLevelResult
, tianchuangBlacklist
, highRiskTotal
, t3.effective_date
, t3.max_late_day
, t3.late_day
, t3.order_num
from dw_dc_compensatory_cp_core_application t4
left join dw_dc_uc_customer t0 on t0.customer_id = t4.customer_id
left join (select * from dw_fq_uc_customer where business_id = 'hbw') t1 ON t0.cell_phone = t1.cell_phone
left join (select * from ods_rsk_installmentriskcontrolresult where businessid ='hbw' and pipelinename <> '花不完全拒绝')  t2 on cast(t1.id as varchar) = t2.customerid
left join (select * from t_loan_performance where dt = '20190820' and business_id = 'hbw') t3 on t2.orderno = t3.order_no --id card 明文
where date(t4.created_time) between '2018-09-26' and '2019-08-15'
"""

hbw_data_xjm = get_df_from_pg(hbw_perf_xjm)

hbw_data_xjm = hbw_data_xjm.drop_duplicates()
hbw_data_xjm.shape #(147058, 24)


1- hbw_data_xjm.isnull().sum()/hbw_data_xjm.shape[0]

hbw_data_xjm.columns

hbw_data_xjm.xjm_customer_id.nunique()  #120187

hbw_data_xjm.xjm_order_no.nunique() #128526

hbw_data_xjm.hbw_customer_id.nunique() #30092
hbw_data_xjm.hbw_order_no.nunique() #45200

hbw_data_xjm.max_late_day.max()


hbw_data_xjm.to_excel(os.path.join(data_path,'xjm_hbw_perf.xlsx'))

#合并数据
set(hbw_data_xjm.columns) - set(hbw_data.columns)
set(hbw_data.columns) - set(hbw_data_xjm.columns)

#
hbw_data.business_id.value_counts(dropna = False)
hbw_data.business_id = hbw_data.business_id.replace('bk','xjbk')
hbw_data_xjm.business_id.value_counts(dropna = False)

#
hbw_data = hbw_data.rename(columns = {'bk_customer_id':'customer_id','bk_order_no':'order_no'})
hbw_data_xjm = hbw_data_xjm.rename(columns = {'xjm_customer_id':'customer_id','xjm_order_no':'order_no'})

hbw_data_perf = pd.concat([hbw_data,hbw_data_xjm])
hbw_data_perf.to_excel(os.path.join(data_path,'all_hbw_perf.xlsx'))
hbw_data_perf = pd.read_excel(os.path.join(data_path, 'all_hbw_perf.xlsx'))

hbw_data_perf.shape
hbw_data_perf.columns

########################################
#    白卡 & 小橘猫 在花不完有放款表现    #
########################################

hbw_data_perf.loc[hbw_data_perf.effective_date.notnull()].shape
hbw_data_perf.loc[hbw_data_perf.effective_date.notnull()].risklevel.value_counts(dropna = False)
hbw_dd = hbw_data_perf.loc[hbw_data_perf.effective_date.notnull()][['order_no','created_time','business_id','customer_id','id','hbw_customer_id','hbw_order_no','effective_date','max_late_day','late_day','order_num']] \

hbw_dd = hbw_dd.drop_duplicates()
hbw_dd.shape #6683

hbw_dd.order_no = hbw_dd.order_no.astype(str)
hbw_dd.customer_id = hbw_dd.customer_id.astype(str)
hbw_dd.hbw_order_no = hbw_dd.hbw_order_no.astype(str)
hbw_dd.hbw_customer_id = hbw_dd.hbw_customer_id.astype(str)

hbw_dd.order_no.nunique()  #5235
hbw_dd.customer_id.nunique()  #4657
hbw_dd.hbw_order_no.nunique() #5833
hbw_dd.hbw_customer_id.nunique() #4612

hbw_dd[['order_no','customer_id']].groupby('customer_id')['order_no'].nunique().sort_index()  # 1000000078 对应了2条Bk的order_no
hbw_dd[['hbw_customer_id','customer_id']].groupby('hbw_customer_id')['customer_id'].nunique().sort_index()  # hbw_customer_id = 400213120 在xjbk和tb上各自申请了一次，
hbw_dd.to_excel(os.path.join(data_path,'hbw_dd.xlsx'))

hbw_dd.effective_date.value_counts().sort_index()
hbw_dd = hbw_dd.loc[hbw_dd.]

#以在白卡和小橘猫上的order_no为主键，计算在花不完的表现
hbw_temp = hbw_dd.groupby('order_no')['max_late_day'].max().reset_index()
hbw_temp2 = hbw_dd[['order_no','hbw_customer_id','effective_date','max_late_day']].drop_duplicates().merge(hbw_temp, how = 'inner', on = ['order_no','max_late_day'])
hbw_temp2.order_no.value_counts()
hbw_temp2.loc[hbw_temp2.order_no == 'CP2019010510342622110517']


hbw_dd2 = hbw_dd[['order_no','created_time','business_id','customer_id','id','hbw_customer_id']].drop_duplicates().merge(hbw_temp2, on = 'order_no')
hbw_dd2 = hbw_dd2.sort_values(by = ['order_no','effective_date'], ascending= True).drop_duplicates( 'order_no', keep='first')
hbw_dd2.shape

hbw_dd2.to_excel(os.path.join(data_path,'hbw_dd_final.xlsx'))

hbw_dd2.customer_id.value_counts()

hbw_dd2.loc[hbw_dd2.customer_id == '21980']

########################################
#    白卡 & 小橘猫 在花不完有申请记录    #
########################################

has_dd_list = list(hbw_dd2.order_no)
hbw_apply = hbw_data_perf.loc[(hbw_data_perf.hbw_order_no.notnull()) & (~hbw_data_perf.order_no.isin( has_dd_list ))]
hbw_apply = hbw_data_perf.loc[(hbw_data_perf.hbw_order_no.notnull())]

hbw_apply.shape

hbw_apply = hbw_apply.drop_duplicates()
hbw_apply.risklevel.value_counts()

hbw_apply.shape #43179

hbw_apply.order_no.nunique() #29883
hbw_apply.hbw_order_no.nunique() #37931

hbw_apply.hbw_customer_id.nunique() #27242

hbw_apply.order_no.value_counts()

hbw_apply.hbw_order_no.value_counts()
hbw_apply.loc[hbw_apply.hbw_order_no == 'IM2018122610541133473358']
hbw_apply.loc[hbw_apply.order_no == '154329057318666242']

#以order_no维度计算risklevel, 各个规则通过拒绝情况
hbw_apply_temp = hbw_apply.groupby(['order_no'])[['risklevel','existloanwith4fieldsxqd','denied30dayswith6fieldshbw','duplicatecustomerwith6fieldshbw','rejectbyxqdin30day','callwithantifraud30dguys','lastoverduedays','auditblack','blacklist','tianqifraudlevelresult','tianchuangblacklist','highrisktotal']].max()
hbw_apply_temp

rule_list = ['risklevel'
    ,'existloanwith4fieldsxqd'
    ,'denied30dayswith6fieldshbw'
    ,'duplicatecustomerwith6fieldshbw'
    ,'rejectbyxqdin30day'
    ,'callwithantifraud30dguys'
    ,'lastoverduedays'
    ,'auditblack'
    ,'blacklist'
    ,'tianqifraudlevelresult'
    ,'tianchuangblacklist'
    ,'highrisktotal']

hbw_apply = hbw_apply.replace('R',1).replace('P',0).replace('N',0).replace('reject',1).replace('pass',0)

hbw_apply_temp = hbw_apply.groupby(['order_no'])[['risklevel','existloanwith4fieldsxqd','denied30dayswith6fieldshbw','duplicatecustomerwith6fieldshbw','rejectbyxqdin30day',\
                                                  'callwithantifraud30dguys','lastoverduedays','auditblack','blacklist','tianqifraudlevelresult','tianchuangblacklist','highrisktotal']].sum().reset_index()

hbw_apply_temp.shape
hbw_apply_temp.order_no.nunique()

writer = pd.ExcelWriter(os.path.join(data_path, 'hbw_only_apply_v2.xlsx'))
#hbw_apply.to_excel(writer, 'hbw_apply', index=True)
hbw_apply_temp.to_excel(writer, 'hbw_apply_temp',  index=True)
writer.save()


max(['R','P','N'])
max(['pass','reject',''])

"""          白卡&小橘猫申请用户命中黑名单的申请                 """

"""白卡"""
bk_apply_sql = """
select a.order_no
, a.customer_id
, date(a.created_time) as created_time
, rc.risklevel
, rc.pipelinename
,case when xinyan is null then null when cast(xinyan ::json #>>'{t,behavior_data,data,report_detail,loans_overdue_count}' as int) >= 1 then 1 else 0 end as xy_overdue_flag
,case when bl.id is null then null else 1 end as audit_flag
, case when blacklist is null then null when blacklist='reject' then 1 else 0 end as blacklist_flag
, case when shumeimaxoverduelevel is null then null else 1 end as tot_third
, case when shumeimaxoverduelevel is null then null when shumeimaxoverduelevel ='reject' then 1 else 0 end as shumeimaxoverduelevel_flag
, case when xinyanadviceblack is null then null when xinyanadviceblack ='reject' then 1 else 0 end as xinyanadviceblack
, case when tianyublacklist is null then null when tianyublacklist ='reject' then 1 else 0 end as tianyublacklist
, case when xdblacklist='' or xdblacklist is null then null when xdblacklist ='reject' then 1 else 0 end as xdblacklist
from dw_bk_bk_core_bk_core_application a
left join bk_xinyan_application xy on a.order_no = xy.order_no  and xinyan <> ''
left join dw_bk_uc_bk_customer c on a.customer_id = c.id
left join dw_xs_credit_audit_audit_blacklist_review bl on c.cell_phone = bl.phone
left join ods_rsk_installmentriskcontrolresult rc on a.order_no = rc.orderno --and rc.id not in ('5d329d8989b6c918f925b69d', '5d329d9e89b6c918f925b6a0', '5d329d98d738c97a19ab3f59', '5c30d804efaf1e368b09dbf6')
where date(a.created_time) between '2018-09-26' and '2019-08-15'
"""
bk_apply_flag2 = get_df_from_pg(bk_apply_sql)
bk_apply_flag2 = bk_apply_flag2.drop_duplicates()
bk_apply_flag2.shape #27870
bk_apply_flag2.order_no.nunique()  #27870
bk_apply_flag2.customer_id.nunique() #25603
a = bk_apply_flag2.customer_id.value_counts().reset_index().rename(columns = {'index':'customer_id','customer_id':'cnt_apply'})

bk_apply_flag2 = bk_apply_flag2.merge(a, on = 'customer_id', how = 'left')


bk_apply_flag2.loc[bk_apply_flag2.customer_id == 1000012590]

bk_apply_flag2.to_excel(os.path.join(data_path,'bk_apply_flag.xlsx'))


"""卡贷"""
dc_apply_sql = """
select a.order_no, a.customer_id, date(a.created_time) as created_time
,case when xinyan is null then null when cast(xinyan ::json #>>'{t,behavior_data,data,report_detail,loans_overdue_count}' as int) >= 1 then 1 else 0 end as xy_overdue_flag
,case when bl.id is null then null else 1 end as audit_flag
, case when blacklist is null then null when blacklist='reject' then 1 else 0 end as blacklist_flag
, case when shumeimaxoverduelevel is null then null else 1 end as tot_third
, case when shumeimaxoverduelevel is null then null when shumeimaxoverduelevel ='reject' then 1 else 0 end as shumeimaxoverduelevel_flag
, case when xinyanadviceblack is null then null when xinyanadviceblack ='reject' then 1 else 0 end as xinyanadviceblack
, case when tianyublacklist is null then null when tianyublacklist ='reject' then 1 else 0 end as tianyublacklist
, case when xdblacklist='' or xdblacklist is null then null when xdblacklist ='reject' then 1 else 0 end as xdblacklist
from dw_dc_compensatory_cp_core_application a
left join dc_xinyan_application xy on a.order_no = xy.order_no  and xinyan <> ''
left join dw_dc_uc_customer c on a.customer_id = c.customer_id
left join dw_xs_credit_audit_audit_blacklist_review bl on c.cell_phone = bl.phone
left join ods_rsk_installmentriskcontrolresult rc on a.order_no = rc.orderno and rc.id not in ('5d329d8989b6c918f925b69d', '5d329d9e89b6c918f925b6a0', '5d329d98d738c97a19ab3f59', '5c30d804efaf1e368b09dbf6')
where date(a.created_time) between '2018-09-26' and '2019-08-15'
"""

dc_apply_flag = get_df_from_pg(dc_apply_sql)
dc_apply_flag = dc_apply_flag.drop_duplicates()
dc_apply_flag.shape #128526
dc_apply_flag.order_no.nunique()  #128526
dc_apply_flag.customer_id.nunique() #120187

dc_apply_flag.customer_id.value_counts()
dc_apply_flag.loc[dc_apply_flag.customer_id == 852]
b = dc_apply_flag.customer_id.value_counts().reset_index().rename(columns = {'index':'customer_id','customer_id':'cnt_apply'})

dc_apply_flag = dc_apply_flag.merge(b, on = 'customer_id', how = 'left')

dc_apply_flag.to_excel(os.path.join(data_path,'dc_apply_flag.xlsx'))

dc_apply_flag['business_id'] = 'xjm'
bk_apply_flag2['business_id'] = 'xjbk'

all_apply_flag = pd.concat([dc_apply_flag, bk_apply_flag2]) #156396
all_apply_flag.to_excel(os.path.join(data_path,'all_apply_flag.xlsx'))


"""                  现金白卡有表现样本                 """

xjbk_sql \
    xjm_sql= """
select t0.order_no
    , date(createtime) as createtime
    , pipelinename
    , risklevel
    ,blacklist
    ,contactEmpty
    ,contactsCountLessThan14
    ,existLoanWith6fieldsHbw
    ,existLoanWith7fieldsKadai
    ,denied30daysWith6fieldsHbw
    ,denied30daysWith7fieldsKadai
    ,duplicateCustomerWith7fieldsKadai
    ,genderMatchingIdcard
    ,idAddressForbidenArea
    ,invalidAge
    ,maxOverdueDaysKadai
    ,referenceOveringOrOveredKadai
    ,callWithBlackList
    ,callWithContact
    ,callWithOverdue30dGuys
    ,notCallWithReference
    ,openTimeLess6Month
    ,operatorPhoneNotBankBindPhone
    --,operatorEmpty
    --,identityCodeValid
    --,receiverAddressHasNone
    --,taobaoReceiverPhoneListIsCellphoneOrReferphone
    --,taobaoUserMobileIsCellphone
    --,tbOrderTime
    ,shumeiMaxOverdueLevel
    ,xinyanAdviceBlack
    --,juxinliEmpty
    --,loanActionScore
    --,loanFailureCountIn12Month
    --,tianqiFraudLevelResult
    --,tianyuBlacklist
    --,xinyanBehaviorDataCode
    --,xinyanEmpty
    --,xinyanDeductFailLast1month
    ,XDBlacklist
    --,t2.effective_date
    --,t2.late_day
    --,t2.max_late_day
    --,t2.fst_term_due_date
    --,t2.fst_term_late_day
--from dw_bk_bk_core_bk_core_application t0 
from dw_dc_compensatory_cp_core_application t0
left join (select * from ods_rsk_installmentriskcontrolresult where businessid = 'tb') t1 on t0.order_no = t1.orderno
--left join (select * from t_loan_performance where dt = '20190822') t2 on t1.orderno = t2.order_no
where date(t0.created_time) between '2018-09-26' and '2019-08-15'
"""

xjbk_perf = get_df_from_pg(xjbk_perf)
xjbk_perf = xjbk_perf.drop_duplicates()
xjbk_perf.head()

xjbk_perf.shape
xjbk_perf.order_no.nunique()


xjbk_perf.pipelinename.value_counts()

xjbk_perf.max_late_day.value_counts().sort_index()
xjbk_perf.fst_term_late_day.value_counts().sort_index()

xjbk_perf['fid7'] = np.where(xjbk_perf.max_late_day>=7,1,0)

xjbk_perf.fid7.value_counts() #只有16个人有7+表现
xjbk_perf.loc[xjbk_perf.fst_term_due_date <= '2019-08-15']['fst_term_due_date'].shape
16/1100

xjm_perf = get_df_from_pg(xjm_sql)
xjm_perf = xjm_perf.drop_duplicates('order_no')
xjm_perf.shape
xjm_perf.order_no.nunique()

#读入表现数据
xjm_perf_0801 = pd.read_excel(os.path.join(data_path,'all_perf_0801.xlsx'), sheetname = 'perf_terms')
xjm_perf_0801 = xjm_perf_0801.rename(columns = {'t1.order_no':'order_no','late_days':'max_late_days'})[['order_no','effective_date','max_late_days']]

xjm_perf = xjm_perf.merge(xjm_perf_0801, on = 'order_no', how = 'left')
xjm_perf.columns
xjm_perf = xjm_perf.rename(columns = {'effective_date_x':'effective_date'}).drop(['late_day'],1)
xjm_perf = xjm_perf.drop(['effective_date_y'],1)


xjm_bk_perf = pd.concat([xjm_perf, xjbk_perf.drop(['late_day','fst_term_due_date','fst_term_late_day','fid7'],1)])
xjm_bk_perf.shape

xjm_bk_perf.shape

# fraud_list = ['blacklist'
# ,'contactempty'
# ,'contactscountlessthan14'
# ,'existloanwith6fieldshbw'
# ,'existloanwith7fieldskadai'
# ,'denied30dayswith6fieldshbw'
# ,'denied30dayswith7fieldskadai'
# ,'duplicatecustomerwith7fieldskadai'
# ,'gendermatchingidcard'
# ,'idaddressforbidenarea'
# ,'invalidage'
# ,'maxoverduedayskadai'
# ,'referenceoveringoroveredkadai'
# ] #13
#
# oper_list = ['callwithblacklist'
# ,'callwithcontact'
# ,'callwithoverdue30dguys'
# ,'notcallwithreference'
# ,'opentimeless6month'
# ,'operatorphonenotbankbindphone'] #6
#
#
# third_list = ['shumeimaxoverduelevel'
# ,'xinyanadviceblack'
# ,'xdblacklist'
# ] #3




#读入数据
tb_blacklist = pd.read_csv(os.path.join(data_path, 'raw_blacklist_tb.csv'))
xjbk_blacklist = pd.read_csv(os.path.join(data_path, 'raw_blacklist_xjbk.csv'))

tb_blacklist['business_id'] = 'tb'
xjbk_blacklist['business_id'] = 'xjbk'

xjbk_blacklist.shape

black_list = pd.concat([tb_blacklist, xjbk_blacklist]) #轻松分期命中黑名单

hbw_dd2 = pd.read_excel(os.path.join(data_path,'hbw_dd_final.xlsx')) #花不完有贷款表现
hbw_temp #花不完有申请表现

xjm_bk_perf.head() #轻松分期风控结果&有表现数据

all_data = xjm_bk_perf.merge(black_list, on = 'order_no', how = 'left').merge(hbw_dd2.drop(['created_time','business_id','hbw_customer_id_x'],1), on = 'order_no', how = 'left')
all_data.columns
all_data.to_excel(os.path.join(data_path, 'all_data_v2.xlsx'))


all_data = black_list.merge(hbw_apply_temp, how = 'left', on = 'order_no').merge(hbw_dd2.drop(['created_time','business_id','hbw_customer_id_x'],1), on = 'order_no', how = 'left')
all_data.columns
all_data.created_time =all_data.created_time.apply(lambda x:str(x)[0:11])
all_data.head()
all_data.to_excel(os.path.join(data_path, 'all_data.xlsx'))

all_data.loc[all_data.order_no == '156272421572182019']

all_data = pd.read_excel(os.path.join(data_path, 'all_data.xlsx'))
all_data.rename(columns = {'effective_date_x':'effective_date','effective_date_y':'hbw_effective_date'})
all_data.columns

