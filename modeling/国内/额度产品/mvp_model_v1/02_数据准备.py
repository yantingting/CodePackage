import os
import numpy
import pandas

sys.path.append('/Users/Mint/Desktop/repos/genie')

import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *
from sklearn.model_selection import train_test_split



data_path = 'D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/01_data/raw data/'
local_data_path = 'D:/Model/201910_mvp_model/01_data/'
result_path = 'D:/Model/201910_mvp_model/02_result_model1/'

#2019/10/27取tb4000+9/20有贷后表现的数据

perf_sql = """
select order_no, effective_date, business_id, case when max_late_day >= 7 then 1 else 0 end as mob2_ever7
from t_loan_performance
where dt = '20191027' and business_id in ('tb', 'xjbk','rong360') and effective_date between '2019-07-04' and '2019-09-20'
union
SELECT t1.order_no, effective_date, business_id, case when sum(flag)=0 then 0 when sum(flag) >0 then 1 else null end as mob2_ever7
from (
	SELECT t1.order_no, t1.effective_date, t1.business_id, t2.total_terms ,t2.current_term,
		case when  t2.repay_status='COLLECTION' or 
		(t2.repay_status='COLLECTION_GATEWAY_REPAID' or t2.repay_status='COLLECTION_REPAID' or t2.repay_status='THIRDPART_COLLECTION_REPAID')
		and date(t3.paid_time) - t2.due_date >= 7 then 1 ELSE 0 end as flag
-- 		, t2.repay_status, to_date(t3.paid_time), t2.due_date
	FROM temp_credit_modelscore_v6yl t1
	INNER JOIN dw_dc_compensatory_cp_core_schedule t2 on t1.order_no=t2.order_no and t2.current_term<=2
	LEFT JOIN dw_dc_compensatory_cp_core_total_pay_flow t3 on t2.id=t3.schedule_id and t3.paychannel_flow_status= 'PAID'
	where t1.sample_set in ('train', 'test')) t1
group by order_no, effective_date, business_id
"""

perf_1027 = get_df_from_pg(perf_sql)
perf_1027.shape
perf_1027.mob2_ever7.value_counts()
perf_1027.loc[perf_1027.effective_date == '2019-09-19']

perf_1027['sample_set'] = np.where(perf_1027.effective_date >= '2019-08-22', 'test', 'train')

#>=8月22 test
perf_1027.loc[ perf_1027.effective_date >= '2019-08-15', 'sample_set'] = 'test'
perf_1027.loc[ (perf_1027.effective_date < '2019-08-15') & (perf_1027.effective_date >= '2019-07-04'), 'sample_set'] = 'train'
perf_1027.sample_set.value_counts(dropna = False)

perf_1027.to_excel(os.path.join(data_path, 'flag_20191027_wtb.xlsx'))
perf_1027 = pd.read_excel(os.path.join(data_path, 'flag_20191027_wtb.xlsx'))

tb_orderno = perf_1027.loc[perf_1027.sample_set.isnull(),'order_no']

train_order, test_order = train_test_split(tb_orderno, test_size= 0.5, random_state= 6 )
len(train_order)
len(test_order)

perf_1027.loc[(perf_1027.order_no.isin(train_order)) & (perf_1027.RMS002 > 200)].mob2_ever7.value_counts() #601
perf_1027.loc[(perf_1027.order_no.isin(test_order)) & (perf_1027.RMS002 > 200)].mob2_ever7.value_counts() #600

perf_1027.loc[(perf_1027.order_no.isin(train_order)),'cp_sampleset'] = 'train'
perf_1027.loc[(perf_1027.order_no.isin(test_order)),'cp_sampleset'] = 'test'

perf_1027.to_excel(os.path.join(data_path, 'flag_20191027_wtb.xlsx'))

perf_1027 = perf_1027.drop(['cp_sample_set','cp_yl_sampleset'],1)

#导入数据 13个excel
baseinfo = pd.read_excel(os.path.join(data_path, 'r_cust_f.xlsx'))
gps = pd.read_excel(os.path.join(data_path, 'r_gps_f.xlsx'))
#set(br_all_data.columns) - set(br.columns)
hds = pd.read_excel(os.path.join(data_path, 'r_hds_20.xlsx'))
jxl = pd.read_excel(os.path.join(data_path, 'r_jxl_f.xlsx'))
td_loanview = pd.read_excel(os.path.join(data_path, 'r_td_loanreview_f.xlsx')) #4000单没有数据
td_micscore = pd.read_excel(os.path.join(data_path, 'r_td_microscore_f.xlsx'))
td_var = pd.read_excel(os.path.join(data_path, 'r_td_var_f.xlsx'))
ty = pd.read_excel(os.path.join(data_path, 'r_ty_f.xlsx'))
xd = pd.read_excel(os.path.join(data_path, 'r_xd_f.xlsx'))
xy = pd.read_excel(os.path.join(data_path, 'r_xy_f.xlsx'))
yl = pd.read_excel(os.path.join(data_path, 'r_yl.xlsx'))
zzc = pd.read_excel(os.path.join(data_path, 'r_zzc.xlsx'))
br = pd.read_excel(os.path.join(data_path, 'br_data.xlsx'))

baseinfo.shape
gps.shape
hds.shape
jxl.shape
td_loanview.shape
td_micscore.shape
td_var.shape
set(baseinfo.order_no.unique()) - set(td_var.order_no.unique())


all_x = baseinfo.merge(gps, on = 'order_no').merge(hds, on = 'order_no').merge(jxl, on = 'order_no').merge(td_loanview, on = 'order_no').merge(td_micscore, on = 'order_no')\
                .merge(td_var, on = 'order_no', how = 'left').merge(ty, on = 'order_no').merge(xd, on = 'order_no').merge(xy, on = 'order_no').merge(yl, on = 'order_no').merge(zzc, on = 'order_no')\
                .merge(br, on = 'order_no')

all_x.shape

var_dict = pd.read_excel('D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/建模代码可用变量字典.xlsx', sheet_name = '02_字典')

pd.DataFrame(list(set(all_x.columns) - set(var_dict.指标英文.unique()))).to_excel(local_data_path + 'add_cols_v2.xlsx')


x_cols = list(set(all_x.columns).intersection(set(var_dict.指标英文.unique())))
len(x_cols) #1155

all_x[x_cols] =all_x[x_cols].replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1]).fillna(-1)

for i in all_x.columns:
    if i not in ['order_no']:
        all_x[i] = all_x[i].astype(float)

save_data_to_pickle(all_x, local_data_path, 'all_x_1028.pkl') # (12299, 1156)

#合并x和y
x_with_y = perf_1027.drop(['drs_nodebtscore','RMS002'],1).merge(all_x, on = 'order_no')


save_data_to_pickle(x_with_y, local_data_path, 'x_with_y_1028.pkl') # (11157, 1161)

x_with_y = load_data_from_pickle(local_data_path, 'x_with_y_1028.pkl')

all_x = load_data_from_pickle(local_data_path, 'all_x_1028.pkl')

base_table = pd.read_excel(os.path.join(data_path, 'r_base_f.xlsx'))
perf_1101 = pd.read_excel(os.path.join(data_path, 'flag_20191101_wtb.xlsx'))


x_with_y = base_table.merge(perf_1101.drop(['effective_date','business_id','fst_term_due_date'],1), on = 'order_no' ,how = 'left').merge(all_x, on = 'order_no', how = 'left')
x_with_y.shape


"""
10/29 更新ever0 flag
"""
perf_sql2 = """
select order_no, effective_date, business_id, case when max_late_day >= 7 then 1 else 0 end as mob2_ever7
, case when max_late_day = 0 then 1 else 0 end as mob2_ever0
from t_loan_performance
where dt = '20191031' and business_id in ('tb', 'xjbk','rong360') and effective_date between '2019-07-04' and '2019-09-20'
union
select order_no
,effective_date
, business_id
, case when sum(ever7_flag)=0 then 0 when sum(ever7_flag) >0 then 1 else null end as mob2_ever7
, case when sum(ever0_flag)<2 then 0 when sum(ever0_flag) = 2 then 1 else null end as mob2_ever0
from (
SELECT t1.order_no
, t1.effective_date
, t1.business_id
, t2.total_terms 
,t2.current_term
,case when  t2.repay_status='COLLECTION' or 
         (t2.repay_status='COLLECTION_GATEWAY_REPAID' or t2.repay_status='COLLECTION_REPAID' or t2.repay_status='THIRDPART_COLLECTION_REPAID'
		or t2.repay_status = 'COLLECTION_ADVANCED_PAIDOFF')
		and date(t3.paid_time) - t2.due_date >= 7 then 1 ELSE 0 end as ever7_flag
,case when  t2.repay_status in ('ADVANCED_REPAID','REPAID','ADVANCED_PAIDOFF') then 1 else 0 end as ever0_flag
FROM temp_credit_modelscore_v6yl t1
INNER JOIN dw_dc_compensatory_cp_core_schedule t2 on t1.order_no=t2.order_no and t2.current_term<=2
LEFT JOIN dw_dc_compensatory_cp_core_total_pay_flow t3 on t2.id=t3.schedule_id and t3.paychannel_flow_status= 'PAID'
where t1.sample_set in ('train', 'test')) t1
group by order_no, effective_date, business_id
"""

perf_1027_v2 = get_df_from_pg(perf_sql2)
perf_1027_v2.mob2_ever0.value_counts()
perf_1027 = perf_1027.merge(perf_1027_v2[['order_no','mob2_ever0']])
perf_1027.to_excel(data_path + 'flag_20191027_wtb_v2.xlsx')

"""
2019/10/30 更新flag
"""

perf_sql3 = """
select order_no
, effective_date
, business_id
, case when fst_term_late_day >= 1 then 1 else 0 end as fidx
, case when fst_term_late_day >= 3 then 1 else 0 end as fid3
, case when fst_term_late_day >= 7 then 1 else 0 end as fid7
--, case when fst_term_late_day >= 15 then 1 else 0 end as fid15
--, case when fst_term_late_day >= 30 then 1 else 0 end as fid30
--, case when max_late_day = 0 then 1 else 0 end as ever0
, case when max_late_day >= 1 then 1 else 0 end as evex 
, case when max_late_day >= 3 then 1 else 0 end as ever3
, case when max_late_day >= 7 then 1 else 0 end as ever7
--, case when max_late_day >= 15 then 1 else 0 end as ever15
--, case when max_late_day >= 30 then 1 else 0 end as ever30
, fst_term_due_date
--, round(floor((date(dt) - date(effective_date))/30)) as mob
from t_loan_performance
where dt = '20191106' and business_id in ('tb', 'xjbk','rong360') and effective_date between '2019-07-04' and '2019-09-30'
union
select order_no
,effective_date
, business_id
, sum(case when current_term = 1 and everx_flag = 1 then 1 else 0 end) as fidx
, sum(case when current_term = 1 and ever3_flag = 1 then 1 else 0 end) as fid3
=, sum(case when current_term = 1 and ever7_flag = 1 then 1 else 0 end) as fid7
--, sum(case when current_term = 1 and ever15_flag = 1 then 1 else 0 end) as fid15
--, sum(case when current_term = 1 and ever30_flag = 1 then 1 else 0 end) as fid30
--, case when sum(ever0_flag)<2 then 0 when sum(ever0_flag) = 2 then 1 else null end as ever0
, case when sum(everx_flag)=0 then 0 when sum(everx_flag) >0 then 1 else null end as everx
, case when sum(ever3_flag)=0 then 0 when sum(ever3_flag) >0 then 1 else null end as ever3
, case when sum(ever7_flag)=0 then 0 when sum(ever7_flag) >0 then 1 else null end as ever7
--, case when sum(ever15_flag)=0 then 0 when sum(ever15_flag) >0 then 1 else null end as ever15
--, case when sum(ever30_flag)=0 then 0 when sum(ever30_flag) >0 then 1 else null end as ever30
, fst_term_due_date
--, mob
from (
SELECT t1.order_no
, t1.effective_date
, t1.business_id
, t2.total_terms 
,t2.current_term
,case when  t2.repay_status in ('ADVANCED_REPAID','REPAID','ADVANCED_PAIDOFF') then 1 else 0 end as ever0_flag
,case when  t2.repay_status='COLLECTION' or 
         (t2.repay_status='COLLECTION_GATEWAY_REPAID' or t2.repay_status='COLLECTION_REPAID' or t2.repay_status='THIRDPART_COLLECTION_REPAID'
		or t2.repay_status = 'COLLECTION_ADVANCED_PAIDOFF')
		and date(t3.paid_time) - t2.due_date >= 1 then 1 ELSE 0 end as everx_flag
,case when  t2.repay_status='COLLECTION' or 
         (t2.repay_status='COLLECTION_GATEWAY_REPAID' or t2.repay_status='COLLECTION_REPAID' or t2.repay_status='THIRDPART_COLLECTION_REPAID'
		or t2.repay_status = 'COLLECTION_ADVANCED_PAIDOFF')
		and date(t3.paid_time) - t2.due_date >= 3 then 1 ELSE 0 end as ever3_flag
,case when  t2.repay_status='COLLECTION' or 
         (t2.repay_status='COLLECTION_GATEWAY_REPAID' or t2.repay_status='COLLECTION_REPAID' or t2.repay_status='THIRDPART_COLLECTION_REPAID'
		or t2.repay_status = 'COLLECTION_ADVANCED_PAIDOFF')
		and date(t3.paid_time) - t2.due_date >= 7 then 1 ELSE 0 end as ever7_flag
,case when  t2.repay_status='COLLECTION' or 
         (t2.repay_status='COLLECTION_GATEWAY_REPAID' or t2.repay_status='COLLECTION_REPAID' or t2.repay_status='THIRDPART_COLLECTION_REPAID'
		or t2.repay_status = 'COLLECTION_ADVANCED_PAIDOFF')
		and date(t3.paid_time) - t2.due_date >= 15 then 1 ELSE 0 end as ever15_flag
,case when  t2.repay_status='COLLECTION' or 
         (t2.repay_status='COLLECTION_GATEWAY_REPAID' or t2.repay_status='COLLECTION_REPAID' or t2.repay_status='THIRDPART_COLLECTION_REPAID'
		or t2.repay_status = 'COLLECTION_ADVANCED_PAIDOFF')
		and date(t3.paid_time) - t2.due_date >= 30 then 1 ELSE 0 end as ever30_flag
, null as fst_term_due_date
, 2  as mob
FROM temp_credit_modelscore_v6yl t1
INNER JOIN dw_dc_compensatory_cp_core_schedule t2 on t1.order_no=t2.order_no and t2.current_term<=2
LEFT JOIN dw_dc_compensatory_cp_core_total_pay_flow t3 on t2.id=t3.schedule_id and t3.paychannel_flow_status= 'PAID'
where t1.sample_set in ('train', 'test')
) t1
group by order_no, effective_date, business_id, fst_term_due_date, mob
"""
perf_1106 = get_df_from_pg(perf_sql3)

perf_1106.head()
perf_1106.order_no = perf_1101.order_no.astype(str)
perf_1106.fst_term_due_date.value_counts().sort_index()
perf_1106.ever7.sum()
perf_1106.isnull().sum()

perf_1106.to_excel(data_path + 'flag_20191106_wtb.xlsx')

perf_1027_v2 = pd.read_excel(local_data_path + 'flag_20191027_wtb_local.xlsx',sheet_name = 'raw_data')
perf_1027_v2 = perf_1027_v2.merge(perf_1027_v3.drop(['effective_date', 'business_id', 'mob2_ever7', 'mob2_ever0'],1 ), on = 'order_no')
perf_1027_v2.to_excel(local_data_path + 'flag_20191027_wtb_local_v2.xlsx')
