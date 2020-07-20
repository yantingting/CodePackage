import psycopg2
import pandas as pd
import json
import os

sys.path.append('/Users/Mint/Desktop/repos/genie')

import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *

data_path = 'D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/01_data/raw data/'
local_data_path = 'D:/Model/201910_mvp_model/01_data/'
result_path = 'D:/Model/201910_mvp_model/02_result_model1/'

perf_sql = """
select a.order_no, a.effective_date, a.business_id
from t_loan_performance a 
where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
"""
perf_data = get_df_from_pg(perf_sql)




----百融压力偿债指数----
br_stress_loan_sql = """
select t0.order_no, t0.effective_date
, cast(oss::json #>> '{result}' as json)::json #>>'{DebtRepayStress,nodebtscore}' as drs_nodebtscore
from (select a.order_no, a.effective_date, a.business_id
       from t_loan_performance a 
       where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22'
       ) t0 
left join (select *
			from risk_mongo_installmentmessagerelated
			where businessid in ('bk','xjbk','rong360','tb') and topicname in ('Application_thirdPart_bairongdebtrepaystress')
			and databasename = 'installmentBairongDebtRepayStress'
			)t1 on t0.order_no = t1.orderno
left join risk_oss_bairong_debt_repay_stress t2 on t1.messageno = t2.taskid
"""
br_stress_data = get_df_from_pg(br_stress_loan_sql)
br_stress_data.isnull().sum()
br_stress_data.shape
br_stress_data.order_no.nunique()

br_fraud_loan_sql = """
select t0.order_no, t0.effective_date
, cast(oss::json #>> '{result}' as json)::json #>>'{FraudRelation_g,list_level}' as frg_list_level
, cast(oss::json #>> '{result}' as json)::json #>>'{FraudRelation_g,group_num}' as frg_group_num
from (select a.order_no, a.effective_date, a.business_id
       from t_loan_performance a 
       where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-07-04' and '2019-10-22') t0
left join (select *
			from risk_mongo_installmentmessagerelated
			where businessid in ('bk','xjbk','rong360','tb') and topicname in ('Application_thirdPart_bairongfraudrelation')
			and databasename = 'installmentBairongFraudRelation'
			)t1 on t0.order_no = t1.orderno
left join risk_oss_bairong_fraud_relation t2 on t1.messageno = t2.taskid
"""
br_fraud_data = get_df_from_pg(br_fraud_loan_sql)
br_fraud_data.isnull().sum()
br_fraud_data.shape
br_fraud_data.order_no.nunique()


def dict_generator(indict, pre=None):
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                if len(value) == 0:
                    yield pre+[key, '{}']
                else:
                    for d in dict_generator(value, pre + [key]):
                        yield d
            elif isinstance(value, list):
                if len(value) == 0:
                    yield pre+[key, '[]']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            elif isinstance(value, tuple):
                if len(value) == 0:
                    yield pre+[key, '()']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            else:
                yield pre + [key, value]
    else:
        yield indict


br_loan_before = """
with br as (
select customerid, taskid, oss::json #>> '{result}' as result
from risk_oss_bairong_new_loan_before 
where businessid in ('tb','xjbk','rong360') 
),
related as (
select orderno, messageno
from  risk_mongo_installmentmessagerelated 
where businessid in ('xjbk','tb','rong360') 
and topicname in ('Application_thirdPart_bairongnewloanbefore') 
and databasename = 'installmentBairongNewLoanBefore'
) 
select t0.order_no, t0.effective_date, t2.*
from  (select a.order_no, a.effective_date, a.business_id
       from t_loan_performance a 
       where dt='20191108' and business_id in ('tb', 'xjbk', 'rong360') and effective_date between '2019-10-01' and '2019-10-22'
       ) t0
left join related t1 on t0.order_no = t1.orderno
left join br t2 on t1.messageno = t2.taskid
"""




br_loanbf_data = get_df_from_pg(br_loan_before)


br_loanbf_data.isnull().sum()

#result_df = pd.DataFrame(columns=col_list)
result_df = pd.DataFrame()

product_list = {'InfoRelation': 'ir', 'ApplyLoanUsury': 'alu','TotalLoan': 'tl',}


#for i in range(df.shape[0]):
for i in range(br_l  oanbf_data.shape[0]):
    print (i)
    a1 = json.loads(br_loanbf_data.iloc[i, 4])
    sValue = {key: value for key, value in a1.items() if key in product_list.keys()}
    result_dict = {}
    for var in dict_generator(sValue):
        var[0] = product_list[var[0]]
        # print('_'.join(var[0:-1]), ':', var[-1])
        result_dict['_'.join(var[0:-1])] = var[-1]
        result_dict_new = {key: value for key, value in result_dict.items() if value != '{}'}
        result_dict_new2 = {key: value for key, value in result_dict_new.items() if key[-17:] not in ['_bank_else_allnum', '_bank_else_orgnum']}
    a2 = pd.DataFrame.from_dict(result_dict_new2, orient='index').T
    result_df = pd.concat([result_df, a2], ignore_index=True)

result_df.head()

result_df2 = br_loanbf_data[['order_no']].merge(result_df, left_index = True, right_index = True)
save_data_to_pickle(br_loanbf_data,local_data_path,'br_loanbefore_json_1001to1022.pkl')


br_online_data = br_stress_data.merge(br_fraud_data, on = 'order_no').merge(result_df2, on = 'order_no', how = 'left')
br_online_data.to_excel(os.path.join(data_path,'br_online_data.xlsx'))
br_online_data = pd.read_excel(os.path.join(data_path, 'br_online_data.xlsx'))

br_online_data = perf_data.merge(br_online_data, on = 'order_no')

br_online_data.loc[br_online_data.br_nodebtscore.isnull(),'effective_date'].value_counts().sort_index()

br_online_data2 = br_online_data.loc[br_online_data.effective_date >= '2019-07-27']

#br_online_data2 = perf_data.loc[perf_data.effective_date >='2019-07-27'].merge(br_online_data, on = 'order_no')

br_online_data2.to_excel(os.path.join(data_path,'br_online_data_v2_0727.xlsx'))

"""
百融数据拼数
"""
data_path = 'D:/Model/201910_mvp_model/01_data/bairong/'
perf_data.shape
br_online_data2.shape

orderlist = list(set(perf_data.order_no) - set(br_online_data.order_no)) #2015


perf_data.loc[perf_data.order_no.isin(orderlist)].groupby(['business_id','effective_date'])['order_no'].count().sort_index().to_excel(os.path.join(data_path, 'br_online_check.xlsx')) #由于跑风控时间在百融调用之前

#拼数据

#7&8月份数据
br_debt_stress = pd.read_excel(os.path.join(data_path, 'bairong_xjbk_xjm_20190929/01偿债压力指数.xlsx'), sheet_name = '偿债压力指数')
br_debt_stress = br_debt_stress.drop(0)
br_debt_stress = br_debt_stress[['cus_num','drs_nodebtscore']] #(2556, 2)

br_ir = pd.read_excel(os.path.join(data_path, 'bairong_xjbk_xjm_20191025/实名信息验证.xlsx'), sheet_name = '实名信息验证').drop(['name','id','cell','user_date','flag_inforelation'],1)
br_ir = br_ir.drop(0) #(2556, 149)

br_tl = pd.read_excel(os.path.join(data_path, 'bairong_xjbk_xjm_20190929/03借贷行为验证.xlsx'), sheet_name = '借贷行为验证').drop(['name','id','cell','user_date','swift_number','cus_username','code','flag_totalloan'],1)
br_tl = br_tl.drop(0) #(2556, 179)

br_alu = pd.read_excel(os.path.join(data_path, 'bairong_xjbk_xjm_20191025/高风险借贷意向验证.xlsx'), sheet_name = '高风险借贷意向验证').drop(['name','id','cell','user_date','swift_number','cus_username','code','flag_applyloanusury'],1)
br_alu = br_alu.drop(0) #(2556, 57)

br_frg = pd.read_excel(os.path.join(data_path, 'bairong_xjbk_xjm_20190929/05团伙欺诈排查（通用版）.xlsx'), sheet_name = '团伙欺诈排查（通用版）').drop(['name','id','cell','user_date','swift_number','cus_username','code','flag_fraudrelation_g'],1)
br_frg = br_frg.drop(0) #(2556, 3)

br_huisu1 = br_debt_stress.merge(br_ir, on = 'cus_num').merge(br_tl, on = 'cus_num').merge(br_alu, on = 'cus_num').merge(br_frg, on = 'cus_num')
br_huisu1.shape #(2556, 386)


#代偿4000单

br_debt_stress2 = pd.read_excel(os.path.join(data_path, 'bairong_tb4000/明特数据_0530偿债压力指数.xlsx'), sheet_name = '明特数据_0530偿债压力指数_DebtRepayStress')
br_debt_stress2 = br_debt_stress2.drop(0)
br_debt_stress2 = br_debt_stress2[['cus_num','drs_nodebtscore']] #(4326, 2)

br_ir2 = pd.read_excel(os.path.join(data_path, 'bairong_tb4000/明特数据_0530实名信息验证.xlsx'), sheet_name = '实名信息验证').drop(['user_date','name','cell','id','sl_user_date','user_time','swift_number','cus_username'
                                                                                                               ,'code','flag_inforelation'],1)
br_ir2 = br_ir2.drop(0) #4326, 149

br_tl2 = pd.read_excel(os.path.join(data_path, 'bairong_tb4000/明特数据_0530借贷行为验证.xlsx'), sheet_name = '借贷行为验证').drop(['user_date','name','cell','id','sl_user_date','user_time','swift_number','cus_username'
                                                                                                               ,'code','flag_totalloan'],1)
br_tl2 = br_tl2.drop(0) #(4326, 179)

br_alu2 = pd.read_excel(os.path.join(data_path, 'bairong_tb4000/明特数据_0530高风险借贷意向验证.xlsx'), sheet_name = '明特数据_0530高风险借贷意向验证_ApplyLoanUsu')\
    .drop(['user_date','name','cell','id','sl_user_date','user_time','swift_number','cus_username','code','flag_applyloanusury'],1)
br_alu2 = br_alu2.drop(0) #(2556, 57)

br_frg2 = pd.read_excel(os.path.join(data_path, 'bairong_tb4000/明特数据_0530团伙欺诈排查（通用版）.xlsx'), sheet_name = '明特数据_0530团伙欺诈排查（通用版）_FraudRelat') \
    .drop(['user_date', 'name', 'cell', 'id', 'sl_user_date', 'user_time', 'swift_number', 'cus_username', 'code', 'flag_fraudrelation_g'], 1)
br_frg2 = br_frg2.drop(0) #(4326, 3)

br_huisu2 = br_debt_stress2.merge(br_ir2, on = 'cus_num').merge(br_tl2, on = 'cus_num').merge(br_alu2, on = 'cus_num').merge(br_frg2, on = 'cus_num')
br_huisu2.shape #(4326, 386)

br_huisu2.columns

br_huisu1 = br_huisu1.rename(columns = {'cus_num':'order_no','drs_nodebtscore':'br_nodebtscore','frg_list_level':'br_frg_list_level','frg_group_num':'br_frg_group_num'})
br_huisu2 = br_huisu2.rename(columns = {'cus_num':'order_no','drs_nodebtscore':'br_nodebtscore','frg_list_level':'br_frg_list_level','frg_group_num':'br_frg_group_num'})

set(br_huisu1) - set(br_online_data.columns)
set(br_online_data) - set(br_huisu1.columns)

br_huisu1 = perf_data.merge(br_huisu1, on = 'order_no')
br_huisu1 = br_huisu1.loc[br_huisu1.effective_date < '2019-07-27']


#br_huisu1 refill
for j in br_online_data.columns.unique():
    if j not in ['order_no']:
        for i in  br_huisu1.order_no.unique():
            print (j, i )
            if br_online_data.loc[br_online_data.order_no == i, j].iloc[0] is not None:
                print (i, br_online_data.loc[br_online_data.order_no == i, j].iloc[0] )
                br_huisu1.loc[br_huisu1.order_no == i, j] = br_online_data.loc[br_online_data.order_no == i, j].iloc[0]





br_huisu1.loc[br_huisu1.order_no == '154968011013357570','br_nodebtscore'] #46
br_online_data.loc[br_online_data.order_no == '154968011013357570','br_nodebtscore'] #63

br_huisu1.loc[br_huisu1.order_no == '154886330399066114','br_frg_list_level'] #7
br_online_data.loc[br_online_data.order_no == '154886330399066114','br_frg_list_level'] #5



#检查三部分数据的缺失情况
br_online_data2 = pd.read_excel(os.path.join(data_path, 'br_online_data_v2.xlsx'))

br_online_data2['sample_set'] = 'online'
br_huisu2['sample_set'] = 'huisu2'
br_huisu1['sample_set'] = 'huisu1'

online_cols = list(set(br_online_data2.columns).intersection(set(br_huisu1.columns)))
online_cols.remove('business_id')
online_cols.remove('effective_date')

set(br_online_data2.columns) - set(br_huisu2.columns)



br_all_data = pd.concat([br_online_data2.drop(['effective_date','business_id'],1),br_huisu1[online_cols],br_huisu2[online_cols]])

br_all_data = br_all_data.replace('a',1).replace('b',2).replace('c',3).replace('d',4).replace('e',5).replace('f',6).replace('g',7).replace('h',8).replace('i',9).replace('j',10).replace('k',11).replace('l',12)
br_all_data = br_all_data.replace('',-1).replace(' ',-1).fillna(-1)
br_all_data = br_all_data.drop(['tl_cell_lasttime','tl_id_lasttime'],1)

for i in br_all_data.columns:
    if i not in ['sample_set','order_no']:
        try:
             br_all_data[i] = br_all_data[i].astype(float)
        except:
             print(i)
        pass

br_all_data.to_excel(os.path.join(data_path, 'br_data.xlsx'))

br_all_data.sample_set.value_counts()


a = pd.DataFrame()

for i in br_all_data.sample_set.unique():
    print(i)
    b = (br_all_data.loc[br_all_data.sample_set == i] == -1).sum()
    a = pd.concat([a,b])


br_all_data.loc[br_all_data.sample_set == 'huisu1'].shape

writer = pd.ExcelWriter(os.path.join(data_path, 'br_data_missing_check.xlsx'))
(br_all_data.loc[br_all_data.sample_set == 'online'] == -1).sum().to_excel(writer, 'online', index=True)
(br_all_data.loc[br_all_data.sample_set == 'huisu1'] == -1).sum().to_excel(writer, 'huisu1', index=True)
(br_all_data.loc[br_all_data.sample_set == 'huisu2'] == -1).sum().to_excel(writer, 'huisu2', index=True)
writer.save()

#######检查tl系列,压力偿债指数,团伙欺诈的变量分布情况###################
br_check_cols = [i for i in br_all_data.columns]
br_check_cols = ['order_no','sample_set','br_frg_group_num','br_frg_list_level','br_nodebtscore']  + br_check_cols #181




writer = pd.ExcelWriter(os.path.join(data_path, 'br_data_distrb_check.xlsx'))
br_all_data.loc[br_all_data.sample_set == 'online'].describe().to_excel(writer, 'online', index=True)
br_all_data.loc[br_all_data.sample_set == 'huisu1'].describe().to_excel(writer, 'huisu1', index=True)
br_all_data.loc[br_all_data.sample_set == 'huisu2'].describe().to_excel(writer, 'huisu2', index=True)
writer.save()


#只保留tl系列,压力偿债指数,团伙欺诈的数据
br_all_data2 = br_all_data[br_check_cols]

br_var_dict = pd.read_excel('D:/Model/201910_mvp_model/建模代码可用变量字典.xlsx', sheet_name = '02_字典')

set(br_all_data.columns) - set(br_var_dict.指标英文)
br_all_data = br_all_data.rename(columns = {'br_frg_group_num':'frg_group_num','br_frg_list_level':'frg_list_level','br_nodebtscore':'drs_nodebtscore'})

br_all_data2.to_excel(os.path.join(data_path, 'br_data_v2.xlsx'))

"""
2019/11/15 国誉数据
"""
gy_hs = pd.read_excel('D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/01_data/国誉/国誉分期中额分.xlsx')[['order_no','score']].rename(columns = {'score':'gy_xiaoescore'})
gy_hs.order_no = gy_hs.order_no.astype(str)
gy_online = pd.read_excel('D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/01_data/国誉/r_gy.xlsx')[['order_no','gy_xiaoescore']]
gy_online.order_no = gy_online.order_no.astype(str)





gy_hs.order_no.nunique()
gy_online.order_no.nunique()

len(set(list(gy_hs.order_no) + list (gy_online.order_no )))


a = list(set(gy_hs.order_no).intersection(set(gy_online.order_no )))
len(a) #134
gy_keep_order = list(gy_online.loc[(gy_online.order_no.isin(a)) & gy_online.gy_xiaoescore >0]['order_no']) #79
gy_drop_order = [i for i in a if i not in gy_keep_order] #55

gy_hs = gy_hs.loc[~gy_hs.order_no.isin(gy_keep_order)]
gy_online = gy_online.loc[~gy_online.order_no.isin(gy_drop_order)]
gy_hs.shape[0] + gy_online.shape[0]

gy_data = pd.concat([gy_hs, gy_online])
gy_data.to_excel(result_path + 'r_gy_f.xlsx')


"""
2019.11.8 百融补充抽取10.1-10.22数据
"""
br_data_bef = pd.read_excel(data_path + 'br_data.xlsx')
set(br_data_bef.columns) - set(result_df.columns)
cp_list = [i for i in br_data_bef.order_no if 'CP' in i]
cp_list.remove('CP2019071019381930072363')
len(cp_list)
br_data_bef = br_data_bef.loc[~br_data_bef.order_no.isin(cp_list)]

br_stress_data = br_stress_data.loc[br_stress_data.effective_date >= '2019-10-01']
br_fraud_data = br_fraud_data.loc[br_fraud_data.effective_date >= '2019-10-01']


br_data_oct = br_fraud_data.drop(['effective_date'],1).merge(br_stress_data.drop(['effective_date'],1), on = 'order_no').merge(result_df2, on = 'order_no')

br_data_oct = br_data_oct.replace('a',1).replace('b',2).replace('c',3).replace('d',4).replace('e',5).replace('f',6).replace('g',7).replace('h',8).replace('i',9).replace('j',10).replace('k',11).replace('l',12)\
                         .replace('',-1).replace(' ',-1).fillna(-1)
br_data_oct = br_data_oct.drop(['tl_cell_lasttime','tl_id_lasttime'],1)

br_data_oct = convert_data_to_float(br_data_oct, ['order_no','effective_date'])

br_data_all = pd.concat([br_data_oct, br_data_bef])
set(br_data_all.columns) - set(br_data_oct.columns)
set(br_data_oct.columns) - set(br_data_all.columns)
br_data_all = br_data_all.fillna(-1)


br_data_all = perf_data.merge(br_data_all, on = 'order_no')

x_cols = [i for i in br_data_all.columns if i not in (['order_no','effective_date','business_id'])]

def check_var_missing_by_time(data, x_cols, time_cols):
    missing_df = pd.DataFrame(br_data_all[time_cols].unique(), columns = {time_cols})
    for i in x_cols:
        print(i)
        temp = data.loc[data[i] == -1].groupby([time_cols ])[time_cols].agg([(i, lambda x: len(x)/data.shape[0])])
        temp2 = temp.reset_index()
        print(temp2)
        missing_df = missing_df.merge(temp2, on = time_cols, how = 'left')
        print(missing_df)
    return missing_df

var_missing = check_var_missing_by_time(br_data_all, x_cols, 'effective_date')
var_missing.to_excel(local_data_path + 'var_missing.xlsx')

writer = pd.ExcelWriter('D:/Model/201910_mvp_model/01_data/bairong/br_data_1108.xlsx')
br_data_all.to_excel(writer, 'br_data_all', index=True)
writer.save()



#数据字典清理
var_dict1 = pd.read_excel('D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/建模代码可用变量字典.xlsx', sheet_name= '02_字典')
var_dict2 = pd.read_excel('D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/建模代码可用变量字典 (2).xlsx', sheet_name= '02_字典')
var_dict3 = pd.read_excel('D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/建模代码可用变量字典 20191106.xlsx', sheet_name= '02_字典')
var_dict4 = pd.read_excel('D:/Model/201910_mvp_model/建模代码可用变量字典.xlsx', sheet_name= '02_字典')

set(var_dict1.指标英文) - set(var_dict2.指标英文)
set(var_dict2.指标英文) - set(var_dict1.指标英文)
set(var_dict1.指标英文) - set(var_dict3.指标英文)
set(var_dict3.指标英文) - set(var_dict1.指标英文)

var_dict = pd.concat([var_dict1, var_dict2, var_dict3, var_dict4])
var_dict = var_dict.drop_duplicates()
var_dict.指标英文.nunique()
var_dict.指标英文.value_counts()
var_dict.loc[var_dict.指标英文 == 'D3_id_leasing_company']
var_dict = var_dict.sort_values(by = ['数据源','指标英文'])
var_dict.to_excel('D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/建模代码可用变量字典1112.xlsx')

var_dict_new = pd.read_excel('D:/seafile/Seafile/风控/模型/02 额度产品模型/mvp_model_v1/建模代码可用变量字典1112.xlsx')
var_dict_new.指标英文.nunique()
var_dict_new.shape


var_dict1.指标英文.nunique()
var_dict2.指标英文.nunique()
var_dict3.指标英文.nunique()
var_dict4.指标英文.nunique()

set(var_dict4.指标英文) - set(var_dict.指标英文)