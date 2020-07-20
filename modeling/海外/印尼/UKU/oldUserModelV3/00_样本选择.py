import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/genie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *

#建模所用flag
perf_sql = """
select id as loan_id
,customer_id
,approved_period
,effective_date
,paid_off_time
,due_date
,loan_status
,extend_times
,return_flag
,current_date
,late_days
, case when late_days >= 7 then 1 else 0 end as ever7
, case when late_days >= 15 then 1 else 0 end as ever15
, case when late_days >= 30 then 1 else 0 end as ever30
, case when current_date - due_date >= 7 then 1 else 0 end as due_flag7
, case when current_date - due_date >= 15 then 1 else 0 end as due_flag15
, case when current_date - due_date >= 30 then 1 else 0 end as due_flag30
from dw_gocash_go_cash_loan_gocash_core_loan 
where effective_date!='1970-01-01' and effective_date between '2020-01-17' and '2020-04-27' and return_flag = 'true' and product_id = 1
"""

perf_df = get_df_from_pg(perf_sql)
perf_df.loan_id.nunique()

perf_df['effective_month'] = perf_df.effective_date.apply(lambda x:str(x)[0:7])
perf_df.effective_month.value_counts()
perf_df.loan_id = perf_df.loan_id.astype(str)
perf_df.customer_id = perf_df.customer_id.astype(str)
perf_df.to_excel('D:/Model/indn/202004_uku_old_model/df.xlsx')

"""分析advanceAI在新客上的效果"""

from math import log

def score_to_p(score, PDO=20.0, Base=600, Ratio=1.0):
    """
    分数转换逾期概率

    Args:
    score (float): 模型分数
    PDO (float): points double odds. default = 75
    Base (int): base points. default = 660
    Ratio (float): bad:good ratio. default = 1.0/15.0

    Returns:
    转化后的逾期概率
    """
    B = 1.0 * PDO / log(2)
    A = Base + B * log(Ratio)
    alpha = (A - score) / B
    p = np.exp(alpha) / (1 + np.exp(alpha))
    return p

sql_base = '''
with flag as (
	select t1.id loan_id,
		customer_id,
		effective_date,
		return_flag,
		late_date,
		approved_period,
		case when loan_status = 'COLLECTION' then current_date-late_date else round(late_fee /(approved_principal*0.025))::int end as DPD		
	from ( select *
		   from rt_t_gocash_core_loan
		   where effective_date between '2020-03-04' and '2020-04-15' and product_id='1' and loan_status not in ('DENIED','RESCIND','APPROVING','CREATED') and approved_period in (8,15)
		         and return_flag = 'false' and device_approval = 'ANDROID') t1
	left join ( select *
		        from ( select order_id, late_date, row_number() over(partition by order_id order by late_date asc) as num
			           from dw_gocash_gocash_collection_col_case
			           where order_status in ('COLLECTION_PAIDOFF', 'COLLECTION') and app_id not in ('Credits', 'KASANDAAI')) t
		        where num = 1) t2 on t1.id = t2.order_id	
) ,
f as (
select distinct flag.*, 
case when late_date::date-effective_date::date = approved_period then 1 else 0 end as dpd1,
case when dpd>=3 then 1 else 0 end as dpd3,
ruleresultname, score, varname, varvalue
from flag 
left join rt_risk_mongo_gocash_riskcontrolresult c on flag.loan_id::text = c.loanid 
left join (
	select orderno, ruleresultname, substring(ruleresult,1,3)::int as score,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varName'  varname,
	json_array_elements(cast(json_array_elements(datasources::json) ->> 'vars' as json)) ->>'varValue'  varvalue
	from rt_risk_mongo_gocash_riskreport) b on coalesce(c.preloanid, c.loanid) = b.orderno and ruleresultname in ('newUserModelScoreV6') and varname = 'advMultiscore'
)
select --effective_date, count(*), count(distinct loan_id), count(multiscore)
*
from f 
where score is not null
'''
df = get_df_from_pg(sql_base)
df.loan_id = df.loan_id.astype(str)
print(df.shape)
print(df.loan_id.nunique())

df['prob'] = score_to_p(df.score, PDO=20.0, Base=600, Ratio=1.0)
print(df.dtypes)
print(df.groupby(['effective_date','ruleresultname'])['dpd1'].size())
print(df.groupby(['effective_date','ruleresultname'])['dpd1'].mean())

print(df.groupby(['ruleresultname']).size())
print(df.groupby(['ruleresultname'])['dpd1'].mean())

data_prod = df[['loan_id', 'effective_date', 'dpd1', 'varvalue']]
data_prod.rename(columns={'dpd1': 'flag', 'varvalue': 'multiscore'}, inplace = True)
data_prod.multiscore = data_prod.multiscore.astype(float)

data_prod.loc[data_prod.multiscore.isna()].shape
data_prod.flag.value_counts()


#data_train = pd.read_csv('D:/Model Development/202001 IDN new v6/01 Data/raw data 20200212/r_all_app2_tddate_adv.csv', dtype = {'loan_id': str})
#data_train_adv = data_train[['loan_id', 'effective_date', 'flag7','multiscore']]
#data_train_adv.rename(columns={'flag7': 'flag'}, inplace = True)

#data_all = pd.concat([data_prod, data_train_adv],0)
#data_all = data_all[data_all.multiscore.isna()==False]

data_prod.loc[data_prod.effective_date<= '2020-03-20', 'group'] = data_prod.loc[data_prod.effective_date<= '2020-03-20', 'effective_date']
data_prod.loc[(data_prod.effective_date >= '2020-03-21') & (data_prod.effective_date <= '2020-04-06'), 'group'] = '2020/03/21-2020/04/06'
data_prod.loc[(data_prod.effective_date >= '2020-04-07'), 'group'] = '2020/04/07-2020/04-15'

from sklearn.metrics import roc_auc_score
def auc_eventrate_table(data, time_cols, flag , score):
    data[[flag, score]] = data[[flag, score]].astype(float)
    data[time_cols] = data[time_cols].astype(str)
    auc_df = pd.DataFrame(columns=[time_cols, 'AUC'], index=range(data[time_cols].nunique()))
    index = 0
    for i in data[time_cols].unique():
        try:
            #temp_data = data.loc[data[time_cols] == i]
            temp_data = data.loc[data[time_cols] == i]
            auc_result = 1-roc_auc_score(temp_data[flag], temp_data[score])
            #auc_result = roc_auc_score(temp_data[flag], temp_data[score])
            auc_df.iloc[index][0] = i
            auc_df.iloc[index][1] = auc_result
            index += 1
        except:
            pass

    event_df = pd.crosstab(data[time_cols], data[flag]).rename(columns={0: 'N_nonEvent', 1: 'N_Event'})
    if 'N_Event' not in event_df.columns:
        event_df.loc[:, 'N_Event'] = 0
    if 'N_nonEvent' not in event_df.columns:
        event_df.loc[:, 'N_nonEvent'] = 0
    event_df.index.name = None
    event_df.loc[:, 'N_sample'] = data[time_cols].value_counts()
    event_df.loc[:, 'EventRate'] = event_df.N_Event * 1.0 / event_df.N_sample
    event_df = event_df.reset_index()
    event_df.rename(columns={'index': time_cols}, inplace=True)
    df = event_df.merge(auc_df, on = time_cols, how = 'left')
    df.rename(columns = {'N_nonEvent':'N_Good', 'N_Event': 'N_Bad','EventRate':'BadRate'}, inplace= True)
    #reorder_cols = [time_cols,'总样本数','好样本数','坏样本数','BadRate','AUC']
    reorder_cols = [time_cols, 'N_sample', 'N_Good', 'N_Bad', 'BadRate', 'AUC']
    result = df[reorder_cols]
    return(result)


data_prod.groupby(['effective_date', 'flag'])['loan_id'].count().unstack()

auc = auc_eventrate_table(data_prod, 'effective_date', 'flag', 'multiscore')
auc = auc_eventrate_table(data_prod, 'group', 'flag', 'multiscore')

print(auc)

data_prod['multiscore_bin'] = pd.qcut(data_prod['multiscore'], q = 10, precision= 0)

auc.to_excel('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/00_前期分析/temp.xlsx')

writer = pd.ExcelWriter('D:/seafile/Seafile/风控/模型/10 印尼/202004 老客模型 V3/00_前期分析/advanceAI在新客上效果分析.xlsx')
data_prod.to_excel(writer, 'data_prod')
auc.to_excel(writer, 'auc')
writer.save()

df = pd.DataFrame({"a" : ["1","2","3","4"], "b" : ["t1.var1","t1.var2","t1.var3","t1.var4"]})

#print(df['b'].str.split('.', 1).str)

