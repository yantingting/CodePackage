import numpy as np
import pandas as pd
from imp import reload

sys.path.append('/Users/Mint/Desktop/repos/newgenie')

from utils3.data_io_utils import *

izi_sql = """
SELECT  t1.id as loanid
      , t1.customer_id
      , t1.apply_time
      , t1.customer_type
      , t1.grouping
      , t1.effective_date
      , t1.loan_status
      , case when t1.late_days <7 then 0 else 1 end as flag_7
      , t1.due_date
      , t2.cell_phone 
      , md5(concat('+628',substring(t2.cell_phone,3))) as md5_cellphone_izi
      , md5(t2.id_card_no) as md5_idcardno
      , t2.id_card_name 
      , md5(t2.id_card_name) as md5name
      , date(t3.createtime) as rc_time
      , t3.risklevel
      , t3.pipelinename
      , case when pipelinename like '%ios%' then 'ios' else 'android' end as device
      , oldusermodelv1score
      , oldusermodelv1result
FROM  (
        SELECT *
        FROM public.dw_gocash_go_cash_loan_gocash_core_loan
        WHERE (date(apply_time) between '2020-01-10' and '2020-02-09') and return_flag = 'true'
        ) t1 
LEFT JOIN public.dw_gocash_go_cash_loan_gocash_core_customer t2
ON t1.customer_id= t2.id
LEFT JOIN  risk_mongo_gocash_installmentriskcontrolresult t3
ON t1.id :: varchar = t3.loanid
"""
izi_huisu = get_df_from_pg(izi_sql)


izi_huisu.apply_time = izi_huisu.apply_time.astype(str)
izi_huisu.apply_time = izi_huisu.apply_time.apply(lambda x:str(x)[0:10])

#准备近期全量申请样本

izi_huisu.effective_date = izi_huisu.effective_date.astype(str)
izi_huisu.effective_date.value_counts().sort_index()
izi_huisu.due_date = izi_huisu.due_date.astype(str)

#非一键免审用户
modify_df = izi_huisu.loc[(izi_huisu.oldusermodelv1score.notnull())] #4182
modify_df.shape

#一键免审
onekey_df = izi_huisu.loc[izi_huisu.oldusermodelv1score.isnull()]
onekey_df.shape

#近期
onekey_df.loc[(onekey_df.apply_time>='2020-02-06')].shape #7078

onekey_df.loc[~onekey_df.loan_status.isin(['FUNDED','FUNDED','APPROVED','RESCIND','DENIED'])].shape

onekey_df.loc[(onekey_df.apply_time>='2020-01-17') & (onekey_df.apply_time <= '2020-01-23')].shape
onekey_df.due_date = onekey_df.due_date.astype(str)


#合并数据
modify_df['label'] = 'modify_0110-0209_apply'

onekey_df.loc[onekey_df.apply_time >='2020-02-06 00:00:00','label'] = 'onekeyloan_02060209_apply'
onekey_df.loc[(onekey_df.apply_time >= '2020-01-17 00:00:00') & (onekey_df.apply_time < '2020-01-23 00:00:00'),'label'] = 'onekeyloan_01170122_apply'

all_sample = pd.concat([modify_df, onekey_df])
all_sample.label.value_counts()

all_sample.loanid = all_sample.loanid.astype(str)
all_sample.customer_id = all_sample.customer_id.astype(str)
all_sample.loc[~all_sample.label.isnull()].to_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/izi_回溯0210.xlsx')


izi_online_sql = """
SELECT  t1.id as loanid
      , t1.customer_id
      , t1.apply_time
      , t1.customer_type
      , t1.grouping
      , t1.effective_date
      , t1.loan_status
      , case when t1.late_days <7 then 0 else 1 end as flag_7
      , t1.due_date
      , t2.cell_phone 
      , md5(concat('+628',substring(t2.cell_phone,3))) as md5_cellphone_izi
      , md5(t2.id_card_no) as md5_id_card_no
      , t2.id_card_name 
      , md5(t2.id_card_name) as md5name
      , date(t3.createtime) as rc_time
      , t3.risklevel
      , t3.pipelinename
      , case when pipelinename like '%ios%' then 'ios' else 'android' end as device
      , oldusermodelv1score
      , oldusermodelv1result
FROM (SELECT distinct orderno, customerid
      FROM rt_risk_mongo_gocash_riskreportgray
      WHERE ruleresultname like '%izi%' and date(createtime) > '2020-02-08' and businessid = 'uku' limit 100) t
LEFT  JOIN (SELECT *
        FROM public.dw_gocash_go_cash_loan_gocash_core_loan
        ) t1  
ON t.orderno = t1.id :: varchar
LEFT JOIN public.dw_gocash_go_cash_loan_gocash_core_customer t2
ON t1.customer_id= t2.id
LEFT JOIN  risk_mongo_gocash_installmentriskcontrolresult t3
ON t1.id :: varchar = t3.loanid
"""
izi_online = get_df_from_pg(izi_online_sql)
izi_online.loanid.nunique()

izi_online.loanid = izi_online.loanid.astype(str)
izi_online.customer_id = izi_online.customer_id.astype(str)

izi_online.to_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/izi_回溯0210_online.xlsx')

"""回溯样本打分"""



perf_sql = """
SELECT loan_id
--近N天放款次数
, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan30

, count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan90

, count(distinct case when date(t1.apply_time) > date(t2.effective_date) and t2.effective_date != '1970-01-01'
					then t2.id else null end) as cnt_loan

--第一次/最近一次申请距离现在申请时间间隔
, min(date(t1.apply_time) - date(t2.apply_time)) as lastapply_curds

----展期次数
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.extend_times >0 and t2.effective_date != '1970-01-01'  then t2.id else null end) as cnt_extend_times_30d
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.extend_times >0 and t2.effective_date != '1970-01-01'  then t2.id else null end) as cnt_extend_times_90d

, 1.0 * sum(t2.extend_times)/ count(case when t2.effective_date != '1970-01-01'  then t2.id end) as avg_extend_times
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then t2.extend_times else null end)/
    count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as avg_extend_times_60d
, case when count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id else null end) > 0 
    then 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then t2.extend_times else null end)/
    count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 and t2.effective_date != '1970-01-01' then t2.id else null end) 
    else null end as avg_extend_times_90d

----贷后
, count(case when t2.loan_status = 'ADVANCE_PAIDOFF' then t2.id else null end) as cnt_advance_paidoff
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.loan_status = 'ADVANCE_PAIDOFF' then t2.id else null end) as cnt_advance_paidoff30d

, count(case when t2.loan_status = 'PAIDOFF' then t2.id else null end) as cnt_paidoff
, count(case when (t1.apply_time :: date - t2.apply_time :: date) <= 30 and t2.loan_status = 'PAIDOFF' then t2.id else null end) as cnt_paidoff30d

from (SELECT id as loan_id
                , apply_time
                , effective_date
                , customer_id
            FROM dw_gocash_go_cash_loan_gocash_core_loan
            --FROM rt_t_gocash_core_loan
            --WHERE apply_time >= '2020-02-24' and return_flag = 'true' --WHERE customer_id = '156821999134838784'
            --WHERE date(effective_date) between '2019-10-20' and '2020-01-17' and return_flag = 'true'
            WHERE apply_time >= '2020-02-28 13:30:00' and return_flag = 'true'
    )t1
left join dw_gocash_go_cash_loan_gocash_core_loan t2 on t1.customer_id = t2.customer_id
where t1.apply_time :: timestamp > t2.apply_time :: timestamp
group by loan_id
"""

perf_data = get_df_from_pg(perf_sql)
perf_data['apply_month'] = perf_data.apply_date.apply(lambda x:str(x)[0:7])
perf_data.apply_month.value_counts()
perf_data.columns

perf_data.avg_extend_times_60d = perf_data.avg_extend_times_60d.astype(float)
perf_data.avg_extend_times_90d = perf_data.avg_extend_times_90d.astype(float)
perf_data.avg_extend_times = perf_data.avg_extend_times.astype(float)

perf_data.loan_id = perf_data.loan_id.astype(str)

perf_data.to_excel('D:/Model/indn/202001_mvp_model/03_策略/验收数据/perf_data_0228_0908.xlsx')

rebin_spec = load_data_from_pickle('D:/Model/indn/202001_mvp_model/02_result/0205/','rebin_spec_bin_adjusted.pkl')
var_dict = pd.read_excel('D:/Model/indn/202001_mvp_model/建模代码可用变量字典.xlsx')

bin_obj = mt.BinWoe()
X_cat_train = bin_obj.convert_to_category(perf_data, var_dict, rebin_spec)


perf_latedays_sql = """
SELECT loanid
--最大逾期天数
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 60 then late_days else null end) as max_his_latedays_60d 
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 90 then late_days else null end) as max_his_latedays_90d 
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then late_days else null end) as max_his_latedays_180d 
, max(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then late_days else null end) as max_his_latedays_360d 

--最小逾期天数
, min(case when (t1.apply_time :: date - t2.apply_time :: date) <= 360 then late_days else null end) as min_his_latedays_360d 

----平均
, 1.0 * sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then late_days else null end)/
	count(distinct case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 then t2.id else null end)as avg_his_latedays_180d 
	
FROM (select  id as loanid
            , customer_id
            , apply_time
            , effective_date
       FROM dw_gocash_go_cash_loan_gocash_core_loan
       WHERE date(apply_time) between '2020-01-10' and '2020-02-09' and return_flag = 'true'
       --WHERE apply_time > '2020-02-28 13:30:00' and return_flag = 'true'
       --WHERE effective_date between '2019-09-17' and '2019-11-30' and return_flag = 'true'
       )t1 -- and customer_id = 352418834367426560
LEFT JOIN (SELECT id, apply_time, customer_id FROM dw_gocash_go_cash_loan_gocash_core_loan WHERE apply_time < '2020-03-03 00:00:00') t2 on t1.customer_id = t2.customer_id
LEFT JOIN (SELECT * FROM public.dw_gocash_go_cash_loan_gocash_core_loan_pay_flow WHERE status = 'SUCCESS' ) t3 ON t2.id = t3.loan_id 
WHERE t1.apply_time :: timestamp > t2.apply_time :: timestamp
group by 1
"""

perf_latedays_data = get_df_from_pg(perf_latedays_sql)
perf_latedays_data.loanid = perf_latedays_data.loanid.astype(str)
perf_latedays_data.avg_his_latedays_180d = perf_latedays_data.avg_his_latedays_180d.astype(float)

perf_latedays_data.to_excel('D:/Model/indn/202001_mvp_model/03_策略/perf_latedays_data_0227_1843.xlsx')


perf_paidhour_sql = """
SELECT loanid
, sum(case when (t1.apply_time :: date - t2.apply_time :: date) <= 180 and date_part('hour', t3.paid_off_time::timestamp) between 6 and 12 then 1 else 0 end) as cnt_paidoffhour_6to12_180d
FROM (select  id as loanid
            , customer_id
            , apply_time
            , effective_date
       FROM rt_t_gocash_core_loan
       WHERE apply_time >= '2020-02-28 13:30:00' and return_flag = 'true'
       )t1 
LEFT JOIN (SELECT id, apply_time, customer_id FROM rt_t_gocash_core_loan WHERE apply_time < '2020-03-03 00:00:00') t2 on t1.customer_id = t2.customer_id
LEFT JOIN (SELECT * FROM public.rt_t_gocash_core_loan_pay_flow WHERE status = 'SUCCESS' and create_time < '2020-03-03 00:00:00') t3 ON t2.id = t3.loan_id 
WHERE t1.apply_time :: timestamp > t2.apply_time :: timestamp
group by 1
"""
perf_paidhour_data = get_df_from_pg(perf_paidhour_sql)


base_data_f.shape
base_data_f.merge(base_data2)

all_x_base = base_data_f.merge(base_data2.drop(['apply_time','effective_date','customer_id','update_time','cell_phone','id_card_name','id_card_no', 'rn'],1),on = 'loan_id', how = 'left')\
                        .merge(bank_data_f.drop(['apply_time','effective_date','customer_id','update_time', 'rn'],1), on = 'loan_id', how = 'left')\
                        .merge(refer_data.drop(['customer_id'],1), on = 'loan_id', how = 'left')\
                        .merge(prof_data.drop(['apply_time','effective_date','customer_id'],1), on = 'loan_id', how = 'left')

perf_paidhour_data.rename(columns = {'loanid':'loan_id'}, inplace = True)
perf_latedays_data.rename(columns = {'loanid':'loan_id'}, inplace = True)

all_x = all_x_base.merge(perf_data, on = 'loan_id', how = 'left').merge(perf_paidhour_data, on = 'loan_id', how = 'left').merge(perf_latedays_data, on = 'loan_id', how = 'left')

izi_data2 = izi_data2.drop(['apply_time_x','apply_time_y','md5_cellphone_izi','md5_idcardno','id_card_name','md5name','身份证','号码','phoneage_status','phonescore_status','topup_status'],1)
izi_data2.rename(columns = {'loanid':'loan_id'}, inplace = True)
izi_data2.index = izi_data2.loan_id
izi_data2 = izi_data2.reset_index(drop=True)

izi_tel_dummy = pd.get_dummies(izi_data2['telcom'])
izi_tel_dummy.columns = ['izi_telcom_'+i for i in izi_tel_dummy.columns]

all_x = all_x.reset_index(drop=True)
all_x.loan_id = all_x.loan_id.astype(str)
all_x.index = all_x.loan_id

all_x2 = izi_data2.merge(izi_tel_dummy, left_index = True, right_index = True, how = 'left').merge(all_x, left_index = True, right_index = True, how = 'left')

all_x.head()
all_x2 = all_x2.drop(['loan_id_x'],1)
all_x2.rename(columns = {'loan_id_y' : 'loan_id'}, inplace=True)

all_x2 = all_x2.drop(['customer_id_x'],1)
all_x2.rename(columns = {'customer_id_y' : 'customer_id'}, inplace=True)
all_x2.columns

"""更新数据"""
all_x2 = load_data_from_pickle('D:/Model/indn/202001_mvp_model/01_data/', 'all_x2_0110to0209.pkl')
all_x2 = all_x2.reset_index(drop=True)
all_x2 = all_x2.drop(['refer_update_minutes','monthly_salary'],1)
all_x2 = all_x2.drop(['avg_his_latedays_180d'],1)

all_x2.loan_id = all_x2.loan_id.astype(str)

refer_data.loan_id = refer_data.loan_id.astype(str)
prof_data.loan_id = prof_data.loan_id.astype(str)
perf_latedays_data.rename(columns = {'loanid':'loan_id'}, inplace=True)
perf_latedays_data.loan_id = perf_latedays_data.loan_id.astype(str)

all_x2 = all_x2.merge(refer_data[['loan_id','refer_update_minutes']], on = 'loan_id', how = 'left')\
    .merge(prof_data[['loan_id','monthly_salary']], on = 'loan_id', how = 'left')

all_x2 = all_x2.merge(perf_latedays_data[['loan_id','avg_his_latedays_180d']], on = 'loan_id', how = 'left')

save_data_to_pickle(all_x2, 'D:/Model/indn/202001_mvp_model/01_data/', 'all_x2_0110to0209.pkl')

from utils3.misc_utils import convert_right_data_type

var_dict = pd.read_excel('D:/Model/indn/202001_mvp_model/建模代码可用变量字典.xlsx')
all_x2, _ = convert_right_data_type(all_x2, var_dict)
pd.DataFrame(all_x.columns).to_excel('D:/Model/indn/202001_mvp_model/var_cols.xlsx')


"""打分"""
import xgboost as xgb
from xgboost import DMatrix

features_used = pd.read_excel('D:/Model/indn/202001_mvp_model/02_result/0205/grid_46_200210-09.xlsx', sheet_name = '04_model_importance')
len(features_used)

set(features_used['varName']) - set(all_x2.columns)

all_x['A_14d']

all_x.index = all_x.loan_id
all_x2 = all_x2.fillna(-1)
all_x2.index = all_x2.loan_id
all_x_used =  all_x2[features_used['varName']]
all_x_used =  all_x2[features_used['指标英文']]

for i in all_x_used.columns:
    try:
        all_x_used[i] = all_x_used[i].astype(float)
    except:
        print(i)

mymodel = xgb.Booster()
mymodel.load_model("D:/Model/indn/202001_mvp_model/02_result/0205/grid_46_200225-20.model")


data_lean = DMatrix(all_x_used)
ypred = mymodel.predict(data_lean)
score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([all_x_used.index, score, ypred]).T
data_scored.rename(columns = {0:'loan_id', 1:'score', 2:'prob'}, inplace=True)
data_scored.head()

train_ks = [-np.inf, 622.0, 635.0, 643.0, 650.0, 655.0, 661.0, 667.0, 673.0, 681.0, np.inf]
train_20ks = [-np.inf, 612.0, 622.0, 629.0, 635.0, 639.0, 643.0, 647.0, 650.0, 653.0, 655.0, 659.0, 661.0, 664.0, 667.0, 670.0, 673.0, 677.0,\
            681.0, 688.0, np.inf]

train_ks = [-np.inf, 621.0, 633.0, 642.0, 649.0, 656.0, 662.0, 669.0, 676.0, 685.0, np.inf]


data_scored['bin']  = pd.cut(data_scored['score'], bins = train_ks)
data_scored['20bin']  = pd.cut(data_scored['score'], bins = train_20ks)



data_scored['prob_bin']  = pd.cut(data_scored['prob'], bins = train_prob_ks)
data_scored['prob_20bin']  = pd.cut(data_scored['prob'], bins = train_prob_20ks)

save_data_to_pickle(data_scored,'D:/Model/indn/202001_mvp_model/01_data/', 'data_scored_0225_final_final.pkl')

save_data_to_pickle(all_x2,'D:/Model/indn/202001_mvp_model/01_data/', 'all_x2_0110to0209.pkl')


data_scored = load_data_from_pickle('D:/Model/indn/202001_mvp_model/01_data/','data_scored.pkl')

data_scored.dtypes
data_scored.loan_id = data_scored.loan_id.astype(str)

data_scored.bin = data_scored.bin.astype(str)
data_scored['20bin'] = data_scored['20bin'].astype(str)
data_scored.prob_bin = data_scored.prob_bin.astype(str)
data_scored.prob_20bin = data_scored.prob_20bin.astype(str)

def Prob2Score(prob, basePoint, PDO):
    #将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    return (basePoint+PDO/np.log(2)*(-y))

SQL_CREATE_TABLE = """
CREATE TABLE temp_uku_oldscore(
    loan_id VARCHAR,
    score VARCHAR,
    prob VARCHAR,
    bin VARCHAR,
    bin20 VARCHAR,
    prob_bin VARCHAR,
    prob_20bin VARCHAR
)
"""
upload_df_to_pg(SQL_CREATE_TABLE)

insert = """
INSERT INTO temp_uku_oldscore
 VALUES
{% for var in var_list %}
{{ var }},
{% endfor %}
"""

var_list = []

for i in data_scored.columns:
    data_scored[i] = data_scored[i].astype(str)

for cols, rows in data_scored.iterrows():
    c = tuple(rows)
    var_list.append(c)

var_list

insert_sql = Template(insert).render(var_list=var_list)[:-2]
insert_sql = insert_sql.replace('\n\n','')
insert_sql = insert_sql.replace('\n','')

upload_df_to_pg(insert_sql)

#
data_scored.head()

perf_sql = """
SELECT *
FROM (SELECT id as loan_id
        , apply_time
        , effective_date
        , loan_status
        , late_days
        , due_date  
        , customer_type
        , approved_period
        , case when late_days > 7 then 1 else 0 end as flag_7
        , grouping
     FROM dw_gocash_go_cash_loan_gocash_core_loan
    WHERE date(effective_date) between '2020-01-10' and '2020-02-09' and return_flag = 'true'
    and due_date <= '2020-02-18') t1
LEFT JOIn (SELECT loanid
            , customerid
            , createtime
            , oldusermodelv1result
            , oldusermodelv1score
            FROM rt_risk_mongo_gocash_installmentriskcontrolresult
            WHERE createtime > '2020-01-10 00:00:00' and oldusermodelv1result <> '') t2
ON t1.loan_id :: varchar = t2.loanid
"""
perf_df = get_df_from_pg(perf_sql)
perf_df2 = get_df_from_pg(perf_sql)

perf_df.isnull().sum()

perf_df.loan_id = perf_df.loan_id.astype(str)
df_all = perf_df.merge(data_scored, on = 'loan_id')

df_all.loc[df_all.oldusermodelv1score.isnull()]['customer_type'].value_counts()


df_all.to_excel('D:/Model/indn/202001_mvp_model/02_result/0205/score_0110to0123.xlsx')

df_all.oldusermodelv1score = df_all.oldusermodelv1score.astype(float)
df_all.score = df_all.score.astype(float)
df_all.prob = df_all.prob.astype(float)

import utils3.metrics as mt

df_all.effective_date = df_all.effective_date.astype(str)

onekey_bin = [-np.inf, 624.0, 636.0, 644.0, 650.0, 656.0, 662.0, 668.0, 674.0, 682.0, np.inf]
modify_bin = [-np.inf, 616.0, 626.0, 634.0, 642.0, 648.0, 654.0, 660.0, 667.0, 676.0, np.inf]

onekey_bin = [-np.inf, 623.0, 635.0, 644.0, 651.0, 657.0, 663.0, 669.0, 676.0, 684.0, np.inf]
modify_bin = [-np.inf, 615.0, 624.0, 633.0, 640.0, 647.0, 653.0, 659.0, 667.0, 677.0, np.inf]

ks_all = mt.Performance().calculate_ks_by_decile(df_all.loc[df_all.effective_date >='2020-01-17']['score'],
                                df_all.loc[df_all.effective_date >='2020-01-17']['flag_7'], 'decile',
                                              manual_cut_bounds = train_ks)
ks_all.to_excel('D:/Model/indn/202001_mvp_model/02_result/0326/ks.xlsx')

one_key_loan_ks = mt.Performance().calculate_ks_by_decile(df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              manual_cut_bounds = onekey_bin)

one_key_loan_ks_self = mt.Performance().calculate_ks_by_decile(df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              q = 10)

one_key_loan_20ks_self = mt.Performance().calculate_ks_by_decile(df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              q = 20)

one_key_loan_ks8 = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')\
                                & (df_all.approved_period == 8)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              manual_cut_bounds = onekey_bin)

one_key_loan_ks_self8 = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')
                                & (df_all.approved_period == 8)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              q = 10)

one_key_loan_20ks_self8 = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')
                                & (df_all.approved_period == 8)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              q = 20)

one_key_loan_ks15 = mt.Performance().calculate_ks_by_decile(dfw_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')
                                & (df_all.approved_period == 15)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              manual_cut_bounds = onekey_bin)

one_key_loan_ks_self15 = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')
                                & (df_all.approved_period == 15)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              q = 10)

one_key_loan_20ks_self15 = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')
                                & (df_all.approved_period == 15)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              q = 20)

one_key_loan_ks22 = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')
                                & (df_all.approved_period == 22)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              manual_cut_bounds = onekey_bin)

one_key_loan_ks_self22 = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')
                                & (df_all.approved_period == 22)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              q = 10)

one_key_loan_20ks_self22 = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN')
                                & (df_all.approved_period == 22)]['score'],
                                df_all.loc[df_all.customer_type == 'ONE_KEY_LOAN']['flag_7'], 'decile',
                                              q = 20)

modify_ks = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.effective_date >='2020-01-17')]['score'],
                                df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.effective_date >='2020-01-17')]['flag_7'], 'decile',
                                              manual_cut_bounds = modify_bin)

modify_ks_self = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.effective_date >='2020-01-17')]['score'],
                                df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.effective_date >='2020-01-17')]['flag_7'], 'decile',
                                              q = 10)

modify_20ks_self = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.effective_date >='2020-01-17')]['score'],
                                df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.effective_date >='2020-01-17')]['flag_7'], 'decile',
                                              q = 20)

modify_ks_self_before = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'MODIFY')]['score'],
                                df_all.loc[(df_all.customer_type == 'MODIFY')]['flag_7'], 'decile',
                                              q = 10)

modify_20ks_self_before = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'MODIFY')]['score'],
                                df_all.loc[(df_all.customer_type == 'MODIFY')]['flag_7'], 'decile',
                                              q = 20)

modify_ks_self_old = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.oldusermodelv1score.notnull()) & (df_all.effective_date >='2020-01-17')]['oldusermodelv1score'],
                                df_all.loc[(df_all.customer_type == 'MODIFY')&(df_all.oldusermodelv1score.notnull()) & (df_all.effective_date >='2020-01-17')]['flag_7'], 'decile',
                                              q = 10)

modify_20ks_self_old = mt.Performance().calculate_ks_by_decile(df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.oldusermodelv1score.notnull()) & (df_all.effective_date >='2020-01-17')]['oldusermodelv1score'],
                                df_all.loc[(df_all.customer_type == 'MODIFY')&(df_all.oldusermodelv1score.notnull()) & (df_all.effective_date >='2020-01-17')]['flag_7'], 'decile',
                                              q = 20)

writer = pd.ExcelWriter('D:/Model/indn/202001_mvp_model/03_策略/ks_0117to0203_0226_0304.xlsx')
one_key_loan_ks.append(one_key_loan_ks_self).append(one_key_loan_20ks_self).to_excel(writer,'one_key_loan_ks')
modify_ks.append(modify_ks_self).append(modify_20ks_self).to_excel(writer,'modify_ks')
modify_ks_self_old.append(modify_20ks_self_old).to_excel(writer,'modify_ks_self_old')
one_key_loan_ks8.append(one_key_loan_ks_self8).append(one_key_loan_20ks_self8).to_excel(writer,'onekey_8')
one_key_loan_ks15.append(one_key_loan_ks_self15).append(one_key_loan_20ks_self15).to_excel(writer,'onekey_15')
one_key_loan_ks22.append(one_key_loan_ks_self22).append(one_key_loan_20ks_self22).to_excel(writer,'onekey_22')
writer.save()

import utils3.plot_tools as pt
import matplotlib.pyplot as plt

#分train, test, oot 看模型的效果
FIG_PATH = os.path.join('D:/Model/indn/202001_mvp_model/02_result/0205/', 'figure', 'lifechart_46_0210')
if not os.path.exists(FIG_PATH):
    os.makedirs(FIG_PATH)

onekeylc = pt.show_result_new(df_all.loc[(df_all.customer_type == 'ONE_KEY_LOAN') & (df_all.effective_date >='2020-01-17')], 'prob','flag_7', n_bins = 10, feature_label='ONE_KEY_LOAN')
modify_lc = pt.show_result_new(df_all.loc[(df_all.customer_type == 'MODIFY') & (df_all.effective_date >='2020-01-17')] , 'prob','flag_7', n_bins = 10, feature_label='MODIFY')
path = os.path.join(FIG_PATH, "LiftChart_" + '0110to0123' + ".png")
plt.savefig(path, format='png', dpi=100)
plt.close()

#稳定性分析
all_x = load_data_from_pickle('D:/Model/indn/202001_mvp_model/01_data/', 'all_x2_0110to0209.pkl')
all_x.shape

model_data = load_data_from_pickle('D:/Model/indn/202001_mvp_model/01_data/', 'x_with_y_v4.pkl')

model_data.sample_set.value_counts()
model_data.avg_extend_times = model_data.avg_extend_times.astype(float)
model_data.index = model_data.loan_id

keep_list = list(features_used['varName'])
keep_list.append('sample_set')

model_data2 = model_data[keep_list]

#
features_used = pd.read_excel('D:/Model/indn/202001_mvp_model/02_result/0205/grid_46_200210-09.xlsx', sheet_name = '04_model_importance')

all_x3 = all_x2[features_used['varName']]
all_x3['sample_set'] = 'oot2'


all_data = pd.concat([model_data2, all_x3])

var_dict = pd.read_excel('D:/Model/indn/202001_mvp_model/建模代码可用变量字典.xlsx')
rebin_spec = load_data_from_pickle('D:/Model/indn/202001_mvp_model/02_result/','rebin_spec_bin_adjusted.pkl')
rebin_spec

import utils3.metrics as mt
import utils3.summary_statistics as ss

bin_obj = mt.BinWoe()
X_cat_train = bin_obj.convert_to_category(all_data.loc[all_data.sample_set.isin(['train'])][features_used['varName']], var_dict, rebin_spec)
X_cat_test = bin_obj.convert_to_category(all_data.loc[all_data.sample_set.isin(['test','oot'])][features_used['varName']], var_dict, rebin_spec)
X_cat_oot = bin_obj.convert_to_category(all_data.loc[all_data.sample_set.isin(['oot2'])][features_used['varName']], var_dict, rebin_spec)

all_data.loc[all_data.sample_set.isin(['oot2'])].shape


all_data.sample_set = all_data.sample_set.replace('test','test&oot')
all_data.sample_set = all_data.sample_set.replace('oot','test&oot')

## train
train_df['appmon'] = '0_train'

## test
test_df['appmon'] = '1_test'

#oot
oot_df['appmon'] = '2_oot'
## all

all_cat = pd.concat([X_cat_train,X_cat_test, X_cat_oot])

all_data['label'] = 1

app_data = all_data[['label','sample_set']]

X_cat_with_y_appmon_all = pd.merge(all_cat,app_data[['label','sample_set']] ,left_index=True,right_index=True)

var_dist_badRate_by_time_all = ss.get_badRate_and_dist_by_time(X_cat_with_y_appmon_all,features_used['varName'],'sample_set','label')
var_dist_badRate_by_time_all.to_excel('D:/Model/indn/202001_mvp_model/02_result/0205/var_dist0225.xlsx')

#izi其他数据检查

x_cols = set(all_x.columns).intersection(var_dict['指标英文'])
ss.eda(X = all_x[x_cols], var_dict=var_dict, data_path = 'D:/Model/indn/202001_mvp_model/02_result/0205/', useless_vars = [],exempt_vars = [], save_label ='all_0218', uniqvalue_cutoff=0.97)

izi_id = pd.read_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/izi_回溯0210_online.xlsx')
izi_id['loanid']

all_x.loc[all_x.loan_id == '432551721540419584']['B_30d']

izi = load_data_from_pickle('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/','izi_var_0212.pkl')
izi.columns
izi.loc[izi.loanid == '432551721540419584']['B_30d']

"""izi回溯数据和线上数据对比"""
from jinja2 import Template

izi_sql2 = """
SELECT customer_id, date(create_time), message
FROM gocash_oss_inquiries_v4
WHERE customer_id in {{id_list}}
"""
izi_id.customer_id = izi_id.customer_id.astype(str)
izi_online_df = get_df_from_pg(Template(izi_sql2).render(id_list = tuple(izi_id['customer_id'])))
izi_online_df.columns

izi_huisu = pd.read_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/iziData_test0210.xlsx')
temp = izi_huisu.loc[izi_huisu.customer_id.isin(izi_id['customer_id'])][['customer_id','apply_time', '身份证多头v4_result']]
temp.customer_id = temp.customer_id.astype(str)
temp2 = temp.merge(izi_online_df, on = 'customer_id')
temp2.to_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/数据对比.xlsx')

temp2 = pd.read_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/数据对比.xlsx')

try1= from_json(temp2, 'message')
try2= from_json(try1, 'detail')
try3 = try2.copy()
try3['AA'] = try3['A'].apply(lambda x: extract_date(x))
try3['BB'] = try3['B'].apply(lambda x: extract_date(x))
try3['CC'] = try3['C'].apply(lambda x: extract_date(x))


"""灰度验收"""
all_sample = pd.read_excel('D:/Model/indn/202001_mvp_model/01_data/izi_td/0210回溯/izi_回溯0210.xlsx')\
    .drop(['Unnamed: 0'],1)
all_sample.rename(columns = {'loanid':'loan_id'}, inplace= True)
all_sample.loan_id = all_sample.loan_id.astype(str)

all_sample.loc[(all_sample.customer_type == 'MODIFY')&(all_sample.apply_time >= '2020-02-06')].shape

score = load_data_from_pickle('D:/Model/indn/202001_mvp_model/01_data','data_scored_0225_final.pkl')
score.columns

all_sample = all_sample.merge(score, on = 'loan_id' )


rc_sql = """
select customerid
    , orderno
    , createtime
    , device
    , isdecision
    , pipelineid
    , pipelinename
    , pipelineresult
    , nodename
    , noderesult
    , ruletemplatename
    , ruleresultname
    , ruleresult
    , datasources
from public.rt_risk_mongo_gocash_riskreportgray
--where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and isdecision = '1'
where pipelinename like '%old%' and createtime >= '2020-02-28 13:30:00' and businessid = 'uku' 
and nodename = 'modelNode' and ruletemplatename = 'oldUserModelV2'
"""
rc_df = get_df_from_pg(rc_sql)

rc_df2 = from_json(rc_df, 'datasources')
rc_df3 = from_json(rc_df2, 'vars')
rc_df3 = rc_df3.drop_duplicates()

pivoted = pd.pivot(rc_df3, index = 'orderno', columns = 'varName', values = 'varValue')

perf_paidhour_data.rename(columns = {'loanid':'loan_id'}, inplace= True)
perf_latedays_data.rename(columns = {'loanid':'loan_id'}, inplace= True)

perf_data.loan_id = perf_data.loan_id.astype(str)
perf_paidhour_data.loan_id = perf_paidhour_data.loan_id.astype(str)
perf_latedays_data.loan_id = perf_latedays_data.loan_id.astype(str)

pivoted = pivoted.reset_index()
pivoted.rename(columns = {'orderno':'loan_id'}, inplace= True)

pivoted = pivoted.merge(perf_data, on = 'loan_id').merge(perf_paidhour_data, on = 'loan_id').merge(perf_latedays_data, on = 'loan_id')
pivoted = pivoted.merge(rc_df[['orderno','createtime','ruleresult']].drop_duplicates(), left_on = 'loan_id', right_on = 'orderno')
pivoted.ruleresult = pivoted.ruleresult.astype(float)

pivoted.loc[(pivoted.ruleresult > 634) & (pivoted.createtime >= '2020-02-28 23:59:59')].shape
pivoted.loc[(pivoted.createtime > '2020-02-28 23:59:59')].shape

pivoted.loc[pivoted.ruleresult > 634].shape

pivoted.to_excel('D:/Model/indn/202001_mvp_model/03_策略/验收数据/rc_df_0218_15_29.xlsx')

all_sample2 = all_sample.merge(pivoted, left_on = 'loan_id', right_on = 'orderno', how = 'left')
all_sample.apply_time = all_sample.apply_time.apply(lambda x:str(x)[0:10])

all_sample = all_sample.drop(['flag_7','due_date','cell_phone','md5_cellphone_izi','md5_idcardno','id_card_name','md5name'],1)
all_sample['modify_apply_bin'] = pd.qcut(all_sample.loc[all_sample.customer_type == 'MODIFY','score'], q = 10, precision= 0, duplicates='drop').astype('str')
all_sample['onekeyloan_apply_bin'] = pd.qcut(all_sample.loc[all_sample.customer_type == 'ONE_KEY_LOAN','score'], q = 10, precision= 0, duplicates='drop').astype('str')
all_sample = all_sample.drop(['loan_status'],1)

all_sample.merge(perf_df2, on = '')
perf_df2.shape
all_sample.to_excel('D:/Model/indn/202001_mvp_model/03_策略/apply_date_0110to0209_0225.xlsx')

all_sample = pd.read_excel('D:/Model/indn/202001_mvp_model/03_策略/apply_date_0110to0209_0225.xlsx')

all_sample = all_sample.drop_duplicates('loan_id')
all_sample = all_sample.drop(['Unnamed: 0'],1)
all_sample.loan_id = all_sample.loan_id.astype(str)
all_sample.customer_id = all_sample.customer_id.astype(str)

"""
线上模型的通过率分析
"""

old_rc_sql = """
SELECT loanid,
customerid,
date(createtime) as createtime,
oldusermodelv1result,
oldusermodelv1score,
device,
pipelinename
FROM public.rt_risk_mongo_gocash_installmentriskcontrolresult
WHERE createtime between '2020-01-17 00:00:00' and '2020-03-02 10:00:00' and businessid = 'uku' and oldusermodelv1result <>''
"""
old_data = get_df_from_pg(old_rc_sql)
old_data.device.value_counts()

old_data.oldusermodelv1score = old_data.oldusermodelv1score.astype(float)
old_rc_result = old_data.groupby(['createtime','oldusermodelv1result'])['loanid'].count().unstack()
old_rc_result['total'] = old_rc_result['pass'] + old_rc_result['reject']
old_rc_result['通过率'] = old_rc_result['pass']/ (old_rc_result['pass'] + old_rc_result['reject'])
old_rc_result = old_rc_result.reset_index()
old_rc_result.createtime = old_rc_result.createtime.astype(str)

#只看2.29， 3.1数据
(47+29)/(163+137) #25%

old_rc_result.loc[(old_rc_result.createtime >= '2020-02-24') & (old_rc_result.createtime <= '2020-02-28')]['pass'].sum()
old_rc_result.loc[(old_rc_result.createtime >= '2020-02-24') & (old_rc_result.createtime <= '2020-02-28')]['total'].sum()
269/897 #29%

import itertools

def from_json(data, var_name: str):
    """
    :param data: dataframe
    :param var_name: column name of json in dataframe, json object: dict or [dict]
    :return:
    """

    a1 = data.copy()
    a1 = a1[~a1[var_name].isna()].reset_index(drop=True)
    other_col_list = list(a1.columns)
    other_col_list.remove(var_name)

    a1[var_name] = a1[var_name].map(lambda x: json.loads(x) if isinstance(x, str) else x)

    if not isinstance(a1[var_name][0], dict) or not isinstance(a1[var_name][1], dict):
        a1[var_name] = a1[var_name].map(lambda x: [{'temp': None}] if len(x) == 0 else x)
        list_len = list(map(len, a1[var_name].values))
        newvalues = np.hstack((np.repeat(a1[other_col_list].values, list_len, axis=0),
                               np.array([np.concatenate(a1[var_name].values)]).T))
        a1 = pd.DataFrame(data=newvalues, columns=other_col_list + [var_name])

    start = time.time()
    # 新增一列'columns'用于存储每一列的json串的字段名
    a1['columns'] = a1[str(var_name)].map(
        lambda x: list(x.keys()) if isinstance(x, dict) else list(json.loads(x).keys()))
    print('new columns done')
    # 获取json串中的所有字段名称
    add_columns_list = list(set(list(itertools.chain(*a1['columns']))))
    for columns in add_columns_list:
        # 将json串展开
        a1[str(columns)] = a1[str(var_name)].map(
            lambda x: x.get(str(columns)) if isinstance(x, dict) else json.loads(x).get(str(columns)))
        print(str(columns))
    if 'temp' in a1.columns:
        del a1['temp']
    del a1['columns'], a1[str(var_name)]
    end = time.time()
    print("run time = {}".format(end - start))

    return a1

"""验收"""
rc_df_f = rc_df.merge(pivoted, left_on = 'orderno', right_index = True )
rc_df_f.ruleresult = rc_df_f.ruleresult.astype(float)
rc_df_f.loc[rc_df_f.ruleresult > 634].shape

features_used = pd.read_excel('D:/Model/indn/202001_mvp_model/最终文档/需求文档/印尼UKU产品老客模型入模变量.xlsx', sheet_name = '01_变量汇总')

set(features_used['线上变量名']) - set(pivoted.columns)
#打分一致性
pivoted.index = pivoted.loan_id
pivoted2 = pivoted[features_used['线上变量名']]
pivoted2 = pivoted2.astype(float)
pivoted2 = pivoted2.fillna(-1)

data_lean = DMatrix(pivoted2)

ypred = mymodel.predict(data_lean)
score = [round(Prob2Score(value, 600, 20)) for value in ypred]
data_scored = pd.DataFrame([pivoted2.index, score, ypred]).T
data_scored.rename(columns = {0:'loan_id', 1:'score', 2:'prob'}, inplace=True)
temp = data_scored.merge(pivoted, left_on = 'loan_id', right_on = 'orderno')
temp.loan_id = temp.loan_id.astype(str)
temp.orderno = temp.orderno.astype(str)

temp['']
temp.to_excel('D:/Model/indn/202001_mvp_model/03_策略/模型分0228to0302.xlsx')


temp['score'] - temp['']
rc_df_f = rc_df_f.merge(data_scored,left_index = True,  right_on = 'loan_id')
rc_df_f.to_excel('D:/Model/indn/202001_mvp_model/03_策略/数据check.xlsx')
rc_df_f = pd.read_excel('D:/Model/indn/202001_mvp_model/03_策略/数据check.xlsx')
rc_df_f.orderno = rc_df_f.orderno.astype(float)

temp = rc_df_f.merge(base_data_f, left_on = 'orderno', right_on = 'loan_id', how = 'left').merge(base_data2, left_on = 'orderno', right_on  = 'loan_id', how = 'left')\
.merge(prof_data, left_on = 'orderno', right_on = 'loan_id', how = 'left').merge(bank_data_f, left_on = 'orderno', right_on = 'loan_id', how = 'left')\
.merge(refer_data, left_on = 'orderno', right_on = 'loan_id', how = 'left')

temp2 = rc_df_f.merge(perf_data, left_on = 'orderno', right_on = 'loan_id', how = 'left').merge(perf_latedays_data, left_on = 'orderno', right_on  = 'loan_id', how = 'left')\
.merge(perf_paidhour_data, left_on = 'orderno', right_on = 'loan_id', how = 'left')

temp2.avg_extend_times_60d = temp2.avg_extend_times_60d.astype(float)
temp2.avg_extend_times_90d = temp2.avg_extend_times_90d.astype(float)

temp.to_excel('D:/Model/indn/202001_mvp_model/03_策略/数据check_0225.xlsx')
temp2.to_excel('D:/Model/indn/202001_mvp_model/03_策略/数据check_0225v2.xlsx')


base_data2.head()

base_data_f.loan_id = base_data_f.loan_id.astype(str)
base_data2.loan_id = base_data2.loan_id.astype(str)
prof_data.loan_id = prof_data.loan_id.astype(str)
bank_data_f.loan_id = bank_data_f.loan_id.astype(str)
refer_data.loan_id = refer_data.loan_id.astype(str)


#base_data.to_excel('D:/Model/indn/202001_mvp_model/03_策略/base_data.xlsx')
#base_data2.to_excel('D:/Model/indn/202001_mvp_model/03_策略/base_data2.xlsx')

perf_data.loan_id = perf_data.loan_id.astype(str)
perf_data.avg_extend_times = perf_data.avg_extend_times.astype(float)
perf_latedays_data.loan_id =perf_latedays_data.loan_id.astype(str)
perf_paidhour_data.loan_id =perf_paidhour_data.loan_id.astype(str)

perf_data.to_excel('D:/Model/indn/202001_mvp_model/03_策略/perf_data.xlsx')


"""swap analysis"""
all_sample = pd.read_excel('D:/Model/indn/202001_mvp_model/03_策略/apply_date_0110to0209.xlsx',sheet_name = 'Sheet1')
all_sample = all_sample.loc[all_sample.apply_time >= '2020-01-17']

all_sample = all_sample.loc[all_sample.oldusermodelv1score != '']

all_sample.oldusermodelv1score.describe()
all_sample.score.describe()

all_sample = all_sample.drop(['loan_status'],1)
all_sample2 = all_sample.merge(perf_df[['loan_id','flag_7','loan_status']], on = 'loan_id', how = 'left')

perf_df2.loan_id = perf_df2.loan_id.astype(str)

all_sample2 = all_sample2.merge(perf_df2[['loan_id','loan_status']], on = 'loan_id', how = 'left')

all_sample2.loc[all_sample2.oldusermodelv1score.notnull()].shape

all_sample2.oldusermodelv1score
all_sample2.loc[(all_sample2.oldusermodelv1score > 673) & all_sample2.oldusermodelv1score.notnull()].shape
1296/4696

all_sample2.loc[(all_sample2.score > 636) & all_sample2.oldusermodelv1score.notnull()].shape
1326/4696

all_sample2.loc[(all_sample2.oldusermodelv1score > 606) & all_sample2.oldusermodelv1score.notnull()].shape
3594/4696

all_sample2.loc[(all_sample2.score > 609) & all_sample2.oldusermodelv1score.notnull()].shape

all_sample2.loc[(all_sample2.oldusermodelv1score > 673) & all_sample2.oldusermodelv1score.notnull(),'old_label'] = '>673'
all_sample2.loc[(all_sample2.oldusermodelv1score <= 673) & all_sample2.oldusermodelv1score.notnull(),'old_label'] = '<=673'

all_sample2.loc[(all_sample2.score > 636) & all_sample2.oldusermodelv1score.notnull(),'new_label'] = '>636'
all_sample2.loc[(all_sample2.score <= 636) & all_sample2.oldusermodelv1score.notnull(),'new_label'] = '<=636'

all_sample2.loc[(all_sample2.oldusermodelv1score > 606) & all_sample2.oldusermodelv1score.notnull(),'old_label_pre'] = '>606'
all_sample2.loc[(all_sample2.oldusermodelv1score <= 606) & all_sample2.oldusermodelv1score.notnull(),'old_label_pre'] = '<=606'

all_sample2.loc[(all_sample2.score > 609) & all_sample2.oldusermodelv1score.notnull(),'new_label_pre'] = '>609'
all_sample2.loc[(all_sample2.score <= 609) & all_sample2.oldusermodelv1score.notnull(),'new_label_pre'] = '<=609'


all_sample2.isnull().sum()

all_sample2.oldusermodelv1score = all_sample2.oldusermodelv1score.astype(float)
all_sample2.score = all_sample2.score.astype(float)

all_sample2.to_excel('D:/Model/indn/202001_mvp_model/03_策略/apply_date_0110to0209_0226.xlsx')

all_sample.to_excel('D:/Model/indn/202001_mvp_model/03_策略/apply_date_0110to0209_0225.xlsx')


temp = get_swap_table(all_sample2.loc[all_sample2.oldusermodelv1score_y.notnull()], 'oldusermodelv1score_y', 'score', 'flag_7')
temp.to_excel('D:/Model/indn/202001_mvp_model/03_策略/swap.xlsx')

def get_swap_table(data, base_score_name, compare_score_name, y_col):
    medians = {}
    medians['base'] = np.nanmedian(data[base_score_name])
    medians['compare'] = np.nanmedian(data[compare_score_name])

    data.loc[:, '%sapprove' % base_score_name] = np.where(data[base_score_name] > medians['base'], 1, 0)
    data.loc[:, '%sapprove' % compare_score_name] = np.where(data[compare_score_name] > medians['compare'], 1, 0)

    ct = pd.crosstab(data['%sapprove' % base_score_name], data['%sapprove' % compare_score_name]).reset_index()
    ct.loc[:, 'label'] = '%s_ct' % y_col

    ratecross = data.groupby(['%sapprove' % base_score_name, '%sapprove' % compare_score_name], as_index=False)\
                    [y_col].mean()\
                    .pivot(index='%sapprove' % base_score_name,
                           columns='%sapprove' % compare_score_name,
                           values=y_col)\
                    .reset_index()

    ratecross.loc[:, 'label'] = '%s_rate' % y_col
    #
    result = pd.concat([ct, ratecross])
    result = result.rename(columns={0:'拒绝', 1:'通过'})
    result = result.replace(0, '拒绝').replace(1, '通过')
    return result

"""
izi
"""

izi_sql = """
WITH rc as (
select customerid
    , orderno
    , date(createtime) as createtime
    , device
    , isdecision
    , pipelineid
    , pipelinename
    , pipelineresult
    , nodename
    , noderesult
    , ruletemplatename
    , ruleresultname
    , ruleresult
    , datasources
from public.rt_risk_mongo_gocash_riskreportgray
--where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and isdecision = '1'
where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and nodename = 'modelNode' and ruletemplatename = 'oldUserModelV2'
)
SELECT rc.*
, concat(year, month, day) as apply_time
, date(t1.apply_time) - interval '1 day' as pre_dt
, date(t2.create_time) as create_time,  t2.message
FROM rc
LEFT JOIN (select *,  to_char(apply_time, 'YYYY') as year , to_char(apply_time, 'MM') as month, to_char(apply_time, 'DD') as day
            FROM  dw_gocash_go_cash_loan_gocash_core_loan) t1 on rc.orderno = t1.id::varchar
LEFT JOIN gocash_oss_inquiries_v4 t2 ON rc.customerid = t2.customer_id
WHERE rc.createtime = date(t2.create_time)
"""

izi_df = get_df_from_pg(izi_sql)

try1= from_json(izi_df, 'message')
try2= from_json(try1, 'detail')

try3 = try2.copy()
try3['AA'] = try3['A'].apply(lambda x: extract_date(x))
try3['BB'] = try3['B'].apply(lambda x: extract_date(x))
try3['CC'] = try3['C'].apply(lambda x: extract_date(x))
try3.head()

try4 = try3.copy()

try4.pre_dt = try4.pre_dt.astype(str)

#str(try4.loc[8,'AA']).replace('Within24hours',try4.loc[8, 'pre_dt']).replace('-','')

for index, row in try4.iterrows():
    try:
        try4.loc[index, 'CC'] = str(try4.loc[index,'CC']).replace('Within24hours',try4.loc[index, 'pre_dt'].replace('-',''))
    except:
        print(index)
        pass

try4['AA'] = try4['AA'].apply(lambda x:ast.literal_eval(x))
try4['BB'] = try4['BB'].apply(lambda x:ast.literal_eval(x))
try4['CC'] = try4['CC'].apply(lambda x:ast.literal_eval(x))


for row in range(try4.shape[0]):
    #print(row)
    a=try4.iloc[row]
    print(a[['AA']])
    print(a[['BB']])
    print(a[['CC']])
    try4.loc[row,'A_daysdiff']= ','.join(date_to_daysdiff(a, len(a['AA']), 'AA'))
    try4.loc[row,'B_daysdiff']= ','.join(date_to_daysdiff(a, len(a['BB']), 'BB'))
    try4.loc[row,'C_daysdiff']= ','.join(date_to_daysdiff(a, len(a['CC']), 'CC'))


try4.to_excel('D:/Model/indn/202001_mvp_model/03_策略/try4.xlsx')

try4.A_daysdiff = try4.A_daysdiff.replace('', '-9999')
try4.B_daysdiff = try4.B_daysdiff.replace('', '-9999')
try4.C_daysdiff = try4.C_daysdiff.replace('', '-9999')

try5 = try4.copy()
try5['A_3d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['A_7d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['A_14d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['A_21d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['A_30d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['A_60d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['A_90d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['A_180d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['A_360d'] = try5['A_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

try5['B_3d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['B_7d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['B_14d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['B_21d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['B_30d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['B_60d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['B_90d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['B_180d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['B_360d'] = try5['B_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

try5['C_3d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=3 and int(a)>=0]))
try5['C_7d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=7 and int(a)>=0]))
try5['C_14d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=14 and int(a)>=0]))
try5['C_21d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=21 and int(a)>=0]))
try5['C_30d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=30 and int(a)>=0]))
try5['C_60d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=60 and int(a)>=0]))
try5['C_90d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=90 and int(a)>=0]))
try5['C_180d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=180 and int(a)>=0]))
try5['C_360d'] = try5['C_daysdiff'].apply(lambda x : len([a for a in x.split(',') if int(a)<=360 and int(a)>=0]))

try5.columns

try5 = try5[['orderno','A_14d','A_60d','B_30d','C_3d','C_21d']]
try5.to_excel('D:/Model/indn/202001_mvp_model/03_策略/try5v2.xlsx')

try5 = pd.read_excel('D:/Model/indn/202001_mvp_model/03_策略/try5.xlsx')
try4.loc[0,'CC'] == "['']"

try4.loc[0,'CC']

import ast
x = u'[ "A","B","C" , " D"]'
x = ast.literal_eval(x)
x = ast.literal_eval(try4.loc[0,'CC'])


"""topup"""
topup_sql = """
WITH rc as (
select customerid
    , orderno
    , date(createtime) as createtime
    , device
    , isdecision
    , pipelineid
    , pipelinename
    , pipelineresult
    , nodename
    , noderesult
    , ruletemplatename
    , ruleresultname
    , ruleresult
    , datasources
from public.rt_risk_mongo_gocash_riskreportgray
--where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and isdecision = '1'
where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and nodename = 'modelNode' and ruletemplatename = 'oldUserModelV2'
)
SELECT *
FROM rc
LEFT JOIN (select customer_id, create_time,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_90,min}' end as topup_0_90_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,times}' end as topup_0_180_times,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_180,min}' end as topup_0_180_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_0_360,min}' end as topup_0_360_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_60_90,min}' end as topup_60_90_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_90_180,min}' end as topup_90_180_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_180_360,min}' end as topup_180_360_min,
case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,avg}' end as topup_360_720_avg,
case when substring(message,1,1) = '{' then message::json #>>'{topup_360_720,min}' end as topup_360_720_min
from gocash_oss_to_pup) t ON rc.customerid = t.customer_id
WHERE rc.createtime = date(t.create_time)
"""

topup_df = get_df_from_pg(topup_sql)

topup_df2 = from_json(topup_df, 'datasources')
topup_df3 = from_json(topup_df2, 'vars')
topup_df4 = topup_df3[['orderno','varName','varValue']].drop_duplicates()
topup_df5 = topup_df3[['orderno','customerid','topup_0_90_min','topup_0_180_times','topup_0_180_min','topup_0_360_min','topup_60_90_min'
                       ,'topup_90_180_min','topup_180_360_min','topup_360_720_avg','topup_360_720_min']].drop_duplicates()

pivoted = pd.pivot(topup_df4, index = 'orderno', columns = 'varName', values = 'varValue')

temp = pivoted.merge(topup_df5, on = 'orderno', how = 'left')
temp.to_excel('D:/Model/indn/202001_mvp_model/03_策略/topup_compare.xlsx')

"""topup"""
phonescore_sql = """
WITH rc as (
select customerid
    , orderno
    , date(createtime) as createtime
    , device
    , isdecision
    , pipelineid
    , pipelinename
    , pipelineresult
    , nodename
    , noderesult
    , ruletemplatename
    , ruleresultname
    , ruleresult
    , datasources
from public.rt_risk_mongo_gocash_riskreportgray
--where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and isdecision = '1'
where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and nodename = 'modelNode' and ruletemplatename = 'oldUserModelV2'
)
SELECT *
FROM rc
LEFT JOIN (
select *, message::json ->> 'score'
from gocash_oss_phone_score
where status = 'OK') t ON rc.customerid = t.customer_id
WHERE rc.createtime = date(t.create_time)
"""

phonescore_df = get_df_from_pg(phonescore_sql)
phonescore_df.orderno = phonescore_df.orderno.astype(str)
phonescore_df.to_excel('D:/Model/indn/202001_mvp_model/03_策略/phonescore_compare.xlsx')

tel_sql = """
WITH rc as (
select customerid
    , orderno
    , date(createtime) as createtime
    , device
    , isdecision
    , pipelineid
    , pipelinename
    , pipelineresult
    , nodename
    , noderesult
    , ruletemplatename
    , ruleresultname
    , ruleresult
    , datasources
from public.rt_risk_mongo_gocash_riskreportgray
--where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and isdecision = '1'
where pipelinename like '%old%' and createtime >= '2020-02-24 15:10:00' and businessid = 'uku' and nodename = 'modelNode' and ruletemplatename = 'oldUserModelV2'
)
SELECT *
FROM rc
LEFT JOIN (
select *, message::json ->> 'carrier'
from gocash_oss_phone_operator
where status = 'ok') t ON rc.customerid = t.customer_id
WHERE rc.createtime = date(t.create_time)
"""

tel_df = get_df_from_pg(tel_sql)
tel_df.orderno = tel_df.orderno.astype(str)
tel_df.to_excel('D:/Model/indn/202001_mvp_model/03_策略/tel_compare.xlsx')

perf_data.loan_id = perf_data.loan_id.astype(str)
temp = pivoted.merge(perf_data, left_on = 'orderno', right_on = 'loan_id')
temp.to_excel('D:/Model/indn/202001_mvp_model/03_策略/temp.xlsx')
