"""
申请样本打分
"""

#近期申请人群

apply_sql = """
SELECT customerid
, loanid
, createtime
, risklevel
, risklevelfirst
, risklevelsecond
, oldusermodelv1score
, existsLoanWithContacts
, contactsDanger
, installDangerApp
, callWithBadGuys
, deniedByMessagesHistory
, blackList
, invalidAge
, deniedByOccupation
, locationCompare
, installSpecifyApp
, denyiziinquiriesbytypetotal
, denyiziphoneage
--rc2
, deniedloanwith5fields
, existsloanwith5fields
, duplicatecustomerwith5fields
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null   
"""
apply_data = get_df_from_pg(apply_sql)
apply_data.shape[0]==apply_data.customerid.nunique()
a = apply_data.customerid.value_counts().reset_index().rename(columns = {'index':'customerid', 'customerid':'cnt'})
a.loc[a.cnt >1].shape

apply_data2 = apply_data.drop(['loanid'],1)

apply_data.loc[apply_data.customerid == '359555709129494528']
apply_data = apply_data.drop_duplicates()


#老客模型分is not null

""""基本信息"""
baseinfo_sql = """
WITH apply as 
( 
SELECT distinct customerid
    , createtime
    , loanid
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null  
),
baseinfo as 
(
SELECT id as customer_id
    , create_time as update_time 
    , cell_phone
    , mail
    , marital_status
    , religion
    , education
FROM dw_gocash_go_cash_loan_gocash_core_customer
UNION
SELECT customer_id
    , update_time --create_time都是主表的create_time
    , cell_phone
    , mail
    , marital_status
    , religion
    , education
FROM dw_gocash_go_cash_loan_gocash_core_customer_history 
)
SELECT *
FROM (SELECT apply.customerid
            , apply.loanid
            , apply.createtime
            , baseinfo.*
            , row_number() over(partition by apply.customerid order by baseinfo.update_time desc) as rn
      FROM apply
      LEFT JOIN baseinfo  ON apply.customerid = baseinfo.customer_id::varchar
      WHERE apply.createtime :: timestamp >= baseinfo.update_time
) t
WHERE rn = 1
"""
apply_base = get_df_from_pg(baseinfo_sql)

apply_base.customer_id.nunique() == apply_base.shape[0] #true
len(set(apply_data.customerid) - set(apply_base.customerid)) #14条缺失, 检查发现表里少数

apply_base.loc[apply_base.customerid == '359555709129494528'].T
apply_data.loc[apply_data.customerid == '359555709129494528'].T

save_data_to_pickle(apply_base, data_path, 'apply_base_0807to0828.pkl')

baseinfo_sql2 = """
WITH apply as 
( 
SELECT distinct customerid
    , createtime
    , loanid
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null  
),
para as (
SELECT customerid as customer_id
    , createtime
    , provincecode
    , gender
    , age
FROM public.risk_mongo_gocash_installmentriskcontrolparams
)
SELECT *
FROM (SELECT apply.customerid
            , apply.loanid
            , apply.createtime
            , para.*
            --, row_number() over(partition by apply.customerid order by para.createtime desc) as rn
      FROM apply
      LEFT join para ON apply.customerid = para.customer_id
      --WHERE apply.createtime >= para.createtime
) t
--WHERE rn = 1
"""
apply_base2 = get_df_from_pg(baseinfo_sql2)

apply_base2 = apply_base2.drop(['loanid'],1)

apply_base2 =apply_base2.drop_duplicates()

apply_base2.customerid.value_counts()
apply_base2.loc[apply_base2.customerid == '342597365194076160'].T


apply_base2.customer_id.nunique() == apply_base2.shape[0] #true
apply_base2.shape
apply_base2.customerid.value_counts()

apply_base2 = apply_base2.drop(['createtime','createtime','customer_id'],1)
apply_base2 = apply_base2.drop_duplicates()


save_data_to_pickle(apply_base2, data_path, 'apply_base2_0808to0828.pkl')

"""职业信息"""
prof_sql = """
WITH apply as 
(
SELECT distinct customerid
    , createtime
    --, loanid
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null  
),
prof as 
(
SELECT customer_id
    , create_time as update_time
    , occupation_type
    , job
FROM dw_gocash_go_cash_loan_gocash_core_cusomer_profession
UNION
SELECT customer_id
    , update_time
    , occupation_type
    , job
FROM dw_gocash_go_cash_loan_gocash_core_cusomer_profession_history 
)
SELECT customerid
    , createtime
    , customer_id
    , occupation_type
    , case when occupation_type in ('OFFICE') then job end as job
FROM (SELECT apply.customerid
            , apply.createtime
            , prof.*
            , row_number() over(partition by apply.customerid order by prof.update_time desc) as rn
      FROM apply
      LEFT JOIN prof ON apply.customerid = prof.customer_id :: varchar
      WHERE apply.createtime::timestamp >= prof.update_time
) t
WHERE rn = 1
"""
prof_data = get_df_from_pg(prof_sql)
prof_data.shape[0] == prof_data.customerid.nunique()


len(set(apply_data.customerid) - set(prof_data.customerid)) #14条缺失


save_data_to_pickle(prof_data, data_path, 'apply_prof_0808to0828.pkl')
prof_data = load_data_from_pickle(data_path, 'prof_0501to1128.pkl')
prof_data.occupation_type.value_counts() #11个学生


"""银行卡信息"""
bank_sql = """
WITH apply as 
(
SELECT distinct customerid
    , createtime
    --, loanid
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null  
),
bank as (
SELECT customer_id
    , create_time as update_time
    , bank_code
FROM dw_gocash_go_cash_loan_gocash_core_customer_account
UNION
SELECT customer_id
    , update_time
    , bank_code
FROM dw_gocash_go_cash_loan_gocash_core_customer_account_history 
)
SELECT *
FROM (SELECT apply.customerid
            , apply.createtime
            , bank.*
            , row_number() over(partition by apply.customerid order by bank.update_time desc) as rn
      FROM apply
      LEFT join bank ON apply.customerid = bank.customer_id :: varchar
      WHERE apply.createtime :: timestamp + '8 hour'>= bank.update_time
) t
WHERE rn = 1
"""
bank_data = get_df_from_pg(bank_sql)

len(set(apply_data.customerid) - set(bank_data.customerid)) #154条缺失

bank_data.isnull().sum()

bank_data.bank_code.value_counts()
bank_data['bank_code_BCA'].value_counts(dropna = False)

bank_data['bank_code_BCA'] = bank_data.bank_code.apply(lambda x:1 if x == 'BCA'  else (-1 if x is None else 0))
bank_data['bank_code_PERMATA'] = bank_data.bank_code.apply(lambda x:1 if x == 'PERMATA'  else (-1 if x is None else 0))
bank_data['bank_code_BRI'] = bank_data.bank_code.apply(lambda x:1 if x == 'BRI'  else (-1 if x is None else 0))
bank_data['bank_code_MANDIRI']  = bank_data.bank_code.apply(lambda x:1 if x == 'MANDIRI'  else (-1 if x is None else 0))

save_data_to_pickle(bank_data, data_path, 'apply_bank_0808to0828.pkl')

"""紧急联系人"""
refer_sql = """
WITH apply as 
(
SELECT distinct customerid
    , createtime
    --, loanid
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null  
),
refer as 
(
SELECT customer_id
    , create_time
    , refer_type
    , case when refer_type = 'BROTHERS_AND_SISTERS' then 1 else 0 end as refer_bro_sis 
    , case when refer_type = 'PARENTS' then 1 else 0 end as refer_parents
    , case when refer_type = 'SPOUSE' then 1 else 0 end as refer_spouse    
FROM dw_gocash_go_cash_loan_gocash_core_customer_refer
WHERE create_time != '1970-01-01' and refer_type not in ('SELF','KINSFOLK','FRIEND')
)
SELECT customerid
    , createtime
    , max(refer_bro_sis) as refer_bro_sis
    , max(refer_parents) as refer_parents
    , max(refer_spouse) as refer_spouse
FROM (SELECT apply.customerid
            , apply.createtime
            , refer.*
            , dense_rank() over(partition by apply.customerid order by refer.create_time desc) as rn
      FROM apply
      LEFT JOIN refer  ON apply.customerid = refer.customer_id::varchar
      WHERE apply.createtime:: timestamp + '8 hour'  >= refer.create_time) t
WHERE rn = 1
GROUP BY customerid, createtime
"""
refer_data = get_df_from_pg(refer_sql)
len(set(apply_data.customerid) - set(refer_data.customerid)) #14条缺失

"""izi"""
izi_sql1 = """
WITH apply as 
(
SELECT distinct customerid
    , createtime
    --, loanid
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null  
),
izi as 
(
SELECT customerid as customer_id
    , createtime
    , age as phone_age
FROM risk_gocash_mongo_iziphoneage
--WHERE age is not null
)
SELECT *
FROM (SELECT apply.customerid
            , apply.createtime
            , izi.*
            , row_number() over(partition by apply.customerid order by izi.createtime desc) as rn
      FROM apply
      LEFT join izi ON apply.customerid = izi.customer_id::varchar
      WHERE apply.createtime::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
izi_data1 = get_df_from_pg(izi_sql1)
izi_data1.shape

a = list(set(apply_data.customerid) - set(izi_data1.customerid)) #154条缺失
a

apply_data['createtime_2'] = apply_data.createtime.apply(lambda x:str(x)[0:10])
apply_data.loc[apply_data.customerid.isin(a)].createtime_2.value_counts().sort_index()

apply_data.loc[apply_data.customerid.isin(['370311672191033344','370322218537754624',
 '368171942271823872',
 '339361974475337728',
 '369847379783884800',
 '331400796012843008'])]['denyiziphoneage']

izi_data1.effective_date = izi_data1.effective_date.astype(str)
izi_data1.head()
izi_data1.loc[izi_data1.customer_id  == 338691645822242816]



save_data_to_pickle(izi_data1, data_path, 'apply_izi1_0808to0828.pkl')



izi_sql2 = """
WITH apply as 
(
SELECT distinct customerid
    , createtime
    --, loanid
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null  
),
izi as (
SELECT customerid as customer_id
    , createtime
    , "07d"
    , "14d"
    , "21d"
    , "30d"
    , "60d"
    , "90d"
    , total
FROM risk_gocash_mongo_iziinquiriesbytype
)
SELECT *
FROM (SELECT apply.customerid
            , apply.createtime
            , izi.*
            , row_number() over(partition by apply.customerid order by izi.createtime desc) as rn
      FROM apply
      LEFT join izi on apply.customerid = izi.customer_id :: varchar
      WHERE apply.createtime::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""

izi_data2 = get_df_from_pg(izi_sql2)

len(set(apply_data.customerid) - set(izi_data2.customerid)) #686条缺失

save_data_to_pickle(izi_data2, data_path, 'apply_izi2_0808to0828.pkl')


izi_sql3 = """
WITH apply as 
(
SELECT distinct customerid
    , createtime
    --, loanid
FROM risk_gocash_mongo_riskcontrolresult 
WHERE date(createtime) between '2019-08-08' and '2019-08-28' and oldusermodelv1score is null  
),
izi as 
(
SELECT customerid as customer_id
    , createtime
    , result
FROM risk_gocash_mongo_iziphoneverify
)
SELECT *
FROM (SELECT apply.customerid
            , apply.createtime
            , izi.*
        , row_number() over(partition by apply.customerid order by izi.createtime desc) as rn
      FROM apply
      LEFT JOIN izi on apply.customerid = izi.customer_id :: varchar
      WHERE apply.createtime::timestamp + '8 hour' >= izi.createtime::timestamp
) t
WHERE rn = 1
"""
izi_data3 = get_df_from_pg(izi_sql3)

izi_data3.isnull().sum()



save_data_to_pickle(izi_data3, data_path, 'izi3_0621to1128_v2.pkl') #更新了取数逻辑
izi_data3 = load_data_from_pickle(data_path, 'izi3_0621to1128.pkl')


"""
合并数据
"""
apply_data['createtime_2'] = apply_data.createtime.apply(lambda x:str(x)[0:10])
apply_data = apply_data.loc[(apply_data.createtime_2 <= '2019-08-12')| (apply_data.createtime_2 >= '2019-08-23')]
apply_data.createtime_2.value_counts().sort_index()
apply_data.shape
apply_data.customerid.nunique()

a = apply_data.customerid.value_counts().reset_index().rename(columns = {'index':'customerid', 'customerid':'customerid_cnt'})
a.head()
a = a.loc[a.customerid_cnt > 1]

apply_data_2.loc[apply_data_2.customerid.isin(list(a.customerid))].to_excel(os.path.join(data_path, 'temp.xlsx'))
apply_data_2.loc[apply_data_2.customerid.isin(a)].createtime.min()

b = apply_data_2.loc[apply_data_2.customerid.isin(list(a.customerid))]

b.groupby(['customerid'])['createtime'].min().to_excel(os.path.join(data_path, 'temp.xlsx'))
b.groupby(['customerid'])['createtime'].max().to_excel(os.path.join(data_path, 'temp2.xlsx'))



apply_data_2 = apply_data.drop(['createtime'],1).drop(['loan_id'],1)
apply_data_2 = apply_data_2.drop(['loanid'],1)

apply_data_2 = apply_data_2.drop_duplicates()
apply_data_2 = apply_data_2.rename(columns = {'createtime_2':'createtime'})
apply_data_2 = apply_data_2

apply_data_2.head()

apply_data_2 = apply_data_2.drop_duplicates('customerid')
apply_data_2.shape

#基本信息

apply_x = apply_data_2.merge(apply_base.drop(['loanid','createtime','customer_id','update_time','cell_phone','rn'],1), on = 'customerid', how = 'left').merge(apply_base2, on = 'customerid', how = 'left')\
    .merge(prof_data.drop(['createtime','customer_id'],1), on = 'customerid', how = 'left').merge(bank_data.drop(['createtime','customer_id','update_time','rn'],1), on = 'customerid',  how = 'left')\
    .merge(refer_data.drop(['createtime'],1), on = 'customerid',  how = 'left').merge(izi_data1.drop(['createtime','customer_id','createtime','rn'],1), on = 'customerid',  how = 'left')\
    .merge(izi_data2.drop(['createtime','customer_id','createtime','rn'], 1), on = 'customerid',  how = 'left').merge(izi_data3.drop(['createtime','customer_id','createtime','rn'], 1), on = 'customerid',  how = 'left')

apply_x.customerid.value_counts()

apply_x = apply_x.drop_duplicates()

apply_x.loc[apply_x.customerid == '359555709129494528'].T
apply_x.shape[0] == apply_x.customerid.nunique()


#数据处理

def get_mailaddress(data, mail_cols, id):
    temp = data.loc[data[mail_cols] != -1]
    temp['mail_address1'] = temp[mail_cols].apply(lambda x: x.split('@'))
    temp['mail_address2'] = temp.mail_address1.apply(lambda x: x[-1])
    temp['mail_address3'] = temp.mail_address2.apply(lambda x: x.lower().split('.')[0])
    temp['mail_address'] = temp.mail_address3.apply(lambda x: 'gmail' if x in ['gmail'] else 'yahoo'  if x in ['yahoo','ymail','rocketmail'] else 'others')
    temp = temp[[mail_cols, id, 'mail_address']]
    return(temp)

mail_x = get_mailaddress(apply_x, 'mail', 'customerid')

apply_x['mail'] = apply_x['mail'].fillna(-1)

apply_x_2 = apply_x.merge(mail_x.drop(['mail'], 1), on = 'customerid', how = 'left')
apply_x_2.result = apply_x_2.result.replace('MATCH', 1).replace('NOT_MATCH', 0)

apply_x_2.occupation_type.value_counts()
#OFFICE          30995
#ENTREPRENEUR     5756
#UNEMPLOYED       2310
#PELAJAR           729
#-1                 13
#occupation

apply_x_2['occupation_office'] = apply_x_2.occupation_type.apply(lambda x:1 if x == 'OFFICE'  else (-1 if x == -1 else 0))
apply_x_2['occupation_entre'] = apply_x_2.occupation_type.apply(lambda x:1 if x == 'ENTREPRENEUR' else (-1 if x == -1 else 0))
apply_x_2['occupation_unemp'] = apply_x_2.occupation_type.apply(lambda x:1 if x == 'UNEMPLOYED' else (-1 if x == -1 else 0))
apply_x_2['occupation_student'] = apply_x_2.occupation_type.apply(lambda x:1 if x == 'PELAJAR' else (-1 if x == -1 else 0))


#映射
var_map = load_data_from_json('D:/Model/201911_uku_ios_model/04_最终文档/需求文档/','变量映射关系.json')

for i in apply_x_2.columns:
    if i in list(var_map.keys()):
        if i != 'provincecode':
            print(i)
            apply_x_2[i] = apply_x_2[i].map(var_map[i])


apply_x_2 = apply_x_2.fillna(-1)

#打分
features26= pd.read_excel(os.path.join(result_path, 'result2_grid_26_1206_v2.xlsx'), sheet_name = '04_model_importance').sort_values(by = 'index', ascending= True)
len(features26)

apply_x_2.index = apply_x_2.customerid
apply_x_2[list(features26.varName)] = apply_x_2[list(features26.varName)].astype(float)

apply_x_3 = apply_x_2[list(features26.varName)]

mymodel = xgb.Booster()
mymodel.load_model(os.path.join(result_path,"result2_grid_26_1206_v2.model"))

from xgboost import DMatrix
data_lean = DMatrix(apply_x_3)
ypred = mymodel.predict(data_lean)

score = [round(Prob2Score(value, 600, 20)) for value in ypred]
score

data_scored = pd.DataFrame([apply_x_2.index, score, ypred]).T
data_scored = data_scored.rename(columns = {0:'customerid',1:'score',2:'y_pred'})

apply_x_prob = apply_x_2.merge(data_scored, on = 'customerid')


#train_p_bin =[-np.inf, 0.155226, 0.201843, 0.243094,0.28239, 0.323704, 0.369021, 0.429252, 0.51285, 0.657662, np.inf]
#train_score_bin = [-np.inf, 581.0, 599.0, 608.0, 615.0, 621.0, 627.0, 633.0, 640.0, 649.0, np.inf]
#train_p_20bin = [-np.inf, 0.1258, 0.155826, 0.179677, 0.202413, 0.224872, 0.245408, 0.264573, 0.284142, 0.303372, 0.32257, 0.345276, 0.367863, 0.396108, 0.429745, 0.467254, 0.514078, 0.572399, 0.654219, 0.7636, np.inf]
#train_score_20bin =  [-np.inf, 566.0, 582.0, 592.0, 598.0, 604.0, 608.0, 612.0, 616.0, 618.0, 621.0, 624.0, 627.0, 629.0, 632.0, 636.0, 640.0, 644.0, 649.0, 656.0, np.inf]

train_p_bin = [-np.inf, 0.155826, 0.202413, 0.245408, 0.284142, 0.32257, 0.367863, 0.429745, 0.514078, 0.654219, np.inf]
train_score_bin = [-np.inf, 582.0, 598.0, 608.0, 616.0, 621.0, 627.0, 632.0, 640.0, 649.0, 683.0, np.inf]
train_p_20bin = [-np.inf, 0.1258, 0.155826, 0.179677, 0.202413, 0.224872, 0.245408, 0.264573, 0.284142, 0.303372, 0.32257, 0.345276, 0.367863
    , 0.396108, 0.429745, 0.467254, 0.514078, 0.572399, 0.654219, 0.7636, np.inf]
train_score_20bin = [-np.inf, 566.0, 582.0, 592.0, 598.0, 604.0, 608.0, 612.0, 616.0, 618.0, 621.0, 624.0, 627.0, 629.0, 632.0, 636.0, 640.0
    , 644.0, 649.0, 656.0, np.inf]



apply_x_prob['prob_bin'] = pd.cut(apply_x_prob.y_pred, bins = train_p_bin, precision = 6 ).astype(str)
apply_x_prob['score_bin'] = pd.cut(apply_x_prob.score, bins = train_score_bin ).astype(str)

apply_x_prob['prob_bin_self'] = pd.cut(apply_x_prob.y_pred, bins = 10, precision = 6 ).astype(str)
apply_x_prob['prob_20bin_self'] = pd.cut(apply_x_prob.y_pred, bins = 20, precision = 6 ).astype(str)
apply_x_prob['prob_score_self'] = pd.cut(apply_x_prob.score, bins = 10, precision = 0 ).astype(str)
apply_x_prob['prob_20score_self'] = pd.cut(apply_x_prob.score, bins = 20, precision = 0 ).astype(str)

save_data_to_pickle(apply_x_prob, data_path, 'apply_x_prob_0808to0828.pkl')
save_data_to_pickle(apply_x, data_path, 'apply_x_0808to0828.pkl')

apply_x_prob.to_excel(os.path.join(result_path, 'apply_x_prob_0808to0828_1210.xlsx'))

apply_x_prob.prob_bin.value_counts().sort_index()

apply_x_prob.phone_age.value_counts().sort_index()
# -1.0     4344
#  1.0      108
#  2.0     1799
#  3.0     1810
#  4.0     1450
#  5.0     2002
#  6.0    28290
#
# -1.0    0.109138
#  1.0    0.002713
#  2.0    0.045198
#  3.0    0.045474
#  4.0    0.036429
#  5.0    0.050298
#  6.0    0.710750

#test分数

model_score = pd.read_excel(os.path.join(result_path, 'result2_grid26_1206_score_v2.xlsx'))
model_score.order_no = model_score.order_no.astype(str)
model_score.loan_id = model_score.loan_id.astype(str)

model_score = model_score.merge(x_with_y[['loan_id','customer_id','total','phone_age']], how = 'left', on = 'loan_id')
model_score.isnull().sum()
model_score.customer_id = model_score.customer_id.astype(str)
model_score['prob']

x_with_y[['loan_id','customer_id','total','phone_age']]

model_score.to_excel(os.path.join(result_path,'result2_grid26_1206_score_v2_addvar.xlsx'))


####准备测试样例

a = list(x_with_y.loc[x_with_y.total == -1, 'customer_id'].head(2)) + list(x_with_y.loc[x_with_y.total != -1, 'customer_id'].head(2)) + list(x_with_y.loc[x_with_y.result == 1, 'customer_id'].head(2)) + \
    list(x_with_y.loc[x_with_y.result == 0, 'customer_id'].head(2)) + list(x_with_y.loc[x_with_y.phone_age == -1, 'customer_id'].head(5)) + list(x_with_y.loc[x_with_y.phone_age != -1, 'customer_id'].head(2))
a = list(set(a))
temp1 = x_with_y.loc[x_with_y.customer_id.isin(a)]
b = [str(i) for i in a]

temp2 = model_score.loc[model_score.customer_id.isin(b)].drop(['sample_set','loan_id','extend_times','applied_at','total','phone_age'],1)
temp2
temp2.customer_id = temp2.customer_id.astype(str)

temp1.customer_id = temp1.customer_id.astype(str)

temp = temp1.merge(temp2, on = 'customer_id')

temp.loan_id = temp.loan_id.astype(str)
temp.customer_id = temp.customer_id.astype(str)

temp2.to_excel(os.path.join(data_path, '测试数据样例2.xlsx'))

temp.to_excel(os.path.join(data_path, '测试数据样例.xlsx'))
temp = pd.read_excel(os.path.join(data_path, '测试数据样例.xlsx'))

temp = temp[['loan_id','customer_id','applied_at','mail']  + list(features26.varName)]


var_map_reverse = var_map.copy()

for i in var_map_reverse.keys():
    if i != 'provincecode':
        print (i)
        var_map_reverse[i] = {v:k for k,v in var_map_reverse[i].items()}
        print(var_map_reverse[i])

save_data_to_json(var_map_reverse, result_path, 'var_map_reverse.json')

del var_map_reverse['provincecode']

for i in temp.columns:
    if i in var_map_reverse.keys():
        print(var_map_reverse[i])
        temp[i] = temp[i].map(var_map_reverse[i])

###找对应的原始数据
all_x = load_data_from_pickle(data_path, 'all_x_20191203.pkl')
refer = load_data_from_pickle(data_path, 'refer_0501to1128.pkl')
temp_new = all_x.loc[all_x.customer_id.isin(a)][['customer_id','07d','14d','21d','30d','60d','90d','total']]


