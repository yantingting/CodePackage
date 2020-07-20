import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/newgenie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *
import utils3.data_processing as dp

data_path = 'D:/Model/indn/202001_mvp_model/01_data/'
result_path = 'D:/Model/202001_mvp_model/02_result/'

#数据清理

base_data.shape #143192
base_data2.shape #143192

prof_data.shape #143185 需要先填缺失再转dummy

bank_data.shape #143192

refer_data.shape #143192

device_data_f.shape #143192
gps_data_f.shape #143192


all_x_base.columns
prof_data.columns

#

#基本信息
all_x_base = base_data_f.merge(base_data2.drop(['apply_time','effective_date','customer_id','update_time','cell_phone','id_card_name','id_card_no', 'rn'],1), on = 'loan_id', how = 'left')\
                        .merge(bank_data_f.drop(['apply_time','effective_date','customer_id','update_time', 'rn'],1), on = 'loan_id', how = 'left')\
                        .merge(refer_data.drop(['customer_id'],1), on = 'loan_id', how = 'left')\
                        .merge(prof_data.drop(['apply_time','effective_date','customer_id'],1), on = 'loan_id', how = 'left')

all_x_base = all_x_base.fillna(-1)
all_x_base.isnull().sum()

#prof_data转dummy
prof_data.columns

all_x_base.index = all_x_base.loan_id

occupation_dummy = pd.get_dummies(all_x_base['occupation_type'])
occupation_dummy.columns = ['occupationtype_' + str(col) for col in occupation_dummy.columns]

job_dummy = pd.get_dummies(all_x_base['job'])
job_dummy.columns = ['job_' + str(col) for col in job_dummy.columns]

industry_dummy = pd.get_dummies(all_x_base['industry_involved'])
industry_dummy.columns = ['industry_' + str(col) for col in industry_dummy.columns]

preindustry_dummy = pd.get_dummies(all_x_base['pre_work_industry'])
preindustry_dummy.columns = ['preindustry_' + str(col) for col in preindustry_dummy.columns]

all_x_base_f = all_x_base.merge(occupation_dummy, left_index  = True, right_index = True).merge(job_dummy,left_index  = True, right_index = True)\
                .merge(industry_dummy, left_index  = True, right_index = True).merge(preindustry_dummy, left_index  = True, right_index = True)
all_x_base_f.columns
all_x_base_f.shape
save_data_to_pickle(all_x_base_f, data_path, 'all_x_base_f.pkl')

all_x_device = device_data_f.merge(gps_data_f, on = 'loan_id')
save_data_to_pickle(all_x_device, data_path, 'all_x_device_f.pkl')

#izi
all_izi = load_data_from_pickle(data_path, 'izi_var.pkl')
all_izi = all_izi.drop(['apply_time_x','md5_cellphone_x','id_card_no_x','md5name_x','name_x','ktp_x','phone_x','apply_time_y','md5_cellphone_y','id_card_no_y'
                        ,'md5name_y','name_y','ktp_y','phone_y','phoneage_x','phoneage_status_y','apply_time','身份证多头v1_status','身份证多头v1_result',
                        '身份证多头v4_status','身份证多头v4_result','号码多头v4_status','号码多头v4_result','号码多头v1_status','号码多头v1_result',
                        'phoneage_status_x', 'topup_status', 'phonescore_status', 'preference_status', 'iswhatsapp',
                        'whatsapp_updatestatus_time','whatsapp_signature', 'whatsapp_company_account', 'whatsapp_avatar'],1)

all_izi.rename(columns = {'phoneage_y': 'phoneage'},inplace= True)
all_izi.rename(columns = {'total_x': 'total', 'total_y':'total_v4'},inplace= True)
all_izi = all_izi.drop(['A','C','B','AA','BB','CC','A_daysdiff','B_daysdiff','C_daysdiff'],1)
izi_tel_dummy = pd.get_dummies(all_izi['telcom'])
izi_tel_dummy.columns = ['izi_telcom_'+i for i in izi_tel_dummy.columns]
all_izi = all_izi.join(izi_tel_dummy)
all_izi = all_izi.drop('telcom',1)
all_izi.phoneverify = all_izi.phoneverify.replace('MATCH', 1).replace('NOT_MATCH', 0)


#合并X和Y
flag_0119 = pd.read_csv('D:/Model/indn/202001_mvp_model/01_data/flag_0808to1130_0119.csv')
flag_0119.paid_off_time


# flag_0119.loc[(flag_0119.effective_date >= '2019-09-17') & (flag_0119.effective_date <= '2019-10-12')].shape #31253
# flag_0119.loc[(flag_0119.effective_date >= '2019-09-17') & (flag_0119.effective_date <= '2019-10-04')].shape #21615
# flag_0119.loc[(flag_0119.effective_date >= '2019-10-13') & (flag_0119.effective_date <= '2019-10-19')].shape #9049

#flag_0119.loc[(flag_0119.effective_date >= '2019-09-17') & (flag_0119.approved_period == 15)].effective_date.value_counts().sort_index()
#flag_0119.loc[(flag_0119.effective_date >= '2019-09-17') & (flag_0119.approved_period == 8)].effective_date.value_counts().sort_index()


flag_0119.loc[(flag_0119.effective_date < '2019-09-17'),'sample_set'] = 'app_before'
flag_0119.loc[(flag_0119.effective_date >= '2019-09-17') & (flag_0119.effective_date <= '2019-10-04'),'sample_set'] = 'train'
flag_0119.loc[(flag_0119.effective_date > '2019-10-04') & (flag_0119.effective_date <= '2019-10-12'),'sample_set'] = 'test'
flag_0119.loc[(flag_0119.effective_date > '2019-10-12') & (flag_0119.effective_date <= '2019-10-19'),'sample_set'] = 'oot'
flag_0119.loc[(flag_0119.effective_date > '2019-10-19'),'sample_set'] = 'only_8'

flag_0119.sample_set.value_counts()
#train         21615 53%
#test           9638
#oot            9049

flag_0119.loan_id = flag_0119.loan_id.astype(str)
flag_0119.customer_id = flag_0119.customer_id.astype(str)

flag_0119.to_excel('D:/Model/202001_mvp_model/01_data/flag_0808to1130_0119.xlsx')

all_x_base_f = all_x_base_f.reset_index(drop= True)
all_x_base_f = all_x_base_f.drop(['rn','mail_address1','mail_address2','mail_address3','mail_address'],1)

all_x_base_f.loan_id = all_x_base_f.loan_id.astype(str)
perf_data.loan_id = perf_data.loan_id.astype(str)
perf_paidhour_data.loan_id = perf_paidhour_data.loan_id.astype(str)
perf_latedays_data.loan_id = perf_latedays_data.loan_id.astype(str)

flag_0119 = flag_0119.loc[flag_0119.sample_set.isin(['train', 'test', 'oot'])] #40302

#izi
x_with_y = flag_0119.merge(all_x_base_f.drop(['apply_time','customer_id','update_time','cell_phone','mail','id_card_address'], 1), on = 'loan_id', how = 'left')\
    .merge(all_x_device.drop(['customer_id'],1), on = 'loan_id',how = 'left').merge(all_izi, left_on = 'loan_id',right_on = 'loanid', how = 'left')\
    .merge(perf_data, on = 'loan_id', how = 'left').merge(perf_paidhour_data, on = 'loan_id', how = 'left').merge(perf_latedays_data, on = 'loan_id', how = 'left')

x_with_y.rename(columns = {'avg_latedays':'avg_his_latedays','avg_latedays_180d':'avg_his_latedays_180d',
                           'avg_latedays_30d':'avg_his_latedays_30d','avg_latedays_360d':'avg_his_latedays_360d',
                           'avg_latedays_60d':'avg_his_latedays_60d', 'avg_latedays_90d':'avg_his_latedays_90d',
                           'cnt_paidoffhour_0to5_136d':'cnt_paidoffhour_0to5_360d'}, inplace= True)

pd.DataFrame(x_with_y.columns).to_excel(os.path.join(data_path, '建模代码可用变量字典.xlsx'))

var_dict = pd.read_excel('D:/Model/202001_mvp_model/建模代码可用变量字典.xlsx')
set(var_dict.指标英文) - set(x_with_y.columns)

set(x_with_y.columns) - set(var_dict.指标英文)

x_with_y = x_with_y.fillna(-1).replace(-9999, -1)

x_with_y.isnull().sum()

x_with_y,_ = mu.convert_right_data_type(x_with_y,var_dict)

save_data_to_pickle(x_with_y, data_path, 'x_with_y_v2.pkl') #更新历史行为变量

"""20200225更新refer_upate_minutes和monthlyincome"""
x_with_y = load_data_from_pickle(data_path,'x_with_y_v3.pkl') #修复了avg_extend_time变量
x_with_y = x_with_y.drop(['refer_update_minutes','monthly_salary'],1)

refer_data.loan_id = refer_data.loan_id.astype(str)
prof_data.loan_id = prof_data.loan_id.astype(str)
x_with_y = x_with_y.merge(refer_data[['loan_id','refer_update_minutes']], on = 'loan_id', how = 'left').merge(prof_data[['loan_id','monthly_salary']], on = 'loan_id', how = 'left')
x_with_y.avg_extend_times = x_with_y.avg_extend_times.astype(float)

x_with_y = x_with_y.fillna(-1)
save_data_to_pickle(x_with_y, data_path, 'x_with_y_v4.pkl')

x_with_y = load_data_from_pickle(data_path, 'x_with_y_v4.pkl')
perf_latedays_data.avg_his_latedays_180d = perf_latedays_data.avg_his_latedays_180d.astype(float)

x_with_y = x_with_y.drop(['avg_his_latedays_180d'],1)
x_with_y = x_with_y.merge(perf_latedays_data[['loanid','avg_his_latedays_180d']],left_on = 'loan_id', right_on = 'loanid', how = 'left')
x_with_y = x_with_y.fillna(-1)

x_with_y
save_data_to_pickle(x_with_y, data_path, 'x_with_y_v5.pkl')


"""收到产品期限影响，对相关变量进行归一化处理"""

x_with_y = load_data_from_pickle(data_path, 'x_with_y_v5.pkl')

scale_x_list = [
'cntAdvancePaidoff30dPayDay',
'cntExtendTimes90dPayDay',
#'avgExtendTimesPayDay', #无明显变化
#'cntLoan90PayDay', #无明显变化
'avgExtendTimes60dPayDay',  #下降
#'lastApplyCurdsPayDay', #无明显变化
'cntExtendTimes30dPayDay', #下降
#'avgExtendTimes90dPayDay', #无明显变化
'cntPaidoff30dPayDay', #下降
'cntLoan30PayDay' #下降
]

var_rename_dict = pd.read_excel('D:/Model/indn/202001_mvp_model/最终文档/需求文档/印尼UKU产品老客模型入模变量.xlsx', sheet_name = '01_变量汇总')
scale_x_list2 = var_rename_dict.loc[var_rename_dict.线上变量名.isin(scale_x_list),'模型变量名']

#根据训练样本进行Min_max normalization
scale_x = x_with_y.loc[x_with_y.sample_set == 'train'][scale_x_list2]

# min_max_scaler = preprocessing.MinMaxScaler()
# x_minmax = min_max_scaler.fit_transform(scale_x)

x_minmax2 = pd.DataFrame(x_minmax).rename(columns = dict(zip([0,1,2,3,4,5], scale_x.columns)))

scale_x_max = scale_x.max().reset_index().rename(columns = {'index':'var_name', 0:'max'})
scale_x_min = scale_x.min().reset_index().rename(columns = {'index':'var_name', 0:'min'})
scale_x_dict = scale_x_max.merge(scale_x_min, on = 'var_name')
scale_x_dict['max_min_diff'] = scale_x_dict['max'] - scale_x_dict['min']



for i in scale_x_list2:
    max = scale_x_dict.loc[scale_x_dict.var_name == i, 'max'].iloc[0]
    min = scale_x_dict.loc[scale_x_dict.var_name == i, 'min'].iloc[0]
    print(i)
    print(max)
    print(min)
    x_with_y[i] = x_with_y[i].apply(lambda x:MaxMinNormalization(x, max, min))

def MaxMinNormalization(x,Max,Min):
    x = (x - Min) / (Max - Min);
    return x
