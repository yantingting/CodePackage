import re
import math
import sys
import os
import pandas as pd
from jinja2 import Template
from imp import reload
from datetime import datetime, timedelta, date
import dateutil.relativedelta as dr

try:
    sys.path.remove('/home/ops/repos/genie/')
except:
    pass

sys.path.append('/home/ops/repos/analysis/cron/alert/')
sys.path.append('/home/ops/repos/newgenie/')

#from utils3.data_io_utils import *
#from utils3.misc_utils import convert_right_data_type
from utils3.send_email import mailtest
#import utils3.summary_statistics as ss

import execution.monitor_utils as mu


def get_current_data(in_dict):
    time_dict = mu.time_window(query_date = in_dict['query_date'], obs_window = in_dict['obs_window'], time_type= in_dict['time_type']
                            , per_window = in_dict['per_window'])
    print('start pulling data from',time_dict['ObStartDt'], 'to', time_dict['ObEndDt'])
    if isinstance(in_dict['product_name'], list):
        product_name = tuple(in_dict['product_name'])
    if isinstance(in_dict['product_name'], str):
        product_name = in_dict['product_name']
    SQL = Template(in_dict['sql']).render(start_date = time_dict['ObStartDt']
                                                               , end_date = time_dict['ObEndDt']
                                                               , businessid = product_name)
    data = mu.get_df_from_pg(SQL)
    if data.shape[0] == 0:
        print( '当前数据量为0, 执行SQL如下')
        print(SQL)
    else:
        data = data.fillna(-1).replace('', -1)
    return(data)


def get_hourly_newusemodelv4_var(data):
    raw_var = []
    for index, row in data.iterrows():
        var_list =  mu.var_process(bankCode=row['bankcode'], city=row['city'], education=row['education'],
                                  industryInvolved=row['industryinvolved'], job=row['job'],
                                  maritalStatus=row['maritalstatus'],
                                  occupationType=row['occupationtype'], province=row['province'], appNums=None)
        raw_var.append(var_list)
    raw_var_2 = pd.concat(raw_var)
    raw_var_2 = raw_var_2.reset_index(drop=True)
    data_2 = data.merge(raw_var_2, left_index=True, right_index=True, how='left')
    return(data_2)

def get_newusemodelv5_var(data):
    data = data.set_index('loanid')
    columnsSequence = [
        'gender','age','iziPhoneAgeAge','iziInquiriesByType07d','iziInquiriesByType14d',
        'iziInquiriesByType21d','iziInquiriesByType30d','iziInquiriesByType60d','iziInquiriesByType90d',
        'iziInquiriesByTypeTotal','iziPhoneVerifyResult','referSpouse','religionIslam','educationSeniorHighSchool',
        'educationRegularCollegeCourse','bankCodeBca','bankCodeBri','jobManajer','heightPixels1424widthPixels720',
        'brandSamsung','ModelSh04h','midFreqAppV5','rateHighFreqAppV5','rateMidFreqAppV5','rateLowFreqAppV5',
        'AdaKami','Agen Masukan Market','ConfigUpdater','DanaRupiah','Dokumen','Facebook',
        'Facebook Services','File','Finmas','Foto','Gmail','Google','Instagram',
        'KTA KILAT','Kalender','Key Chain','Kontak','Kredinesia','Kredit Pintar','KreditQ','Layanan Google Play',
        'Mesin Google Text-to-speech','Messenger','MyTelkomsel','Pemasang Sertifikat','Penyimpanan Kontak',
        'Rupiah Cepat','SHAREit','Setelan','Setelan Penyimpanan','SmartcardService','Tema','TunaiKita',
        'UKU','UangMe','Unduhan','WPS Office','com.android.carrierconfig','com.android.smspush']
    
    data.newusermodelscorev5inputmodelparams = data.newusermodelscorev5inputmodelparams.str.replace('[','')
    data.newusermodelscorev5inputmodelparams = data.newusermodelscorev5inputmodelparams.str.replace(']','')
    data_features = data.newusermodelscorev5inputmodelparams.str.split(',', expand=True)
    data_features.columns = columnsSequence
    
    data= data.drop(['newusermodelscorev5inputmodelparams'],1)
    data_2 = data.merge(data_features, right_index=True, left_index=True).reset_index()
    data_2.loanid = data_2.loanid.astype(str)
    data_2.gender = data_2.gender.astype(float)
    data_2.iziPhoneVerifyResult = data_2.iziPhoneVerifyResult.astype(float)
    data_2.gender = data_2.gender.astype(int)
    data_2.iziPhoneVerifyResult = data_2.iziPhoneVerifyResult.astype(int)
    return(data_2)

def json_to_var(data):
    # 解析sql从线上取的数
    data_json= mu.from_json(mu.from_json(data, 'datasources'), 'vars')
    data_json= data_json.drop(['dataSourceName'],1)
    # 变为一个变量一列
    data_prepared = data_json.set_index(['loanid','customerid','createtime','score','varName']).unstack('varName')
    data_prepared.columns=[col[1] for col in data_prepared.columns]
    data_prepared=data_prepared.reset_index()
    data_prepared = data_prepared.fillna(-1).replace('', -1)
    return (data_prepared)


def get_newusemodelv6_var(data):
    data = data.set_index('loanid')
    columnsSequence = ['jailbreakStatus', 'altitude', 'educationSeniorHighSchool',
                       'educationRegularCollegeCourse', 'iziTopup0to30Min', 'iziTopup30to60Min', 
                       'iziTopup360to720Min', 'iziB3d','iziB7d','iziB360d','iziC21d','iziC30d',
                       'iziC90d','iziC360d', 'rateHighFreqAppV6', 'rateMidFreqAppV6', 'AdaPundi', 'Gojek', 
                       'JULO', 'KreditPintar', 'LINE', 'Lite', 'MobileJKN', 'OVO', 'TIXID', 
                       'Traveloka', 'UangMe', 'advMultiscore', 'GDM1', 'GDM3', 'GDM8', 
                       'GDM41', 'GDM57', 'GDM59', 'GDM72', 'GDM77', 'GDM82', 
                       'GDM89', 'GDM93', 'GDM105', 'GDM106', 'GDM107', 'GDM140', 
                       'GDM150', 'GDM163', 'GDM164', 'GDM177', 'GDM178', 'GDM180', 
                       'GDM205', 'GDM210', 'GDM225', 'GDM227', 'GDM235', 'GDM237', 
                       'GDM238', 'GDM261', 'GDM303', 'GDM305', 'GDM328', 'GDM333', 
                       'GDM337', 'GDM348', 'GDM370', 'GDM371']
    
    data.newusermodelscorev6inputmodelparams = data.newusermodelscorev6inputmodelparams.str.replace('[','')
    data.newusermodelscorev6inputmodelparams = data.newusermodelscorev6inputmodelparams.str.replace(']','')
    data_features = data.newusermodelscorev6inputmodelparams.str.split(',', expand=True)
    data_features.columns = columnsSequence
    
    data= data.drop(['newusermodelscorev6inputmodelparams'],1)
    data_2 = data.merge(data_features, right_index=True, left_index=True).reset_index()
    data_2.loanid = data_2.loanid.astype(str)
    return(data_2)


def get_files(file_name, eda_name, var_rename = True):
    file = pd.read_excel(file_name,sheet_name = eda_name).rename(columns = {'线上变量名':'指标英文'})
    var_dict = file[['指标英文','指标中文','数据类型','数据源','指标类型']] #var_dict, 方便后续进行变量类型转换
    var_cols_offline = list(file['建模变量名'])
    var_importance = file[['指标英文','数据源','importance']].sort_values(by = 'importance', ascending = False)
    if var_rename:
        var_dict['指标英文'] = [i.lower() for i in var_dict.指标英文]
        var_cols_online = [i.lower() for i in list(file['指标英文'])]
        eda_default = file.drop(['建模变量名'], 1)
        eda_default.指标英文 = eda_default.指标英文.map(dict(zip(eda_default.指标英文, var_cols_online)))
        var_importance.指标英文 = var_importance.指标英文.map(dict(zip(eda_default.指标英文, var_cols_online)))
    else:
        #get_newusemodelv4_var里面的变量名是有大写的，故不需要进行变量名处理
        var_cols_online = [i for i in list(file['指标英文'])]
        eda_default = file.drop(['建模变量名'], 1)
        #eda_default.指标英文 = eda_default.指标英文.map(dict(zip(eda_default.指标英文, var_cols_online)))
        #eda_default.rename(columns = dict(zip(eda_default.指标英文, var_cols_online)), inplace= True)
    return (var_dict, eda_default, var_cols_offline, var_cols_online, var_importance)


def compare_eda(product, model_name, data, var_dict, old_eda, x_cols, report_path):
    save_label = 'product' + '_' +  model_name
    #缺失处理
    X = data[x_cols]
    X, _ = mu.convert_right_data_type(X, var_dict)
    print(_)
    mu.eda(X, var_dict, useless_vars=[], exempt_vars=[], data_path= report_path, save_label=save_label)
    eda_current = pd.read_excel(os.path.join(report_path, '%s_variables_summary.xlsx' %save_label))
    # EDA对比
    check_list = ['NA占比', '-1值占比', '0值占比']
    eda_current = eda_current.drop(['数据源', '指标中文', '指标类型', 'exclusion_reason', '-9999值数量', '-9999值占比', '-8888值数量', '-8888值占比', '-8887值数量','-8887值占比'], 1)
    old_eda = old_eda.drop(['指标类型', '-9999值数量', '-9999值占比', '-8888值数量', '-8888值占比', '-8887值数量', '-8887值占比'], 1)
    eda_compare = pd.merge(eda_current, old_eda, on='指标英文')
    for compare_obj in [i for i in check_list if '占比' in i]:
        eda_compare['compare_%s' % compare_obj] = (
                    eda_compare['%s_x' % compare_obj] - eda_compare['%s_y' % compare_obj]).abs()  # 当前和建模时候的差值
    eda_compare.loc[:, 'alert'] = (eda_compare.loc[:, [i for i in eda_compare.columns if u'compare_' in i]] > 0.05).sum(1) > 0
    eda_compare.loc[:, 'alert'].replace({False: '否', True: '是'}, inplace=True)
    eda_compare.columns = eda_compare.columns.str.replace('_x', '_current').str.replace('_y', '_default')
    default_curr_list = [f(i) for i in check_list for f in (lambda x: x + '_default', lambda x: x + '_current')]
    eda_compare = eda_compare[['alert', '数据源', '指标英文', '指标中文', 'N_current', 'N_default'] + ['compare_' + i for i in check_list] + default_curr_list]
    to_round_list = ['compare_' + i for i in check_list] + default_curr_list
    to_round_dict = dict(zip(to_round_list, [3] * len(to_round_list)))
    eda_compare = eda_compare.round(to_round_dict)
    return (eda_compare)


def compare_eda_V5(product, model_name, data, var_dict, old_eda, x_cols, report_path):
    save_label = 'product' + '_' +  model_name
    #缺失处理
    X = data[x_cols]
    X, _ = mu.convert_right_data_type(X, var_dict)
    print(_)
    mu.eda(X, var_dict, useless_vars=[], exempt_vars=[], data_path= report_path, save_label=save_label)
    eda_current = pd.read_excel(os.path.join(report_path, '%s_variables_summary.xlsx' %save_label))

    # EDA对比
    check_list = ['NA占比', '-1值占比', '0值占比', 'min', 'max']
    eda_current = eda_current.drop(['数据源', '指标中文', '指标类型', 'exclusion_reason', '-9999值数量', '-9999值占比', '-8888值数量', '-8888值占比', '-8887值数量','-8887值占比'], 1)
    old_eda = old_eda.drop(['指标类型', '-9999值数量', '-9999值占比', '-8888值数量', '-8888值占比', '-8887值数量', '-8887值占比'], 1)
    eda_compare = pd.merge(eda_current, old_eda, on='指标英文')
    for compare_obj in [i for i in check_list if '占比' in i]:
        eda_compare['compare_%s' % compare_obj] = (
                    eda_compare['%s_x' % compare_obj] - eda_compare['%s_y' % compare_obj]).abs()  # 当前和建模时候的差值
    eda_compare.loc[:, 'alert_pct'] = (eda_compare.loc[:, [i for i in eda_compare.columns if u'compare_' in i]] > 0.05).sum(1) > 0
    eda_compare.loc[:, 'alert_max'] = (eda_compare['max_x'] > eda_compare['max_y']) 
    eda_compare.loc[:, 'alert_min'] = (eda_compare['min_x'] < eda_compare['min_y']) 
    eda_compare.loc[eda_compare['alert_max']==True, 'alert']='是'
    eda_compare.loc[eda_compare['alert_min']==True, 'alert']='是'
    eda_compare.loc[eda_compare['alert_pct']==True, 'alert']='是'
    eda_compare.loc[:, 'alert_pct'].replace({False: '否', True: '是'}, inplace=True)
    eda_compare.loc[:, 'alert_max'].replace({False: '否', True: '是'}, inplace=True)
    eda_compare.loc[:, 'alert_min'].replace({False: '否', True: '是'}, inplace=True)
    eda_compare.alert = eda_compare.alert.fillna('否')
    eda_compare.columns = eda_compare.columns.str.replace('_x', '_current').str.replace('_y', '_default')
    default_curr_list = [f(i) for i in check_list for f in (lambda x: x + '_default', lambda x: x + '_current')]
    eda_compare = eda_compare[['alert','alert_max','alert_min','alert_pct', '数据源', '指标英文', '指标中文', 'N_current', 'N_default'] + ['compare_' + i for i in check_list if '占比' in i] + default_curr_list]
    to_round_list = ['compare_' + i for i in check_list if '占比' in i] + default_curr_list
    to_round_dict = dict(zip(to_round_list, [3] * len(to_round_list)))
    eda_compare = eda_compare.round(to_round_dict)
    return (eda_compare)

def compare_eda_V6(product, model_name, data, var_dict, old_eda, x_cols, report_path):
    # 不比较Importance为0的变量
    exclude_cols = ['jailbreakStatus','iziC21d', 'AdaPundi', 'Gojek', 'TIXID', 'Traveloka',
                                        'GDM1','GDM3','GDM8','GDM59','GDM77','GDM82','GDM93','GDM140','GDM150',
                                        'GDM163','GDM178','GDM180','GDM205','GDM225','GDM235','GDM238','GDM303',
                                        'GDM305','GDM328','GDM333','GDM370','GDM371']
    x_cols = [x for x in x_cols if x not in exclude_cols]
    save_label = 'product' + '_' +  model_name
    #缺失处理
    X = data[x_cols]
    X, _ = mu.convert_right_data_type(X, var_dict)
    print(_)
    mu.eda(X, var_dict, useless_vars=[], exempt_vars=[], data_path= report_path, save_label=save_label)
    eda_current = pd.read_excel(os.path.join(report_path, '%s_variables_summary.xlsx' %save_label))

    # EDA对比
    check_list = ['NA占比', '-1值占比', '0值占比', 'min', 'max']
    eda_current = eda_current.drop(['数据源', '指标中文', '指标类型', 'exclusion_reason', '-9999值数量', '-9999值占比', '-8888值数量', '-8888值占比', '-8887值数量','-8887值占比'], 1)
    old_eda = old_eda.drop(['指标类型', '-9999值数量', '-9999值占比', '-8888值数量', '-8888值占比', '-8887值数量', '-8887值占比'], 1)
    eda_compare = pd.merge(eda_current, old_eda, on='指标英文')
    for compare_obj in [i for i in check_list if '占比' in i]:
        eda_compare['compare_%s' % compare_obj] = (
                    eda_compare['%s_x' % compare_obj] - eda_compare['%s_y' % compare_obj]).abs()  # 当前和建模时候的差值
    eda_compare.loc[:, 'alert_pct'] = (eda_compare.loc[:, [i for i in eda_compare.columns if u'compare_' in i]] > 0.05).sum(1) > 0
    eda_compare.loc[:, 'alert_max'] = (eda_compare['max_x'] > eda_compare['max_y']) 
    eda_compare.loc[:, 'alert_min'] = (eda_compare['min_x'] < eda_compare['min_y']) 
    eda_compare.loc[eda_compare['alert_max']==True, 'alert']='是'
    eda_compare.loc[eda_compare['alert_min']==True, 'alert']='是'
    eda_compare.loc[eda_compare['alert_pct']==True, 'alert']='是'
    eda_compare.loc[:, 'alert_pct'].replace({False: '否', True: '是'}, inplace=True)
    eda_compare.loc[:, 'alert_max'].replace({False: '否', True: '是'}, inplace=True)
    eda_compare.loc[:, 'alert_min'].replace({False: '否', True: '是'}, inplace=True)
    eda_compare.alert = eda_compare.alert.fillna('否')
    eda_compare.columns = eda_compare.columns.str.replace('_x', '_current').str.replace('_y', '_default')
    default_curr_list = [f(i) for i in check_list for f in (lambda x: x + '_default', lambda x: x + '_current')]
    eda_compare = eda_compare[['alert','alert_max','alert_min','alert_pct', '数据源', '指标英文', '指标中文', 'N_current', 'N_default'] + ['compare_' + i for i in check_list if '占比' in i] + default_curr_list]
    to_round_list = ['compare_' + i for i in check_list if '占比' in i] + default_curr_list
    to_round_dict = dict(zip(to_round_list, [3] * len(to_round_list)))
    eda_compare = eda_compare.round(to_round_dict)
    return (eda_compare)

def alert_eda(in_dict):
    query_time = in_dict['query_date']
    eda_name = in_dict['eda_name']
    product_name = in_dict['product_name']
    model_name = in_dict['model_name']
    report_path = in_dict['report_path']
    var_data = get_current_data(in_dict)
    print(var_data.shape)

    if model_name == 'newusermodelv4':
        print('start get v4 eda data')
        var_data_2 = get_hourly_newusemodelv4_var(var_data)
        print('get v4 eda data done')
        var_dict, eda_default, var_cols_offline, var_cols_online, var_importance = get_files(in_dict['file_name'],in_dict['eda_name'],var_rename=False)
        eda_compare = compare_eda(product_name, model_name, var_data_2, var_dict, eda_default, var_cols_online, report_path)
    elif model_name == 'newusermodelv5':
        var_data = var_data[var_data.newusermodelscorev5inputmodelparams!=-1]
        var_data_2 = get_newusemodelv5_var(var_data)
        var_dict, eda_default, var_cols_offline, var_cols_online, var_importance = get_files(in_dict['file_name'],in_dict['eda_name'],var_rename=False)
        eda_compare = compare_eda_V5(product_name, model_name, var_data_2, var_dict, eda_default, var_cols_online, report_path)
    elif model_name == 'newusermodelv6':
        var_data_2 = get_newusemodelv6_var(var_data)
        var_dict, eda_default, var_cols_offline, var_cols_online, var_importance = get_files(in_dict['file_name'],in_dict['eda_name'],var_rename=False)
        eda_compare = compare_eda_V6(product_name, model_name, var_data_2, var_dict, eda_default, var_cols_online, report_path)
    else:
        var_data_2 = json_to_var(var_data)
        var_dict, eda_default, var_cols_offline, var_cols_online, var_importance = get_files(in_dict['file_name'],in_dict['eda_name'],var_rename=False)
        eda_compare = compare_eda(product_name, model_name, var_data_2, var_dict, eda_default, var_cols_online, report_path)
    #print(eda_compare['alert'].unique())\

    if '是' in eda_compare['alert'].unique():
        #print('是')
        save_file_name = os.path.join(report_path + "alert_missing_rate_%s_%s_%s.xlsx" % (in_dict['product_name'], in_dict['model_name'],
                    pd.to_datetime(in_dict['query_date']).strftime('%Y-%m-%d_%H-%M-%S')))
        print(save_file_name)
        #eda_compare.to_excel(save_file_name, encoding='utf-8')
        writer = pd.ExcelWriter(save_file_name)#, encoding = 'utf-8')
        eda_compare.to_excel(writer, 'eda_compare', index=True)
        if model_name == 'newusermodelv5':
            var_data_2.to_excel(writer, 'var_data', index=True)
        elif model_name  == 'newusermodelv4':
            drop_cols = list(var_dict.loc[var_dict.数据源== 'program','指标英文'])
            var_data_2 = var_data_2.drop(drop_cols, 1)
            var_data_2.to_excel(writer, 'var_data', index=True)
        elif model_name == 'newusermodelv6':
            var_data_2.to_excel(writer, 'var_data', index=True)
        else:
            var_data.to_excel(writer, 'var_data', index=True)
        writer.save()
        mailtest(["riskcontrol-business@huojintech.com"], in_dict['recipients'],"%s_%s_缺失值异常预警%s" %(in_dict['product_name'], in_dict['model_name'],
            pd.to_datetime(in_dict['query_date']).strftime('%Y-%m-%d_%H-%M-%S')), "请参考附件中的Excel", save_file_name)

def main(query_date, in_dict):
    in_dict['query_date'] = query_date
    if alert_eda(in_dict) == False:
        return u'当前无缺失'
    else:
        return u'请检查邮件'

if __name__=='__main__':
    query_time =(datetime.now()-dr.relativedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
    print(query_time)
    main(query_time, in_dict = {'product_name' :'uku',
                    'model_name' : 'newusermodelv4',
                    'obs_window': 24 ,
                    'time_type': 'hour',
                    'sql': VAR_SQL,
                    'query_date': query_time,
                    'file_name': 'D:/project/monitoring/uku_new_model_v4/UKU_NewUserModelV4.xlsx',
                    'eda_name': 'EDA',
                    'recipients': 'fangmingqing@mintechai.com'
                })


