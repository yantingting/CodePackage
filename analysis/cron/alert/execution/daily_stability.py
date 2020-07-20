import numpy as np
import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
from matplotlib import pyplot as plt
import re
import math
import sys
from imp import reload
from datetime import datetime, timedelta, date
import os
import dateutil.relativedelta as dr
from itertools import chain
import xlsxwriter

try:
    sys.path.remove('/home/ops/repos/genie/')
except:
    pass

sys.path.append('/home/ops/repos/newgenie/')
sys.path.append('/home/ops/repos/analysis/cron/alert/')
import execution.monitor_utils  as mu
from execution.monitoring_config import *
#from utils3.data_io_utils import *
from utils3.send_email import mailtest

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

def get_newusemodelv5_var(data):
    data.loanid = data.loanid.astype(str)
    data = data.set_index('loanid')
    columnsSequence = [
        'gender', 'age', 'iziPhoneAgeAge', 'iziInquiriesByType07d', 'iziInquiriesByType14d',
        'iziInquiriesByType21d', 'iziInquiriesByType30d', 'iziInquiriesByType60d', 'iziInquiriesByType90d',
        'iziInquiriesByTypeTotal', 'iziPhoneVerifyResult', 'referSpouse', 'religionIslam', 'educationSeniorHighSchool',
        'educationRegularCollegeCourse', 'bankCodeBca', 'bankCodeBri', 'jobManajer', 'heightPixels1424widthPixels720',
        'brandSamsung', 'ModelSh04h', 'midFreqAppV5', 'rateHighFreqAppV5', 'rateMidFreqAppV5', 'rateLowFreqAppV5',
        'AdaKami', 'Agen Masukan Market', 'ConfigUpdater', 'DanaRupiah', 'Dokumen', 'Facebook',
        'Facebook Services', 'File', 'Finmas', 'Foto', 'Gmail', 'Google', 'Instagram',
        'KTA KILAT', 'Kalender', 'Key Chain', 'Kontak', 'Kredinesia', 'Kredit Pintar', 'KreditQ', 'Layanan Google Play',
        'Mesin Google Text-to-speech', 'Messenger', 'MyTelkomsel', 'Pemasang Sertifikat', 'Penyimpanan Kontak',
        'Rupiah Cepat', 'SHAREit', 'Setelan', 'Setelan Penyimpanan', 'SmartcardService', 'Tema', 'TunaiKita',
        'UKU', 'UangMe', 'Unduhan', 'WPS Office', 'com.android.carrierconfig', 'com.android.smspush']

    data.newusermodelscorev5inputmodelparams = data.newusermodelscorev5inputmodelparams.str.replace('[', '')
    data.newusermodelscorev5inputmodelparams = data.newusermodelscorev5inputmodelparams.str.replace(']', '')
    data_features = data.newusermodelscorev5inputmodelparams.str.split(',', expand=True)
    data_features.columns = columnsSequence

    data = data.drop(['newusermodelscorev5inputmodelparams'], 1)
    data_2 = data.merge(data_features, right_index=True, left_index=True).reset_index()
    data_2.loanid = data_2.loanid.astype(str)
    data_2.gender = data_2.gender.astype(float)
    data_2.iziPhoneVerifyResult = data_2.iziPhoneVerifyResult.astype(float)
    data_2.gender = data_2.gender.astype(int)
    data_2.iziPhoneVerifyResult = data_2.iziPhoneVerifyResult.astype(int)
    return (data_2)

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
    """
    从config文件中的Excel中eda页中,生成var_dict, eda_default(生成日报图片中的default mean, std的来源)， var_cols_offline, var_cols_online, var_importance
    """
    file = pd.read_excel(file_name,sheet_name = eda_name).rename(columns = {'线上变量名':'指标英文'})
    var_dict = file[['指标英文','指标中文','数据类型','数据源','指标类型']] #var_dict, 方便后续进行变量类型转换
    var_cols_offline = list(file['建模变量名'])
    var_importance = file[['指标英文', '数据源', 'importance']].sort_values(by = 'importance', ascending= False)
    if var_rename:
        #需要将线上变量名转成线下的变量名
        var_dict['指标英文'] = [i.lower() for i in var_dict.指标英文]
        var_cols_online = [i.lower() for i in list(file['指标英文'])]
        eda_default = file.drop(['建模变量名'], 1)
        eda_default['指标英文'] = eda_default['指标英文'].map(dict(zip(eda_default.指标英文, var_cols_online)))
        var_importance['指标英文'] = var_importance['指标英文'].map(dict(zip(var_importance.指标英文, var_cols_online)))
        #var_importance = var_importance.sort_values(by='importance', ascending=False)
    else:
        #get_newusemodelv4_var里面的变量名是有大写的，故不需要进行变量名处理
        var_cols_online = [i for i in list(file['指标英文'])]
        eda_default = file.drop(['建模变量名'], 1)
        #eda_default['指标英文'] = eda_default.指标英文.map(dict(zip(eda_default.指标英文, var_cols_online)))
        #var_importance['指标英文'] = var_importance['指标英文'].map(dict(zip(var_importance.指标英文, var_cols_online)))
        #eda_default.rename(columns = dict(zip(eda_default.指标英文, var_cols_online)), inplace= True)
    return (var_dict, eda_default, var_cols_offline, var_cols_online, var_importance)

def data_prepare(data, unique_cols, var_dict, time_cols):
    """
    根据var_dict,分连续型和分类型变量分别计算监测指标
    numerical: 统计申请量,mean,std
    categorical:统计申请量,各类别占比
    """
    data = data.drop_duplicates()
    if data[unique_cols].nunique() != data.shape[0]:
        print ('Warning: The %s is not unique'%unique_cols)
        print ('examples:', ((data[unique_cols].value_counts() > 1) == True).index[0:10])
        data = data.drop_duplicates(unique_cols)
    data, _ = mu.convert_right_data_type(data, var_dict)
    data[time_cols] = data[time_cols].apply(lambda x:str(x)[0:10])
    x_cols = list(set(var_dict.指标英文).intersection(data.columns))
    x_numerical = list(set(var_dict.loc[var_dict.数据类型 != 'varchar','指标英文']).intersection(data.columns))
    x_categorical = list(set(var_dict.loc[var_dict.数据类型 == 'varchar','指标英文']).intersection(data.columns))
    dict_out = {}
    dict_out['x_numerical'] = {}
    dict_out['x_categorical'] = {}
    for i in x_cols:
        #print(i)
        var_kept = [i, time_cols]
        temp_missing = data[var_kept].groupby(time_cols).agg(lambda x: ((x == -1)|(x=='-1')).sum()/x.count()).rename(columns={i: '%s_NA' % i})
        temp_cnt = data[time_cols].value_counts().to_frame()
        if i in x_numerical:
            # temp_mean = data[var_kept].replace(-1, np.nan).groupby(time_cols).mean().rename(
            #     columns={i: '%s_mean' % i})
            temp_mean = data[var_kept].replace(-1, np.nan).groupby(time_cols)[i].agg({'mean':np.mean, 'std': np.std}).rename(columns={'mean': '%s_mean'%i,'std':'%s_std'%i})
            temp = temp_cnt.merge(temp_missing, left_index=True, right_index=True, how='left') \
                .merge(temp_mean, left_index=True, right_index=True, how='left').reset_index() \
                .rename(columns={'index': time_cols, time_cols: '%s_cnt' % i})
            dict_out['x_numerical'][i] = temp
        else:
            temp_category = data.loc[data[i] != '-1'].groupby([time_cols, i])[i].count().unstack()
            temp_category = temp_category.rename(columns=dict(zip(temp_category.columns, ['%s' %i + '_' + j for j in temp_category.columns])))
            temp = temp_cnt.merge(temp_missing, left_index=True, right_index=True, how='left') \
                           .merge(temp_category, left_index=True, right_index=True, how='left').reset_index() \
                           .rename(columns={'index': time_cols, time_cols: '%s_cnt' % i})
            temp.iloc[:, 3:] = temp.iloc[:, 3:].apply(lambda x: x / temp.iloc[:, 1])
            dict_out['x_categorical'][i] = temp
    return(dict_out)

def numerical_var_chart(data,time_cols, eda_default, report_path):
    FIG_PATH = os.path.join(report_path, 'figure')
    pic_dict = {}
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)
    for i in data.keys():
        temp = data[i].sort_index(by = time_cols, ascending= True).reset_index(drop=True)
        # 取做图数据
        x = temp.index #日期
        y1 = temp['%s_mean'%i] #平均值
        y2 = temp['%s_cnt' %i] #每日样本量
        y3 = temp['%s_NA' %i] #每日缺失率
        lower_bound = eda_default.loc[eda_default.指标英文 == i]['mean'] + eda_default.loc[eda_default.指标英文 == i]['std']
        upper_bound = eda_default.loc[eda_default.指标英文 == i]['mean'] - eda_default.loc[eda_default.指标英文 == i]['std']
        default_mean = eda_default.loc[eda_default.指标英文 == i]['mean']

        #y4 = temp['%s_mean'%i] +  temp['%s_std'%i]
        #y5 = temp['%s_mean'%i] -  temp['%s_std'%i]
        #
        # 设置图形大小
        fig = plt.figure(figsize=(8, 6))
        fig.subplots_adjust(right=0.75)

        ax1 = fig.add_subplot(1, 1, 1)
        # 画柱子
        cnt = ax1.bar(x, y2, alpha=.7, color='steelblue', width=0.3, label = 'application count')
        ax1.set_ylabel('application_count', fontsize='10', color = 'steelblue')
        try:
            ax1.set_ylim([0, y2.max() * 1.5])
        except:
            pass
        plt.xticks(x, temp[time_cols], rotation=90, fontsize = 8)
        # 申请量数值标签
        if i=='score':
            for a, b in zip(x, y2):
                plt.text(a, b+0.05, '%.0f' % b, ha='center', va='bottom', fontsize=9, color='steelblue')
        else:
            pass

        ax2 = ax1.twinx()
        mean, = ax2.plot(x, y1, color = 'black', marker='.', ms=10, label = 'mean')
        #upper_std, = ax2.plot(x, lower_bound, color = 'red', marker='.', ms=10, label = 'stand deviation')
        #lower_std, = ax2.plot(x, upper_bound, color = 'red', marker='.', ms=10, label = 'stand deviation')
        #
        upper_std = ax2.axhline(y = upper_bound.iloc[0], c="red", ls="--", lw=2, label = 'upper std')
        default_mean = ax2.axhline(y = default_mean.iloc[0], c="red", ls="--", lw=2, label = 'default_mean')
        lower_std = ax2.axhline(y = lower_bound.iloc[0], c="red", ls="--", lw=2, label = 'lower std')

        ax2.set_ylabel('%s_mean' %i, fontsize='10', color = 'black') #rotation
        # try:
        #     ax1.set_ylim([y1.min() * 0.8, y1.max() * 1.1 ])
        # except:
        #     pass
        plt.xticks(fontsize= 7, rotation=20)
        plt.yticks(fontsize=10)
        plt.xlabel("apply_time")
        # 模型分数值标签
        if i=='score':
            for a, b in zip(x, y1):
                plt.text(a, b+0.05, '%.0f' % b, ha='center', va='bottom', fontsize=9)
        else:
            pass


        #缺失
        ax3 = ax1.twinx()
        ax3.spines["right"].set_position(("axes", 1.2))
        mu.make_patch_spines_invisible(ax3)
        ax3.spines["right"].set_visible(True)
        missing, = ax3.plot(x, y3, alpha=.7, color='blue', label = 'missing rate')
        ax3.set_ylabel('missing rate', fontsize='10', color = 'blue')
        ax3.set_ylim([-0.1, 1])
        #
        tkw = dict( top = True, size=4, width=1.5)
        ax1.tick_params(axis='y', colors= 'steelblue', **tkw)
        ax2.tick_params(axis='y', colors=mean.get_color(), **tkw)
        ax3.tick_params(axis='y', colors=missing.get_color(), **tkw)
        #
        lines = [mean, missing, upper_std, lower_std, default_mean]
        ax1.legend(['application count'], loc = 'upper left')
        ax2.legend(lines, [l.get_label() for l in lines], loc = 'upper right')
        #plt.show()
        plt.title("%s" %i)
        path = os.path.join(FIG_PATH, "numerical" + "_" + i + ".png")
        plt.savefig(path, format='png', dpi=100)
        plt.close()
        names = i
        pic_dict[names] = "numerical" + "_" + i + ".png"
    return(pic_dict)

def categorical_var_chart(data,time_cols, eda_default, report_path):
    pic_dict = {}
    FIG_PATH = os.path.join(report_path, 'figure')
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)
    for i in data.keys():
        if i == 'provinceCode':
            pass
        else:
            data[i] = data[i].sort_values(time_cols,  ascending= True).reset_index(drop=True)
            x = data[i].index
            y_cnt = data[i]['%s_cnt'%i]
            #lower_bound = eda_default.loc[eda_default.指标英文 == i]['mean'] + eda_default.loc[eda_default.指标英文 == i]['std']
            #upper_bound = eda_default.loc[eda_default.指标英文 == i]['mean'] - eda_default.loc[eda_default.指标英文 == i]['std']
            #default_mean = eda_default.loc[eda_default.指标英文 == i]['mean']

            #
            fig = plt.figure(figsize=(8, 6))
            fig.subplots_adjust(right=0.75)
            ax1 = fig.add_subplot(1, 1, 1)
            #每日申请量
            ax1.bar(x, y_cnt, alpha=.7, color='steelblue', width=0.3, label = 'application count')
            ax1.set_ylabel('application_count', fontsize='10', color = 'steelblue')
            try:
                ax1.set_ylim([0, y_cnt.max() * 1.5])
            except:
                pass
            plt.xticks(x, data[i][time_cols], rotation=90,fontsize = 8)
            plt.xlabel("apply_time")
            #
            ax2 = ax1.twinx()
            #每个类型占比
            for (k,v) in data[i].items():
                if k not in ([time_cols,'%s_cnt'%i]):#[time_cols,'%s_cnt'%i, '%s_na'%i]
                    y = v.copy()
                    k = k.replace('NA', 'missing rate')
                    ax2.plot(x, y, marker='.', ms=10, label = k)
                    ax2.set_ylabel('%s_distribution' %i, fontsize='10', color = 'black')

            #upper_std = ax2.axhline(y=upper_bound.iloc[0], c="red", ls="--", lw=2, label='upper std')
            #default_mean = ax2.axhline(y=default_mean.iloc[0], c="red", ls="--", lw=2, label='default_mean')
            #lower_std = ax2.axhline(y=lower_bound.iloc[0], c="red", ls="--", lw=2, label='lower std')

            tkw = dict( top = True, size=4, width=1.5)
            ax1.tick_params(axis='y', colors= 'steelblue', **tkw)

            ax1.legend(['application count'], loc = 'upper left')
            plt.legend(loc = 'upper right',framealpha = 0.5 )

            plt.title("%s" %i)
            i = i.replace('.','_')
            path = os.path.join(FIG_PATH, "categorical" + "_" + i + ".png")
            plt.savefig(path, format='png', dpi=100)
            plt.close()
            names = i
            pic_dict[names] = "categorical" + "_" + i + ".png"
    return(pic_dict)

def get_var_bin(df, var_dict, var_dist):
    """
    根据excel中上传的变量的boundaries对原始数据进行分箱
    rebin_spec: 返回所有变量的boundaries
    model_dist_RiskBand: 返回建模的varbin和分布
    df: 新增分好箱的变量，变量名后缀为_bin
    """
    df,_ = mu.convert_right_data_type(df, var_dict)
    print(_)
    rebin_spec = {}
    for var_name in df.columns:
        if var_name == var_dict.loc[var_dict.指标中文 == '模型分数']['指标英文'].iloc[0]:
        #if var_name in list(var_dict['指标英文']):
            x_dist = var_dist.loc[var_dist['变量名'] == var_name]
            #print(x_dist)
            unique_bin_values = x_dist['分箱'].astype('str').unique()
            boundaries = [i.replace('(', '').replace(']', '').replace('[', '').replace(')', '').split(', ') for i in unique_bin_values if ',' in i]
            boundaries = [float(i) for i in list(chain.from_iterable(boundaries)) if i not in ['nan', 'missing']]
            boundaries = sorted(set(boundaries))
            temp = pd.DataFrame({'upper':boundaries})
            temp['lower']=temp['upper'].shift(1)
            temp['tag']=temp.apply(lambda row: "(" + str(row['lower']) + "," + str(row['upper']) + "]",axis=1)
            temp = temp.reset_index()
            temp['new_tag'] = temp.apply(lambda row:str(row['index']).zfill(2) +") " + row['tag'], axis=1)
            taglist = temp.loc[1:,'new_tag'].tolist()
            df['%s_bin' %var_name] = pd.cut(df[var_name], bins = boundaries, labels = taglist)
            rebin_spec[var_name] = [boundaries, taglist]
            model_dist_temp = pd.DataFrame(columns = ['var_name','var_bin','default_cnt','default_dist'])
            model_dist_temp['var_bin'] = taglist
            model_dist_temp['default_cnt'] = x_dist['样本数']
            model_dist_temp['default_dist'] = x_dist['样本数'] / x_dist['样本数'].sum()
            model_dist_temp['var_name'] = '%s_bin' %var_name
            #model_dist_RiskBand['default_event_rate'] = in_dt[[u'逾期率']]
            # model_dist_RiskBand['backscore_dist'] = in_dt[['backscore_dist']]
    return (model_dist_temp,df)

def calculate_var_psi(time_cols, df, model_dist_RiskBand ):
    """
    按时间计算变量的分布，并和建模分布进行对比计算PSI
    返回数据格式:
    var_name                        var_bin  ...  dist_2019-12-23  dist_2019-12-24  psi_
     newusermodelv4score_bin   01) (-inf,0.114]  ...         0.037736         0.035201
    """
    #var_data_bin[time_cols] = var_data_bin[time_cols].apply(lambda x: str(x)[0:10])
    cnt_list = []
    dist_list = []
    #df[time_cols] = df[time_cols].apply(lambda x:str(x)[0:10])
    for cols in df.columns:
        if '_bin' in cols:
            var_cnt_bytime_tmp = df.groupby([time_cols,cols]).agg({time_cols:len}).rename(columns = {time_cols:'cnt'}).unstack(level = 0)
            var_cnt_bytime_tmp.columns = var_cnt_bytime_tmp.columns.map('_'.join)
            var_cnt_bytime_tmp = var_cnt_bytime_tmp.reset_index()
            var_cnt_bytime_tmp.rename(columns = {cols:'var_bin'}, inplace= True)
            var_cnt_bytime_tmp['var_name'] = cols

            var_dist_bytime_tmp = (df.groupby([time_cols,cols]).agg({time_cols:len})/df.groupby([time_cols])[time_cols].agg({time_cols:len}))\
                    .rename(columns = {time_cols:'dist'}).unstack(level = 0)
            var_dist_bytime_tmp.columns = var_dist_bytime_tmp.columns.map('_'.join)
            var_dist_bytime_tmp = var_dist_bytime_tmp.reset_index()
            var_dist_bytime_tmp.rename(columns = {cols:'var_bin'}, inplace= True)
            var_dist_bytime_tmp['var_name'] = cols

            cnt_list.append(var_cnt_bytime_tmp)
            dist_list.append(var_dist_bytime_tmp)
    try:
        var_cnt_bytime = pd.concat(cnt_list)
        var_dist_bytime = pd.concat(dist_list)
    except:
        var_cnt_bytime = pd.DataFrame(cnt_list)
        var_dist_bytime = pd.DataFrame(dist_list)
    #print(var_cnt_bytime, var_dist_bytime)
    var_data_bytime = model_dist_RiskBand.merge(var_cnt_bytime, on = ['var_name','var_bin']).merge(var_dist_bytime, on = ['var_name','var_bin'])
    for i in var_data_bytime.columns:
          if 'dist_' in i:
              cols_name = i.replace('dist', 'psi')
              psi_index = (np.log(var_data_bytime[i]/ var_data_bytime['default_dist']) * (var_data_bytime[i] - var_data_bytime['default_dist'])).sum()
              var_data_bytime[cols_name] = psi_index
    return(var_data_bytime)

def var_dist_psi_plot(df, report_path):
    pic_dict ={}
    x_data = [i.replace('psi_', '') for i in df.columns if 'psi_' in i]
    x_data.insert(0, 'modeling')
    y_psi = df.loc[0, [i for i in df.columns if 'psi_' in i]] #
    save_label = df.var_name.iloc[0].replace('_bin', '')
    colors = ['khaki','steelblue','indianred','sandybrown','darkgrey']
    color_index = 0
    index = 0
    FIG_PATH = os.path.join(report_path, 'figure')
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)
    bar_width = 0.3
    space_scale = len(x_data) + 5 #space_scale增加可以提高每组的间距宽度
    fig = plt.figure(figsize=(space_scale * 1 + 10, 10)) #每多一个类别，图的长宽比调大
    ax1 = fig.add_subplot(1, 1, 1)
    ax2 = ax1.twinx()
    #根据bin结果画分布图
    for bin in df.var_bin.unique():
        y_data = df[[i for i in df.columns if 'dist' in i]].iloc[index]
        index  += 1
        ax1.bar(left = np.arange(len(x_data)) * (1 + bar_width*space_scale ) + color_index*bar_width, height=y_data, label= bin,
                align='edge',color=colors[color_index % len(colors)], alpha=0.8,width=bar_width)
        color_index += 1
    ax1.legend(loc = 'upper left', fontsize = '15')
    ax1.set_ylabel('distribution', fontsize='15')
    ax1.set_ylim([0, 0.3])

    #增加psi的折线图
    ax2.plot(np.delete(np.arange(len(x_data)) * (1 + bar_width*space_scale ) + 5 *bar_width, 0), y_psi,color='blue', label = 'psi', marker='o', linestyle='-')
    ax2.set_ylabel('PSI', fontsize='15', color = 'blue')
    ax2.set_ylim([0, y_psi.max() * 2])
    ax2.legend(loc = 'upper right', fontsize = '20')

    tkw = dict(size=4, width=1.5)
    ax2.tick_params(axis='y', colors='blue', **tkw)

    plt.xticks(np.arange(len(x_data)) * (1 + bar_width*space_scale) + color_index * bar_width / 2, x_data, rotation=20)
    plt.title("%s distribution and  psi" %save_label, fontsize = '15')
    ax1.set_xlabel("apply_time", fontsize = '15')
    # psi数字标签
    for a, b in zip(np.delete(np.arange(len(x_data)) * (1 + bar_width*space_scale ) + 5 *bar_width, 0), y_psi):
        plt.text(a, b+0.01, '%.2f' % b, ha='center', va='bottom', fontsize=10, color='blue')


    #plt.legend()
    path = os.path.join(FIG_PATH, "weekly_%s_distributionn_psi"%save_label  + ".png")
    plt.savefig(path, format='png', dpi=80)
    plt.close()
    names = save_label
    pic_dict[names] = 'weekly_%s_distributionn_psi' %save_label +'.png'
    return pic_dict

def daily_stability(report_type_dict, in_dict, daily_report = True, topn_cutoff = 0.1, weekly_report = False, send_report = True):
    query_time = report_type_dict['query_date']
    eda_name = in_dict['eda_name']
    product_name = in_dict['product_name']
    model_name = in_dict['model_name']
    report_path = in_dict['report_path']
    time_dict = mu.time_window(query_date=report_type_dict['query_date'], obs_window=report_type_dict['obs_window']
                               , time_type=report_type_dict['time_type'], per_window=report_type_dict['per_window'])
    var_data= mu.get_current_data(product_name,report_type_dict)
    if model_name == 'newusermodelv4':
        var_dict, eda_default, var_cols_offline, var_cols_online, var_importance = get_files(in_dict['file_name'],in_dict['eda_name'], var_rename=False)
        model_score_name = var_dict.loc[var_dict.指标中文 == '模型分数']['指标英文'].iloc[0]
        var_data_2 = get_newusemodelv4_var(var_data)
        var_data = var_data_2.copy()
    elif model_name == 'newusermodelv5':
        var_dict, eda_default, var_cols_offline, var_cols_online, var_importance = get_files(in_dict['file_name'],in_dict['eda_name'], var_rename=False)
        model_score_name = var_dict.loc[var_dict.指标中文 == '模型分数']['指标英文'].iloc[0]
        var_data = var_data[var_data.newusermodelscorev5inputmodelparams!=-1]
        var_data_2 = get_newusemodelv5_var(var_data)
        var_data = var_data_2.copy()
    elif model_name == 'newusermodelv6':
        var_dict, eda_default, var_cols_offline, var_cols_online, var_importance = get_files(in_dict['file_name'],in_dict['eda_name'], var_rename=False)
        model_score_name = var_dict.loc[var_dict.指标中文 == '模型分数']['指标英文'].iloc[0]
        var_data_2 = get_newusemodelv6_var(var_data)
        var_data_2.rename(columns = {'newusermodelscorev6':'score'}, inplace=True)
        var_data = var_data_2.copy()
    else:
        var_data = json_to_var(var_data)
        var_dict, eda_default, var_cols_offline, var_cols_online, var_importance = get_files(in_dict['file_name'], in_dict['eda_name'],var_rename=False)
        model_score_name = var_dict.loc[var_dict.指标中文 == '模型分数']['指标英文'].iloc[0]
    var_data, _ = mu.convert_right_data_type(var_data, var_dict)

    if daily_report:
        topn = round(len(var_importance) * topn_cutoff)
        topn_var = list(var_importance.iloc[:topn]['指标英文'])
        topn_var.insert(0, model_score_name)
        #topn_var.append(model_score_name)
        remove_var = list(var_importance.iloc[topn:]['指标英文'])
        try:
            remove_var.remove(model_score_name)
        except:
            pass
        var_data = var_data.drop(remove_var,1)

    if weekly_report:
        var_data = mu.calculate_week(var_data, 'createtime', n_week = report_type_dict['obs_window']).drop('createtime',1)
        var_data.rename(columns = {'week_time':'createtime'}, inplace=True)
        model_dist = pd.read_excel(in_dict['file_name'], sheet_name = in_dict['dist_name'])
        model_dist_RiskBand, var_data_bin = get_var_bin(var_data, var_dict, model_dist)
        var_data_bytime = calculate_var_psi('createtime', var_data_bin, model_dist_RiskBand)
        weekly_pic_dict = var_dist_psi_plot(var_data_bytime, report_path = in_dict['report_path'])
        #weekly_pic_dict[model_score_name + '_picture']
        print(weekly_pic_dict)

    data_out = data_prepare(var_data, 'loanid', var_dict, 'createtime')
    if len(data_out['x_numerical']) > 0:
        pic_dict_num = numerical_var_chart(data_out['x_numerical'], 'createtime', eda_default, report_path)
    else:
        pic_dict_num = {}
        pass
    if len(data_out['x_categorical']) > 0:
        pic_dict_cat = categorical_var_chart(data_out['x_categorical'], 'createtime', eda_default, report_path)
    else:
        pic_dict_cat = {}
        pass

    #图片路径
    pic_dict = {**pic_dict_num, **pic_dict_cat}
    score_mean_pic = pic_dict_num[model_score_name]
    #对pic_dict按照importance进行排序
    ordered_pic_dict = pd.DataFrame.from_dict(pic_dict, orient='index').reset_index().rename(
        columns={'index': '指标英文', 0: 'pic_name'}).merge(var_importance, on='指标英文')
    ordered_pic_dict.loc[ordered_pic_dict['指标英文'] == 'score', 'importance'] = 1
    ordered_pic_dict = ordered_pic_dict.sort_values(by='importance', ascending=False)
    pic_dict = dict(zip(ordered_pic_dict['指标英文'], ordered_pic_dict['pic_name']))

    #输出结果
    os.chdir(os.path.join(report_path,'figure' ))
    save_file_name = report_path + '%s %s monitoring %s.xlsx' %(product_name, model_name, query_time)
    workbook = xlsxwriter.Workbook(save_file_name)
    worksheet = workbook.add_worksheet('日监控')
    format = workbook.add_format({'bold': True})    #定义一个加粗的格式对象
    i = 0
    height = 700
    for name, data in pic_dict.items():
        worksheet.write('A%s'%(i * 35), name, format)
        worksheet.insert_image('B1', data,  options = {'y_offset': 0 + i * height, 'x_scale':0.8, 'y_scale':1})
        i = i + 1
    if weekly_report:
        worksheet2 = workbook.add_worksheet('周监控')
        worksheet2.insert_image('B1',weekly_pic_dict[model_score_name] , options={'y_offset': 0, 'x_scale': 0.8, 'y_scale': 0.8})
    workbook.close()

    if send_report:
        mailtest(#["riskcontrol@huojintech.com"]
                 ['riskcontrol-business@huojintech.com']
                 , report_type_dict['recipients']
                 , "%s日报 %s" %(model_name, query_time)
                 , "【模型监控日报】\
                    \n 产品: %s\
                    \n 模型名: %s\
                    \n 时间段: 观测时点从%s 到%s" %(product_name, model_name, time_dict['ObStartDt'], time_dict['ObEndDt'])
                 ,save_file_name, fig = score_mean_pic)
    if daily_report:
        pass

    else:
        #print(pic_dict)
        #print(weekly_pic_dict)
        return(pic_dict, weekly_pic_dict)

def main(query_time, in_dict, daily_report = True):
    if daily_report:
        report_type_dict = in_dict['daily_param']
        report_type_dict['query_date'] = query_time
        daily_stability(report_type_dict, in_dict, daily_report=True, topn_cutoff= 1, weekly_report=False, send_report=True)
    else:
        report_type_dict = in_dict['weekly_param']
        report_type_dict['query_date'] = query_time
        pic_dict, psi_dict = daily_stability(report_type_dict, in_dict,daily_report = False, topn_cutoff = 0.1, weekly_report = True, send_report = False)
        return(pic_dict, psi_dict)

if  __name__ =="__main__":
    query_time =(datetime.now()).strftime('%Y-%m-%d')
    print (query_time)
    main(query_time, in_dict = {'product_name': 'uku',
                    'model_name': 'iosnewusermodelv1',
                    'obs_window': 4,
                    'time_type': 'week',
                    'per_window': 0,
                    'sql': uku_iosnewmodelv1_var_sql.VAR_SQL,
                    #'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_IOSModelV1.xlsx',
                    'file_name': 'D:/project/monitoring/uku_iosnewmodel_v1/UKU_IOSModelV1.xlsx',
                    'report_path': 'D:/project/monitoring/uku_iosnewmodel_v1/',
                    #'report_path': '/home/ops/repos/analysis/cron/alert/excel/daily_report/uku_iosnewmodel_v1/',
                    'eda_name': '02_EDA',
                    'dist_name': '03_distribution',
                    'recipients': ['fangmingqing@mintechai.com']}
    )
