# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import re
import math
import sys
import os
import matplotlib.pyplot as plt
import pandas as pd

sys.path.append('/home/ops/repos/newgenie/')
sys.path.append('/home/ops/repos/analysis/cron/alert/')
import execution.monitor_utils as mu
from execution.monitoring_config import *

from jinja2 import Template

from imp import reload
from datetime import datetime, timedelta, date
import dateutil.relativedelta as dr
#from utils3.data_io_utils import *
from utils3.send_email import mailtest
from sklearn.metrics import roc_auc_score


def calculate_week_auc(df, time_cols, n_week):
    df[time_cols] = df[time_cols].apply(lambda x: str(x)[0:10])
    for i in range(1, n_week + 1):
        start_day = (pd.to_datetime(df[time_cols].max(
        ), format='%Y-%m-%d') - dr.relativedelta(days=7 * i)).strftime("%Y-%m-%d")
        end_day = (pd.to_datetime(df[time_cols].max(
        ), format='%Y-%m-%d') - dr.relativedelta(days=7 * (i - 1))).strftime("%Y-%m-%d")
        df.loc[(df[time_cols] > start_day) & (df[time_cols] <= end_day),
               'week_time'] = '%sTo%s' % (start_day, end_day)
    return(df)

def auc_eventrate_table(data, time_cols, flag, score):
    """
    按天(time_cols)维度, 根据给定的flag, 计算每天的badrate, auc, 如果遇到当天有表现样本量太少导致AUC无法计算(如：仅有好/坏样本),则会填充AUC为0
    """
    data[[flag, score]] = data[[flag, score]].astype(float)
    data[time_cols] = data[time_cols].astype(str)
    auc_df = pd.DataFrame(
        columns=[time_cols, 'AUC'], index=range(data[time_cols].nunique()))
    index = 0
    for i in data[time_cols].unique():
        try:
            #temp_data = data.loc[data[time_cols] == i]
            temp_data = data.loc[data[time_cols] == i]
            auc_result = 1 - roc_auc_score(temp_data[flag], temp_data[score])
            #auc_result = roc_auc_score(temp_data[flag], temp_data[score])
            auc_df.iloc[index][0] = i
            auc_df.iloc[index][1] = auc_result
            index += 1
        except:
            pass

    event_df = pd.crosstab(data[time_cols], data[flag]).rename(
        columns={0: 'N_nonEvent', 1: 'N_Event'})
    if 'N_Event' not in event_df.columns:
        event_df.loc[:, 'N_Event'] = 0
    if 'N_nonEvent' not in event_df.columns:
        event_df.loc[:, 'N_nonEvent'] = 0
    event_df.index.name = None
    event_df.loc[:, 'N_sample'] = data[time_cols].value_counts()
    event_df.loc[:, 'EventRate'] = event_df.N_Event * 1.0 / event_df.N_sample
    event_df = event_df.reset_index()
    event_df.rename(columns={'index': time_cols}, inplace=True)
    df = event_df.merge(auc_df, on=time_cols, how='left')
    df.rename(columns={'N_nonEvent': 'N_Good',
                       'N_Event': 'N_Bad', 'EventRate': 'BadRate'}, inplace=True)
    #reorder_cols = [time_cols,'总样本数','好样本数','坏样本数','BadRate','AUC']
    reorder_cols = [time_cols, 'N_sample', 'N_Good', 'N_Bad', 'BadRate', 'AUC']
    result = df[reorder_cols]
    result['AUC'] = result['AUC'].fillna(0)
    return(result)

def auc_plot(df, time_cols, flag, model_name, label, report_path, benchmark=False):
    save_label = 'auc' + '_' + model_name + '_' + label
    pic_dict = {}
    fig = plt.figure(figsize=(8, 6))
    fig.subplots_adjust(right=0.75)
    df = df.sort_values(by=time_cols, ascending=True)
    # auc
    ax1 = fig.add_subplot(1, 1, 1)
    auc, = ax1.plot(df.index, df['AUC'],
                    color='blue', marker='o', linestyle='-')
    ax1.set_ylabel('auc', fontsize='10', color='blue')
    plt.xticks(fontsize=7, rotation=90)
    ax1.set_ylim([0, 1])
    plt.xticks(df.index, df[time_cols], rotation=90, fontsize=8)
    # AUC数字标签
    for a, b in zip(df.index, df['AUC']):
        plt.text(a, b+0.01, '%.2f' % b, ha='center', va='bottom', fontsize=9, color='blue')

    # badrate
    ax2 = ax1.twinx()
    badrate, =  ax2.plot(
        df.index, df['BadRate'], color='red', marker='o', linestyle='-')
    ax2.set_ylabel('badrate', fontsize='10', color='red')
    ax2.set_ylim([0, 0.6])
    # badRate数字标签
    for a, b in zip(df.index, df['BadRate']):
        plt.text(a, b+0.01, '%.2f' % b, ha='center', va='bottom', fontsize=9, color='red')

    # loanCount
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("axes", 1.2))
    mu.make_patch_spines_invisible(ax3)
    ax3.spines["right"].set_visible(True)
    ax3.set_ylim([0, df['N_sample'].max() * 1.5])

    p3 = ax3.bar(df.index, df['N_Good'] + df['N_Bad'],
                 alpha=.7, color='#f03b20', width=0.7)  # bad 红色
    p4 = ax3.bar(df.index, df['N_Good'], alpha=.7,
                 color='#3182bd', width=0.7)  # , alpha=.7
    ax3.set_ylabel("loan count")
    # loanCount数字标签
    for a, b in zip(df.index, df['N_Good'] + df['N_Bad']):
        plt.text(a, b, b, ha='center', va='bottom', fontsize=9)

    # benchmark
    if benchmark != False:
        model_auc = ax1.axhline(y=benchmark, c="black",
                                ls="--", lw=2, label='modeling auc')

    lines = [auc, badrate, model_auc]
    ax1.legend(lines, [l.get_label() for l in lines], loc='upper right')
    plt.legend(['Bad count', 'Good count'], loc=2)
    plt.title('%s' % save_label)

    tkw = dict(size=4, width=1.5)
    ax1.tick_params(axis='y', colors=auc.get_color(), **tkw)
    ax2.tick_params(axis='y', colors='red', **tkw)

    FIG_PATH = os.path.join(report_path, 'figure')
    if not os.path.exists(FIG_PATH):
        os.makedirs(FIG_PATH)
    pic_dict['auc_pict'] = save_label + '.png'
    path = os.path.join(FIG_PATH, save_label + ".png")
    plt.savefig(path, format='png', dpi=100)
    plt.close()
    return (pic_dict)

def auc_eventrate_table_new(data, time_cols, flag, score):
    """
    按周(time_cols)维度, 根据给定的flag, 计算每天的badrate, auc, 如果遇到当天有表现样本量太少导致AUC无法计算(如：仅有好/坏样本),则会填充AUC为0
    """
    data[[flag, score]] = data[[flag, score]].astype(float)
    data[time_cols] = data[time_cols].astype(str)
    data = data.sort_values(by=time_cols, ascending=True)
    week_list = data['week_time'].unique()

    all_data = []

    for i in week_list:
        lower = i.split('To')[0]
        upper = i.split('To')[1]

        # temp_data = data[(data[time_cols] > lower) & (data[time_cols] < upper)]
        temp_data = data[data['week_time'] == i]
        # effective_date    N_sample   N_Good   N_Bad   BadRate   AUC
        raw_data = []

        temp_data[flag] = temp_data[flag].astype(int)
        N_sample = temp_data.shape[0]

        if temp_data[temp_data[flag] == 1].shape[0] == 0:
            N_Bad = 0
        else:
            N_Bad = temp_data[flag].value_counts().ix[1]

        N_Good = N_sample - N_Bad
        BadRate = N_Bad * 1.0 / N_sample
        if temp_data[flag].nunique()>1:
            auc_result = 1 - roc_auc_score(temp_data[flag], temp_data[score])
        else:
            print('flag仅有一种')
            auc_result = 0

        raw_data.append(i)
        raw_data.append(N_sample)
        raw_data.append(N_Bad)
        raw_data.append(N_Good)
        raw_data.append(BadRate)
        raw_data.append(auc_result)

        all_data.append(raw_data)
    result = pd.DataFrame(all_data, columns=[
                          'effective_date', 'N_sample', 'N_Bad', 'N_Good', 'BadRate', 'AUC'])
    
    return result

def weekly_auc(in_dict):
    perf_data = mu.get_current_data(
        in_dict['product_name'], in_dict['performance_param'])
    if 'score' not in perf_data.columns:
        perf_data.rename(columns={'ruleresult': 'score'}, inplace=True)
    else:
        pass
    if perf_data.shape[0] > 0:
        # 从excel中读入默认的auc值
        model_perf = pd.read_excel(
            in_dict['file_name'], sheet_name=in_dict['perftable_name'])
        model_score_name = model_perf.loc[0]['变量名']
        model_auc = model_perf.AUC.iloc[0]
        week_data = calculate_week_auc(perf_data, in_dict['performance_param'][
                                   'time_cols'], n_week=in_dict['performance_param']['obs_window'])
        auc_eventrate_df = auc_eventrate_table_new(data=week_data, time_cols=in_dict['performance_param'][
                                                   'time_cols'], flag=in_dict['performance_param']['flag'], score=model_score_name)
        auc_eventrate_df.to_excel('./auc_eventrate_df.xlsx', index=False)
        pic_dict = auc_plot(auc_eventrate_df, in_dict['performance_param']['time_cols'], in_dict['performance_param'][
                            'flag'], in_dict['model_name'], in_dict['performance_param']['label'], in_dict['report_path'], benchmark=model_auc)
    else:
        pic_dict = None
    return (pic_dict)

def daily_auc(in_dict):
    perf_data = mu.get_current_data(
        in_dict['product_name'], in_dict['performance_param'])
    if 'score' not in perf_data.columns:
        perf_data.rename(columns={'ruleresult': 'score'}, inplace=True)
    else:
        pass
    if perf_data.shape[0] > 0:
        # 从excel中读入默认的auc值
        model_perf = pd.read_excel(
            in_dict['file_name'], sheet_name=in_dict['perftable_name'])
        model_score_name = model_perf.loc[0]['变量名']
        model_auc = model_perf.AUC.iloc[0]
        auc_eventrate_df = auc_eventrate_table(data=perf_data, time_cols=in_dict['performance_param'][
                                               'time_cols'], flag=in_dict['performance_param']['flag'], score=model_score_name)
        pic_dict = auc_plot(auc_eventrate_df, in_dict['performance_param']['time_cols'], in_dict['performance_param'][
                            'flag'], in_dict['model_name'], in_dict['performance_param']['label'], in_dict['report_path'], benchmark=model_auc)
    else:
        pic_dict = None
    return (pic_dict)

def main(query_time, in_dict):
    in_dict['performance_param']['query_date'] = query_time
    if in_dict['performance_param']['time_type'] == 'week':
        pic_dict = weekly_auc(in_dict)
    else:
        pic_dict = daily_auc(in_dict)
    return pic_dict

if __name__ == '__main__':
    # query_time =(datetime.now()-dr.relativedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
    # print(query_time)
    query_time = '2020-05-29'
    main(query_time, in_dict={'product_name': 'uku',
                              'model_name': 'iosnewusermodelv1',
                              'daily_param': {'obs_window': 30,
                                              'per_window': 0,
                                              'time_type': 'day',
                                              'sql': uku_iosnewmodelv1_var_sql.VAR_SQL,
                                              'recipients': ['huangweijie@huojintech.com']},
                              'weekly_param': {'obs_window': 4,
                                               'per_window': 0,
                                               'time_type': 'week',
                                               'sql': uku_iosnewmodelv1_var_sql.VAR_SQL,
                                               'recipients': ['huangweijie@huojintech.com']},
                              'performance_param': {'obs_window': 4,
                                                    'per_window': 21,
                                                    'time_type': 'week',
                                                    'sql': uku_iosnewmodelv1_var_sql.PER_SQL,
                                                    'time_cols': 'effective_date',
                                                    'flag': 'flag_7',
                                                    'label': 'dpd7+',
                                                    'recipients': ['huangweijie@huojintech.com']},
                              'file_name': r'G:\Mintech\05-监控\analysis\cron\alert\excel\UKU_IOSModelV1.xlsx',
                              'report_path': r'G:\Mintech\05-监控\analysis\cron\alert\excel\daily_report\uku_iosnewmodel_v1\\',
                              'eda_name': '02_EDA',
                              'dist_name': '03_distribution',
                              'perftable_name': '04_performance',
                              })
