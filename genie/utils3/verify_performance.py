# encoding=utf-8
import os
import json
import logging

import numpy as np
import pandas as pd
from functools import reduce
try:
    import xgboost as xgb
except:
    pass


import utils3.metrics as mt
import utils3.misc_utils as mu
from utils3.data_io_utils import *





def get_score_card(model_label, DATA_PATH, RESULT_PATH, woe_file_name, rebin_spec_name, var_dict):
    """
    为了延展性输入model_label将相应的评分卡计算并存好

    Args:
    model_label (str): 模型版本名
    RESULT_PATH (str): 存储路径
    woe_file_name (str): 存储的woe_iv_df的文件名，用于转换计算评分卡
    rebin_spec_name (str): 变量粗分箱（或者评分卡建模用的分箱）边界spec文件名
    var_dict (pd.DataFrame): 标准变量字典

    Returns:
    存储评分卡至： RESULT_PATH, '%s_score_card.xlsx' % model_label
    """
    rebin_spec = load_data_from_pickle(DATA_PATH, rebin_spec_name)
    model_result = load_data_from_pickle(DATA_PATH, '%s模型结果.pkl' % model_label)
    coefficients = model_result['model_final'].params.to_frame('beta')\
                              .reset_index().rename(columns={'index':'var_code'})

    woe_iv_df_coarse = pd.read_excel(os.path.join(RESULT_PATH, woe_file_name))
    woe_iv_df_coarse.loc[:, '指标英文'] = woe_iv_df_coarse.loc[:, '指标英文'].astype(str)
    cleaned_woe = woe_iv_df_coarse.loc[woe_iv_df_coarse[u'指标英文'].isin(coefficients.var_code),
                                [u'指标英文', u'分箱', '分箱对应原始分类', 'N', u'分布占比', 'WOE', u'逾期率', u'Bad分布占比']]

    # woe表是根据实际数据计算的，可能实际数据中缺少某些字段的某一箱
    should_haves_list = []
    for var_name in cleaned_woe['指标英文'].unique():
        var_type = woe_iv_df_coarse.loc[woe_iv_df_coarse['指标英文']==var_name, '数据类型'].iloc[0]
        if var_type == 'varchar':
            if var_name in rebin_spec:
                complete_bins_df = pd.Series(list(rebin_spec[var_name].keys())).to_frame('分箱')
                complete_bins_df.loc[:, '指标英文'] = var_name
            else:
                complete_bins_df = cleaned_woe.loc[cleaned_woe['指标英文']==var_name, ['指标英文', '分箱']].copy()
        else:
            a = pd.Series(0)
            # 这样拿到的是最全的分箱值，不依赖数据里是否有这一分箱。且顺序是排好的
            complete_bins = [str(i) for i in pd.cut(a, rebin_spec[var_name]['cut_boundaries']).cat.categories]
            if var_type == 'integer':
                other_categories = [int(i) for i in rebin_spec[var_name]['other_categories']]

            if  var_type == 'float':
                other_categories = [float(i) for i in rebin_spec[var_name]['other_categories']]

            complete_bins_list = sorted(other_categories) + list(complete_bins)
            complete_bins_df = pd.Series(complete_bins_list).to_frame('分箱')
            complete_bins_df.loc[:, '指标英文'] = var_name
        should_haves_list.append(complete_bins_df)

    should_haves_df = pd.concat(should_haves_list)
    should_haves_df = should_haves_df.astype(str)
    cleaned_woe[[u'指标英文', u'分箱']] = cleaned_woe[[u'指标英文', u'分箱']].astype(str)
    cleaned_woe = should_haves_df.merge(cleaned_woe, on=[u'指标英文', u'分箱'], how='left')
    cleaned_woe['N'] = cleaned_woe['N'].fillna(0)
    cleaned_woe['分布占比'] = cleaned_woe['分布占比'].fillna(0)
    cleaned_woe['WOE'] = cleaned_woe['WOE'].fillna(0)


    score_card = mt.Performance().calculate_score(coefficients, cleaned_woe, 'var_score', var_dict)
    score_card.to_excel(os.path.join(RESULT_PATH, '%s_score_card.xlsx' % model_label), index=False)


def summarize_perf_metrics(score_dict, all_Y, y_col):
    """
    把模型在不同样本和Y的表现（KS, AUC)汇总到一个表格

    Args:
    score_dict (dict): the key is the label of the sample set. e.g. 'train',
        'test', 'oot' etc. should be the save as data_cat_dict.keys()
        The value is the scores. index is apply_id
    all_Y (pd.DataFrame): should have ['Y_build', 'Y_fid14'], 'Y_build'
        name can be changed. index is apply_id
    y_col (str): the column name of Y

    """
    metrics_result_build = {}
    for label, score in score_dict.items():
        logging.log(18, label + ' data set starts performance calculation')
        merged_data = score['score'].to_frame('score')\
                           .merge(all_Y, left_index=True, right_index=True)
        if merged_data[y_col].count() > 10:
            merged_data = merged_data.loc[merged_data[y_col].notnull()]
            metrics_result_build[label] = {}
            metrics_result_build[label]['AUC'] = mt.Performance().calculate_auc(merged_data[y_col], 1./merged_data.score)
            metrics_result_build[label]['KS'] = mt.Performance().calculate_ks_by_score(merged_data[y_col], merged_data.score)

    perf_result = pd.DataFrame(metrics_result_build)
    perf_result = perf_result.reset_index()
    perf_result = perf_result.rename(columns={'index': 'metrics'})
    return perf_result




def get_decile(score, varscore, all_Y, y_col, label, RESULT_PATH, manual_cut_bounds=[]):
    """
    根据提供的score和Y相应的Y定义

    Args:
    score (pd.DataFrame): index is apply_id
    all_Y (pd.DataFrame): should have ['Y_build', 'Y_fid14'], 'Y_build'
        name can be changed. index is apply_id
    y_col (str): column name of Y
    label (str): sample set label

    """
    merged_data = score.to_frame('score')\
                       .merge(all_Y, left_index=True, right_index=True)
    merged_data = merged_data.loc[merged_data[y_col].notnull()]
    if len(merged_data) > 10:
        ks_decile = mt.Performance().calculate_ks_by_decile(merged_data.score, \
                                    np.array(merged_data[y_col]), 'decile', 10,
                                    manual_cut_bounds=manual_cut_bounds)
        ks_decile.loc[:, 'sample_set'] = label
    else:
        ks_decile = pd.DataFrame()

    varscore_new = varscore.merge(score.to_frame('score'), left_index=True, right_index=True)

    if not ks_decile.empty:
        if len(manual_cut_bounds) == 0:
            manual_cut_bounds = mt.BinWoe().obtain_boundaries(ks_decile['分箱'])['cut_boundaries']

        varscore_new.loc[:, 'score_10bin'] = pd.cut(varscore_new.score, manual_cut_bounds).astype(str)
        bin_varscore_avg = varscore_new.groupby('score_10bin').mean()\
                                       .reset_index()\
                                       .rename(columns={'score_10bin': '分箱'})
        ks_decile = ks_decile.merge(bin_varscore_avg, on='分箱')
        return ks_decile, manual_cut_bounds
    else:
        return pd.DataFrame(), manual_cut_bounds



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





def score_stability(score_dict, all_Y, y_col, train_set_name, RESULT_PATH, model_label):
    """
    计算所有sample_set的score stability
    Args:
    score_dict (dict): the key is the label of the sample set. e.g. 'train',
        'test', 'oot' etc. should be the save as data_cat_dict.keys()
        The value is the scores. index is apply_id
    all_Y (pd.DataFrame): should have ['Y_build', 'Y_fid14'], 'Y_build'
        name can be changed. index is apply_id
    y_col (str): column name of Y
    """
    all_decile_df = pd.read_excel(os.path.join(RESULT_PATH, 'all_decile.xlsx'))
    the_bool = ((all_decile_df.model_label==model_label)
             & (all_decile_df.sample_set==train_set_name)
             & (all_decile_df.Y_definition==y_col))
    train_decile = all_decile_df.loc[the_bool].copy()
    point_bounds = mt.BinWoe().obtain_boundaries(train_decile['分箱'])['cut_boundaries']

    d_list = []
    compare_set_names = [i for i in list(score_dict.keys()) if i != train_set_name]
    for label in compare_set_names:
        logging.log(18, label + ' data set starts score PSI calculation')
        tmp = mt.Performance().score_psi(score_dict[train_set_name]['score'], score_dict[label]['score'], point_bounds)
        columns_order = tmp.columns
        tmp.loc[:, 'compare_set'] = '%s_vs_%s' % (train_set_name, label)
        d_list.append(tmp)

    columns_order = ['model_label', 'compare_set'] + list(tmp.columns.values)
    all_score_psi = pd.concat(d_list)
    all_score_psi.loc[:, 'model_label'] = model_label
    return all_score_psi[columns_order]




def approval_rate_anaysis(backscore, RESULT_PATH, model_label, y_col):
    all_decile_df = pd.read_excel(os.path.join(RESULT_PATH, 'all_decile.xlsx'))
    the_bool = ((all_decile_df.sample_set=='RUNBOOK')
            &(all_decile_df.model_label==model_label)
            &(all_decile_df.Y_definition==y_col))
    runbook = all_decile_df.loc[the_bool].copy()
    point_bounds = mt.BinWoe().obtain_boundaries(runbook['分箱'])['cut_boundaries']
    backscore_cut = pd.cut(backscore, point_bounds).astype(str)
    dist_ct = backscore_cut.value_counts().to_frame('样本量').sort_index().reset_index()\
                           .rename(columns={'index': '分箱'})
    dist_ct.loc[:, '分布占比'] = dist_ct['样本量'] / dist_ct['样本量'].sum()
    dist_ct = dist_ct.reset_index().rename(columns={'index': 'bin_order'})
    dist_ct = dist_ct.merge(runbook[['分箱', '逾期率']], on='分箱', how='left')
    dist_ct = dist_ct.sort_values('bin_order', ascending=False)
    dist_ct.loc[:, 'ApprovalRate'] = dist_ct['分布占比'].cumsum()
    dist_ct.loc[:, 'ApprovedBadRate'] = ((dist_ct['分布占比']*dist_ct['逾期率']).cumsum() / dist_ct['分布占比'].cumsum())
    dist_ct.loc[:, 'model_label'] = model_label
    return dist_ct
