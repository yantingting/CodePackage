"""
Python 3.6

1、部署logistics评分卡
2、部署xgboost相关文件

"""
import os
import sys
import logging
import numpy as np
import pandas as pd
from jinja2 import Template
import utils3.misc_utils as mu
from utils3.data_io_utils import *
import utils3.metrics as mt

def generate_logistics_scorecard_deployment_documents(model_label, live_abbr,
                                            RESULT_PATH,score_card,model_decile,
                                            eda_table,
                                            coarse_classing_rebin_spec,
                                            production_modelName, product_name,
                                            production_name_map={}):

    """
    生成逻辑回归评分卡部署Excel文件，线上部署json文件，线上部署测试用例

    Args
    model_label (str): 模型名称
    live_abbr (str): 上线后模型输出打分字段尾缀名称
    RESULT_PATH (str): 结果路径
    score_card (dataFrame): 建模评分卡
    model_decile(dataFrame): 建模decile
    eda_table (dataFrame): EDA结果
    coarse_classing_rebin_spec (dict): 建模分箱边界文件
    production_modelName (str): 需要部署的模型在线上部署时所在的条用modelName，取值问部署人员
    product_name (str): 中间层表格中的product_name取值。传入这个模型所部署的产品名
    production_name_map (dict): 当有指标英文线上的名字和线下建模时不一样时传入。key=建模时英文名
        value=线上英文名。 default={} 默认建模时和线上没有命名不一致的情况
    """
    # 创建部署路径
    if not os.path.exists(os.path.join(RESULT_PATH, 'deployment')):
        os.makedirs(os.path.join(RESULT_PATH, 'deployment'))

    DEPLOY_PATH = os.path.join(RESULT_PATH, 'deployment')
    
    score_card_production = score_card.rename(columns={'指标英文': '中间层指标名称', '变量打分': '打分'})
    score_card_production['中间层指标名称'] = score_card_production['中间层指标名称'].replace(production_name_map)
    score_card_production.insert(2, '输出打分指标名称',
                score_card_production['中间层指标名称'].apply(lambda x: 'mlr_' + x + '_scrd_' + live_abbr))

    score_card_production.loc[:, '输出打分指标名称'] = score_card_production.loc[:, '输出打分指标名称']\
                                                        .replace('mlr_intercept_scrd_'+live_abbr, 'mlr_const_scrd_'+live_abbr)

    score_card_production.loc[score_card_production['中间层指标名称']=='intercept', '指标中文'] = '截距分'
    score_card_production.loc[score_card_production['中间层指标名称']=='intercept', '中间层指标名称'] = None

    score_card_production = score_card_production.append({'输出打分指标名称': 'mlr_creditscore_scrd_'+live_abbr}, ignore_index=True)
    score_card_production = score_card_production.append({'输出打分指标名称': 'mlr_prob_scrd_'+live_abbr}, ignore_index=True)
    score_card_production.insert(score_card_production.shape[1], '是否手动调整', None)
    score_card_production.insert(score_card_production.shape[1], 'backscore分布占比', None)
    score_card_production.insert(score_card_production.shape[1], '基准psi', None)
    score_card_production.insert(score_card_production.shape[1], 'psi预警delta', None)

    selected_variables = [i for i in score_card['指标英文'].unique() if i != 'intercept']
    model_eda = eda_table.loc[eda_table['指标英文'].isin(selected_variables)]

    writer = pd.ExcelWriter(os.path.join(DEPLOY_PATH, '%s部署评分卡.xlsx' % model_label))
    score_card_production.to_excel(writer, '2_模型评分卡', index=False)
    model_decile.to_excel(writer, '3_模型decile', index=False)
    model_eda.to_excel(writer, '4_模型EDA', index=False)
    writer.save()
    logging.info("""第六步部署：生成逻辑回归评分卡部署文档。
    1. 模型部署Excel文档存储于：%s
    2. 需添加『0_文档修订记录』、『1_信息总览』页面。详见其他正式部署文档文件。并存储于『/Seafile/模型共享/模型部署文档/』相应文件夹中
    """ % os.path.join(DEPLOY_PATH, '%s部署评分卡.xlsx' % model_label))

    bin_to_score_json = mu.process_bin_to_score(score_card)
    for original, new_name in production_name_map.items():
        coarse_classing_rebin_spec[new_name] = coarse_classing_rebin_spec.pop(original)
        bin_to_score_json[new_name] = bin_to_score_json.pop(original)

    rebin_spec_json = mu.process_rebin_spec(coarse_classing_rebin_spec, score_card, selected_variables)

    save_data_to_json(rebin_spec_json, DEPLOY_PATH, '%s_selected_rebin_spec.json' % live_abbr)
    save_data_to_json(bin_to_score_json, DEPLOY_PATH, '%s_bin_to_score.json' % live_abbr)
    logging.info("""第六步部署：生成逻辑回归评分卡部署文档。
    线上部署配置文件存储于%s路径下
    1. %s
    2. %s
    """ % (DEPLOY_PATH,
           '%s_selected_rebin_spec.json' % live_abbr,
           '%s_bin_to_score.json' % live_abbr
          ))


    writer = pd.ExcelWriter(os.path.join(DEPLOY_PATH, '%stest_cases.xlsx' % model_label))
    test_case = mu.produce_http_test_case(score_card_production, production_modelName, product_name)
    test_case.to_excel(writer, model_label, index=False)
    writer.save()
    logging.info("""第六步部署：生成逻辑回归评分卡部署文档。
    线上部署测试用例存储于 %s
    """ % os.path.join(DEPLOY_PATH, '%stest_cases.xlsx' % model_label))


def generate_xgb_deployment_documents(model_label, live_abbr, eda_table,model_spec,xgb_importance_score,
                                      var_dict,model_decile,model_result,RESULT_PATH
                                      , production_name_map={}):
    """
    生成XGBoost模型部署文档

    Args
    model_label (str): 模型名称
    live_abbr (str): 上线后模型输出打分字段尾缀名称
    eda_table (dataFrame): EDA结果
    model_spec (dataFrame): 模型的分箱文件
    xgb_importance_score (dataFrame): xgboost模型变量重要性排序
    var_dict (dataFrame): 数据字典
    model_decile(dataFrame): 建模decile
    model_result (dict): 模型结果
    RESULT_PATH (str): 结果路径
    production_name_map (dict): 当有指标英文线上的名字和线下建模时不一样时传入。key=建模时英文名
        value=线上英文名。 default={} 默认建模时和线上没有命名不一致的情况
    """
    if not os.path.exists(os.path.join(RESULT_PATH, 'deployment')):
        os.makedirs(os.path.join(RESULT_PATH, 'deployment'))

    DEPLOY_PATH = os.path.join(RESULT_PATH, 'deployment')
    
    eda_table = eda_table
    model_spec = model_spec
    rebin_spec = model_spec['rebin_spec']
    bin_to_label = model_spec['bin_to_label']
    dummy_var_name_map = model_spec['dummy_var_name_map']

    xgb_importance_score = xgb_importance_score
    xgb_importance_score = xgb_importance_score.rename(columns={'feature': 'XGB衍生入模名称', 'fscore': '指标用于Split数据的数量', 'imp_pct': '指标用于Split数据的数量占比'})

    xgb_importance_score[['XGB变量转换类型', '中间层指标名称']] = xgb_importance_score['XGB衍生入模名称']\
                        .apply(lambda x: pd.Series(mt.BinWoe().xgboost_obtain_raw_variable(x, var_dict)))

    xgb_importance_score['建模时指标名称'] = xgb_importance_score['中间层指标名称'].copy()
    xgb_importance_score['建模时XGB衍生入模名称'] = xgb_importance_score['XGB衍生入模名称'].copy()
    for original, new_name in production_name_map.items():
        a = xgb_importance_score.loc[xgb_importance_score['建模时指标名称']==original, '建模时XGB衍生入模名称']\
                                .apply(lambda x: x.replace(original, new_name))
        xgb_importance_score.loc[xgb_importance_score['建模时指标名称']==original, 'XGB衍生入模名称'] = a
        if original in rebin_spec:
            rebin_spec[new_name] = rebin_spec.pop(original)
        if original in bin_to_label:
            bin_to_label[new_name] = bin_to_label.pop(original)
        # 这一步很重要，要将字典里边的变量名也改掉，不然rebin_spec_json = mu.process_rebin_spec(rebin_spec, var_dict, num_variables+bin_variables)会出错
        if original in list(var_dict['指标英文']):
            var_dict['指标英文'].replace({original:new_name},inplace=True)

    xgb_importance_score['中间层指标名称'] = xgb_importance_score['中间层指标名称'].replace(production_name_map)

    xgb_importance_score = var_dict[['数据源', '指标英文', '指标中文', '数据类型']]\
                                .rename(columns={'指标英文':'中间层指标名称'})\
                                .merge(xgb_importance_score, on='中间层指标名称', how='right')

    xgb_importance_score.insert(5, '输出打分指标名称',
                xgb_importance_score['XGB衍生入模名称'].apply(lambda x: 'mlr_' + str(x) + '_xgb_' + live_abbr))


    xgb_importance_score = xgb_importance_score.append({'输出打分指标名称': 'mlr_creditscore_xgb_'+live_abbr}, ignore_index=True)
    xgb_importance_score = xgb_importance_score.append({'输出打分指标名称': 'mlr_prob_xgb_'+live_abbr}, ignore_index=True)

    model_decile = model_decile

    selected_variables = xgb_importance_score['建模时指标名称'].unique()
    model_eda = eda_table.loc[eda_table['指标英文'].isin(selected_variables)].copy()
    model_eda['指标英文'] = model_eda['指标英文'].replace(production_name_map)

    writer = pd.ExcelWriter(os.path.join(DEPLOY_PATH, '%s部署文档.xlsx' % model_label))
    xgb_importance_score.to_excel(writer, '2_模型变量重要性排序', index=False)
    model_decile.to_excel(writer, '3_模型decile', index=False)
    model_eda.to_excel(writer, '4_模型EDA', index=False)
    writer.save()
    logging.info("""第六步部署：生成XGBoost部署文档。
    1. 模型部署Excel文档存储于：%s
    2. 需添加『0_文档修订记录』、『1_信息总览』页面。详见其他正式部署文档文件。并存储于『/Seafile/模型共享/模型部署文档/』相应文件夹中
    """ % os.path.join(DEPLOY_PATH, '%s部署文档.xlsx' % model_label))


    model_result = model_result
    derive_name_map = dict(zip(xgb_importance_score['建模时XGB衍生入模名称'], xgb_importance_score['XGB衍生入模名称']))
    xgbmodel = model_result['model_final']
    
    var_list = []
    for i in xgbmodel.__dict__['feature_names']:
        try:
            var_list.append(derive_name_map[i])
        except:
            var_list.append(i)
    xgbmodel.__dict__['feature_names'] = var_list



    num_variables = list(xgb_importance_score.loc[xgb_importance_score['XGB变量转换类型']=='num_vars_origin', '中间层指标名称'].unique())
    bin_variables = list(xgb_importance_score.loc[xgb_importance_score['XGB变量转换类型']=='bin_vars', '中间层指标名称'].unique())
    rebin_spec_json = mu.process_rebin_spec(rebin_spec, var_dict, num_variables+bin_variables)
    bin_to_label = {k:v for k,v in bin_to_label.items() if k in bin_variables}

    var_transform_method = {}
    var_transform_method['num_vars_origin'] = num_variables
    var_transform_method['bin_vars'] = bin_variables
    var_transform_method['dummy_vars'] = {}
    dummy_vars_df = xgb_importance_score.loc[xgb_importance_score['XGB变量转换类型']=='dummy_vars'].copy()
    dummy_vars_df.loc[:, "dummy原始名称"] = dummy_vars_df['建模时XGB衍生入模名称'].apply(lambda x: dummy_var_name_map[x])
    dummy_vars_df.loc[:, 'dummy原始对应分类'] = dummy_vars_df.loc[:, "dummy原始名称"].apply(lambda x: x.split('DUMMY')[-1])
    for original_variable in dummy_vars_df['中间层指标名称'].unique():
        cat_list = list(dummy_vars_df.loc[dummy_vars_df['中间层指标名称']==original_variable, 'dummy原始对应分类'].unique())
        var_transform_method['dummy_vars'][original_variable] = cat_list



    save_data_to_json(rebin_spec_json, DEPLOY_PATH, '%s_selected_rebin_spec.json' % live_abbr)
    save_data_to_json(bin_to_label, DEPLOY_PATH, '%s_bin_to_label.json' % live_abbr)
    save_data_to_python2_pickle(xgbmodel, DEPLOY_PATH, '%s_xgbmodel.pkl' % live_abbr)
    save_data_to_json(var_transform_method, DEPLOY_PATH, '%s_var_transform_method.json' % live_abbr)

    logging.info("""第六步部署：生成XGBoost部署文档。
    线上部署配置文件存储于%s路径下
    1. %s
    2. %s
    3. %s
    4. %s
    """ % (DEPLOY_PATH,
           '%s_selected_rebin_spec.json' % live_abbr,
           '%sbin_to_label.json' % live_abbr,
           '%s_var_transform_method.json' % live_abbr,
           '%s_xgbmodel.pkl' % live_abbr,
          ))