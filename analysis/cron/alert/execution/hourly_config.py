"""
Python 3.6.8
更新：2019-12-19
小时级预警配置文件
"""

import uku_new_model_v4.var_sql as uku_newmodelv4_var_sql
import uku_iosnewmodel_v1.var_sql as uku_iosnewmodelv1_var_sql
import uku_new_model_v5.var_sql as uku_newmodelv5_var_sql
import uku_oldmodel_v2.var_sql as uku_oldmodelv2_var_sql
import uku_new_model_v6.var_sql as uku_newmodelv6_var_sql
import uku_oldmodel_v3.var_sql as uku_oldmodelv3_var_sql


MODEL_CONFIG = {
    "UKU_A_NEWMODELV4_24H": [{
                    'product_name':'uku',
                    'model_name' : 'newusermodelv4',
                    'obs_window': 2,
                    'time_type': 'hour',
                    'per_window': 0,
                    'sql': uku_newmodelv4_var_sql.HOURLY_VAR_SQL,
                    'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_NewUserModelV4.xlsx',
                    'report_path':'/home/ops/repos/analysis/cron/alert/excel/eda/uku_new_model_v4/',
                    'eda_name': '02_EDA',
                    'recipients': ['fangmingqing@mintechai.com']
                    }],
    "UKU_A_IOSNEWMODELV1_24H": [{
        'product_name': 'uku',
        'model_name': 'iosnewusermodelv1',
        'obs_window': 24,
        'time_type': 'hour',
        'per_window': 0,
        'sql': uku_iosnewmodelv1_var_sql.VAR_SQL,
        'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_IOSModelV1.xlsx',
        'report_path': '/home/ops/repos/analysis/cron/alert/excel/eda/uku_iosnew_model_v1/',
        'eda_name': '02_EDA',
        'recipients': ['fangmingqing@mintechai.com','huangweijie@huojintech.com']
        }],
    "UKU_A_LinkajaNEWMODELV1_24H": [{
        'product_name': 'linkaja',
        'model_name': 'linkajammodelv1',
        'obs_window': 24,
        'time_type': 'hour',
        'per_window': 0,
        'sql': uku_iosnewmodelv1_var_sql.VAR_SQL,
        'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_IOSModelV1.xlsx',
        'report_path': '/home/ops/repos/analysis/cron/alert/excel/eda/uku_iosnew_model_v1/',
        'eda_name': '02_EDA_Linkaja',
        'recipients': ['fangmingqing@mintechai.com','huangweijie@huojintech.com']
    }],
    "UKU_A_OLDMODELV2_24H": [{
        'product_name': 'uku',
        'model_name': 'oldusermodelv2',
        'obs_window': 12,
        'time_type': 'hour',
        'per_window': 0,
        'sql': uku_oldmodelv2_var_sql.VAR_SQL,
        'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_OldUserModelV2.xlsx',
        'report_path': '/home/ops/repos/analysis/cron/alert/excel/eda/uku_old_model_v2/',
        'eda_name': '02_EDA_ONEKEYLOAN',
        'recipients': ['fangmingqing@mintechai.com','yantingting@mintechai.com']
    }],
    "UKU_A_NEWMODELV5_24H": [{
        'product_name': 'uku',
        'model_name': 'newusermodelv5',
        'obs_window': 12,
        'time_type': 'hour',
        'per_window': 0,
        'sql': uku_newmodelv5_var_sql.VAR_SQL,
        'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_NewUserModelV5.xlsx',
        'report_path': '/home/ops/repos/analysis/cron/alert/excel/eda/uku_new_model_v5/',
        'eda_name': '02_EDA',
        'recipients': ['jiangshanjiao@mintechai.com']
        ,'app_list_high':'/home/ops/repos/analysis/cron/alert/excel/high_freq_list.txt'
        ,'app_list_mid':'/home/ops/repos/analysis/cron/alert/excel/mid_freq_list.txt'
        ,'app_list_low':'/home/ops/repos/analysis/cron/alert/excel/low_freq_list.txt'
    }],
    "UKU_A_NEWMODELV5_cashcash_24H": [{
        'product_name': 'cashcash',
        'model_name': 'newusermodelv5',
        'obs_window': 12,
        'time_type': 'hour',
        'per_window': 0,
        'sql': uku_newmodelv5_var_sql.VAR_SQL,
        'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_NewUserModelV5.xlsx',
        'report_path': '/home/ops/repos/analysis/cron/alert/excel/eda/uku_new_model_v5/',
        'eda_name': '02_EDA',
        'recipients': ['jiangshanjiao@mintechai.com']
        ,'app_list_high':'/home/ops/repos/analysis/cron/alert/excel/high_freq_list.txt'
        ,'app_list_mid':'/home/ops/repos/analysis/cron/alert/excel/mid_freq_list.txt'
        ,'app_list_low':'/home/ops/repos/analysis/cron/alert/excel/low_freq_list.txt'
    }],
    "UKU_A_NEWMODELV6_UKU_24H": [{
        'product_name': 'uku',
        'model_name': 'newusermodelv6',
        'obs_window': 12,
        'time_type': 'hour',
        'per_window': 0,
        'sql': uku_newmodelv6_var_sql.VAR_SQL,
        'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_NewUserModelV6.xlsx',
        'report_path': '/home/ops/repos/analysis/cron/alert/excel/eda/uku_new_model_v6/',
        'eda_name': '02_EDA',
        'recipients': ['jiangshanjiao@mintechai.com']
    }],
    "UKU_A_OLDMODELV3_24H": [{
        'product_name': 'uku',
        'model_name': 'oldusermodelv3',
        'obs_window': 12,
        'time_type': 'hour',
        'per_window': 0,
        'sql': uku_oldmodelv3_var_sql.VAR_SQL,
        'file_name': '/home/ops/repos/analysis/cron/alert/excel/UKU_OldUserModelV3.xlsx',
        'report_path': '/home/ops/repos/analysis/cron/alert/excel/eda/uku_old_model_v3/',
        'eda_name': '02_EDA',
        'recipients': ['jiangshanjiao@mintechai.com','yantingting@mintechai.com']
    }],
}

