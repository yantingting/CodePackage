# encoding=utf8
"""
Python 3.6.8
更新：2020-03-03
周级报告调用文件
"""
import sys
import os

sys.path.append('/home/ops/repos/analysis/cron/alert/')
sys.path.append('/home/ops/repos/newgenie/')

import dateutil.relativedelta as dr
from datetime import datetime, timedelta, date
import logging
import argparse

import execution.monitor_utils as mu
import execution.daily_stability as ds
import execution.daily_performance as df

from execution.monitoring_config import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monitoring.log")

def weekly_report_func(query_date, monitoring_name, upload=False):
    monitoring_list = MODEL_CONFIG.get(monitoring_name, '')
    if monitoring_list == '':
        logger.error('monitoring_name provided is wrong!')
    elif isinstance(monitoring_list, list):
        for sub_monitoring in monitoring_list:
            print("周监控检查")
            var_dict, psi_dict = ds.main(query_date, sub_monitoring, daily_report=False)
            auc_dict = df.main(query_date, sub_monitoring)
            mu.send_weekly_report(query_date, sub_monitoring, var_dict, psi_dict, auc_dict)
    else:
        print("周监控检查")
        var_dict, psi_dict = ds.main(query_date, monitoring_list, daily_report=False)
        auc_dict = df.main(query_date, monitoring_list)
        mu.send_weekly_report(query_date,monitoring_list, var_dict, psi_dict, auc_dict)


if __name__ == "__main__":
    today = datetime.now().strftime('%Y-%m-%d')
    print(today)

    parser = argparse.ArgumentParser(description='use product_name and monitoring_name to run monitoring')
    parser.add_argument('monitoring_name',help='monitoring_name')
    parser.add_argument('upload',help='whether or not upload data to model_monitoring',default=False)
    args = parser.parse_args()
    print(args.monitoring_name)
    weekly_report_func(query_date=today,monitoring_name=args.monitoring_name,upload=args.upload)

