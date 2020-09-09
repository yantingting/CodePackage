# encoding=utf8
"""
Python 3.6.8
更新：2019-12-19
日级预警调用文件
"""
import sys

sys.path.append('/home/ops/repos/analysis/cron/alert/')

import dateutil.relativedelta as dr
from datetime import datetime, timedelta, date
import logging
import argparse

#from execution.daily_config import *
import execution.monitor_utils as mu
import execution.daily_stability as ds
from execution.monitoring_config import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monitoring.log")


def daily_report_func(query_date, monitoring_name, upload=False):
    monitoring_list = MODEL_CONFIG.get(monitoring_name, '')
    if monitoring_list == '':
        logger.error('monitoring_name provided is wrong!')
    elif isinstance(monitoring_list, list):
        for sub_monitoring in monitoring_list:
            print("日监控检查")
            ds.main(query_time=query_date, in_dict=sub_monitoring, daily_report = True)
    else:
        print("日监控检查")
        ds.main(query_time=query_date, in_dict=monitoring_list, daily_report = True)

if __name__ == "__main__":
    today = datetime.now().strftime('%Y-%m-%d')
    print(today)

    parser = argparse.ArgumentParser(description='use product_name and monitoring_name to run monitoring')
    parser.add_argument('monitoring_name',help='monitoring_name')
    parser.add_argument('upload',help='whether or not upload data to model_monitoring',default=False)
    args = parser.parse_args()
    print(args.monitoring_name)
    daily_report_func(query_date=today,monitoring_name=args.monitoring_name,upload=args.upload)
