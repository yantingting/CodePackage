# encoding=utf8
"""
Python 3.6.8
更新：2019-12-19
小时级预警调用文件
"""
import sys

sys.path.append('/home/ops/repos/analysis/cron/alert/')
#sys.path.append('/home/ops/repos/genie/')


import dateutil.relativedelta as dr
from datetime import datetime, timedelta, date
import logging
import argparse

from execution.hourly_config import *
import execution.monitor_utils as mu
import execution.eda_alert as ea

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monitoring.log")

def hourly_alert_func(query_date, monitoring_name,upload = False):
    monitoring_list = MODEL_CONFIG.get(monitoring_name, '')
    if monitoring_list == '':
        logger.error('monitoring_name provided is wrong!')
    elif isinstance(monitoring_list, list):
        for sub_monitoring in monitoring_list:
            print(sub_monitoring)
            print("EDA检查")
            ea.main(query_date=query_date,in_dict=sub_monitoring)
    else:
        print("EDA检查")
        ea.main(query_date=query_date,in_dict=monitoring_list)

if __name__ == "__main__":
    today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print (today)

    parser = argparse.ArgumentParser(description='use product_name and monitoring_name to run monitoring')
    parser.add_argument('monitoring_name',help='monitoring_name')
    parser.add_argument('upload',help='whether or not upload data to model_monitoring',default=False)
    args = parser.parse_args()
    print(args.monitoring_name)
    hourly_alert_func(query_date=today,monitoring_name=args.monitoring_name,upload=args.upload)

