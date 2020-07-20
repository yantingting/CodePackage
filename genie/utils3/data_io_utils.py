# encoding=utf8
import os
import pickle as pickle
import simplejson as json
import codecs
import smtplib # python3 自带
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders


import pandas as pd
import numpy as np
from prestodb.client import *
# import MySQLdb
import configparser
# from impala.dbapi import connect
from pyhive import hive
import psycopg2
from jinja2 import Template

import time


def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        result = func(*args, **kw)
        print('current Function [%s] run time is %.2f'%(func.__name__ ,time.time() - local_time))
        return result
    return wrapper

def get_psi_index_from_dist(before, after):
    if round(before,2)==0 or round(after,2)==0 or round(before/after,2)==0:
        psi_index=0
    else:
        psi_index = (round(before,2)-round(after,2))*np.log(round(before,2)/round(after,2))
    return round(psi_index,3)

@print_run_time
def get_df_from_pg(SQL):
    usename = "postgres"
    password = "Mintq2019"
    db = "risk_dm"
    host = "192.168.2.19"
    port = "5432"
    try:
        conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)
        # print("Opened database successfully")
    except Exception as e:
        print(e)
    cur = conn.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    df = pd.DataFrame(rows,columns=[i.name for i in cur.description])
    df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
    return df

@print_run_time
def upload_df_to_pg(sql):
    """
    If sql is table creation, then rows = [[True]] is succeed.
    If sql is insert query, then rows = [[4]] where 4 is changeable according to
    number of rows inserted
    """
    usename = "postgres"
    password = "Mintq2019"
    db = "risk_dm"
    host = "192.168.2.19"
    port = "5432"
    try:
        conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)
        # print("Opened database successfully")
    except Exception as e:
        print(e)
    cur = conn.cursor()
    cur.execute(sql)
    try:
        conn.commit()
        return 'success'
    except:
        print("upload failed, check query")
        return 'failure'


# impala连接hive
# def get_df_from_hive(sql,user,pw):
#     conn = connect(host='192.168.2.62', port=10000, user=user, password=pw,auth_mechanism='PLAIN')
#     cursor = conn.cursor()
#     cursor.execute(sql)
#     results = cursor.fetchall()
#     df = pd.DataFrame(results,columns=[i[0] for i in cursor.description])
#     df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
#     return df

conf = configparser.ConfigParser()
# DATABASE_CONFIGURE_FILE = os.path.join(os.path.split(os.path.realpath('mysql.cfg'))[0],'mysql.cfg')
DATABASE_CONFIGURE_FILE = os.path.join(os.path.split(os.path.realpath(__file__))[0],'mysql.cfg')
with open(DATABASE_CONFIGURE_FILE, 'r') as cfgfile:
    conf.readfp(cfgfile)

# pyhive连接hive
@print_run_time
def get_df_from_hive(sql):
    conn = hive.Connection(host='192.168.2.43', port=10000, \
    username=conf.get('hive', 'USER'), password=conf.get('hive', 'PASSWORD'), \
    database='risk_dm',auth='LDAP')
    df = pd.read_sql(sql, conn)
    df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
    return df

def presto_read_sql_df(sql):
    request = PrestoRequest(host=conf.get('presto', 'PRESTO_HOST'), port=conf.get('presto', 'PRESTO_PORT')\
                    , user=conf.get('presto', 'PRESTO_USER'), source=conf.get('presto', 'PRESTO_PASSWORD'))
    query = PrestoQuery(request, sql)
    rows = list(query.execute())
    columns = [col['name'] for col in query._columns]
    df = pd.DataFrame(rows, columns=columns)
    return df

def upload_df_by_sql(df,sql):
    var_list =[]
    for index, row in df.iterrows():
        var_list.append(tuple(row))
    num_lines = 5000
    upload_status = {}
    for line in range(0, len(var_list), num_lines):
        # print(line)
        insert_sql = Template(sql).render(var_list=var_list[line:line+num_lines])[:-2]
        status = upload_df_to_pg(insert_sql)



def presto_upload_data(sql):
    """
    If sql is table creation, then rows = [[True]] is succeed.
    If sql is insert query, then rows = [[4]] where 4 is changeable according to
    number of rows inserted
    """
    request = PrestoRequest(host=conf.get('presto', 'PRESTO_HOST'), port=conf.get('presto', 'PRESTO_PORT')\
                    , user=conf.get('presto', 'PRESTO_USER'), source=conf.get('presto', 'PRESTO_PASSWORD'))
    query = PrestoQuery(request, sql)
    try:
        rows = list(query.execute())
        return 'success'
    except:
        print("upload failed, check query")
        return 'failure'


def save_data_to_pickle(obj, file_path, file_name):
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'wb') as outfile:
        pickle.dump(obj, outfile)


def save_data_to_python2_pickle(obj, file_path, file_name):
    """
    python2默认protocol=0，最大可取2，python2存储过程中protocol=2，则python3可读
    python3默认protocol=3，最大可取4，python3存储过程中protocol=0，则python2可读
    """
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'wb') as outfile:
        pickle.dump(obj, outfile,protocol=0)


def load_data_from_pickle(file_path, file_name):
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'rb') as infile:
        result = pickle.load(infile)
    return result

def load_data_from_python2_pickle(file_path, file_name):
    """
    python2与python3在编码格式上不同，python3load过程默认的encoding='ASCII'
    """
    file_path_name = os.path.join(file_path, file_name)
    with open(file_path_name, 'rb') as infile:
        result = pickle.load(infile,encoding='iso-8859-1')
    return result


def save_data_dict_to_pickle(data_dict, file_path, file_name):
    for vintage, sub_data in list(data_dict.items()):
        print(vintage)
        file_path_name = os.path.join(file_path, vintage + '_' + file_name)
        with open(file_path_name, 'wb') as outfile:
            pickle.dump(sub_data, outfile)


def load_data_dict_to_pickle(data_dict_keys, file_path, file_name):
    # data_dict_keys = ['201612', '201701', '201610', '201611', '201607', '201606', '201609', '201608', 'additional']
    result = {}
    for vintage in data_dict_keys:
        file_path_name = os.path.join(file_path, vintage + '_' + file_name)
        with open(file_path_name, 'rb') as infile:
            result[vintage] = pickle.load(infile)
    return result


def save_data_to_json(data_dict, file_path, file_name):
    with codecs.open(os.path.join(file_path, file_name), 'wb', encoding='utf-8') as outfile:
        json.dump(data_dict, outfile, ensure_ascii=False, indent='\t')


def load_data_from_json(file_path, file_name):
    with codecs.open(os.path.join(file_path, file_name), 'rb', encoding='utf-8') as infile:
        data_dict = json.load(infile)
    return data_dict


def send_email(subject, email_content, recipients=['xiangyu.hu@pintec.com'], attachment_path='', attachment_name=[]):
    sender = conf.get('email','EMAIL_USER')
    pwd = conf.get('email', 'EMAIL_PASSWORD')
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    main_content = MIMEText(email_content, 'plain', 'utf-8')
    msg.attach(main_content)
    ctype = 'application/octet-stream'
    maintype, subtype = ctype.split('/', 1)
    # 附件-文件
    if len(attachment_name) > 0:
        for attachment_name1 in attachment_name:
            attachment_path_name = os.path.join(attachment_path, attachment_name1)
            file = MIMEBase(maintype, subtype)
            file.set_payload(open(attachment_path_name, 'rb').read())
            file.add_header('Content-Disposition', 'attachment', filename=attachment_name1)
            encoders.encode_base64(file)
            msg.attach(file)

    # 发送
    smtp = smtplib.SMTP()
    smtp.connect(conf.get('email', 'EMAIL_HOST'), 25)
    smtp.login(sender, pwd)
    smtp.sendmail(sender, recipients, msg.as_string())
    smtp.quit()
    print('success')
