import pandas as pd
import numpy as np
import sys
import os
import json
from urllib.request import urlopen, quote
# from decimal import *
import requests
import psycopg2

work_dir = '/Users/wuxiongbin/Desktop/反欺诈/图谱/'
sys.path.append(r'/Users/wuxiongbin/Desktop/utils_code/')
from json_analyze import from_json

if not os.path.exists(work_dir + 'data_clean'):
    os.makedirs(work_dir + 'data_clean/')

if not os.path.exists(work_dir + 'import'):
    os.makedirs(work_dir + 'import/')

if not os.path.exists(work_dir + 'application'):
    os.makedirs(work_dir + 'application/')

def get_df_from_pg(SQL, save=False, save_dir=None, filename=None):
    usename = "postgres"
    password = "Mintq2019"
    db = "risk_dm"
    host = "192.168.2.19"
    port = "5432"
    try:
        conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)
        print("Opened database successfully")
    except Exception as e:
        print(e)

    query_df = pd.read_sql(SQL, conn)

    if save:
        query_df.to_csv(save_dir + filename + '.csv', index=False)
        print(filename + ' downloaded!')
    return query_df

bk_sql = """
select distinct t1.order_no as order_no, t1.customer_id as customer_id, t1.business_id as business_id, t1.created_time as created_time,
t2.effective_date as effective_date,
t3.cellphone as cellphone, t3.name as name, t3.age as age, t3.idcardgender, t3.idcardno, t3.idcardaddress as idcardaddress,
t3.companyprovincename as companyprovince, t3.companycityname as companycity, t3.companycountyname as companycounty, t3.companyaddress as companyaddress,
t3.companyname as companyname, t3.companytelephone as companyphone,
t3.refername1 as refername1, t3.referphone1 as referphone1, t3.relationshipname1 as relation1,
t3.refername2 as refername2, t3.referphone2 as referphone2, t3.relationshipname2 as relation2,
t4.latitude as latitude, t4.longitude as longitude,
t4.province, t4.city, t4.district, t4.address, t4.imei
from (select * from dw_bk_bk_core_bk_core_application where rc_time != '1970-01-01 00:00:00') t1
left join dw_bk_bk_core_bk_core_loan t2
on t1.order_no = t2.order_no
left join bk_customer_history_result t3
on t1.order_no = t3.order_no
left join bk_gps_device_result t4
on t1.order_no = t4.order_no
"""

dc_sql = """
select distinct t1.order_no as order_no, t1.customer_id as customer_id, t1.business_id as business_id, t1.created_time as created_time,
t2.effective_date as effective_date,
t3.cellphone as cellphone, t3.name as name, t3.age as age, t3.idcardgender, t3.idcardno, t3.idcardaddress as idcardaddress,
t3.companyprovincename as companyprovince, t3.companycityname as companycity, t3.companycountyname as companycounty, t3.companyaddress as companyaddress,
t3.companyname as companyname, t3.companytelephone as companyphone,
t3.refername1 as refername1, t3.referphone1 as referphone1, t3.relationshipname1 as relation1,
t3.refername2 as refername2, t3.referphone2 as referphone2, t3.relationshipname2 as relation2,
t4.latitude as latitude, t4.longitude as longitude,
t4.province, t4.city, t4.district, t4.address, t4.imei
from (select * from dw_dc_compensatory_cp_core_application where rc_time != '1970-01-01 00:00:00' and business_id='tb') t1
left join dw_dc_compensatory_cp_core_loan t2
on t1.order_no = t2.order_no
left join dc_customer_history_result t3
on t1.order_no = t3.order_no
left join dc_gps_device_result t4
on t1.order_no = t4.order_no
"""

r3_sql_1 = """
select distinct t1.order_no as order_no, t1.customer_id as customer_id, t1.business_id as business_id, t1.created_time as created_time,
t2.effective_date as effective_date,
t3.cellphone as cellphone, t3.name as name, t3.age as age, t3.idcardgender, t3.idcardno, t3.idcardaddress as idcardaddress,
t3.companyprovincename as companyprovince, t3.companycityname as companycity, t3.companycountyname as companycounty, t3.companyaddress as companyaddress,
t3.companyname as companyname, t3.companytelephone as companyphone,
t3.refername1 as refername1, t3.referphone1 as referphone1, t3.relationshipname1 as relation1,
t3.refername2 as refername2, t3.referphone2 as referphone2, t3.relationshipname2 as relation2
from (select * from dw_dc_compensatory_cp_core_application where rc_time != '1970-01-01 00:00:00' and business_id='rong360') t1
left join dw_dc_compensatory_cp_core_loan t2
on t1.order_no = t2.order_no
left join rong360_customer_history_result t3
on t1.order_no = t3.order_no
"""

r3_sql_2 = """
select order_no, program::json ->> 'gps' as gps, program::json ->> 'imei' as imei from dc_program_application where businessid = 'rong360'
"""

bk_df = pd.read_sql(bk_sql, conn)
dc_df = pd.read_sql(dc_sql, conn)
r3_df1 = pd.read_sql(r3_sql_1, conn)
r3_df2 = pd.read_sql(r3_sql_2, conn)

# r3_df1.to_csv(work_dir + 'application/r3_df1.csv', index=False)
# r3_df2.to_csv(work_dir + 'application/r3_df2.csv', index=False)

# r3_df1 = pd.read_csv(work_dir + 'application/r3_df1.csv', dtype={'order_no': str})
# r3_df2 = pd.read_csv(work_dir + 'application/r3_df2.csv', dtype={'order_no': str})

# bk_df = pd.read_csv(work_dir + 'application/dat_bk.csv', dtype={'order_no': str})
# dc_df = pd.read_csv(work_dir + 'application/dat_dc.csv', dtype={'order_no': str})

bk_df = bk_df.drop_duplicates(subset=['order_no'], keep='first').reset_index(drop=True)

r3_df2_new = from_json(r3_df2, 'gps')
r3_df2_new.drop('country', axis=1, inplace=True)
r3_df2_new = r3_df2_new.drop_duplicates().reset_index(drop=True)
r3_df = pd.merge(r3_df1, r3_df2_new, how='left', on='order_no')
r3_df = r3_df[list(bk_df.columns)]

x = ['order_no', 'customer_id', 'created_time', 'effective_date', 'cellphone', 'referphone1', 'referphone2', 'idcardno']
for col in x:
    bk_df[col] = bk_df[col].astype(str)
    dc_df[col] = dc_df[col].astype(str)
    r3_df[col] = r3_df[col].astype(str)

# bk_df.to_csv(work_dir + 'application/dat_bk.csv', index=False)
# dc_df.to_csv(work_dir + 'application/dat_dc.csv', index=False)
# r3_df.to_csv(work_dir + 'application/dat_r3.csv', index=False)


# bk_df = pd.read_csv(work_dir + 'application/dat_bk.csv',
#                           dtype={'order_no': str, 'cellphone': str, 'referphone1': str, 'referphone2': str, 'idcardno': str})
# dc_df = pd.read_csv(work_dir + 'application/dat_dc.csv',
#                           dtype={'order_no': str, 'cellphone': str, 'referphone1': str, 'referphone2': str, 'idcardno': str})
# r3_df = pd.read_csv(work_dir + 'application/dat_r3.csv',
#                           dtype={'order_no': str, 'cellphone': str, 'referphone1': str, 'referphone2': str, 'idcardno': str})
# bk_df.drop(['gps', 'gps_0', 'gps_1', 'gps_2', 'gps_3', 'gps_4'], axis=1, inplace=True)
##################################################################################################
for df in [bk_df, dc_df, r3_df]:
    df['latitude'].replace(' ', np.nan, inplace=True)
    df['longitude'].replace(' ', np.nan, inplace=True)
    df['gps'] = '(' + df.latitude.astype(str) + ', ' + df.longitude.astype(str) + ')'
    df.gps.replace('(nan, nan)', np.nan, inplace=True)
    df.gps.replace('(None, None)', np.nan, inplace=True)

    df['gps_0'] = '(' + round(pd.to_numeric(df.latitude), 0).astype(str) + ', ' \
                     + round(pd.to_numeric(df.longitude), 0).astype(str) + ')'
    df.gps_0.replace('(nan, nan)', np.nan, inplace=True)
    df.gps_0.replace('(None, None)', np.nan, inplace=True)

    df['gps_1'] = '(' + round(pd.to_numeric(df.latitude), 1).astype(str) + ', ' \
                     + round(pd.to_numeric(df.longitude), 1).astype(str) + ')'
    df.gps_1.replace('(nan, nan)', np.nan, inplace=True)
    df.gps_1.replace('(None, None)', np.nan, inplace=True)

    df['gps_2'] = '(' + round(pd.to_numeric(df.latitude), 2).astype(str) + ', ' \
                     + round(pd.to_numeric(df.longitude), 2).astype(str) + ')'
    df.gps_2.replace('(nan, nan)', np.nan, inplace=True)
    df.gps_2.replace('(None, None)', np.nan, inplace=True)

    df['gps_3'] = '(' + round(pd.to_numeric(df.latitude), 3).astype(str) + ', ' \
                     + round(pd.to_numeric(df.longitude), 3).astype(str) + ')'
    df.gps_3.replace('(nan, nan)', np.nan, inplace=True)
    df.gps_3.replace('(None, None)', np.nan, inplace=True)

    df['gps_4'] = '(' + round(pd.to_numeric(df.latitude), 4).astype(str) + ', ' \
                     + round(pd.to_numeric(df.longitude), 4).astype(str) + ')'
    df.gps_4.replace('(nan, nan)', np.nan, inplace=True)
    df.gps_4.replace('(None, None)', np.nan, inplace=True)

# bk_df.to_csv(work_dir + 'data_clean/dat_bk.csv', index=False)
# dc_df.to_csv(work_dir + 'data_clean/dat_dc.csv', index=False)
# r3_df.to_csv(work_dir + 'data_clean/dat_r3.csv', index=False)
##################################################################################################

r3_df['company_district'] = r3_df['companyprovince'].astype(str) + r3_df[
    'companycity'].astype(str) + r3_df['companycounty'].astype(str)
r3_df['company_district'] = r3_df['company_district'].str.replace('nan', '')
r3_df['company_district'] = r3_df['company_district'].str.replace('None', '')

r3_df['company_address'] = r3_df['companyaddress'].str.replace(' ', '')

r3_df['cs_index'] = r3_df['idcardaddress']. \
    apply(lambda x:
          str(x).find('社区') + 2
          if str(x).find('社区') != -1
          else (str(x).find('村') + 1
                if str(x).find('村') != -1
                else (str(x).find('小区') + 2
                      if str(x).find('小区') != -1
                      else str(x).find('弄') + 1)))

r3_df['idcard_address_cs'] = r3_df.apply(lambda x: str(x['idcardaddress'])[:x['cs_index']], axis=1)
r3_df.drop('cs_index', axis=1, inplace=True)

r3_df['zh_index'] = r3_df['idcardaddress']. \
    apply(lambda x:
          str(x).find('镇', 4) + 1
          if str(x).find('镇', 4) != -1
          else (str(x).find('乡') + 1
                if str(x).find('乡') != -1
                else (str(x).find('街道') + 2
                      if str(x).find('街道') != -1
                      else 0)))
r3_df['idcard_address_zh'] = r3_df.apply(lambda x: str(x['idcardaddress'])[:x['zh_index']], axis=1)
r3_df.drop('zh_index', axis=1, inplace=True)

r3_df.referphone1 = r3_df.referphone1.str.replace('.0', '', regex=False)
r3_df.referphone2 = r3_df.referphone2.str.replace('.0', '', regex=False)
r3_df['refer1'] = '(' + r3_df['refername1'] + ', ' + r3_df['referphone1'] + ')'
r3_df['refer2'] = '(' + r3_df['refername2'] + ', ' + r3_df['referphone2'] + ')'

r3_df.to_csv(work_dir + 'data_clean/dat_r3.csv', index=False)

##################################################################################################

dc_df['company_district'] = dc_df['companyprovince'].astype(str) + dc_df[
    'companycity'].astype(str) + dc_df['companycounty'].astype(str)
dc_df['company_district'] = dc_df['company_district'].str.replace('nan', '')

dc_df['companyprovince'].replace({np.nan: ''}, inplace=True)
dc_df['companycity'].replace({np.nan: ''}, inplace=True)
dc_df['companycounty'].replace({np.nan: ''}, inplace=True)
dc_df['companyaddress'].replace({np.nan: ''}, inplace=True)

dc_df['companyaddress'] = dc_df.apply(lambda x: x['companyaddress'].replace(x['companyprovince'], '', 1), axis=1)
dc_df['companyaddress'] = dc_df.apply(lambda x: x['companyaddress'].replace(x['companycity'], '', 1), axis=1)
dc_df['companyaddress'] = dc_df.apply(lambda x: x['companyaddress'].replace(x['companycounty'], '', 1), axis=1)

dc_df['company_address'] = dc_df.\
    apply(lambda x: x['companyprovince'] + x['companycity'] + x['companycounty'] + x['companyaddress'], axis=1)


dc_df['cs_index'] = dc_df['idcardaddress']. \
    apply(lambda x:
          str(x).find('社区') + 2
          if str(x).find('社区') != -1
          else (str(x).find('村') + 1
                if str(x).find('村') != -1
                else (str(x).find('小区') + 2
                      if str(x).find('小区') != -1
                      else str(x).find('弄') + 1)))

dc_df['idcard_address_cs'] = dc_df.apply(lambda x: str(x['idcardaddress'])[:x['cs_index']], axis=1)
dc_df.drop('cs_index', axis=1, inplace=True)

dc_df['zh_index'] = dc_df['idcardaddress']. \
    apply(lambda x:
          str(x).find('镇', 4) + 1
          if str(x).find('镇', 4) != -1
          else (str(x).find('乡') + 1
                if str(x).find('乡') != -1
                else (str(x).find('街道') + 2
                      if str(x).find('街道') != -1
                      else 0)))
dc_df['idcard_address_zh'] = dc_df.apply(lambda x: str(x['idcardaddress'])[:x['zh_index']], axis=1)
dc_df.drop('zh_index', axis=1, inplace=True)

dc_df['refer1'] = '(' + dc_df['refername1'] + ', ' + dc_df['referphone1'] + ')'
dc_df['refer2'] = '(' + dc_df['refername2'] + ', ' + dc_df['referphone2'] + ')'

dc_df.to_csv(work_dir + 'data_clean/dat_dc.csv', index=False)

##################################################################################################
bk_df['company_district'] = bk_df['companyprovince'].astype(str) + bk_df[
    'companycity'].astype(str) + bk_df['companycounty'].astype(str)
bk_df['company_district'] = bk_df['company_district'].str.replace('nan', '')

bk_df['company_address'] = bk_df['companyaddress']

bk_df['cs_index'] = bk_df['idcardaddress']. \
    apply(lambda x:
          str(x).find('社区') + 2
          if str(x).find('社区') != -1
          else (str(x).find('村') + 1
                if str(x).find('村') != -1
                else (str(x).find('小区') + 2
                      if str(x).find('小区') != -1
                      else str(x).find('弄') + 1)))

bk_df['idcard_address_cs'] = bk_df.apply(lambda x: str(x['idcardaddress'])[:x['cs_index']], axis=1)
bk_df.drop('cs_index', axis=1, inplace=True)

bk_df['zh_index'] = bk_df['idcardaddress']. \
    apply(lambda x:
          str(x).find('镇', 4) + 1
          if str(x).find('镇', 4) != -1
          else (str(x).find('乡') + 1
                if str(x).find('乡') != -1
                else (str(x).find('街道') + 2
                      if str(x).find('街道') != -1
                      else 0)))
bk_df['idcard_address_zh'] = bk_df.apply(lambda x: str(x['idcardaddress'])[:x['zh_index']], axis=1)
bk_df.drop('zh_index', axis=1, inplace=True)

bk_df['refer1'] = '(' + bk_df['refername1'] + ', ' + bk_df['referphone1'] + ')'
bk_df['refer2'] = '(' + bk_df['refername2'] + ', ' + bk_df['referphone2'] + ')'

bk_df.to_csv(work_dir + 'data_clean/dat_bk.csv', index=False)

##################################################################################################

# address = bk_base_info.idcardaddress[52]
# address = '江苏省江阴市临港街道江南花园33幢401室'
# address = '飞跃东路282号'
# address = '北京市海淀区上地十街10号'
# address = bk_base_info.idcardaddress[5]
# address = bk_base_info.idcardaddress[6]
# address = bk_base_info.idcardaddress[1]


# address to gps
def getlnglat(address, api_type):
    """

    :param address: address to be transferred to latitude and longitude
    :param api_type: 'baidu' or 'gaode'
    :return: json dict
    """
    if api_type == 'baidu':
        url = 'http://api.map.baidu.com/geocoding/v3/?ret_coordtype=bd09ll&address='
        ak = 'aQBoE64elEMcrwK5CXCD2qSuVn8RVii6'  # 需填入自己申请应用后生成的ak
        # add = quote(address)  # 本文城市变量为中文，为防止乱码，先用quote进行编码
        url2 = url + address + '&output=json' + "&ak=" + ak  # + '&callback=showLocation'
        response = requests.get(url2)
        temp = response.json()
        lng = str(temp['result']['location']['lng'])
        lat = str(temp['result']['location']['lat'])

    if api_type == 'gaode':
        key = '7fe6cfa1dfb3f8f0b559ae3c1cc58c53'
        url2 = 'https://restapi.amap.com/v3/geocode/geo?key={}&address={}'.format(key, address)
        response = requests.get(url2)
        temp = response.json()
        lng = temp['geocodes'][0]['location'].split(',')[0]
        lat = temp['geocodes'][0]['location'].split(',')[1]
    """
    req = urlopen(url2)
    res = req.read().decode()
    temp = json.loads(res)
    """
    return lat, lng


# gps to address
def getAddress(latitude, longitude, api_type):
    """

    :param longitude: float
    :param latitude: float
    :param api_type: 'baidu' or 'gaode'
    :return: json dict
    """
    if api_type == 'baidu':
        ak = 'aQBoE64elEMcrwK5CXCD2qSuVn8RVii6'
        url = 'http://api.map.baidu.com/reverse_geocoding/v3/?ak=' + ak + '&output=json&coordtype=bd09ll&location=' \
            + latitude + ',' + longitude + '&extensions_road=true&extensions_town=true'

    if api_type == 'gaode':
        key = '7fe6cfa1dfb3f8f0b559ae3c1cc58c53'
        url = 'https://restapi.amap.com/v3/geocode/regeo?key={}&location={},{}'.format(key, longitude, latitude)
    response = requests.get(url)
    temp = response.json()
    """
    req = urlopen(url)
    res = req.read().decode()
    temp = json.loads(res)
    """
    return temp


# lat = r3_df.latitude[14]
# lng = r3_df.longitude[14]
# lat1, lng1 = getlnglat(address, api_type='baidu')
# lat2, lng2 = getlnglat(address, api_type='gaode')

# add = getAddress(lat2, lng2, api_type='gaode')

##################################################################################################

bk_df = pd.read_csv(work_dir + 'data_clean/dat_bk.csv',
                           dtype={'order_no': str, 'cellphone': str, 'referphone1': str, 'referphone2': str})
dc_df = pd.read_csv(work_dir + 'data_clean/dat_dc.csv',
                           dtype={'order_no': str, 'cellphone': str, 'referphone1': str, 'referphone2': str})
r3_df = pd.read_csv(work_dir + 'data_clean/dat_r3.csv',
                           dtype={'order_no': str, 'cellphone': str, 'referphone1': str, 'referphone2': str})

for df in [bk_df, dc_df, r3_df]:
    df['imei'] = df['imei'].replace(' ', np.nan)
    df['imei'] = df['imei'].replace('00000000-0000-0000-0000-000000000000', np.nan)

bk_df = bk_df[list(dc_df.columns)]
r3_df = r3_df[list(dc_df.columns)]

base_info = pd.concat([dc_df, bk_df], ignore_index=True)
base_info = pd.concat([base_info, r3_df], ignore_index=True)
base_info.rename(columns={'order_no': 'order_no:ID(order_no-ID)'}, inplace=True)
base_info = base_info.reset_index().rename(columns={'index': 'order_id'})
base_info['order_id'] += 1
base_info.rename(columns={'companyphone': 'company_phone'}, inplace=True)
base_info.to_csv(work_dir + 'import/base_info.csv', index=False)

# base_info = pd.read_csv(work_dir + 'import/base_info.csv', dtype={'cellphone': str, 'referphone1': str, 'referphone2': str})

company_address = base_info[['company_address']].dropna().reset_index(drop=True)
company_address = company_address.drop_duplicates().reset_index(drop=True)
company_address = company_address[company_address['company_address'] != ' '].reset_index(drop=True)
company_address = company_address[company_address['company_address'] != ''].reset_index(drop=True)
company_address = company_address.reset_index().rename(columns={'index': 'company_address_id:ID(company_address-ID)'})
company_address['company_address_id:ID(company_address-ID)'] += 1

company_address.to_csv(work_dir + 'import/company_address_n.csv', index=False)

"""
base_info = pd.read_csv(work_dir + 'import/base_info.csv',
                        dtype={'order_no:ID(order_no-ID)': str, 'cellphone': str, 'referphone1': str, 'referphone2': str})
company_address = pd.read_csv(work_dir + 'import/company_address_n.csv')
"""

company_address_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'company_address', 'companyname', 'companyphone']], company_address, on='company_address')
company_address_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'company_address_id:ID(company_address-ID)': ':END_ID(company_address-ID)'}, inplace=True)
company_address_r.to_csv(work_dir + 'import/company_address_r.csv', index=False)


company_district = base_info[['company_district']].dropna().reset_index(drop=True)
company_district = company_district.drop_duplicates().reset_index(drop=True)
company_district = company_district[company_district['company_district'] != ' '].reset_index(drop=True)
company_district['company_district'] = company_district['company_district'].str.replace('None', '')
company_district = company_district[company_district['company_district'] != ''].reset_index(drop=True)

company_district = company_district.reset_index().rename(columns={'index': 'company_district_id:ID(company_district-ID)'})
company_district['company_district_id:ID(company_district-ID)'] += 1

company_district.to_csv(work_dir + 'import/company_district_n.csv', index=False)

company_district_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'company_district']], company_district, on='company_district')
company_district_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'company_district_id:ID(company_district-ID)': ':END_ID(company_district-ID)'}, inplace=True)
company_district_r.to_csv(work_dir + 'import/company_district_r.csv', index=False)



company_name = base_info[['companyname']].dropna().reset_index(drop=True)
company_name = company_name.drop_duplicates().reset_index(drop=True)
company_name = company_name[company_name['companyname'] != ' '].reset_index(drop=True)
company_name['companyname'] = company_name['companyname'].str.replace('None', '')
company_name = company_name[company_name['companyname'] != ''].reset_index(drop=True)

company_name = company_name.reset_index().rename(columns={'index': 'company_name_id:ID(company_name-ID)'})
company_name['company_name_id:ID(company_name-ID)'] += 1

company_name.to_csv(work_dir + 'import/company_name_n.csv', index=False)

company_name_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'companyname', 'company_address']], company_name, on='companyname')
company_name_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'company_name_id:ID(company_name-ID)': ':END_ID(company_name-ID)'}, inplace=True)
company_name_r.to_csv(work_dir + 'import/company_name_r.csv', index=False)


company_phone = base_info[['companyphone']].dropna().reset_index(drop=True)
company_phone = company_phone.drop_duplicates().reset_index(drop=True)
company_phone = company_phone[company_phone['companyphone'] != ' '].reset_index(drop=True)
company_phone['companyphone'] = company_phone['companyphone'].str.replace('None', '')
company_phone = company_phone[company_phone['companyphone'] != ''].reset_index(drop=True)

company_phone.companyphone = company_phone.companyphone.apply(lambda x: x.replace('-', '', 1) if (x.find('-') <= 4 and x.find('-') != -1) else x)
company_phone.companyphone = company_phone.companyphone.apply(lambda x: x.replace('-', '', 1) if x.find('-') == len(x) - 1 else x)
company_phone.companyphone = company_phone.companyphone.str.replace('\(|\)|\s','',regex=True)
company_phone = company_phone.drop_duplicates().reset_index(drop=True)

company_phone = company_phone.reset_index().rename(columns={'index': 'company_phone_id:ID(company_phone-ID)'})
company_phone['company_phone_id:ID(company_phone-ID)'] += 1
company_phone.rename(columns={'companyphone': 'company_phone'}, inplace=True)
company_phone.to_csv(work_dir + 'import/company_phone_n.csv', index=False)

company_phone_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'company_phone', 'company_address', 'companyname']], company_phone, on='company_phone')
company_phone_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'company_phone_id:ID(company_phone-ID)': ':END_ID(company_phone-ID)'}, inplace=True)

company_phone_r.to_csv(work_dir + 'import/company_phone_r.csv', index=False)

idcard_address = base_info[['idcardaddress']].dropna().reset_index(drop=True)
idcard_address = idcard_address.drop_duplicates().reset_index(drop=True)
idcard_address = idcard_address[idcard_address['idcardaddress'] != ' '].reset_index(drop=True)
idcard_address['idcardaddress'] = idcard_address['idcardaddress'].str.replace('None', '')
idcard_address = idcard_address[idcard_address['idcardaddress'] != ''].reset_index(drop=True)

idcard_address = idcard_address.reset_index().rename(columns={'index': 'idcard_address_id:ID(idcard_address-ID)'})
idcard_address['idcard_address_id:ID(idcard_address-ID)'] += 1

idcard_address.to_csv(work_dir + 'import/idcard_address_n.csv', index=False)

idcard_address_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'idcardaddress']], idcard_address, on='idcardaddress')
idcard_address_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'idcard_address_id:ID(idcard_address-ID)': ':END_ID(idcard_address-ID)'}, inplace=True)
idcard_address_r.to_csv(work_dir + 'import/idcard_address_r.csv', index=False)


idcard_address_cs = base_info[['idcard_address_cs']].dropna().reset_index(drop=True)
idcard_address_cs = idcard_address_cs.drop_duplicates().reset_index(drop=True)
idcard_address_cs = idcard_address_cs[idcard_address_cs['idcard_address_cs'] != ' '].reset_index(drop=True)
idcard_address_cs['idcard_address_cs'] = idcard_address_cs['idcard_address_cs'].str.replace('None', '')
idcard_address_cs = idcard_address_cs[idcard_address_cs['idcard_address_cs'] != ''].reset_index(drop=True)

idcard_address_cs = idcard_address_cs.reset_index().rename(columns={'index': 'idcard_address_cs_id:ID(idcard_address_cs-ID)'})
idcard_address_cs['idcard_address_cs_id:ID(idcard_address_cs-ID)'] += 1

idcard_address_cs.to_csv(work_dir + 'import/idcard_address_cs_n.csv', index=False)

idcard_address_cs_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'idcard_address_cs']], idcard_address_cs, on='idcard_address_cs')
idcard_address_cs_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'idcard_address_cs_id:ID(idcard_address_cs-ID)': ':END_ID(idcard_address_cs-ID)'}, inplace=True)
idcard_address_cs_r.to_csv(work_dir + 'import/idcard_address_cs_r.csv', index=False)



idcard_address_zh = base_info[['idcard_address_zh']].dropna().reset_index(drop=True)
idcard_address_zh = idcard_address_zh.drop_duplicates().reset_index(drop=True)
idcard_address_zh = idcard_address_zh[idcard_address_zh['idcard_address_zh'] != ' '].reset_index(drop=True)
idcard_address_zh['idcard_address_zh'] = idcard_address_zh['idcard_address_zh'].str.replace('None', '')
idcard_address_zh = idcard_address_zh[idcard_address_zh['idcard_address_zh'] != ''].reset_index(drop=True)

idcard_address_zh = idcard_address_zh.reset_index().rename(columns={'index': 'idcard_address_zh_id:ID(idcard_address_zh-ID)'})
idcard_address_zh['idcard_address_zh_id:ID(idcard_address_zh-ID)'] += 1

idcard_address_zh.to_csv(work_dir + 'import/idcard_address_zh_n.csv', index=False)

idcard_address_zh_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'idcard_address_zh']], idcard_address_zh, on='idcard_address_zh')
idcard_address_zh_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'idcard_address_zh_id:ID(idcard_address_zh-ID)': ':END_ID(idcard_address_zh-ID)'}, inplace=True)
idcard_address_zh_r.to_csv(work_dir + 'import/idcard_address_zh_r.csv', index=False)


for i in ['0', '1', '2', '3', '4']:
    a = base_info[['gps_' + i]].dropna().reset_index(drop=True)
    a = a.drop_duplicates().reset_index(drop=True)
    a = a[a['gps_' + i] != ' '].reset_index(drop=True)
    a['gps_' + i] = a['gps_' + i].str.replace('None', '')
    a = a[a['gps_' + i] != ''].reset_index(drop=True)

    a = a.reset_index().rename(columns={'index': 'gps_' + i + '_id:ID(gps_' + i + '-ID)'})
    a['gps_' + i + '_id:ID(gps_' + i + '-ID)'] += 1

    a.to_csv(work_dir + 'import/gps_' + i + '_n.csv', index=False)

    a_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'gps_' + i]], a, on='gps_' + i)
    a_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)',
                        'gps_' + i + '_id:ID(gps_' + i + '-ID)': ':END_ID(gps_' + i + '-ID)'}, inplace=True)
    # a_r = a_r[[':START_ID(order_no-ID)', ':END_ID(gps_' + i + '-ID)', 'gps_' + i]]
    a_r.to_csv(work_dir + 'import/gps_' + i + '_r.csv', index=False)


refer1 = base_info[['order_no:ID(order_no-ID)', 'refer1', 'relation1']].dropna().reset_index(drop=True).rename(columns={'refer1': 'refer', 'relation1':'relation'})
refer2 = base_info[['order_no:ID(order_no-ID)', 'refer2', 'relation2']].dropna().reset_index(drop=True).rename(columns={'refer2': 'refer', 'relation2':'relation'})
refer = pd.concat([refer1, refer2], ignore_index=True)
refer = refer.drop_duplicates().reset_index(drop=True)

refer_n = refer[['refer']]
refer_n = refer_n.drop_duplicates().reset_index(drop=True)
refer_n = refer_n.reset_index().rename(columns={'index': 'refer_id:ID(refer-ID)'})
refer_n['refer_id:ID(refer-ID)'] += 1

refer_n.to_csv(work_dir + 'import/refer_n.csv', index=False)

refer_r = pd.merge(refer[['order_no:ID(order_no-ID)', 'refer', 'relation']], refer_n, on='refer')
refer_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'refer_id:ID(refer-ID)': ':END_ID(refer-ID)'}, inplace=True)
refer_r.to_csv(work_dir + 'import/refer_r.csv', index=False)

imei = base_info[['imei']].dropna().reset_index(drop=True)
imei = imei.drop_duplicates().reset_index(drop=True)
imei = imei[imei['imei'] != ' '].reset_index(drop=True)
imei['imei'] = imei['imei'].str.replace('None', '')
imei = imei[imei['imei'] != ''].reset_index(drop=True)

imei = imei.reset_index().rename(columns={'index': 'imei_id:ID(imei-ID)'})
imei['imei_id:ID(imei-ID)'] += 1

imei.to_csv(work_dir + 'import/imei_n.csv', index=False)

imei_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'imei']], imei, on='imei')
imei_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'imei_id:ID(imei-ID)': ':END_ID(imei-ID)'}, inplace=True)
imei_r.to_csv(work_dir + 'import/imei_r.csv', index=False)


idcardno = base_info[['idcardno']].dropna().reset_index(drop=True)
idcardno = idcardno.drop_duplicates().reset_index(drop=True)
idcardno = idcardno[idcardno['idcardno'] != ' '].reset_index(drop=True)
idcardno['idcardno'] = idcardno['idcardno'].str.replace('None', '')
idcardno = idcardno[idcardno['idcardno'] != ''].reset_index(drop=True)

idcardno = idcardno.reset_index().rename(columns={'index': 'idcardno_id:ID(idcardno-ID)'})
idcardno['idcardno_id:ID(idcardno-ID)'] += 1

idcardno.to_csv(work_dir + 'import/idcardno_n.csv', index=False)

idcardno_r = pd.merge(base_info[['order_no:ID(order_no-ID)', 'idcardno']], idcardno, on='idcardno')
idcardno_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'idcardno_id:ID(idcardno-ID)': ':END_ID(idcardno-ID)'}, inplace=True)
idcardno_r.to_csv(work_dir + 'import/idcardno_r.csv', index=False)

cellphone1 = base_info[['order_no:ID(order_no-ID)', 'cellphone']].dropna().drop_duplicates().reset_index(drop=True)
cellphone2 = base_info[['order_no:ID(order_no-ID)', 'referphone1']].dropna().drop_duplicates().reset_index(drop=True).rename(columns={'referphone1': 'cellphone'})
cellphone3 = base_info[['order_no:ID(order_no-ID)', 'referphone2']].dropna().drop_duplicates().reset_index(drop=True).rename(columns={'referphone2': 'cellphone'})

cellphone = pd.concat([cellphone1, cellphone2], ignore_index=True)
cellphone = pd.concat([cellphone, cellphone3], ignore_index=True)

cellphone_n = cellphone[['cellphone']].drop_duplicates().reset_index().rename(columns={'index': 'cellphone:ID(cellphone-ID)'})
cellphone_n['cellphone:ID(cellphone-ID)'] += 1
cellphone_n.to_csv(work_dir + 'import/cellphone_n.csv', index=False)

cellphone_r = pd.merge(cellphone1, cellphone_n, on='cellphone')\
    .rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'cellphone:ID(cellphone-ID)': ':END_ID(cellphone-ID)'})
cellphone_r.to_csv(work_dir + 'import/cellphone_r.csv', index=False)

referphone_r = pd.merge(pd.concat([cellphone1, cellphone2], ignore_index=True).drop_duplicates().reset_index(drop=True), cellphone_n, on='cellphone')\
    .rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)', 'cellphone:ID(cellphone-ID)': ':END_ID(cellphone-ID)'})
referphone_r.to_csv(work_dir + 'import/referphone_r.csv', index=False)

"""
import numpy as np
files_csv = np.sort(files_csv)
files_csv = ['company_address_r.csv', 'company_district_r.csv',
       'company_name_r.csv', 'company_phone_r.csv',
       'idcard_address_cs_r.csv', 'idcard_address_r.csv',
       'idcard_address_zh_r.csv', 'refer_r.csv']

a = pd.read_csv(work_dir + '/import/' + files_csv[7])
a.columns
a = a[[':START_ID(order_no-ID)', ':END_ID(refer-ID)', 'refer', 'relation']]
a.to_csv(work_dir + 'import/' + files_csv[7], index=False)
"""

flag_bk = pd.read_csv(work_dir + 'flag_bk.csv')
flag_xjm = pd.read_csv(work_dir + 'flag_dc.csv')

flag = pd.concat([flag_bk, flag_xjm], ignore_index=True)
flag.rename(columns={'order_no': 'order_no:ID(order_no-ID)'}, inplace=True)


base_info = pd.merge(base_info, flag[['order_no:ID(order_no-ID)', 'flag']], on='order_no:ID(order_no-ID)', how='left')
base_info.flag.fillna(-9, inplace=True)
base_info.rename(columns={'flag': 'flag:int'}, inplace=True)
base_info['flag:int'] = base_info['flag:int'].astype(int)

base_info['flag_1:int'] = base_info['flag:int'].map(lambda x: 1 if x in [1, 6, 10] else (0 if x in [0, 2, 3, 4, 5, 7, 8, 9] else (-3 if x==11 else x)))
base_info['flag_2:int'] = base_info['flag:int'].map(lambda x: 1 if x in [1, 3, 4, 6, 8, 10] else (0 if x in [0, 5, 9] else (-3 if x in [2, 7, 11] else x)))

base_info.to_csv(work_dir + 'test2/basic_info.csv', index=False)

