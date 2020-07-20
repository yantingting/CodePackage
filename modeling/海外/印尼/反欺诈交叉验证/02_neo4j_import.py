# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 15:04:44 2019

@author: yuexin
"""
import os
import time
import pandas as pd
import numpy as np
import psycopg2

work_dir = 'D:/Model Development/201911 IDN Anti-fraud/Graph Analysis/02 Data/raw data/'
neo4j_dir = 'D:/Model Development/201911 IDN Anti-fraud/Graph Analysis/02 Data/neo4j import/'

usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)


''' **************************** cust **************************** '''
r_cust = pd.read_csv(work_dir + 'r_cust.csv', dtype = str)

''' id card address'''
idcard_address = r_cust[['id_card_address']].dropna().reset_index(drop=True).rename(columns={'id_card_address': 'idcardaddress'})
idcard_address = idcard_address.drop_duplicates().reset_index(drop=True)
idcard_address = idcard_address[idcard_address['idcardaddress'] != ' '].reset_index(drop=True)
idcard_address['idcardaddress'] = idcard_address['idcardaddress'].str.replace('None', '')
idcard_address = idcard_address[idcard_address['idcardaddress'] != ''].reset_index(drop=True)
idcard_address = idcard_address.reset_index().rename(columns={'index': 'idcard_address_id:ID(idcard_address-ID)'})
idcard_address['idcard_address_id:ID(idcard_address-ID)'] += 1

idcard_address.to_csv(neo4j_dir + 'idcard_address_n.csv', index=False)

idcard_address_r = pd.merge(r_cust[['loan_id', 'id_card_address']].rename(columns={'id_card_address': 'idcardaddress'}), idcard_address, on='idcardaddress')
idcard_address_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'idcard_address_id:ID(idcard_address-ID)': ':END_ID(idcard_address-ID)'}, inplace=True)

idcard_address_r.to_csv(neo4j_dir + 'idcard_address_r.csv', index=False)

'''id card dist1'''
idcard_dist1 = r_cust[['id_card_dist1']].dropna().reset_index(drop=True).rename(columns={'id_card_dist1': 'idcarddist1'})
idcard_dist1 = idcard_dist1.drop_duplicates().reset_index(drop=True)
idcard_dist1 = idcard_dist1[idcard_dist1['idcarddist1'] != ' '].reset_index(drop=True)
idcard_dist1['idcarddist1'] = idcard_dist1['idcarddist1'].str.replace('None', '')
idcard_dist1 = idcard_dist1[idcard_dist1['idcarddist1'] != ''].reset_index(drop=True)
idcard_dist1 = idcard_dist1.reset_index().rename(columns={'index': 'idcard_dist1_id:ID(idcard_dist1-ID)'})
idcard_dist1['idcard_dist1_id:ID(idcard_dist1-ID)'] += 1

idcard_dist1.to_csv(neo4j_dir + 'idcard_dist1_n.csv', index=False)

idcard_dist1_r = pd.merge(r_cust[['loan_id', 'id_card_dist1']].rename(columns={'id_card_dist1': 'idcarddist1'}), idcard_dist1, on='idcarddist1')
idcard_dist1_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'idcard_dist1_id:ID(idcard_dist1-ID)': ':END_ID(idcard_dist1-ID)'}, inplace=True)

idcard_dist1_r.to_csv(neo4j_dir + 'idcard_dist1_r.csv', index=False)

'''id card dist2'''
idcard_dist2 = r_cust[['id_card_dist2']].dropna().reset_index(drop=True).rename(columns={'id_card_dist2': 'idcarddist2'})
idcard_dist2 = idcard_dist2.drop_duplicates().reset_index(drop=True)
idcard_dist2 = idcard_dist2[idcard_dist2['idcarddist2'] != ' '].reset_index(drop=True)
idcard_dist2['idcarddist2'] = idcard_dist2['idcarddist2'].str.replace('None', '')
idcard_dist2 = idcard_dist2[idcard_dist2['idcarddist2'] != ''].reset_index(drop=True)
idcard_dist2 = idcard_dist2.reset_index().rename(columns={'index': 'idcard_dist2_id:ID(idcard_dist2-ID)'})
idcard_dist2['idcard_dist2_id:ID(idcard_dist2-ID)'] += 1

idcard_dist2.to_csv(neo4j_dir + 'idcard_dist2_n.csv', index=False)

idcard_dist2_r = pd.merge(r_cust[['loan_id', 'id_card_dist2']].rename(columns={'id_card_dist2': 'idcarddist2'}), idcard_dist2, on='idcarddist2')
idcard_dist2_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'idcard_dist2_id:ID(idcard_dist2-ID)': ':END_ID(idcard_dist2-ID)'}, inplace=True)

idcard_dist2_r.to_csv(neo4j_dir + 'idcard_dist2_r.csv', index=False)

'''id card dist3'''
idcard_dist3 = r_cust[['id_card_dist3']].dropna().reset_index(drop=True).rename(columns={'id_card_dist3': 'idcarddist3'})
idcard_dist3 = idcard_dist3.drop_duplicates().reset_index(drop=True)
idcard_dist3 = idcard_dist3[idcard_dist3['idcarddist3'] != ' '].reset_index(drop=True)
idcard_dist3['idcarddist3'] = idcard_dist3['idcarddist3'].str.replace('None', '')
idcard_dist3 = idcard_dist3[idcard_dist3['idcarddist3'] != ''].reset_index(drop=True)
idcard_dist3 = idcard_dist3.reset_index().rename(columns={'index': 'idcard_dist3_id:ID(idcard_dist3-ID)'})
idcard_dist3['idcard_dist3_id:ID(idcard_dist3-ID)'] += 1

idcard_dist3.to_csv(neo4j_dir + 'idcard_dist3_n.csv', index=False)

idcard_dist3_r = pd.merge(r_cust[['loan_id', 'id_card_dist3']].rename(columns={'id_card_dist3': 'idcarddist3'}), idcard_dist3, on='idcarddist3')
idcard_dist3_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'idcard_dist3_id:ID(idcard_dist3-ID)': ':END_ID(idcard_dist3-ID)'}, inplace=True)

idcard_dist3_r.to_csv(neo4j_dir + 'idcard_dist3_r.csv', index=False)

'''id card name'''
idcard_name = r_cust[['id_card_name']].dropna().reset_index(drop=True).rename(columns={'id_card_name': 'idcardname'})
idcard_name = idcard_name.drop_duplicates().reset_index(drop=True)
idcard_name = idcard_name[idcard_name['idcardname'] != ' '].reset_index(drop=True)
idcard_name['idcardname'] = idcard_name['idcardname'].str.replace('None', '')
idcard_name = idcard_name[idcard_name['idcardname'] != ''].reset_index(drop=True)
idcard_name = idcard_name.reset_index().rename(columns={'index': 'idcard_name_id:ID(idcard_name-ID)'})
idcard_name['idcard_name_id:ID(idcard_name-ID)'] += 1

idcard_name.to_csv(neo4j_dir + 'idcard_name_n.csv', index=False)

idcard_name_r = pd.merge(r_cust[['loan_id', 'id_card_name']].rename(columns={'id_card_name': 'idcardname'}), idcard_name, on='idcardname')
idcard_name_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'idcard_name_id:ID(idcard_name-ID)': ':END_ID(idcard_name-ID)'}, inplace=True)

idcard_name_r.to_csv(neo4j_dir + 'idcard_name_r.csv', index=False)

'''id card no'''
idcard_no = r_cust[['id_card_no']].dropna().reset_index(drop=True).rename(columns={'id_card_no': 'idcardno'})
idcard_no = idcard_no.drop_duplicates().reset_index(drop=True)
idcard_no = idcard_no[idcard_no['idcardno'] != ' '].reset_index(drop=True)
idcard_no['idcardno'] = idcard_no['idcardno'].str.replace('None', '')
idcard_no = idcard_no[idcard_no['idcardno'] != ''].reset_index(drop=True)
idcard_no = idcard_no.reset_index().rename(columns={'index': 'idcard_no_id:ID(idcard_no-ID)'})
idcard_no['idcard_no_id:ID(idcard_no-ID)'] += 1

idcard_no.to_csv(neo4j_dir + 'idcard_no_n.csv', index=False)

idcard_no_r = pd.merge(r_cust[['loan_id', 'id_card_no']].rename(columns={'id_card_no': 'idcardno'}), idcard_no, on='idcardno')
idcard_no_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'idcard_no_id:ID(idcard_no-ID)': ':END_ID(idcard_no-ID)'}, inplace=True)

idcard_no_r.to_csv(neo4j_dir + 'idcard_no_r.csv', index=False)

'''cell phone'''
cell_phone = r_cust[['cell_phone']].dropna().reset_index(drop=True).rename(columns={'cell_phone': 'cellphone'})
cell_phone = cell_phone.drop_duplicates().reset_index(drop=True)
cell_phone = cell_phone[cell_phone['cellphone'] != ' '].reset_index(drop=True)
cell_phone['cellphone'] = cell_phone['cellphone'].str.replace('None', '')
cell_phone = cell_phone[cell_phone['cellphone'] != ''].reset_index(drop=True)
cell_phone = cell_phone.reset_index().rename(columns={'index': 'cellphone_id:ID(cellphone-ID)'})
cell_phone['cellphone_id:ID(cellphone-ID)'] += 1

#cell_phone.to_csv(neo4j_dir + 'cell_phone_n.csv', index=False)
#cell_phone = pd.read_csv(neo4j_dir + 'cell_phone_n.csv', dtype = str)

cell_phone_r = pd.merge(r_cust[['loan_id', 'cell_phone']].rename(columns={'cell_phone': 'phone'}), phone, on='phone')
cell_phone_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'phone_id:ID(phone-ID)': ':END_ID(phone-ID)'}, inplace=True)

cell_phone_r.to_csv(neo4j_dir + 'cell_phone_r.csv', index=False)

'''mail'''
mail = r_cust[['mail']].dropna().reset_index(drop=True).rename(columns={'mail': 'mail'})
mail = mail.drop_duplicates().reset_index(drop=True)
mail = mail[mail['mail'] != ' '].reset_index(drop=True)
mail['mail'] = mail['mail'].str.replace('None', '')
mail = mail[mail['mail'] != ''].reset_index(drop=True)
mail = mail.reset_index().rename(columns={'index': 'mail_id:ID(mail-ID)'})
mail['mail_id:ID(mail-ID)'] += 1

mail.to_csv(neo4j_dir + 'mail_n.csv', index=False)

mail_r = pd.merge(r_cust[['loan_id', 'mail']].rename(columns={'mail': 'mail'}), mail, on='mail')
mail_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'mail_id:ID(mail-ID)': ':END_ID(mail-ID)'}, inplace=True)

mail_r.to_csv(neo4j_dir + 'mail_r.csv', index=False)

''' **************************** profession **************************** '''
r_profession = pd.read_csv(work_dir + 'r_profession.csv', dtype = str)

'''company_name'''
company_name = r_profession[['company_name']].dropna().reset_index(drop=True).rename(columns={'company_name': 'companyname'})
company_name = company_name.drop_duplicates().reset_index(drop=True)
company_name = company_name[company_name['companyname'] != ' '].reset_index(drop=True)
company_name['companyname'] = company_name['companyname'].str.replace('None', '')
company_name = company_name[company_name['companyname'] != ''].reset_index(drop=True)
company_name = company_name.reset_index().rename(columns={'index': 'companyname_id:ID(companyname-ID)'})
company_name['companyname_id:ID(companyname-ID)'] += 1

company_name.to_csv(neo4j_dir + 'company_name_n.csv', index=False)

company_name_r = pd.merge(r_profession[['loan_id', 'company_name']].rename(columns={'company_name': 'companyname'}), company_name, on='companyname')
company_name_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'companyname_id:ID(companyname-ID)': ':END_ID(companyname-ID)'}, inplace=True)

company_name_r.to_csv(neo4j_dir + 'company_name_r.csv', index=False)

'''company_phone'''
company_phone = r_profession[['company_phone']].dropna().reset_index(drop=True).rename(columns={'company_phone': 'companyphone'})
company_phone = company_phone.drop_duplicates().reset_index(drop=True)
company_phone = company_phone[company_phone['companyphone'] != ' '].reset_index(drop=True)
company_phone['companyphone'] = company_phone['companyphone'].str.replace('None', '')
company_phone = company_phone[company_phone['companyphone'] != ''].reset_index(drop=True)
company_phone = company_phone.reset_index().rename(columns={'index': 'companyphone_id:ID(companyphone-ID)'})
company_phone['companyphone_id:ID(companyphone-ID)'] += 1

company_phone.to_csv(neo4j_dir + 'company_phone_n.csv', index=False)

company_phone_r = pd.merge(r_profession[['loan_id', 'company_phone']].rename(columns={'company_phone': 'companyphone'}), company_phone, on='companyphone')
company_phone_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'companyphone_id:ID(companyphone-ID)': ':END_ID(companyphone-ID)'}, inplace=True)

company_phone_r.to_csv(neo4j_dir + 'company_phone_r.csv', index=False)

'''company_dist1'''
company_dist1 = r_profession[['company_dist1']].dropna().reset_index(drop=True).rename(columns={'company_dist1': 'companydist1'})
company_dist1 = company_dist1.drop_duplicates().reset_index(drop=True)
company_dist1 = company_dist1[company_dist1['companydist1'] != ' '].reset_index(drop=True)
company_dist1['companydist1'] = company_dist1['companydist1'].str.replace('None', '')
company_dist1 = company_dist1[company_dist1['companydist1'] != ''].reset_index(drop=True)
company_dist1 = company_dist1.reset_index().rename(columns={'index': 'companydist1_id:ID(companydist1-ID)'})
company_dist1['companydist1_id:ID(companydist1-ID)'] += 1

company_dist1.to_csv(neo4j_dir + 'company_dist1_n.csv', index=False)

company_dist1_r = pd.merge(r_profession[['loan_id', 'company_dist1']].rename(columns={'company_dist1': 'companydist1'}), company_dist1, on='companydist1')
company_dist1_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'companydist1_id:ID(companydist1-ID)': ':END_ID(companydist1-ID)'}, inplace=True)

company_dist1_r.to_csv(neo4j_dir + 'company_dist1_r.csv', index=False)

'''company_dist2'''
company_dist2 = r_profession[['company_dist2']].dropna().reset_index(drop=True).rename(columns={'company_dist2': 'companydist2'})
company_dist2 = company_dist2.drop_duplicates().reset_index(drop=True)
company_dist2 = company_dist2[company_dist2['companydist2'] != ' '].reset_index(drop=True)
company_dist2['companydist2'] = company_dist2['companydist2'].str.replace('None', '')
company_dist2 = company_dist2[company_dist2['companydist2'] != ''].reset_index(drop=True)
company_dist2 = company_dist2.reset_index().rename(columns={'index': 'companydist2_id:ID(companydist2-ID)'})
company_dist2['companydist2_id:ID(companydist2-ID)'] += 1

company_dist2.to_csv(neo4j_dir + 'company_dist2_n.csv', index=False)

company_dist2_r = pd.merge(r_profession[['loan_id', 'company_dist2']].rename(columns={'company_dist2': 'companydist2'}), company_dist2, on='companydist2')
company_dist2_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'companydist2_id:ID(companydist2-ID)': ':END_ID(companydist2-ID)'}, inplace=True)

company_dist2_r.to_csv(neo4j_dir + 'company_dist2_r.csv', index=False)

'''company_dist3'''
company_dist3 = r_profession[['company_dist3']].dropna().reset_index(drop=True).rename(columns={'company_dist3': 'companydist3'})
company_dist3 = company_dist3.drop_duplicates().reset_index(drop=True)
company_dist3 = company_dist3[company_dist3['companydist3'] != ' '].reset_index(drop=True)
company_dist3['companydist3'] = company_dist3['companydist3'].str.replace('None', '')
company_dist3 = company_dist3[company_dist3['companydist3'] != ''].reset_index(drop=True)
company_dist3 = company_dist3.reset_index().rename(columns={'index': 'companydist3_id:ID(companydist3-ID)'})
company_dist3['companydist3_id:ID(companydist3-ID)'] += 1

company_dist3.to_csv(neo4j_dir + 'company_dist3_n.csv', index=False)

company_dist3_r = pd.merge(r_profession[['loan_id', 'company_dist3']].rename(columns={'company_dist3': 'companydist3'}), company_dist3, on='companydist3')
company_dist3_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'companydist3_id:ID(companydist3-ID)': ':END_ID(companydist3-ID)'}, inplace=True)

company_dist3_r.to_csv(neo4j_dir + 'company_dist3_r.csv', index=False)

'''company_fulladdr'''
company_fulladdr = r_profession[['company_fulladdr']].dropna().reset_index(drop=True).rename(columns={'company_fulladdr': 'companyfulladdr'})
company_fulladdr = company_fulladdr.drop_duplicates().reset_index(drop=True)
company_fulladdr = company_fulladdr[company_fulladdr['companyfulladdr'] != ' '].reset_index(drop=True)
company_fulladdr['companyfulladdr'] = company_fulladdr['companyfulladdr'].str.replace('None', '')
company_fulladdr = company_fulladdr[company_fulladdr['companyfulladdr'] != ''].reset_index(drop=True)
company_fulladdr = company_fulladdr.reset_index().rename(columns={'index': 'companyfulladdr_id:ID(companyfulladdr-ID)'})
company_fulladdr['companyfulladdr_id:ID(companyfulladdr-ID)'] += 1

company_fulladdr.to_csv(neo4j_dir + 'company_fulladdr_n.csv', index=False)

company_fulladdr_r = pd.merge(r_profession[['loan_id', 'company_fulladdr']].rename(columns={'company_fulladdr': 'companyfulladdr'}), company_fulladdr, on='companyfulladdr')
company_fulladdr_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'companyfulladdr_id:ID(companyfulladdr-ID)': ':END_ID(companyfulladdr-ID)'}, inplace=True)

company_fulladdr_r.to_csv(neo4j_dir + 'company_fulladdr_r.csv', index=False)


''' **************************** refer **************************** '''
r_refer = pd.read_csv(work_dir + 'r_refer.csv', dtype = str)

'''refer_namephone'''
refer_namephone = r_refer[['refer_namephone']].dropna().reset_index(drop=True).rename(columns={'refer_namephone': 'refernamephone'})
refer_namephone = refer_namephone.drop_duplicates().reset_index(drop=True)
refer_namephone = refer_namephone[refer_namephone['refernamephone'] != ' '].reset_index(drop=True)
refer_namephone['refernamephone'] = refer_namephone['refernamephone'].str.replace('None', '')
refer_namephone = refer_namephone[refer_namephone['refernamephone'] != ''].reset_index(drop=True)
refer_namephone = refer_namephone.reset_index().rename(columns={'index': 'refernamephone_id:ID(refernamephone-ID)'})
refer_namephone['refernamephone_id:ID(refernamephone-ID)'] += 1

refer_namephone.to_csv(neo4j_dir + 'refer_namephone_n.csv', index=False)

refer_namephone_r = pd.merge(r_refer[['loan_id', 'refer_namephone', 'refer_type', 'refer_name']].rename(columns={'refer_namephone': 'refernamephone'}), refer_namephone, on='refernamephone')
refer_namephone_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'refernamephone_id:ID(refernamephone-ID)': ':END_ID(refernamephone-ID)'}, inplace=True)

refer_namephone_r.to_csv(neo4j_dir + 'refer_namephone_r.csv', index=False)
refer_namephone_r = pd.read_csv(neo4j_dir + 'refer_namephone_r.csv', dtype=str)

'''refer_phone'''
refer_phone = r_refer[['refer_phone']].dropna().reset_index(drop=True).rename(columns={'refer_phone': 'referphone'})
refer_phone = refer_phone.drop_duplicates().reset_index(drop=True)
refer_phone = refer_phone[refer_phone['referphone'] != ' '].reset_index(drop=True)
refer_phone['referphone'] = refer_phone['referphone'].str.replace('None', '')
refer_phone = refer_phone[refer_phone['referphone'] != ''].reset_index(drop=True)
refer_phone = refer_phone.reset_index().rename(columns={'index': 'referphone_id:ID(referphone-ID)'})
refer_phone['referphone_id:ID(referphone-ID)'] += 1

# combine cellphone and referphone to create one node
phone = pd.concat([cell_phone.rename(columns={'cellphone': 'phone', 'cellphone_id:ID(cellphone-ID)': 'phone_id:ID(phone-ID)'}), \
                   refer_phone.rename(columns={'referphone': 'phone', 'referphone_id:ID(referphone-ID)': 'phone_id:ID(phone-ID)'})], \
ignore_index=True)  
phone = phone.drop_duplicates(subset='phone').reset_index(drop=True)
phone = phone.reset_index().drop('phone_id:ID(phone-ID)', axis =1).rename(columns={'index': 'phone_id:ID(phone-ID)'})
phone['phone_id:ID(phone-ID)'] += 1

phone.to_csv(neo4j_dir + 'phone_n.csv', index=False)
phone = pd.read_csv(neo4j_dir + 'phone_n.csv', dtype = str)

#refer_phone.to_csv(neo4j_dir + 'refer_phone_n.csv', index=False)
#refer_phone = pd.read_csv(neo4j_dir + 'refer_phone_n.csv', dtype = str)

refer_phone_r = pd.merge(r_refer[['loan_id', 'refer_phone', 'refer_type', 'refer_name']].rename(columns={'refer_phone': 'phone'}), phone, on='phone')
refer_phone_r.rename(columns={'loan_id': ':START_ID(order_no-ID)', 'phone_id:ID(phone-ID)': ':END_ID(phone-ID)'}, inplace=True)

refer_phone_r.to_csv(neo4j_dir + 'refer_phone_r.csv', index=False)
refer_phone_r = pd.read_csv(neo4j_dir + 'refer_phone_r.csv', dtype = str)


''' **************************** gps **************************** '''
r_gps = pd.read_csv(work_dir + 'r_gps.csv', dtype = str)

for i in ['0', '1', '2', '3', '4']:
    a = r_gps[['gps_' + i]].dropna().reset_index(drop=True)
    a = a.drop_duplicates().reset_index(drop=True)
    a = a[a['gps_' + i] != ' '].reset_index(drop=True)
    a['gps_' + i] = a['gps_' + i].str.replace('None', '')
    a = a[a['gps_' + i] != ''].reset_index(drop=True)

    a = a.reset_index().rename(columns={'index': 'gps_' + i + '_id:ID(gps_' + i + '-ID)'})
    a['gps_' + i + '_id:ID(gps_' + i + '-ID)'] += 1

    a.to_csv(work_dir + 'gps_' + i + '_n.csv', index=False)

    a_r = pd.merge(r_gps[['order_no:ID(order_no-ID)', 'gps_' + i]], a, on='gps_' + i)
    a_r.rename(columns={'order_no:ID(order_no-ID)': ':START_ID(order_no-ID)',
                        'gps_' + i + '_id:ID(gps_' + i + '-ID)': ':END_ID(gps_' + i + '-ID)'}, inplace=True)
    # a_r = a_r[[':START_ID(order_no-ID)', ':END_ID(gps_' + i + '-ID)', 'gps_' + i]]
    a_r.to_csv(work_dir + 'gps_' + i + '_r.csv', index=False)

''' **************************** flag **************************** '''
r_flag = pd.read_csv(work_dir + 'r_flag.csv', dtype = {'loan_id': str, 'customer_id': str})
r_base = pd.read_csv(work_dir + 'r_base.csv', dtype = {'loan_id': str, 'customer_id': str})

base_info = pd.merge(r_base, r_flag[['loan_id', 'due_date', 'flag3', 'flag7', 'flag30']], on='loan_id', how='left')
base_info = pd.merge(base_info, r_cust, on='loan_id', how='left')
base_info = pd.merge(base_info, r_profession, on='loan_id', how='left')
base_info = pd.merge(base_info, r_gps, on='loan_id', how='left')

base_info.flag3.fillna(-9, inplace=True)
base_info.flag7.fillna(-9, inplace=True)
base_info.flag30.fillna(-9, inplace=True)

base_info.rename(columns={'flag3': 'flag3:int', 'flag7': 'flag7:int', 'flag30': 'flag30:int', 'loan_id': 'order_no:ID(order_no-ID)'}, inplace=True)

base_info['flag3:int'] = base_info['flag3:int'].astype(int)
base_info['flag7:int'] = base_info['flag7:int'].astype(int)
base_info['flag30:int'] = base_info['flag30:int'].astype(int)

base_info.to_csv(neo4j_dir + 'basic_info.csv', index=False)
base_info = pd.read_csv(neo4j_dir + 'basic_info.csv', dtype=str)





test = base_info[(pd.to_datetime(base_info.apply_time)>='2019-09-01') & (pd.to_datetime(base_info.apply_time)<='2019-10-01')]

test.shape
