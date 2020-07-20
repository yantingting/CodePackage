import psycopg2
import pandas as pd
import json


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


def dict_generator(indict, pre=None):
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                if len(value) == 0:
                    yield pre+[key, '{}']
                else:
                    for d in dict_generator(value, pre + [key]):
                        yield d
            elif isinstance(value, list):
                if len(value) == 0:
                    yield pre+[key, '[]']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            elif isinstance(value, tuple):
                if len(value) == 0:
                    yield pre+[key, '()']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            else:
                yield pre + [key, value]
    else:
        yield indict


br_loan_before = """
with br as (
select customerid, taskid, oss::json #>> '{taskId}' as task_id, oss::json #>> '{result}' as result
from risk_oss_bairong_new_loan_before where businessid in ('tb','xjbk')
),
related as (
select orderno, messageno
from  risk_mongo_installmentmessagerelated 
where businessid in ('xjbk','bk','tb') and topicname in ('Application_thirdPart_bairongnewloanbefore') and databasename = 'installmentBairongNewLoanBefore'
) 
select t0.order_no, t2.*
from  (select * from t_loan_performance where dt = '20190829' and business_id in ('xjbk','tb') and effective_date between '2018-08-26' and '2019-08-15') t0
inner join related t1 on t0.order_no = t1.orderno
left join br t2 on t1.messageno = t2.taskid
"""

br_loanbf_data = get_df_from_pg(br_loan_before)
br_loanbf_data.isnull().sum()

product_list = {'InfoRelation': 'ir', 'ApplyLoan_d': 'ald', 'ApplyLoanUsury': 'alu',
                'TotalLoan': 'tl', 'ApplyLoanStr': 'als', 'ApplyLoanMon': 'alm'}

product_list = {'InfoRelation': 'ir', 'ApplyLoanUsury': 'alu','TotalLoan': 'tl',}

col_list = ['ir_id_x_cell_cnt', 'ir_id_x_mail_cnt', 'ir_id_x_name_cnt', 'ir_cell_x_id_cnt', 'ir_cell_x_mail_cnt',
            'ir_cell_x_name_cnt', 'ir_id_inlistwith_cell', 'ir_cell_inlistwith_id', 'ir_allmatch_days',
            'ir_id_x_cell_notmat_days', 'ir_cell_x_id_notmat_days', 'ir_id_x_cell_lastchg_days',
            'ir_cell_x_id_lastchg_days', 'ir_id_is_reabnormal', 'ir_cell_is_reabnormal', 'ir_mail_is_reabnormal',
            'ir_m1_id_x_cell_cnt', 'ir_m1_id_x_mail_cnt', 'ir_m1_id_x_name_cnt', 'ir_m1_cell_x_id_cnt',
            'ir_m1_cell_x_mail_cnt', 'ir_m1_cell_x_name_cnt', 'ir_m1_id_x_tel_home_cnt', 'ir_m1_id_x_home_addr_cnt',
            'ir_m1_id_x_biz_addr_cnt', 'ir_m1_cell_x_tel_home_cnt', 'ir_m1_cell_x_home_addr_cnt',
            'ir_m1_cell_x_biz_addr_cnt', 'ir_m1_linkman_cell_x_id_cnt', 'ir_m1_linkman_cell_x_cell_cnt',
            'ir_m1_linkman_cell_x_tel_home_cnt', 'ir_m1_tel_home_x_cell_cnt', 'ir_m1_tel_home_x_id_cnt',
            'ir_m1_home_addr_x_cell_cnt', 'ir_m1_home_addr_x_id_cnt', 'ir_m1_tel_home_x_home_addr_cnt',
            'ir_m1_home_addr_x_tel_home_cnt', 'ir_m3_id_x_cell_cnt', 'ir_m3_id_x_mail_cnt', 'ir_m3_id_x_name_cnt',
            'ir_m3_cell_x_id_cnt', 'ir_m3_cell_x_mail_cnt', 'ir_m3_cell_x_name_cnt', 'ir_m3_id_x_tel_home_cnt',
            'ir_m3_id_x_home_addr_cnt', 'ir_m3_id_x_biz_addr_cnt', 'ir_m3_cell_x_tel_home_cnt',
            'ir_m3_cell_x_home_addr_cnt', 'ir_m3_cell_x_biz_addr_cnt', 'ir_m3_linkman_cell_x_id_cnt',
            'ir_m3_linkman_cell_x_cell_cnt', 'ir_m3_linkman_cell_x_tel_home_cnt', 'ir_m3_tel_home_x_cell_cnt',
            'ir_m3_tel_home_x_id_cnt', 'ir_m3_home_addr_x_cell_cnt', 'ir_m3_home_addr_x_id_cnt',
            'ir_m3_tel_home_x_home_addr_cnt', 'ir_m3_home_addr_x_tel_home_cnt', 'ir_m6_id_x_cell_cnt',
            'ir_m6_id_x_mail_cnt', 'ir_m6_id_x_name_cnt', 'ir_m6_cell_x_id_cnt', 'ir_m6_cell_x_mail_cnt',
            'ir_m6_cell_x_name_cnt', 'ir_m6_id_x_tel_home_cnt', 'ir_m6_id_x_home_addr_cnt', 'ir_m6_id_x_biz_addr_cnt',
            'ir_m6_cell_x_tel_home_cnt', 'ir_m6_cell_x_home_addr_cnt', 'ir_m6_cell_x_biz_addr_cnt',
            'ir_m6_linkman_cell_x_id_cnt', 'ir_m6_linkman_cell_x_cell_cnt', 'ir_m6_linkman_cell_x_tel_home_cnt',
            'ir_m6_tel_home_x_cell_cnt', 'ir_m6_tel_home_x_id_cnt', 'ir_m6_home_addr_x_cell_cnt',
            'ir_m6_home_addr_x_id_cnt', 'ir_m6_tel_home_x_home_addr_cnt', 'ir_m6_home_addr_x_tel_home_cnt',
            'ir_m12_id_x_cell_cnt', 'ir_m12_id_x_mail_cnt', 'ir_m12_id_x_name_cnt', 'ir_m12_cell_x_id_cnt',
            'ir_m12_cell_x_mail_cnt', 'ir_m12_cell_x_name_cnt', 'ir_m12_id_x_tel_home_cnt', 'ir_m12_id_x_home_addr_cnt',
            'ir_m12_id_x_biz_addr_cnt', 'ir_m12_cell_x_tel_home_cnt', 'ir_m12_cell_x_home_addr_cnt',
            'ir_m12_cell_x_biz_addr_cnt', 'ir_m12_linkman_cell_x_id_cnt', 'ir_m12_linkman_cell_x_cell_cnt',
            'ir_m12_linkman_cell_x_tel_home_cnt', 'ir_m12_tel_home_x_cell_cnt', 'ir_m12_tel_home_x_id_cnt',
            'ir_m12_home_addr_x_cell_cnt', 'ir_m12_home_addr_x_id_cnt', 'ir_m12_tel_home_x_home_addr_cnt',
            'ir_m12_home_addr_x_tel_home_cnt', 'ald_id_x_cell_num', 'ald_id_pdl_allnum', 'ald_id_pdl_orgnum',
            'ald_id_caon_allnum', 'ald_id_caon_orgnum', 'ald_id_rel_allnum', 'ald_id_rel_orgnum', 'ald_id_caoff_allnum',
            'ald_id_caoff_orgnum', 'ald_id_cooff_allnum', 'ald_id_cooff_orgnum', 'ald_id_af_allnum', 'ald_id_af_orgnum',
            'ald_id_coon_allnum', 'ald_id_coon_orgnum', 'ald_id_oth_allnum', 'ald_id_oth_orgnum', 'ald_id_bank_selfnum',
            'ald_id_bank_allnum', 'ald_id_bank_tra_allnum', 'ald_id_bank_ret_allnum', 'ald_id_bank_orgnum',
            'ald_id_bank_tra_orgnum', 'ald_id_bank_ret_orgnum', 'ald_id_nbank_selfnum', 'ald_id_nbank_allnum',
            'ald_id_nbank_p2p_allnum', 'ald_id_nbank_mc_allnum', 'ald_id_nbank_ca_allnum', 'ald_id_nbank_ca_on_allnum',
            'ald_id_nbank_ca_off_allnum', 'ald_id_nbank_cf_allnum', 'ald_id_nbank_cf_on_allnum',
            'ald_id_nbank_cf_off_allnum', 'ald_id_nbank_com_allnum', 'ald_id_nbank_oth_allnum',
            'ald_id_nbank_nsloan_allnum', 'ald_id_nbank_autofin_allnum', 'ald_id_nbank_sloan_allnum',
            'ald_id_nbank_cons_allnum', 'ald_id_nbank_finlea_allnum', 'ald_id_nbank_else_allnum', 'ald_id_nbank_orgnum',
            'ald_id_nbank_p2p_orgnum', 'ald_id_nbank_mc_orgnum', 'ald_id_nbank_ca_orgnum', 'ald_id_nbank_ca_on_orgnum',
            'ald_id_nbank_ca_off_orgnum', 'ald_id_nbank_cf_orgnum', 'ald_id_nbank_cf_on_orgnum',
            'ald_id_nbank_cf_off_orgnum', 'ald_id_nbank_com_orgnum', 'ald_id_nbank_oth_orgnum',
            'ald_id_nbank_nsloan_orgnum', 'ald_id_nbank_autofin_orgnum', 'ald_id_nbank_sloan_orgnum',
            'ald_id_nbank_cons_orgnum', 'ald_id_nbank_finlea_orgnum', 'ald_id_nbank_else_orgnum', 'ald_cell_pdl_allnum',
            'ald_cell_pdl_orgnum', 'ald_cell_caon_allnum', 'ald_cell_caon_orgnum', 'ald_cell_rel_allnum',
            'ald_cell_rel_orgnum', 'ald_cell_caoff_allnum', 'ald_cell_caoff_orgnum', 'ald_cell_cooff_allnum',
            'ald_cell_cooff_orgnum', 'ald_cell_af_allnum', 'ald_cell_af_orgnum', 'ald_cell_coon_allnum',
            'ald_cell_coon_orgnum', 'ald_cell_oth_allnum', 'ald_cell_oth_orgnum', 'ald_cell_bank_selfnum',
            'ald_cell_bank_allnum', 'ald_cell_bank_tra_allnum', 'ald_cell_bank_ret_allnum', 'ald_cell_bank_orgnum',
            'ald_cell_bank_tra_orgnum', 'ald_cell_bank_ret_orgnum', 'ald_cell_nbank_selfnum', 'ald_cell_nbank_allnum',
            'ald_cell_nbank_p2p_allnum', 'ald_cell_nbank_mc_allnum', 'ald_cell_nbank_ca_allnum',
            'ald_cell_nbank_ca_on_allnum', 'ald_cell_nbank_ca_off_allnum', 'ald_cell_nbank_cf_allnum',
            'ald_cell_nbank_cf_on_allnum', 'ald_cell_nbank_cf_off_allnum', 'ald_cell_nbank_com_allnum',
            'ald_cell_nbank_oth_allnum', 'ald_cell_nbank_nsloan_allnum', 'ald_cell_nbank_autofin_allnum',
            'ald_cell_nbank_sloan_allnum', 'ald_cell_nbank_cons_allnum', 'ald_cell_nbank_finlea_allnum',
            'ald_cell_nbank_else_allnum', 'ald_cell_nbank_orgnum', 'ald_cell_nbank_p2p_orgnum',
            'ald_cell_nbank_mc_orgnum', 'ald_cell_nbank_ca_orgnum', 'ald_cell_nbank_ca_on_orgnum',
            'ald_cell_nbank_ca_off_orgnum', 'ald_cell_nbank_cf_orgnum', 'ald_cell_nbank_cf_on_orgnum',
            'ald_cell_nbank_cf_off_orgnum', 'ald_cell_nbank_com_orgnum', 'ald_cell_nbank_oth_orgnum',
            'ald_cell_nbank_nsloan_orgnum', 'ald_cell_nbank_autofin_orgnum', 'ald_cell_nbank_sloan_orgnum',
            'ald_cell_nbank_cons_orgnum', 'ald_cell_nbank_finlea_orgnum', 'ald_cell_nbank_else_orgnum',
            'alu_d7_id_allnum', 'alu_d7_id_orgnum', 'alu_d7_cell_allnum', 'alu_d7_cell_orgnum', 'alu_d15_id_allnum',
            'alu_d15_id_orgnum', 'alu_d15_cell_allnum', 'alu_d15_cell_orgnum', 'alu_m1_id_allnum', 'alu_m1_id_orgnum',
            'alu_m1_cell_allnum', 'alu_m1_cell_orgnum', 'alu_m3_id_allnum', 'alu_m3_id_orgnum', 'alu_m3_id_tot_monnum',
            'alu_m3_id_avg_monnum', 'alu_m3_id_max_monnum', 'alu_m3_id_min_monnum', 'alu_m3_cell_allnum',
            'alu_m3_cell_orgnum', 'alu_m3_cell_tot_monnum', 'alu_m3_cell_avg_monnum', 'alu_m3_cell_max_monnum',
            'alu_m3_cell_min_monnum', 'alu_m6_id_allnum', 'alu_m6_id_orgnum', 'alu_m6_id_tot_monnum',
            'alu_m6_id_avg_monnum', 'alu_m6_id_max_monnum', 'alu_m6_id_min_monnum', 'alu_m6_cell_allnum',
            'alu_m6_cell_orgnum', 'alu_m6_cell_tot_monnum', 'alu_m6_cell_avg_monnum', 'alu_m6_cell_max_monnum',
            'alu_m6_cell_min_monnum', 'alu_m12_id_allnum', 'alu_m12_id_orgnum', 'alu_m12_id_tot_monnum',
            'alu_m12_id_avg_monnum', 'alu_m12_id_max_monnum', 'alu_m12_id_min_monnum', 'alu_m12_cell_allnum',
            'alu_m12_cell_orgnum', 'alu_m12_cell_tot_monnum', 'alu_m12_cell_avg_monnum', 'alu_m12_cell_max_monnum',
            'alu_m12_cell_min_monnum', 'alu_y1_id_allnum', 'alu_y1_id_orgnum', 'alu_y1_cell_allnum',
            'alu_y1_cell_orgnum', 'alu_fst_id_inteday', 'alu_lst_id_inteday', 'alu_fst_cell_inteday',
            'alu_lst_cell_inteday', 'tl_cell_lasttime', 'tl_cell_lasttype', 'tl_cell_nowamount', 'tl_cell_pdl_allnum',
            'tl_cell_pdl_orgnum', 'tl_id_lasttime', 'tl_id_lasttype', 'tl_id_nowamount', 'tl_id_pdl_allnum',
            'tl_id_pdl_orgnum', 'tl_m1_cell_nobank_allamount', 'tl_m1_cell_nobank_allorgnum',
            'tl_m1_cell_nobank_newallnum', 'tl_m1_cell_nobank_neworgnum', 'tl_m1_cell_nobank_passamount',
            'tl_m1_cell_nobank_passnum', 'tl_m1_cell_nobank_payamount', 'tl_m1_id_nobank_allamount',
            'tl_m1_id_nobank_allorgnum', 'tl_m1_id_nobank_newallnum', 'tl_m1_id_nobank_neworgnum',
            'tl_m1_id_nobank_passamount', 'tl_m1_id_nobank_passnum', 'tl_m1_id_nobank_payamount',
            'tl_m10_cell_nobank_allamount', 'tl_m10_cell_nobank_allorgnum', 'tl_m10_cell_nobank_newallnum',
            'tl_m10_cell_nobank_neworgnum', 'tl_m10_cell_nobank_passamount', 'tl_m10_cell_nobank_passnum',
            'tl_m10_cell_nobank_payamount', 'tl_m10_id_nobank_allamount', 'tl_m10_id_nobank_allorgnum',
            'tl_m10_id_nobank_newallnum', 'tl_m10_id_nobank_neworgnum', 'tl_m10_id_nobank_passamount',
            'tl_m10_id_nobank_passnum', 'tl_m10_id_nobank_payamount', 'tl_m11_cell_nobank_allamount',
            'tl_m11_cell_nobank_allorgnum', 'tl_m11_cell_nobank_newallnum', 'tl_m11_cell_nobank_neworgnum',
            'tl_m11_cell_nobank_passamount', 'tl_m11_cell_nobank_passnum', 'tl_m11_cell_nobank_payamount',
            'tl_m11_id_nobank_allamount', 'tl_m11_id_nobank_allorgnum', 'tl_m11_id_nobank_newallnum',
            'tl_m11_id_nobank_neworgnum', 'tl_m11_id_nobank_passamount', 'tl_m11_id_nobank_passnum',
            'tl_m11_id_nobank_payamount', 'tl_m12_cell_nobank_allamount', 'tl_m12_cell_nobank_allorgnum',
            'tl_m12_cell_nobank_newallnum', 'tl_m12_cell_nobank_neworgnum', 'tl_m12_cell_nobank_passamount',
            'tl_m12_cell_nobank_passnum', 'tl_m12_cell_nobank_payamount', 'tl_m12_id_nobank_allamount',
            'tl_m12_id_nobank_allorgnum', 'tl_m12_id_nobank_newallnum', 'tl_m12_id_nobank_neworgnum',
            'tl_m12_id_nobank_passamount', 'tl_m12_id_nobank_passnum', 'tl_m12_id_nobank_payamount',
            'tl_m2_cell_nobank_allamount', 'tl_m2_cell_nobank_allorgnum', 'tl_m2_cell_nobank_newallnum',
            'tl_m2_cell_nobank_neworgnum', 'tl_m2_cell_nobank_passamount', 'tl_m2_cell_nobank_passnum',
            'tl_m2_cell_nobank_payamount', 'tl_m2_id_nobank_allamount', 'tl_m2_id_nobank_allorgnum',
            'tl_m2_id_nobank_newallnum', 'tl_m2_id_nobank_neworgnum', 'tl_m2_id_nobank_passamount',
            'tl_m2_id_nobank_passnum', 'tl_m2_id_nobank_payamount', 'tl_m3_cell_nobank_allamount',
            'tl_m3_cell_nobank_allorgnum', 'tl_m3_cell_nobank_newallnum', 'tl_m3_cell_nobank_neworgnum',
            'tl_m3_cell_nobank_passamount', 'tl_m3_cell_nobank_passnum', 'tl_m3_cell_nobank_payamount',
            'tl_m3_id_nobank_allamount', 'tl_m3_id_nobank_allorgnum', 'tl_m3_id_nobank_newallnum',
            'tl_m3_id_nobank_neworgnum', 'tl_m3_id_nobank_passamount', 'tl_m3_id_nobank_passnum',
            'tl_m3_id_nobank_payamount', 'tl_m4_cell_nobank_allamount', 'tl_m4_cell_nobank_allorgnum',
            'tl_m4_cell_nobank_newallnum', 'tl_m4_cell_nobank_neworgnum', 'tl_m4_cell_nobank_passamount',
            'tl_m4_cell_nobank_passnum', 'tl_m4_cell_nobank_payamount', 'tl_m4_id_nobank_allamount',
            'tl_m4_id_nobank_allorgnum', 'tl_m4_id_nobank_newallnum', 'tl_m4_id_nobank_neworgnum',
            'tl_m4_id_nobank_passamount', 'tl_m4_id_nobank_passnum', 'tl_m4_id_nobank_payamount',
            'tl_m5_cell_nobank_allamount', 'tl_m5_cell_nobank_allorgnum', 'tl_m5_cell_nobank_newallnum',
            'tl_m5_cell_nobank_neworgnum', 'tl_m5_cell_nobank_passamount', 'tl_m5_cell_nobank_passnum',
            'tl_m5_cell_nobank_payamount', 'tl_m5_id_nobank_allamount', 'tl_m5_id_nobank_allorgnum',
            'tl_m5_id_nobank_newallnum', 'tl_m5_id_nobank_neworgnum', 'tl_m5_id_nobank_passamount',
            'tl_m5_id_nobank_passnum', 'tl_m5_id_nobank_payamount', 'tl_m6_cell_nobank_allamount',
            'tl_m6_cell_nobank_allorgnum', 'tl_m6_cell_nobank_newallnum', 'tl_m6_cell_nobank_neworgnum',
            'tl_m6_cell_nobank_passamount', 'tl_m6_cell_nobank_passnum', 'tl_m6_cell_nobank_payamount',
            'tl_m6_id_nobank_allamount', 'tl_m6_id_nobank_allorgnum', 'tl_m6_id_nobank_newallnum',
            'tl_m6_id_nobank_neworgnum', 'tl_m6_id_nobank_passamount', 'tl_m6_id_nobank_passnum',
            'tl_m6_id_nobank_payamount', 'tl_m7_cell_nobank_allamount', 'tl_m7_cell_nobank_allorgnum',
            'tl_m7_cell_nobank_newallnum', 'tl_m7_cell_nobank_neworgnum', 'tl_m7_cell_nobank_passamount',
            'tl_m7_cell_nobank_passnum', 'tl_m7_cell_nobank_payamount', 'tl_m7_id_nobank_allamount',
            'tl_m7_id_nobank_allorgnum', 'tl_m7_id_nobank_newallnum', 'tl_m7_id_nobank_neworgnum',
            'tl_m7_id_nobank_passamount', 'tl_m7_id_nobank_passnum', 'tl_m7_id_nobank_payamount',
            'tl_m8_cell_nobank_allamount', 'tl_m8_cell_nobank_allorgnum', 'tl_m8_cell_nobank_newallnum',
            'tl_m8_cell_nobank_neworgnum', 'tl_m8_cell_nobank_passamount', 'tl_m8_cell_nobank_passnum',
            'tl_m8_cell_nobank_payamount', 'tl_m8_id_nobank_allamount', 'tl_m8_id_nobank_allorgnum',
            'tl_m8_id_nobank_newallnum', 'tl_m8_id_nobank_neworgnum', 'tl_m8_id_nobank_passamount',
            'tl_m8_id_nobank_passnum', 'tl_m8_id_nobank_payamount', 'tl_m9_cell_nobank_allamount',
            'tl_m9_cell_nobank_allorgnum', 'tl_m9_cell_nobank_newallnum', 'tl_m9_cell_nobank_neworgnum',
            'tl_m9_cell_nobank_passamount', 'tl_m9_cell_nobank_passnum', 'tl_m9_cell_nobank_payamount',
            'tl_m9_id_nobank_allamount', 'tl_m9_id_nobank_allorgnum', 'tl_m9_id_nobank_newallnum',
            'tl_m9_id_nobank_neworgnum', 'tl_m9_id_nobank_passamount', 'tl_m9_id_nobank_passnum',
            'tl_m9_id_nobank_payamount', 'als_d7_id_pdl_allnum', 'als_d7_id_pdl_orgnum', 'als_d7_id_caon_allnum',
            'als_d7_id_caon_orgnum', 'als_d7_id_rel_allnum', 'als_d7_id_rel_orgnum', 'als_d7_id_caoff_allnum',
            'als_d7_id_caoff_orgnum', 'als_d7_id_cooff_allnum', 'als_d7_id_cooff_orgnum', 'als_d7_id_af_allnum',
            'als_d7_id_af_orgnum', 'als_d7_id_coon_allnum', 'als_d7_id_coon_orgnum', 'als_d7_id_oth_allnum',
            'als_d7_id_oth_orgnum', 'als_d7_id_bank_selfnum', 'als_d7_id_bank_allnum', 'als_d7_id_bank_tra_allnum',
            'als_d7_id_bank_ret_allnum', 'als_d7_id_bank_orgnum', 'als_d7_id_bank_tra_orgnum',
            'als_d7_id_bank_ret_orgnum', 'als_d7_id_bank_week_allnum', 'als_d7_id_bank_week_orgnum',
            'als_d7_id_bank_night_allnum', 'als_d7_id_bank_night_orgnum', 'als_d7_id_nbank_selfnum',
            'als_d7_id_nbank_allnum', 'als_d7_id_nbank_p2p_allnum', 'als_d7_id_nbank_mc_allnum',
            'als_d7_id_nbank_ca_allnum', 'als_d7_id_nbank_cf_allnum', 'als_d7_id_nbank_com_allnum',
            'als_d7_id_nbank_oth_allnum', 'als_d7_id_nbank_nsloan_allnum', 'als_d7_id_nbank_autofin_allnum',
            'als_d7_id_nbank_sloan_allnum', 'als_d7_id_nbank_cons_allnum', 'als_d7_id_nbank_finlea_allnum',
            'als_d7_id_nbank_else_allnum', 'als_d7_id_nbank_orgnum', 'als_d7_id_nbank_p2p_orgnum',
            'als_d7_id_nbank_mc_orgnum', 'als_d7_id_nbank_ca_orgnum', 'als_d7_id_nbank_cf_orgnum',
            'als_d7_id_nbank_com_orgnum', 'als_d7_id_nbank_oth_orgnum', 'als_d7_id_nbank_nsloan_orgnum',
            'als_d7_id_nbank_autofin_orgnum', 'als_d7_id_nbank_sloan_orgnum', 'als_d7_id_nbank_cons_orgnum',
            'als_d7_id_nbank_finlea_orgnum', 'als_d7_id_nbank_else_orgnum', 'als_d7_id_nbank_week_allnum',
            'als_d7_id_nbank_week_orgnum', 'als_d7_id_nbank_night_allnum', 'als_d7_id_nbank_night_orgnum',
            'als_d7_cell_pdl_allnum', 'als_d7_cell_pdl_orgnum', 'als_d7_cell_caon_allnum', 'als_d7_cell_caon_orgnum',
            'als_d7_cell_rel_allnum', 'als_d7_cell_rel_orgnum', 'als_d7_cell_caoff_allnum', 'als_d7_cell_caoff_orgnum',
            'als_d7_cell_cooff_allnum', 'als_d7_cell_cooff_orgnum', 'als_d7_cell_af_allnum', 'als_d7_cell_af_orgnum',
            'als_d7_cell_coon_allnum', 'als_d7_cell_coon_orgnum', 'als_d7_cell_oth_allnum', 'als_d7_cell_oth_orgnum',
            'als_d7_cell_bank_selfnum', 'als_d7_cell_bank_allnum', 'als_d7_cell_bank_tra_allnum',
            'als_d7_cell_bank_ret_allnum', 'als_d7_cell_bank_orgnum', 'als_d7_cell_bank_tra_orgnum',
            'als_d7_cell_bank_ret_orgnum', 'als_d7_cell_bank_week_allnum', 'als_d7_cell_bank_week_orgnum',
            'als_d7_cell_bank_night_allnum', 'als_d7_cell_bank_night_orgnum', 'als_d7_cell_nbank_selfnum',
            'als_d7_cell_nbank_allnum', 'als_d7_cell_nbank_p2p_allnum', 'als_d7_cell_nbank_mc_allnum',
            'als_d7_cell_nbank_ca_allnum', 'als_d7_cell_nbank_cf_allnum', 'als_d7_cell_nbank_com_allnum',
            'als_d7_cell_nbank_oth_allnum', 'als_d7_cell_nbank_nsloan_allnum', 'als_d7_cell_nbank_autofin_allnum',
            'als_d7_cell_nbank_sloan_allnum', 'als_d7_cell_nbank_cons_allnum', 'als_d7_cell_nbank_finlea_allnum',
            'als_d7_cell_nbank_else_allnum', 'als_d7_cell_nbank_orgnum', 'als_d7_cell_nbank_p2p_orgnum',
            'als_d7_cell_nbank_mc_orgnum', 'als_d7_cell_nbank_ca_orgnum', 'als_d7_cell_nbank_cf_orgnum',
            'als_d7_cell_nbank_com_orgnum', 'als_d7_cell_nbank_oth_orgnum', 'als_d7_cell_nbank_nsloan_orgnum',
            'als_d7_cell_nbank_autofin_orgnum', 'als_d7_cell_nbank_sloan_orgnum', 'als_d7_cell_nbank_cons_orgnum',
            'als_d7_cell_nbank_finlea_orgnum', 'als_d7_cell_nbank_else_orgnum', 'als_d7_cell_nbank_week_allnum',
            'als_d7_cell_nbank_week_orgnum', 'als_d7_cell_nbank_night_allnum', 'als_d7_cell_nbank_night_orgnum',
            'als_d15_id_pdl_allnum', 'als_d15_id_pdl_orgnum', 'als_d15_id_caon_allnum', 'als_d15_id_caon_orgnum',
            'als_d15_id_rel_allnum', 'als_d15_id_rel_orgnum', 'als_d15_id_caoff_allnum', 'als_d15_id_caoff_orgnum',
            'als_d15_id_cooff_allnum', 'als_d15_id_cooff_orgnum', 'als_d15_id_af_allnum', 'als_d15_id_af_orgnum',
            'als_d15_id_coon_allnum', 'als_d15_id_coon_orgnum', 'als_d15_id_oth_allnum', 'als_d15_id_oth_orgnum',
            'als_d15_id_bank_selfnum', 'als_d15_id_bank_allnum', 'als_d15_id_bank_tra_allnum',
            'als_d15_id_bank_ret_allnum', 'als_d15_id_bank_orgnum', 'als_d15_id_bank_tra_orgnum',
            'als_d15_id_bank_ret_orgnum', 'als_d15_id_bank_week_allnum', 'als_d15_id_bank_week_orgnum',
            'als_d15_id_bank_night_allnum', 'als_d15_id_bank_night_orgnum', 'als_d15_id_nbank_selfnum',
            'als_d15_id_nbank_allnum', 'als_d15_id_nbank_p2p_allnum', 'als_d15_id_nbank_mc_allnum',
            'als_d15_id_nbank_ca_allnum', 'als_d15_id_nbank_cf_allnum', 'als_d15_id_nbank_com_allnum',
            'als_d15_id_nbank_oth_allnum', 'als_d15_id_nbank_nsloan_allnum', 'als_d15_id_nbank_autofin_allnum',
            'als_d15_id_nbank_sloan_allnum', 'als_d15_id_nbank_cons_allnum', 'als_d15_id_nbank_finlea_allnum',
            'als_d15_id_nbank_else_allnum', 'als_d15_id_nbank_orgnum', 'als_d15_id_nbank_p2p_orgnum',
            'als_d15_id_nbank_mc_orgnum', 'als_d15_id_nbank_ca_orgnum', 'als_d15_id_nbank_cf_orgnum',
            'als_d15_id_nbank_com_orgnum', 'als_d15_id_nbank_oth_orgnum', 'als_d15_id_nbank_nsloan_orgnum',
            'als_d15_id_nbank_autofin_orgnum', 'als_d15_id_nbank_sloan_orgnum', 'als_d15_id_nbank_cons_orgnum',
            'als_d15_id_nbank_finlea_orgnum', 'als_d15_id_nbank_else_orgnum', 'als_d15_id_nbank_week_allnum',
            'als_d15_id_nbank_week_orgnum', 'als_d15_id_nbank_night_allnum', 'als_d15_id_nbank_night_orgnum',
            'als_d15_cell_pdl_allnum', 'als_d15_cell_pdl_orgnum', 'als_d15_cell_caon_allnum',
            'als_d15_cell_caon_orgnum', 'als_d15_cell_rel_allnum', 'als_d15_cell_rel_orgnum',
            'als_d15_cell_caoff_allnum', 'als_d15_cell_caoff_orgnum', 'als_d15_cell_cooff_allnum',
            'als_d15_cell_cooff_orgnum', 'als_d15_cell_af_allnum', 'als_d15_cell_af_orgnum', 'als_d15_cell_coon_allnum',
            'als_d15_cell_coon_orgnum', 'als_d15_cell_oth_allnum', 'als_d15_cell_oth_orgnum',
            'als_d15_cell_bank_selfnum', 'als_d15_cell_bank_allnum', 'als_d15_cell_bank_tra_allnum',
            'als_d15_cell_bank_ret_allnum', 'als_d15_cell_bank_orgnum', 'als_d15_cell_bank_tra_orgnum',
            'als_d15_cell_bank_ret_orgnum', 'als_d15_cell_bank_week_allnum', 'als_d15_cell_bank_week_orgnum',
            'als_d15_cell_bank_night_allnum', 'als_d15_cell_bank_night_orgnum', 'als_d15_cell_nbank_selfnum',
            'als_d15_cell_nbank_allnum', 'als_d15_cell_nbank_p2p_allnum', 'als_d15_cell_nbank_mc_allnum',
            'als_d15_cell_nbank_ca_allnum', 'als_d15_cell_nbank_cf_allnum', 'als_d15_cell_nbank_com_allnum',
            'als_d15_cell_nbank_oth_allnum', 'als_d15_cell_nbank_nsloan_allnum', 'als_d15_cell_nbank_autofin_allnum',
            'als_d15_cell_nbank_sloan_allnum', 'als_d15_cell_nbank_cons_allnum', 'als_d15_cell_nbank_finlea_allnum',
            'als_d15_cell_nbank_else_allnum', 'als_d15_cell_nbank_orgnum', 'als_d15_cell_nbank_p2p_orgnum',
            'als_d15_cell_nbank_mc_orgnum', 'als_d15_cell_nbank_ca_orgnum', 'als_d15_cell_nbank_cf_orgnum',
            'als_d15_cell_nbank_com_orgnum', 'als_d15_cell_nbank_oth_orgnum', 'als_d15_cell_nbank_nsloan_orgnum',
            'als_d15_cell_nbank_autofin_orgnum', 'als_d15_cell_nbank_sloan_orgnum', 'als_d15_cell_nbank_cons_orgnum',
            'als_d15_cell_nbank_finlea_orgnum', 'als_d15_cell_nbank_else_orgnum', 'als_d15_cell_nbank_week_allnum',
            'als_d15_cell_nbank_week_orgnum', 'als_d15_cell_nbank_night_allnum', 'als_d15_cell_nbank_night_orgnum',
            'als_m1_id_pdl_allnum', 'als_m1_id_pdl_orgnum', 'als_m1_id_caon_allnum', 'als_m1_id_caon_orgnum',
            'als_m1_id_rel_allnum', 'als_m1_id_rel_orgnum', 'als_m1_id_caoff_allnum', 'als_m1_id_caoff_orgnum',
            'als_m1_id_cooff_allnum', 'als_m1_id_cooff_orgnum', 'als_m1_id_af_allnum', 'als_m1_id_af_orgnum',
            'als_m1_id_coon_allnum', 'als_m1_id_coon_orgnum', 'als_m1_id_oth_allnum', 'als_m1_id_oth_orgnum',
            'als_m1_id_bank_selfnum', 'als_m1_id_bank_allnum', 'als_m1_id_bank_tra_allnum', 'als_m1_id_bank_ret_allnum',
            'als_m1_id_bank_orgnum', 'als_m1_id_bank_tra_orgnum', 'als_m1_id_bank_ret_orgnum',
            'als_m1_id_bank_week_allnum', 'als_m1_id_bank_week_orgnum', 'als_m1_id_bank_night_allnum',
            'als_m1_id_bank_night_orgnum', 'als_m1_id_nbank_selfnum', 'als_m1_id_nbank_allnum',
            'als_m1_id_nbank_p2p_allnum', 'als_m1_id_nbank_mc_allnum', 'als_m1_id_nbank_ca_allnum',
            'als_m1_id_nbank_cf_allnum', 'als_m1_id_nbank_com_allnum', 'als_m1_id_nbank_oth_allnum',
            'als_m1_id_nbank_nsloan_allnum', 'als_m1_id_nbank_autofin_allnum', 'als_m1_id_nbank_sloan_allnum',
            'als_m1_id_nbank_cons_allnum', 'als_m1_id_nbank_finlea_allnum', 'als_m1_id_nbank_else_allnum',
            'als_m1_id_nbank_orgnum', 'als_m1_id_nbank_p2p_orgnum', 'als_m1_id_nbank_mc_orgnum',
            'als_m1_id_nbank_ca_orgnum', 'als_m1_id_nbank_cf_orgnum', 'als_m1_id_nbank_com_orgnum',
            'als_m1_id_nbank_oth_orgnum', 'als_m1_id_nbank_nsloan_orgnum', 'als_m1_id_nbank_autofin_orgnum',
            'als_m1_id_nbank_sloan_orgnum', 'als_m1_id_nbank_cons_orgnum', 'als_m1_id_nbank_finlea_orgnum',
            'als_m1_id_nbank_else_orgnum', 'als_m1_id_nbank_week_allnum', 'als_m1_id_nbank_week_orgnum',
            'als_m1_id_nbank_night_allnum', 'als_m1_id_nbank_night_orgnum', 'als_m1_cell_pdl_allnum',
            'als_m1_cell_pdl_orgnum', 'als_m1_cell_caon_allnum', 'als_m1_cell_caon_orgnum', 'als_m1_cell_rel_allnum',
            'als_m1_cell_rel_orgnum', 'als_m1_cell_caoff_allnum', 'als_m1_cell_caoff_orgnum',
            'als_m1_cell_cooff_allnum', 'als_m1_cell_cooff_orgnum', 'als_m1_cell_af_allnum', 'als_m1_cell_af_orgnum',
            'als_m1_cell_coon_allnum', 'als_m1_cell_coon_orgnum', 'als_m1_cell_oth_allnum', 'als_m1_cell_oth_orgnum',
            'als_m1_cell_bank_selfnum', 'als_m1_cell_bank_allnum', 'als_m1_cell_bank_tra_allnum',
            'als_m1_cell_bank_ret_allnum', 'als_m1_cell_bank_orgnum', 'als_m1_cell_bank_tra_orgnum',
            'als_m1_cell_bank_ret_orgnum', 'als_m1_cell_bank_week_allnum', 'als_m1_cell_bank_week_orgnum',
            'als_m1_cell_bank_night_allnum', 'als_m1_cell_bank_night_orgnum', 'als_m1_cell_nbank_selfnum',
            'als_m1_cell_nbank_allnum', 'als_m1_cell_nbank_p2p_allnum', 'als_m1_cell_nbank_mc_allnum',
            'als_m1_cell_nbank_ca_allnum', 'als_m1_cell_nbank_cf_allnum', 'als_m1_cell_nbank_com_allnum',
            'als_m1_cell_nbank_oth_allnum', 'als_m1_cell_nbank_nsloan_allnum', 'als_m1_cell_nbank_autofin_allnum',
            'als_m1_cell_nbank_sloan_allnum', 'als_m1_cell_nbank_cons_allnum', 'als_m1_cell_nbank_finlea_allnum',
            'als_m1_cell_nbank_else_allnum', 'als_m1_cell_nbank_orgnum', 'als_m1_cell_nbank_p2p_orgnum',
            'als_m1_cell_nbank_mc_orgnum', 'als_m1_cell_nbank_ca_orgnum', 'als_m1_cell_nbank_cf_orgnum',
            'als_m1_cell_nbank_com_orgnum', 'als_m1_cell_nbank_oth_orgnum', 'als_m1_cell_nbank_nsloan_orgnum',
            'als_m1_cell_nbank_autofin_orgnum', 'als_m1_cell_nbank_sloan_orgnum', 'als_m1_cell_nbank_cons_orgnum',
            'als_m1_cell_nbank_finlea_orgnum', 'als_m1_cell_nbank_else_orgnum', 'als_m1_cell_nbank_week_allnum',
            'als_m1_cell_nbank_week_orgnum', 'als_m1_cell_nbank_night_allnum', 'als_m1_cell_nbank_night_orgnum',
            'als_m3_id_max_inteday', 'als_m3_id_min_inteday', 'als_m3_id_tot_mons', 'als_m3_id_avg_monnum',
            'als_m3_id_max_monnum', 'als_m3_id_min_monnum', 'als_m3_id_pdl_allnum', 'als_m3_id_pdl_orgnum',
            'als_m3_id_caon_allnum', 'als_m3_id_caon_orgnum', 'als_m3_id_rel_allnum', 'als_m3_id_rel_orgnum',
            'als_m3_id_caoff_allnum', 'als_m3_id_caoff_orgnum', 'als_m3_id_cooff_allnum', 'als_m3_id_cooff_orgnum',
            'als_m3_id_af_allnum', 'als_m3_id_af_orgnum', 'als_m3_id_coon_allnum', 'als_m3_id_coon_orgnum',
            'als_m3_id_oth_allnum', 'als_m3_id_oth_orgnum', 'als_m3_id_bank_selfnum', 'als_m3_id_bank_allnum',
            'als_m3_id_bank_tra_allnum', 'als_m3_id_bank_ret_allnum', 'als_m3_id_bank_orgnum',
            'als_m3_id_bank_tra_orgnum', 'als_m3_id_bank_ret_orgnum', 'als_m3_id_bank_tot_mons',
            'als_m3_id_bank_avg_monnum', 'als_m3_id_bank_max_monnum', 'als_m3_id_bank_min_monnum',
            'als_m3_id_bank_max_inteday', 'als_m3_id_bank_min_inteday', 'als_m3_id_bank_week_allnum',
            'als_m3_id_bank_week_orgnum', 'als_m3_id_bank_night_allnum', 'als_m3_id_bank_night_orgnum',
            'als_m3_id_nbank_selfnum', 'als_m3_id_nbank_allnum', 'als_m3_id_nbank_p2p_allnum',
            'als_m3_id_nbank_mc_allnum', 'als_m3_id_nbank_ca_allnum', 'als_m3_id_nbank_cf_allnum',
            'als_m3_id_nbank_com_allnum', 'als_m3_id_nbank_oth_allnum', 'als_m3_id_nbank_nsloan_allnum',
            'als_m3_id_nbank_autofin_allnum', 'als_m3_id_nbank_sloan_allnum', 'als_m3_id_nbank_cons_allnum',
            'als_m3_id_nbank_finlea_allnum', 'als_m3_id_nbank_else_allnum', 'als_m3_id_nbank_orgnum',
            'als_m3_id_nbank_p2p_orgnum', 'als_m3_id_nbank_mc_orgnum', 'als_m3_id_nbank_ca_orgnum',
            'als_m3_id_nbank_cf_orgnum', 'als_m3_id_nbank_com_orgnum', 'als_m3_id_nbank_oth_orgnum',
            'als_m3_id_nbank_nsloan_orgnum', 'als_m3_id_nbank_autofin_orgnum', 'als_m3_id_nbank_sloan_orgnum',
            'als_m3_id_nbank_cons_orgnum', 'als_m3_id_nbank_finlea_orgnum', 'als_m3_id_nbank_else_orgnum',
            'als_m3_id_nbank_tot_mons', 'als_m3_id_nbank_avg_monnum', 'als_m3_id_nbank_max_monnum',
            'als_m3_id_nbank_min_monnum', 'als_m3_id_nbank_max_inteday', 'als_m3_id_nbank_min_inteday',
            'als_m3_id_nbank_week_allnum', 'als_m3_id_nbank_week_orgnum', 'als_m3_id_nbank_night_allnum',
            'als_m3_id_nbank_night_orgnum', 'als_m3_cell_max_inteday', 'als_m3_cell_min_inteday',
            'als_m3_cell_tot_mons', 'als_m3_cell_avg_monnum', 'als_m3_cell_max_monnum', 'als_m3_cell_min_monnum',
            'als_m3_cell_pdl_allnum', 'als_m3_cell_pdl_orgnum', 'als_m3_cell_caon_allnum', 'als_m3_cell_caon_orgnum',
            'als_m3_cell_rel_allnum', 'als_m3_cell_rel_orgnum', 'als_m3_cell_caoff_allnum', 'als_m3_cell_caoff_orgnum',
            'als_m3_cell_cooff_allnum', 'als_m3_cell_cooff_orgnum', 'als_m3_cell_af_allnum', 'als_m3_cell_af_orgnum',
            'als_m3_cell_coon_allnum', 'als_m3_cell_coon_orgnum', 'als_m3_cell_oth_allnum', 'als_m3_cell_oth_orgnum',
            'als_m3_cell_bank_selfnum', 'als_m3_cell_bank_allnum', 'als_m3_cell_bank_tra_allnum',
            'als_m3_cell_bank_ret_allnum', 'als_m3_cell_bank_orgnum', 'als_m3_cell_bank_tra_orgnum',
            'als_m3_cell_bank_ret_orgnum', 'als_m3_cell_bank_tot_mons', 'als_m3_cell_bank_avg_monnum',
            'als_m3_cell_bank_max_monnum', 'als_m3_cell_bank_min_monnum', 'als_m3_cell_bank_max_inteday',
            'als_m3_cell_bank_min_inteday', 'als_m3_cell_bank_week_allnum', 'als_m3_cell_bank_week_orgnum',
            'als_m3_cell_bank_night_allnum', 'als_m3_cell_bank_night_orgnum', 'als_m3_cell_nbank_selfnum',
            'als_m3_cell_nbank_allnum', 'als_m3_cell_nbank_p2p_allnum', 'als_m3_cell_nbank_mc_allnum',
            'als_m3_cell_nbank_ca_allnum', 'als_m3_cell_nbank_cf_allnum', 'als_m3_cell_nbank_com_allnum',
            'als_m3_cell_nbank_oth_allnum', 'als_m3_cell_nbank_nsloan_allnum', 'als_m3_cell_nbank_autofin_allnum',
            'als_m3_cell_nbank_sloan_allnum', 'als_m3_cell_nbank_cons_allnum', 'als_m3_cell_nbank_finlea_allnum',
            'als_m3_cell_nbank_else_allnum', 'als_m3_cell_nbank_orgnum', 'als_m3_cell_nbank_p2p_orgnum',
            'als_m3_cell_nbank_mc_orgnum', 'als_m3_cell_nbank_ca_orgnum', 'als_m3_cell_nbank_cf_orgnum',
            'als_m3_cell_nbank_com_orgnum', 'als_m3_cell_nbank_oth_orgnum', 'als_m3_cell_nbank_nsloan_orgnum',
            'als_m3_cell_nbank_autofin_orgnum', 'als_m3_cell_nbank_sloan_orgnum', 'als_m3_cell_nbank_cons_orgnum',
            'als_m3_cell_nbank_finlea_orgnum', 'als_m3_cell_nbank_else_orgnum', 'als_m3_cell_nbank_tot_mons',
            'als_m3_cell_nbank_avg_monnum', 'als_m3_cell_nbank_max_monnum', 'als_m3_cell_nbank_min_monnum',
            'als_m3_cell_nbank_max_inteday', 'als_m3_cell_nbank_min_inteday', 'als_m3_cell_nbank_week_allnum',
            'als_m3_cell_nbank_week_orgnum', 'als_m3_cell_nbank_night_allnum', 'als_m3_cell_nbank_night_orgnum',
            'als_m6_id_max_inteday', 'als_m6_id_min_inteday', 'als_m6_id_tot_mons', 'als_m6_id_avg_monnum',
            'als_m6_id_max_monnum', 'als_m6_id_min_monnum', 'als_m6_id_pdl_allnum', 'als_m6_id_pdl_orgnum',
            'als_m6_id_caon_allnum', 'als_m6_id_caon_orgnum', 'als_m6_id_rel_allnum', 'als_m6_id_rel_orgnum',
            'als_m6_id_caoff_allnum', 'als_m6_id_caoff_orgnum', 'als_m6_id_cooff_allnum', 'als_m6_id_cooff_orgnum',
            'als_m6_id_af_allnum', 'als_m6_id_af_orgnum', 'als_m6_id_coon_allnum', 'als_m6_id_coon_orgnum',
            'als_m6_id_oth_allnum', 'als_m6_id_oth_orgnum', 'als_m6_id_bank_selfnum', 'als_m6_id_bank_allnum',
            'als_m6_id_bank_tra_allnum', 'als_m6_id_bank_ret_allnum', 'als_m6_id_bank_orgnum',
            'als_m6_id_bank_tra_orgnum', 'als_m6_id_bank_ret_orgnum', 'als_m6_id_bank_tot_mons',
            'als_m6_id_bank_avg_monnum', 'als_m6_id_bank_max_monnum', 'als_m6_id_bank_min_monnum',
            'als_m6_id_bank_max_inteday', 'als_m6_id_bank_min_inteday', 'als_m6_id_bank_week_allnum',
            'als_m6_id_bank_week_orgnum', 'als_m6_id_bank_night_allnum', 'als_m6_id_bank_night_orgnum',
            'als_m6_id_nbank_selfnum', 'als_m6_id_nbank_allnum', 'als_m6_id_nbank_p2p_allnum',
            'als_m6_id_nbank_mc_allnum', 'als_m6_id_nbank_ca_allnum', 'als_m6_id_nbank_cf_allnum',
            'als_m6_id_nbank_com_allnum', 'als_m6_id_nbank_oth_allnum', 'als_m6_id_nbank_nsloan_allnum',
            'als_m6_id_nbank_autofin_allnum', 'als_m6_id_nbank_sloan_allnum', 'als_m6_id_nbank_cons_allnum',
            'als_m6_id_nbank_finlea_allnum', 'als_m6_id_nbank_else_allnum', 'als_m6_id_nbank_orgnum',
            'als_m6_id_nbank_p2p_orgnum', 'als_m6_id_nbank_mc_orgnum', 'als_m6_id_nbank_ca_orgnum',
            'als_m6_id_nbank_cf_orgnum', 'als_m6_id_nbank_com_orgnum', 'als_m6_id_nbank_oth_orgnum',
            'als_m6_id_nbank_nsloan_orgnum', 'als_m6_id_nbank_autofin_orgnum', 'als_m6_id_nbank_sloan_orgnum',
            'als_m6_id_nbank_cons_orgnum', 'als_m6_id_nbank_finlea_orgnum', 'als_m6_id_nbank_else_orgnum',
            'als_m6_id_nbank_tot_mons', 'als_m6_id_nbank_avg_monnum', 'als_m6_id_nbank_max_monnum',
            'als_m6_id_nbank_min_monnum', 'als_m6_id_nbank_max_inteday', 'als_m6_id_nbank_min_inteday',
            'als_m6_id_nbank_week_allnum', 'als_m6_id_nbank_week_orgnum', 'als_m6_id_nbank_night_allnum',
            'als_m6_id_nbank_night_orgnum', 'als_m6_cell_max_inteday', 'als_m6_cell_min_inteday',
            'als_m6_cell_tot_mons', 'als_m6_cell_avg_monnum', 'als_m6_cell_max_monnum', 'als_m6_cell_min_monnum',
            'als_m6_cell_pdl_allnum', 'als_m6_cell_pdl_orgnum', 'als_m6_cell_caon_allnum', 'als_m6_cell_caon_orgnum',
            'als_m6_cell_rel_allnum', 'als_m6_cell_rel_orgnum', 'als_m6_cell_caoff_allnum', 'als_m6_cell_caoff_orgnum',
            'als_m6_cell_cooff_allnum', 'als_m6_cell_cooff_orgnum', 'als_m6_cell_af_allnum', 'als_m6_cell_af_orgnum',
            'als_m6_cell_coon_allnum', 'als_m6_cell_coon_orgnum', 'als_m6_cell_oth_allnum', 'als_m6_cell_oth_orgnum',
            'als_m6_cell_bank_selfnum', 'als_m6_cell_bank_allnum', 'als_m6_cell_bank_tra_allnum',
            'als_m6_cell_bank_ret_allnum', 'als_m6_cell_bank_orgnum', 'als_m6_cell_bank_tra_orgnum',
            'als_m6_cell_bank_ret_orgnum', 'als_m6_cell_bank_tot_mons', 'als_m6_cell_bank_avg_monnum',
            'als_m6_cell_bank_max_monnum', 'als_m6_cell_bank_min_monnum', 'als_m6_cell_bank_max_inteday',
            'als_m6_cell_bank_min_inteday', 'als_m6_cell_bank_week_allnum', 'als_m6_cell_bank_week_orgnum',
            'als_m6_cell_bank_night_allnum', 'als_m6_cell_bank_night_orgnum', 'als_m6_cell_nbank_selfnum',
            'als_m6_cell_nbank_allnum', 'als_m6_cell_nbank_p2p_allnum', 'als_m6_cell_nbank_mc_allnum',
            'als_m6_cell_nbank_ca_allnum', 'als_m6_cell_nbank_cf_allnum', 'als_m6_cell_nbank_com_allnum',
            'als_m6_cell_nbank_oth_allnum', 'als_m6_cell_nbank_nsloan_allnum', 'als_m6_cell_nbank_autofin_allnum',
            'als_m6_cell_nbank_sloan_allnum', 'als_m6_cell_nbank_cons_allnum', 'als_m6_cell_nbank_finlea_allnum',
            'als_m6_cell_nbank_else_allnum', 'als_m6_cell_nbank_orgnum', 'als_m6_cell_nbank_p2p_orgnum',
            'als_m6_cell_nbank_mc_orgnum', 'als_m6_cell_nbank_ca_orgnum', 'als_m6_cell_nbank_cf_orgnum',
            'als_m6_cell_nbank_com_orgnum', 'als_m6_cell_nbank_oth_orgnum', 'als_m6_cell_nbank_nsloan_orgnum',
            'als_m6_cell_nbank_autofin_orgnum', 'als_m6_cell_nbank_sloan_orgnum', 'als_m6_cell_nbank_cons_orgnum',
            'als_m6_cell_nbank_finlea_orgnum', 'als_m6_cell_nbank_else_orgnum', 'als_m6_cell_nbank_tot_mons',
            'als_m6_cell_nbank_avg_monnum', 'als_m6_cell_nbank_max_monnum', 'als_m6_cell_nbank_min_monnum',
            'als_m6_cell_nbank_max_inteday', 'als_m6_cell_nbank_min_inteday', 'als_m6_cell_nbank_week_allnum',
            'als_m6_cell_nbank_week_orgnum', 'als_m6_cell_nbank_night_allnum', 'als_m6_cell_nbank_night_orgnum',
            'als_m12_id_max_inteday', 'als_m12_id_min_inteday', 'als_m12_id_tot_mons', 'als_m12_id_avg_monnum',
            'als_m12_id_max_monnum', 'als_m12_id_min_monnum', 'als_m12_id_pdl_allnum', 'als_m12_id_pdl_orgnum',
            'als_m12_id_caon_allnum', 'als_m12_id_caon_orgnum', 'als_m12_id_rel_allnum', 'als_m12_id_rel_orgnum',
            'als_m12_id_caoff_allnum', 'als_m12_id_caoff_orgnum', 'als_m12_id_cooff_allnum', 'als_m12_id_cooff_orgnum',
            'als_m12_id_af_allnum', 'als_m12_id_af_orgnum', 'als_m12_id_coon_allnum', 'als_m12_id_coon_orgnum',
            'als_m12_id_oth_allnum', 'als_m12_id_oth_orgnum', 'als_m12_id_bank_selfnum', 'als_m12_id_bank_allnum',
            'als_m12_id_bank_tra_allnum', 'als_m12_id_bank_ret_allnum', 'als_m12_id_bank_orgnum',
            'als_m12_id_bank_tra_orgnum', 'als_m12_id_bank_ret_orgnum', 'als_m12_id_bank_tot_mons',
            'als_m12_id_bank_avg_monnum', 'als_m12_id_bank_max_monnum', 'als_m12_id_bank_min_monnum',
            'als_m12_id_bank_max_inteday', 'als_m12_id_bank_min_inteday', 'als_m12_id_bank_week_allnum',
            'als_m12_id_bank_week_orgnum', 'als_m12_id_bank_night_allnum', 'als_m12_id_bank_night_orgnum',
            'als_m12_id_nbank_selfnum', 'als_m12_id_nbank_allnum', 'als_m12_id_nbank_p2p_allnum',
            'als_m12_id_nbank_mc_allnum', 'als_m12_id_nbank_ca_allnum', 'als_m12_id_nbank_cf_allnum',
            'als_m12_id_nbank_com_allnum', 'als_m12_id_nbank_oth_allnum', 'als_m12_id_nbank_nsloan_allnum',
            'als_m12_id_nbank_autofin_allnum', 'als_m12_id_nbank_sloan_allnum', 'als_m12_id_nbank_cons_allnum',
            'als_m12_id_nbank_finlea_allnum', 'als_m12_id_nbank_else_allnum', 'als_m12_id_nbank_orgnum',
            'als_m12_id_nbank_p2p_orgnum', 'als_m12_id_nbank_mc_orgnum', 'als_m12_id_nbank_ca_orgnum',
            'als_m12_id_nbank_cf_orgnum', 'als_m12_id_nbank_com_orgnum', 'als_m12_id_nbank_oth_orgnum',
            'als_m12_id_nbank_nsloan_orgnum', 'als_m12_id_nbank_autofin_orgnum', 'als_m12_id_nbank_sloan_orgnum',
            'als_m12_id_nbank_cons_orgnum', 'als_m12_id_nbank_finlea_orgnum', 'als_m12_id_nbank_else_orgnum',
            'als_m12_id_nbank_tot_mons', 'als_m12_id_nbank_avg_monnum', 'als_m12_id_nbank_max_monnum',
            'als_m12_id_nbank_min_monnum', 'als_m12_id_nbank_max_inteday', 'als_m12_id_nbank_min_inteday',
            'als_m12_id_nbank_week_allnum', 'als_m12_id_nbank_week_orgnum', 'als_m12_id_nbank_night_allnum',
            'als_m12_id_nbank_night_orgnum', 'als_m12_cell_max_inteday', 'als_m12_cell_min_inteday',
            'als_m12_cell_tot_mons', 'als_m12_cell_avg_monnum', 'als_m12_cell_max_monnum', 'als_m12_cell_min_monnum',
            'als_m12_cell_pdl_allnum', 'als_m12_cell_pdl_orgnum', 'als_m12_cell_caon_allnum',
            'als_m12_cell_caon_orgnum', 'als_m12_cell_rel_allnum', 'als_m12_cell_rel_orgnum',
            'als_m12_cell_caoff_allnum', 'als_m12_cell_caoff_orgnum', 'als_m12_cell_cooff_allnum',
            'als_m12_cell_cooff_orgnum', 'als_m12_cell_af_allnum', 'als_m12_cell_af_orgnum', 'als_m12_cell_coon_allnum',
            'als_m12_cell_coon_orgnum', 'als_m12_cell_oth_allnum', 'als_m12_cell_oth_orgnum',
            'als_m12_cell_bank_selfnum', 'als_m12_cell_bank_allnum', 'als_m12_cell_bank_tra_allnum',
            'als_m12_cell_bank_ret_allnum', 'als_m12_cell_bank_orgnum', 'als_m12_cell_bank_tra_orgnum',
            'als_m12_cell_bank_ret_orgnum', 'als_m12_cell_bank_tot_mons', 'als_m12_cell_bank_avg_monnum',
            'als_m12_cell_bank_max_monnum', 'als_m12_cell_bank_min_monnum', 'als_m12_cell_bank_max_inteday',
            'als_m12_cell_bank_min_inteday', 'als_m12_cell_bank_week_allnum', 'als_m12_cell_bank_week_orgnum',
            'als_m12_cell_bank_night_allnum', 'als_m12_cell_bank_night_orgnum', 'als_m12_cell_nbank_selfnum',
            'als_m12_cell_nbank_allnum', 'als_m12_cell_nbank_p2p_allnum', 'als_m12_cell_nbank_mc_allnum',
            'als_m12_cell_nbank_ca_allnum', 'als_m12_cell_nbank_cf_allnum', 'als_m12_cell_nbank_com_allnum',
            'als_m12_cell_nbank_oth_allnum', 'als_m12_cell_nbank_nsloan_allnum', 'als_m12_cell_nbank_autofin_allnum',
            'als_m12_cell_nbank_sloan_allnum', 'als_m12_cell_nbank_cons_allnum', 'als_m12_cell_nbank_finlea_allnum',
            'als_m12_cell_nbank_else_allnum', 'als_m12_cell_nbank_orgnum', 'als_m12_cell_nbank_p2p_orgnum',
            'als_m12_cell_nbank_mc_orgnum', 'als_m12_cell_nbank_ca_orgnum', 'als_m12_cell_nbank_cf_orgnum',
            'als_m12_cell_nbank_com_orgnum', 'als_m12_cell_nbank_oth_orgnum', 'als_m12_cell_nbank_nsloan_orgnum',
            'als_m12_cell_nbank_autofin_orgnum', 'als_m12_cell_nbank_sloan_orgnum', 'als_m12_cell_nbank_cons_orgnum',
            'als_m12_cell_nbank_finlea_orgnum', 'als_m12_cell_nbank_else_orgnum', 'als_m12_cell_nbank_tot_mons',
            'als_m12_cell_nbank_avg_monnum', 'als_m12_cell_nbank_max_monnum', 'als_m12_cell_nbank_min_monnum',
            'als_m12_cell_nbank_max_inteday', 'als_m12_cell_nbank_min_inteday', 'als_m12_cell_nbank_week_allnum',
            'als_m12_cell_nbank_week_orgnum', 'als_m12_cell_nbank_night_allnum', 'als_m12_cell_nbank_night_orgnum',
            'als_fst_id_bank_inteday', 'als_fst_id_nbank_inteday', 'als_fst_cell_bank_inteday',
            'als_fst_cell_nbank_inteday', 'als_lst_id_bank_inteday', 'als_lst_id_bank_consnum',
            'als_lst_id_bank_csinteday', 'als_lst_id_nbank_inteday', 'als_lst_id_nbank_consnum',
            'als_lst_id_nbank_csinteday', 'als_lst_cell_bank_inteday', 'als_lst_cell_bank_consnum',
            'als_lst_cell_bank_csinteday', 'als_lst_cell_nbank_inteday', 'als_lst_cell_nbank_consnum',
            'als_lst_cell_nbank_csinteday', 'alm_d7_id_bank_selfnum', 'alm_d7_id_bank_allnum',
            'alm_d7_id_bank_tra_allnum', 'alm_d7_id_bank_ret_allnum', 'alm_d7_id_bank_orgnum',
            'alm_d7_id_bank_tra_orgnum', 'alm_d7_id_bank_ret_orgnum', 'alm_d7_id_nbank_selfnum',
            'alm_d7_id_nbank_allnum', 'alm_d7_id_nbank_p2p_allnum', 'alm_d7_id_nbank_mc_allnum',
            'alm_d7_id_nbank_ca_allnum', 'alm_d7_id_nbank_cf_allnum', 'alm_d7_id_nbank_com_allnum',
            'alm_d7_id_nbank_oth_allnum', 'alm_d7_id_nbank_nsloan_allnum', 'alm_d7_id_nbank_autofin_allnum',
            'alm_d7_id_nbank_sloan_allnum', 'alm_d7_id_nbank_cons_allnum', 'alm_d7_id_nbank_finlea_allnum',
            'alm_d7_id_nbank_else_allnum', 'alm_d7_id_nbank_orgnum', 'alm_d7_id_nbank_p2p_orgnum',
            'alm_d7_id_nbank_mc_orgnum', 'alm_d7_id_nbank_ca_orgnum', 'alm_d7_id_nbank_cf_orgnum',
            'alm_d7_id_nbank_com_orgnum', 'alm_d7_id_nbank_oth_orgnum', 'alm_d7_id_nbank_nsloan_orgnum',
            'alm_d7_id_nbank_autofin_orgnum', 'alm_d7_id_nbank_sloan_orgnum', 'alm_d7_id_nbank_cons_orgnum',
            'alm_d7_id_nbank_finlea_orgnum', 'alm_d7_id_nbank_else_orgnum', 'alm_d7_cell_bank_selfnum',
            'alm_d7_cell_bank_allnum', 'alm_d7_cell_bank_tra_allnum', 'alm_d7_cell_bank_ret_allnum',
            'alm_d7_cell_bank_orgnum', 'alm_d7_cell_bank_tra_orgnum', 'alm_d7_cell_bank_ret_orgnum',
            'alm_d7_cell_nbank_selfnum', 'alm_d7_cell_nbank_allnum', 'alm_d7_cell_nbank_p2p_allnum',
            'alm_d7_cell_nbank_mc_allnum', 'alm_d7_cell_nbank_ca_allnum', 'alm_d7_cell_nbank_cf_allnum',
            'alm_d7_cell_nbank_com_allnum', 'alm_d7_cell_nbank_oth_allnum', 'alm_d7_cell_nbank_nsloan_allnum',
            'alm_d7_cell_nbank_autofin_allnum', 'alm_d7_cell_nbank_sloan_allnum', 'alm_d7_cell_nbank_cons_allnum',
            'alm_d7_cell_nbank_finlea_allnum', 'alm_d7_cell_nbank_else_allnum', 'alm_d7_cell_nbank_orgnum',
            'alm_d7_cell_nbank_p2p_orgnum', 'alm_d7_cell_nbank_mc_orgnum', 'alm_d7_cell_nbank_ca_orgnum',
            'alm_d7_cell_nbank_cf_orgnum', 'alm_d7_cell_nbank_com_orgnum', 'alm_d7_cell_nbank_oth_orgnum',
            'alm_d7_cell_nbank_nsloan_orgnum', 'alm_d7_cell_nbank_autofin_orgnum', 'alm_d7_cell_nbank_sloan_orgnum',
            'alm_d7_cell_nbank_cons_orgnum', 'alm_d7_cell_nbank_finlea_orgnum', 'alm_d7_cell_nbank_else_orgnum',
            'alm_d7_lm_cell_bank_selfnum', 'alm_d7_lm_cell_bank_allnum', 'alm_d7_lm_cell_bank_tra_allnum',
            'alm_d7_lm_cell_bank_ret_allnum', 'alm_d7_lm_cell_bank_orgnum', 'alm_d7_lm_cell_bank_tra_orgnum',
            'alm_d7_lm_cell_bank_ret_orgnum', 'alm_d7_lm_cell_nbank_selfnum', 'alm_d7_lm_cell_nbank_allnum',
            'alm_d7_lm_cell_nbank_p2p_allnum', 'alm_d7_lm_cell_nbank_mc_allnum', 'alm_d7_lm_cell_nbank_ca_allnum',
            'alm_d7_lm_cell_nbank_cf_allnum', 'alm_d7_lm_cell_nbank_com_allnum', 'alm_d7_lm_cell_nbank_oth_allnum',
            'alm_d7_lm_cell_nbank_nsloan_allnum', 'alm_d7_lm_cell_nbank_autofin_allnum',
            'alm_d7_lm_cell_nbank_sloan_allnum', 'alm_d7_lm_cell_nbank_cons_allnum',
            'alm_d7_lm_cell_nbank_finlea_allnum', 'alm_d7_lm_cell_nbank_else_allnum', 'alm_d7_lm_cell_nbank_orgnum',
            'alm_d7_lm_cell_nbank_p2p_orgnum', 'alm_d7_lm_cell_nbank_mc_orgnum', 'alm_d7_lm_cell_nbank_ca_orgnum',
            'alm_d7_lm_cell_nbank_cf_orgnum', 'alm_d7_lm_cell_nbank_com_orgnum', 'alm_d7_lm_cell_nbank_oth_orgnum',
            'alm_d7_lm_cell_nbank_nsloan_orgnum', 'alm_d7_lm_cell_nbank_autofin_orgnum',
            'alm_d7_lm_cell_nbank_sloan_orgnum', 'alm_d7_lm_cell_nbank_cons_orgnum',
            'alm_d7_lm_cell_nbank_finlea_orgnum', 'alm_d7_lm_cell_nbank_else_orgnum', 'alm_d15_id_bank_selfnum',
            'alm_d15_id_bank_allnum', 'alm_d15_id_bank_tra_allnum', 'alm_d15_id_bank_ret_allnum',
            'alm_d15_id_bank_orgnum', 'alm_d15_id_bank_tra_orgnum', 'alm_d15_id_bank_ret_orgnum',
            'alm_d15_id_nbank_selfnum', 'alm_d15_id_nbank_allnum', 'alm_d15_id_nbank_p2p_allnum',
            'alm_d15_id_nbank_mc_allnum', 'alm_d15_id_nbank_ca_allnum', 'alm_d15_id_nbank_cf_allnum',
            'alm_d15_id_nbank_com_allnum', 'alm_d15_id_nbank_oth_allnum', 'alm_d15_id_nbank_nsloan_allnum',
            'alm_d15_id_nbank_autofin_allnum', 'alm_d15_id_nbank_sloan_allnum', 'alm_d15_id_nbank_cons_allnum',
            'alm_d15_id_nbank_finlea_allnum', 'alm_d15_id_nbank_else_allnum', 'alm_d15_id_nbank_orgnum',
            'alm_d15_id_nbank_p2p_orgnum', 'alm_d15_id_nbank_mc_orgnum', 'alm_d15_id_nbank_ca_orgnum',
            'alm_d15_id_nbank_cf_orgnum', 'alm_d15_id_nbank_com_orgnum', 'alm_d15_id_nbank_oth_orgnum',
            'alm_d15_id_nbank_nsloan_orgnum', 'alm_d15_id_nbank_autofin_orgnum', 'alm_d15_id_nbank_sloan_orgnum',
            'alm_d15_id_nbank_cons_orgnum', 'alm_d15_id_nbank_finlea_orgnum', 'alm_d15_id_nbank_else_orgnum',
            'alm_d15_cell_bank_selfnum', 'alm_d15_cell_bank_allnum', 'alm_d15_cell_bank_tra_allnum',
            'alm_d15_cell_bank_ret_allnum', 'alm_d15_cell_bank_orgnum', 'alm_d15_cell_bank_tra_orgnum',
            'alm_d15_cell_bank_ret_orgnum', 'alm_d15_cell_nbank_selfnum', 'alm_d15_cell_nbank_allnum',
            'alm_d15_cell_nbank_p2p_allnum', 'alm_d15_cell_nbank_mc_allnum', 'alm_d15_cell_nbank_ca_allnum',
            'alm_d15_cell_nbank_cf_allnum', 'alm_d15_cell_nbank_com_allnum', 'alm_d15_cell_nbank_oth_allnum',
            'alm_d15_cell_nbank_nsloan_allnum', 'alm_d15_cell_nbank_autofin_allnum', 'alm_d15_cell_nbank_sloan_allnum',
            'alm_d15_cell_nbank_cons_allnum', 'alm_d15_cell_nbank_finlea_allnum', 'alm_d15_cell_nbank_else_allnum',
            'alm_d15_cell_nbank_orgnum', 'alm_d15_cell_nbank_p2p_orgnum', 'alm_d15_cell_nbank_mc_orgnum',
            'alm_d15_cell_nbank_ca_orgnum', 'alm_d15_cell_nbank_cf_orgnum', 'alm_d15_cell_nbank_com_orgnum',
            'alm_d15_cell_nbank_oth_orgnum', 'alm_d15_cell_nbank_nsloan_orgnum', 'alm_d15_cell_nbank_autofin_orgnum',
            'alm_d15_cell_nbank_sloan_orgnum', 'alm_d15_cell_nbank_cons_orgnum', 'alm_d15_cell_nbank_finlea_orgnum',
            'alm_d15_cell_nbank_else_orgnum', 'alm_d15_lm_cell_bank_selfnum', 'alm_d15_lm_cell_bank_allnum',
            'alm_d15_lm_cell_bank_tra_allnum', 'alm_d15_lm_cell_bank_ret_allnum', 'alm_d15_lm_cell_bank_orgnum',
            'alm_d15_lm_cell_bank_tra_orgnum', 'alm_d15_lm_cell_bank_ret_orgnum', 'alm_d15_lm_cell_nbank_selfnum',
            'alm_d15_lm_cell_nbank_allnum', 'alm_d15_lm_cell_nbank_p2p_allnum', 'alm_d15_lm_cell_nbank_mc_allnum',
            'alm_d15_lm_cell_nbank_ca_allnum', 'alm_d15_lm_cell_nbank_cf_allnum', 'alm_d15_lm_cell_nbank_com_allnum',
            'alm_d15_lm_cell_nbank_oth_allnum', 'alm_d15_lm_cell_nbank_nsloan_allnum',
            'alm_d15_lm_cell_nbank_autofin_allnum', 'alm_d15_lm_cell_nbank_sloan_allnum',
            'alm_d15_lm_cell_nbank_cons_allnum', 'alm_d15_lm_cell_nbank_finlea_allnum',
            'alm_d15_lm_cell_nbank_else_allnum', 'alm_d15_lm_cell_nbank_orgnum', 'alm_d15_lm_cell_nbank_p2p_orgnum',
            'alm_d15_lm_cell_nbank_mc_orgnum', 'alm_d15_lm_cell_nbank_ca_orgnum', 'alm_d15_lm_cell_nbank_cf_orgnum',
            'alm_d15_lm_cell_nbank_com_orgnum', 'alm_d15_lm_cell_nbank_oth_orgnum',
            'alm_d15_lm_cell_nbank_nsloan_orgnum', 'alm_d15_lm_cell_nbank_autofin_orgnum',
            'alm_d15_lm_cell_nbank_sloan_orgnum', 'alm_d15_lm_cell_nbank_cons_orgnum',
            'alm_d15_lm_cell_nbank_finlea_orgnum', 'alm_d15_lm_cell_nbank_else_orgnum', 'alm_m1_id_bank_selfnum',
            'alm_m1_id_bank_allnum', 'alm_m1_id_bank_tra_allnum', 'alm_m1_id_bank_ret_allnum', 'alm_m1_id_bank_orgnum',
            'alm_m1_id_bank_tra_orgnum', 'alm_m1_id_bank_ret_orgnum', 'alm_m1_id_nbank_selfnum',
            'alm_m1_id_nbank_allnum', 'alm_m1_id_nbank_p2p_allnum', 'alm_m1_id_nbank_mc_allnum',
            'alm_m1_id_nbank_ca_allnum', 'alm_m1_id_nbank_cf_allnum', 'alm_m1_id_nbank_com_allnum',
            'alm_m1_id_nbank_oth_allnum', 'alm_m1_id_nbank_nsloan_allnum', 'alm_m1_id_nbank_autofin_allnum',
            'alm_m1_id_nbank_sloan_allnum', 'alm_m1_id_nbank_cons_allnum', 'alm_m1_id_nbank_finlea_allnum',
            'alm_m1_id_nbank_else_allnum', 'alm_m1_id_nbank_orgnum', 'alm_m1_id_nbank_p2p_orgnum',
            'alm_m1_id_nbank_mc_orgnum', 'alm_m1_id_nbank_ca_orgnum', 'alm_m1_id_nbank_cf_orgnum',
            'alm_m1_id_nbank_com_orgnum', 'alm_m1_id_nbank_oth_orgnum', 'alm_m1_id_nbank_nsloan_orgnum',
            'alm_m1_id_nbank_autofin_orgnum', 'alm_m1_id_nbank_sloan_orgnum', 'alm_m1_id_nbank_cons_orgnum',
            'alm_m1_id_nbank_finlea_orgnum', 'alm_m1_id_nbank_else_orgnum', 'alm_m1_cell_bank_selfnum',
            'alm_m1_cell_bank_allnum', 'alm_m1_cell_bank_tra_allnum', 'alm_m1_cell_bank_ret_allnum',
            'alm_m1_cell_bank_orgnum', 'alm_m1_cell_bank_tra_orgnum', 'alm_m1_cell_bank_ret_orgnum',
            'alm_m1_cell_nbank_selfnum', 'alm_m1_cell_nbank_allnum', 'alm_m1_cell_nbank_p2p_allnum',
            'alm_m1_cell_nbank_mc_allnum', 'alm_m1_cell_nbank_ca_allnum', 'alm_m1_cell_nbank_cf_allnum',
            'alm_m1_cell_nbank_com_allnum', 'alm_m1_cell_nbank_oth_allnum', 'alm_m1_cell_nbank_nsloan_allnum',
            'alm_m1_cell_nbank_autofin_allnum', 'alm_m1_cell_nbank_sloan_allnum', 'alm_m1_cell_nbank_cons_allnum',
            'alm_m1_cell_nbank_finlea_allnum', 'alm_m1_cell_nbank_else_allnum', 'alm_m1_cell_nbank_orgnum',
            'alm_m1_cell_nbank_p2p_orgnum', 'alm_m1_cell_nbank_mc_orgnum', 'alm_m1_cell_nbank_ca_orgnum',
            'alm_m1_cell_nbank_cf_orgnum', 'alm_m1_cell_nbank_com_orgnum', 'alm_m1_cell_nbank_oth_orgnum',
            'alm_m1_cell_nbank_nsloan_orgnum', 'alm_m1_cell_nbank_autofin_orgnum', 'alm_m1_cell_nbank_sloan_orgnum',
            'alm_m1_cell_nbank_cons_orgnum', 'alm_m1_cell_nbank_finlea_orgnum', 'alm_m1_cell_nbank_else_orgnum',
            'alm_m1_lm_cell_bank_selfnum', 'alm_m1_lm_cell_bank_allnum', 'alm_m1_lm_cell_bank_tra_allnum',
            'alm_m1_lm_cell_bank_ret_allnum', 'alm_m1_lm_cell_bank_orgnum', 'alm_m1_lm_cell_bank_tra_orgnum',
            'alm_m1_lm_cell_bank_ret_orgnum', 'alm_m1_lm_cell_nbank_selfnum', 'alm_m1_lm_cell_nbank_allnum',
            'alm_m1_lm_cell_nbank_p2p_allnum', 'alm_m1_lm_cell_nbank_mc_allnum', 'alm_m1_lm_cell_nbank_ca_allnum',
            'alm_m1_lm_cell_nbank_cf_allnum', 'alm_m1_lm_cell_nbank_com_allnum', 'alm_m1_lm_cell_nbank_oth_allnum',
            'alm_m1_lm_cell_nbank_nsloan_allnum', 'alm_m1_lm_cell_nbank_autofin_allnum',
            'alm_m1_lm_cell_nbank_sloan_allnum', 'alm_m1_lm_cell_nbank_cons_allnum',
            'alm_m1_lm_cell_nbank_finlea_allnum', 'alm_m1_lm_cell_nbank_else_allnum', 'alm_m1_lm_cell_nbank_orgnum',
            'alm_m1_lm_cell_nbank_p2p_orgnum', 'alm_m1_lm_cell_nbank_mc_orgnum', 'alm_m1_lm_cell_nbank_ca_orgnum',
            'alm_m1_lm_cell_nbank_cf_orgnum', 'alm_m1_lm_cell_nbank_com_orgnum', 'alm_m1_lm_cell_nbank_oth_orgnum',
            'alm_m1_lm_cell_nbank_nsloan_orgnum', 'alm_m1_lm_cell_nbank_autofin_orgnum',
            'alm_m1_lm_cell_nbank_sloan_orgnum', 'alm_m1_lm_cell_nbank_cons_orgnum',
            'alm_m1_lm_cell_nbank_finlea_orgnum', 'alm_m1_lm_cell_nbank_else_orgnum', 'alm_m2_id_bank_selfnum',
            'alm_m2_id_bank_allnum', 'alm_m2_id_bank_tra_allnum', 'alm_m2_id_bank_ret_allnum', 'alm_m2_id_bank_orgnum',
            'alm_m2_id_bank_tra_orgnum', 'alm_m2_id_bank_ret_orgnum', 'alm_m2_id_nbank_selfnum',
            'alm_m2_id_nbank_allnum', 'alm_m2_id_nbank_p2p_allnum', 'alm_m2_id_nbank_mc_allnum',
            'alm_m2_id_nbank_ca_allnum', 'alm_m2_id_nbank_cf_allnum', 'alm_m2_id_nbank_com_allnum',
            'alm_m2_id_nbank_oth_allnum', 'alm_m2_id_nbank_nsloan_allnum', 'alm_m2_id_nbank_autofin_allnum',
            'alm_m2_id_nbank_sloan_allnum', 'alm_m2_id_nbank_cons_allnum', 'alm_m2_id_nbank_finlea_allnum',
            'alm_m2_id_nbank_else_allnum', 'alm_m2_id_nbank_orgnum', 'alm_m2_id_nbank_p2p_orgnum',
            'alm_m2_id_nbank_mc_orgnum', 'alm_m2_id_nbank_ca_orgnum', 'alm_m2_id_nbank_cf_orgnum',
            'alm_m2_id_nbank_com_orgnum', 'alm_m2_id_nbank_oth_orgnum', 'alm_m2_id_nbank_nsloan_orgnum',
            'alm_m2_id_nbank_autofin_orgnum', 'alm_m2_id_nbank_sloan_orgnum', 'alm_m2_id_nbank_cons_orgnum',
            'alm_m2_id_nbank_finlea_orgnum', 'alm_m2_id_nbank_else_orgnum', 'alm_m2_cell_bank_selfnum',
            'alm_m2_cell_bank_allnum', 'alm_m2_cell_bank_tra_allnum', 'alm_m2_cell_bank_ret_allnum',
            'alm_m2_cell_bank_orgnum', 'alm_m2_cell_bank_tra_orgnum', 'alm_m2_cell_bank_ret_orgnum',
            'alm_m2_cell_nbank_selfnum', 'alm_m2_cell_nbank_allnum', 'alm_m2_cell_nbank_p2p_allnum',
            'alm_m2_cell_nbank_mc_allnum', 'alm_m2_cell_nbank_ca_allnum', 'alm_m2_cell_nbank_cf_allnum',
            'alm_m2_cell_nbank_com_allnum', 'alm_m2_cell_nbank_oth_allnum', 'alm_m2_cell_nbank_nsloan_allnum',
            'alm_m2_cell_nbank_autofin_allnum', 'alm_m2_cell_nbank_sloan_allnum', 'alm_m2_cell_nbank_cons_allnum',
            'alm_m2_cell_nbank_finlea_allnum', 'alm_m2_cell_nbank_else_allnum', 'alm_m2_cell_nbank_orgnum',
            'alm_m2_cell_nbank_p2p_orgnum', 'alm_m2_cell_nbank_mc_orgnum', 'alm_m2_cell_nbank_ca_orgnum',
            'alm_m2_cell_nbank_cf_orgnum', 'alm_m2_cell_nbank_com_orgnum', 'alm_m2_cell_nbank_oth_orgnum',
            'alm_m2_cell_nbank_nsloan_orgnum', 'alm_m2_cell_nbank_autofin_orgnum', 'alm_m2_cell_nbank_sloan_orgnum',
            'alm_m2_cell_nbank_cons_orgnum', 'alm_m2_cell_nbank_finlea_orgnum', 'alm_m2_cell_nbank_else_orgnum',
            'alm_m2_lm_cell_bank_selfnum', 'alm_m2_lm_cell_bank_allnum', 'alm_m2_lm_cell_bank_tra_allnum',
            'alm_m2_lm_cell_bank_ret_allnum', 'alm_m2_lm_cell_bank_orgnum', 'alm_m2_lm_cell_bank_tra_orgnum',
            'alm_m2_lm_cell_bank_ret_orgnum', 'alm_m2_lm_cell_nbank_selfnum', 'alm_m2_lm_cell_nbank_allnum',
            'alm_m2_lm_cell_nbank_p2p_allnum', 'alm_m2_lm_cell_nbank_mc_allnum', 'alm_m2_lm_cell_nbank_ca_allnum',
            'alm_m2_lm_cell_nbank_cf_allnum', 'alm_m2_lm_cell_nbank_com_allnum', 'alm_m2_lm_cell_nbank_oth_allnum',
            'alm_m2_lm_cell_nbank_nsloan_allnum', 'alm_m2_lm_cell_nbank_autofin_allnum',
            'alm_m2_lm_cell_nbank_sloan_allnum', 'alm_m2_lm_cell_nbank_cons_allnum',
            'alm_m2_lm_cell_nbank_finlea_allnum', 'alm_m2_lm_cell_nbank_else_allnum', 'alm_m2_lm_cell_nbank_orgnum',
            'alm_m2_lm_cell_nbank_p2p_orgnum', 'alm_m2_lm_cell_nbank_mc_orgnum', 'alm_m2_lm_cell_nbank_ca_orgnum',
            'alm_m2_lm_cell_nbank_cf_orgnum', 'alm_m2_lm_cell_nbank_com_orgnum', 'alm_m2_lm_cell_nbank_oth_orgnum',
            'alm_m2_lm_cell_nbank_nsloan_orgnum', 'alm_m2_lm_cell_nbank_autofin_orgnum',
            'alm_m2_lm_cell_nbank_sloan_orgnum', 'alm_m2_lm_cell_nbank_cons_orgnum',
            'alm_m2_lm_cell_nbank_finlea_orgnum', 'alm_m2_lm_cell_nbank_else_orgnum', 'alm_m3_id_bank_selfnum',
            'alm_m3_id_bank_allnum', 'alm_m3_id_bank_tra_allnum', 'alm_m3_id_bank_ret_allnum', 'alm_m3_id_bank_orgnum',
            'alm_m3_id_bank_tra_orgnum', 'alm_m3_id_bank_ret_orgnum', 'alm_m3_id_nbank_selfnum',
            'alm_m3_id_nbank_allnum', 'alm_m3_id_nbank_p2p_allnum', 'alm_m3_id_nbank_mc_allnum',
            'alm_m3_id_nbank_ca_allnum', 'alm_m3_id_nbank_cf_allnum', 'alm_m3_id_nbank_com_allnum',
            'alm_m3_id_nbank_oth_allnum', 'alm_m3_id_nbank_nsloan_allnum', 'alm_m3_id_nbank_autofin_allnum',
            'alm_m3_id_nbank_sloan_allnum', 'alm_m3_id_nbank_cons_allnum', 'alm_m3_id_nbank_finlea_allnum',
            'alm_m3_id_nbank_else_allnum', 'alm_m3_id_nbank_orgnum', 'alm_m3_id_nbank_p2p_orgnum',
            'alm_m3_id_nbank_mc_orgnum', 'alm_m3_id_nbank_ca_orgnum', 'alm_m3_id_nbank_cf_orgnum',
            'alm_m3_id_nbank_com_orgnum', 'alm_m3_id_nbank_oth_orgnum', 'alm_m3_id_nbank_nsloan_orgnum',
            'alm_m3_id_nbank_autofin_orgnum', 'alm_m3_id_nbank_sloan_orgnum', 'alm_m3_id_nbank_cons_orgnum',
            'alm_m3_id_nbank_finlea_orgnum', 'alm_m3_id_nbank_else_orgnum', 'alm_m3_cell_bank_selfnum',
            'alm_m3_cell_bank_allnum', 'alm_m3_cell_bank_tra_allnum', 'alm_m3_cell_bank_ret_allnum',
            'alm_m3_cell_bank_orgnum', 'alm_m3_cell_bank_tra_orgnum', 'alm_m3_cell_bank_ret_orgnum',
            'alm_m3_cell_nbank_selfnum', 'alm_m3_cell_nbank_allnum', 'alm_m3_cell_nbank_p2p_allnum',
            'alm_m3_cell_nbank_mc_allnum', 'alm_m3_cell_nbank_ca_allnum', 'alm_m3_cell_nbank_cf_allnum',
            'alm_m3_cell_nbank_com_allnum', 'alm_m3_cell_nbank_oth_allnum', 'alm_m3_cell_nbank_nsloan_allnum',
            'alm_m3_cell_nbank_autofin_allnum', 'alm_m3_cell_nbank_sloan_allnum', 'alm_m3_cell_nbank_cons_allnum',
            'alm_m3_cell_nbank_finlea_allnum', 'alm_m3_cell_nbank_else_allnum', 'alm_m3_cell_nbank_orgnum',
            'alm_m3_cell_nbank_p2p_orgnum', 'alm_m3_cell_nbank_mc_orgnum', 'alm_m3_cell_nbank_ca_orgnum',
            'alm_m3_cell_nbank_cf_orgnum', 'alm_m3_cell_nbank_com_orgnum', 'alm_m3_cell_nbank_oth_orgnum',
            'alm_m3_cell_nbank_nsloan_orgnum', 'alm_m3_cell_nbank_autofin_orgnum', 'alm_m3_cell_nbank_sloan_orgnum',
            'alm_m3_cell_nbank_cons_orgnum', 'alm_m3_cell_nbank_finlea_orgnum', 'alm_m3_cell_nbank_else_orgnum',
            'alm_m3_lm_cell_bank_selfnum', 'alm_m3_lm_cell_bank_allnum', 'alm_m3_lm_cell_bank_tra_allnum',
            'alm_m3_lm_cell_bank_ret_allnum', 'alm_m3_lm_cell_bank_orgnum', 'alm_m3_lm_cell_bank_tra_orgnum',
            'alm_m3_lm_cell_bank_ret_orgnum', 'alm_m3_lm_cell_nbank_selfnum', 'alm_m3_lm_cell_nbank_allnum',
            'alm_m3_lm_cell_nbank_p2p_allnum', 'alm_m3_lm_cell_nbank_mc_allnum', 'alm_m3_lm_cell_nbank_ca_allnum',
            'alm_m3_lm_cell_nbank_cf_allnum', 'alm_m3_lm_cell_nbank_com_allnum', 'alm_m3_lm_cell_nbank_oth_allnum',
            'alm_m3_lm_cell_nbank_nsloan_allnum', 'alm_m3_lm_cell_nbank_autofin_allnum',
            'alm_m3_lm_cell_nbank_sloan_allnum', 'alm_m3_lm_cell_nbank_cons_allnum',
            'alm_m3_lm_cell_nbank_finlea_allnum', 'alm_m3_lm_cell_nbank_else_allnum', 'alm_m3_lm_cell_nbank_orgnum',
            'alm_m3_lm_cell_nbank_p2p_orgnum', 'alm_m3_lm_cell_nbank_mc_orgnum', 'alm_m3_lm_cell_nbank_ca_orgnum',
            'alm_m3_lm_cell_nbank_cf_orgnum', 'alm_m3_lm_cell_nbank_com_orgnum', 'alm_m3_lm_cell_nbank_oth_orgnum',
            'alm_m3_lm_cell_nbank_nsloan_orgnum', 'alm_m3_lm_cell_nbank_autofin_orgnum',
            'alm_m3_lm_cell_nbank_sloan_orgnum', 'alm_m3_lm_cell_nbank_cons_orgnum',
            'alm_m3_lm_cell_nbank_finlea_orgnum', 'alm_m3_lm_cell_nbank_else_orgnum', 'alm_m4_id_bank_selfnum',
            'alm_m4_id_bank_allnum', 'alm_m4_id_bank_tra_allnum', 'alm_m4_id_bank_ret_allnum', 'alm_m4_id_bank_orgnum',
            'alm_m4_id_bank_tra_orgnum', 'alm_m4_id_bank_ret_orgnum', 'alm_m4_id_nbank_selfnum',
            'alm_m4_id_nbank_allnum', 'alm_m4_id_nbank_p2p_allnum', 'alm_m4_id_nbank_mc_allnum',
            'alm_m4_id_nbank_ca_allnum', 'alm_m4_id_nbank_cf_allnum', 'alm_m4_id_nbank_com_allnum',
            'alm_m4_id_nbank_oth_allnum', 'alm_m4_id_nbank_nsloan_allnum', 'alm_m4_id_nbank_autofin_allnum',
            'alm_m4_id_nbank_sloan_allnum', 'alm_m4_id_nbank_cons_allnum', 'alm_m4_id_nbank_finlea_allnum',
            'alm_m4_id_nbank_else_allnum', 'alm_m4_id_nbank_orgnum', 'alm_m4_id_nbank_p2p_orgnum',
            'alm_m4_id_nbank_mc_orgnum', 'alm_m4_id_nbank_ca_orgnum', 'alm_m4_id_nbank_cf_orgnum',
            'alm_m4_id_nbank_com_orgnum', 'alm_m4_id_nbank_oth_orgnum', 'alm_m4_id_nbank_nsloan_orgnum',
            'alm_m4_id_nbank_autofin_orgnum', 'alm_m4_id_nbank_sloan_orgnum', 'alm_m4_id_nbank_cons_orgnum',
            'alm_m4_id_nbank_finlea_orgnum', 'alm_m4_id_nbank_else_orgnum', 'alm_m4_cell_bank_selfnum',
            'alm_m4_cell_bank_allnum', 'alm_m4_cell_bank_tra_allnum', 'alm_m4_cell_bank_ret_allnum',
            'alm_m4_cell_bank_orgnum', 'alm_m4_cell_bank_tra_orgnum', 'alm_m4_cell_bank_ret_orgnum',
            'alm_m4_cell_nbank_selfnum', 'alm_m4_cell_nbank_allnum', 'alm_m4_cell_nbank_p2p_allnum',
            'alm_m4_cell_nbank_mc_allnum', 'alm_m4_cell_nbank_ca_allnum', 'alm_m4_cell_nbank_cf_allnum',
            'alm_m4_cell_nbank_com_allnum', 'alm_m4_cell_nbank_oth_allnum', 'alm_m4_cell_nbank_nsloan_allnum',
            'alm_m4_cell_nbank_autofin_allnum', 'alm_m4_cell_nbank_sloan_allnum', 'alm_m4_cell_nbank_cons_allnum',
            'alm_m4_cell_nbank_finlea_allnum', 'alm_m4_cell_nbank_else_allnum', 'alm_m4_cell_nbank_orgnum',
            'alm_m4_cell_nbank_p2p_orgnum', 'alm_m4_cell_nbank_mc_orgnum', 'alm_m4_cell_nbank_ca_orgnum',
            'alm_m4_cell_nbank_cf_orgnum', 'alm_m4_cell_nbank_com_orgnum', 'alm_m4_cell_nbank_oth_orgnum',
            'alm_m4_cell_nbank_nsloan_orgnum', 'alm_m4_cell_nbank_autofin_orgnum', 'alm_m4_cell_nbank_sloan_orgnum',
            'alm_m4_cell_nbank_cons_orgnum', 'alm_m4_cell_nbank_finlea_orgnum', 'alm_m4_cell_nbank_else_orgnum',
            'alm_m4_lm_cell_bank_selfnum', 'alm_m4_lm_cell_bank_allnum', 'alm_m4_lm_cell_bank_tra_allnum',
            'alm_m4_lm_cell_bank_ret_allnum', 'alm_m4_lm_cell_bank_orgnum', 'alm_m4_lm_cell_bank_tra_orgnum',
            'alm_m4_lm_cell_bank_ret_orgnum', 'alm_m4_lm_cell_nbank_selfnum', 'alm_m4_lm_cell_nbank_allnum',
            'alm_m4_lm_cell_nbank_p2p_allnum', 'alm_m4_lm_cell_nbank_mc_allnum', 'alm_m4_lm_cell_nbank_ca_allnum',
            'alm_m4_lm_cell_nbank_cf_allnum', 'alm_m4_lm_cell_nbank_com_allnum', 'alm_m4_lm_cell_nbank_oth_allnum',
            'alm_m4_lm_cell_nbank_nsloan_allnum', 'alm_m4_lm_cell_nbank_autofin_allnum',
            'alm_m4_lm_cell_nbank_sloan_allnum', 'alm_m4_lm_cell_nbank_cons_allnum',
            'alm_m4_lm_cell_nbank_finlea_allnum', 'alm_m4_lm_cell_nbank_else_allnum', 'alm_m4_lm_cell_nbank_orgnum',
            'alm_m4_lm_cell_nbank_p2p_orgnum', 'alm_m4_lm_cell_nbank_mc_orgnum', 'alm_m4_lm_cell_nbank_ca_orgnum',
            'alm_m4_lm_cell_nbank_cf_orgnum', 'alm_m4_lm_cell_nbank_com_orgnum', 'alm_m4_lm_cell_nbank_oth_orgnum',
            'alm_m4_lm_cell_nbank_nsloan_orgnum', 'alm_m4_lm_cell_nbank_autofin_orgnum',
            'alm_m4_lm_cell_nbank_sloan_orgnum', 'alm_m4_lm_cell_nbank_cons_orgnum',
            'alm_m4_lm_cell_nbank_finlea_orgnum', 'alm_m4_lm_cell_nbank_else_orgnum', 'alm_m5_id_bank_selfnum',
            'alm_m5_id_bank_allnum', 'alm_m5_id_bank_tra_allnum', 'alm_m5_id_bank_ret_allnum', 'alm_m5_id_bank_orgnum',
            'alm_m5_id_bank_tra_orgnum', 'alm_m5_id_bank_ret_orgnum', 'alm_m5_id_nbank_selfnum',
            'alm_m5_id_nbank_allnum', 'alm_m5_id_nbank_p2p_allnum', 'alm_m5_id_nbank_mc_allnum',
            'alm_m5_id_nbank_ca_allnum', 'alm_m5_id_nbank_cf_allnum', 'alm_m5_id_nbank_com_allnum',
            'alm_m5_id_nbank_oth_allnum', 'alm_m5_id_nbank_nsloan_allnum', 'alm_m5_id_nbank_autofin_allnum',
            'alm_m5_id_nbank_sloan_allnum', 'alm_m5_id_nbank_cons_allnum', 'alm_m5_id_nbank_finlea_allnum',
            'alm_m5_id_nbank_else_allnum', 'alm_m5_id_nbank_orgnum', 'alm_m5_id_nbank_p2p_orgnum',
            'alm_m5_id_nbank_mc_orgnum', 'alm_m5_id_nbank_ca_orgnum', 'alm_m5_id_nbank_cf_orgnum',
            'alm_m5_id_nbank_com_orgnum', 'alm_m5_id_nbank_oth_orgnum', 'alm_m5_id_nbank_nsloan_orgnum',
            'alm_m5_id_nbank_autofin_orgnum', 'alm_m5_id_nbank_sloan_orgnum', 'alm_m5_id_nbank_cons_orgnum',
            'alm_m5_id_nbank_finlea_orgnum', 'alm_m5_id_nbank_else_orgnum', 'alm_m5_cell_bank_selfnum',
            'alm_m5_cell_bank_allnum', 'alm_m5_cell_bank_tra_allnum', 'alm_m5_cell_bank_ret_allnum',
            'alm_m5_cell_bank_orgnum', 'alm_m5_cell_bank_tra_orgnum', 'alm_m5_cell_bank_ret_orgnum',
            'alm_m5_cell_nbank_selfnum', 'alm_m5_cell_nbank_allnum', 'alm_m5_cell_nbank_p2p_allnum',
            'alm_m5_cell_nbank_mc_allnum', 'alm_m5_cell_nbank_ca_allnum', 'alm_m5_cell_nbank_cf_allnum',
            'alm_m5_cell_nbank_com_allnum', 'alm_m5_cell_nbank_oth_allnum', 'alm_m5_cell_nbank_nsloan_allnum',
            'alm_m5_cell_nbank_autofin_allnum', 'alm_m5_cell_nbank_sloan_allnum', 'alm_m5_cell_nbank_cons_allnum',
            'alm_m5_cell_nbank_finlea_allnum', 'alm_m5_cell_nbank_else_allnum', 'alm_m5_cell_nbank_orgnum',
            'alm_m5_cell_nbank_p2p_orgnum', 'alm_m5_cell_nbank_mc_orgnum', 'alm_m5_cell_nbank_ca_orgnum',
            'alm_m5_cell_nbank_cf_orgnum', 'alm_m5_cell_nbank_com_orgnum', 'alm_m5_cell_nbank_oth_orgnum',
            'alm_m5_cell_nbank_nsloan_orgnum', 'alm_m5_cell_nbank_autofin_orgnum', 'alm_m5_cell_nbank_sloan_orgnum',
            'alm_m5_cell_nbank_cons_orgnum', 'alm_m5_cell_nbank_finlea_orgnum', 'alm_m5_cell_nbank_else_orgnum',
            'alm_m5_lm_cell_bank_selfnum', 'alm_m5_lm_cell_bank_allnum', 'alm_m5_lm_cell_bank_tra_allnum',
            'alm_m5_lm_cell_bank_ret_allnum', 'alm_m5_lm_cell_bank_orgnum', 'alm_m5_lm_cell_bank_tra_orgnum',
            'alm_m5_lm_cell_bank_ret_orgnum', 'alm_m5_lm_cell_nbank_selfnum', 'alm_m5_lm_cell_nbank_allnum',
            'alm_m5_lm_cell_nbank_p2p_allnum', 'alm_m5_lm_cell_nbank_mc_allnum', 'alm_m5_lm_cell_nbank_ca_allnum',
            'alm_m5_lm_cell_nbank_cf_allnum', 'alm_m5_lm_cell_nbank_com_allnum', 'alm_m5_lm_cell_nbank_oth_allnum',
            'alm_m5_lm_cell_nbank_nsloan_allnum', 'alm_m5_lm_cell_nbank_autofin_allnum',
            'alm_m5_lm_cell_nbank_sloan_allnum', 'alm_m5_lm_cell_nbank_cons_allnum',
            'alm_m5_lm_cell_nbank_finlea_allnum', 'alm_m5_lm_cell_nbank_else_allnum', 'alm_m5_lm_cell_nbank_orgnum',
            'alm_m5_lm_cell_nbank_p2p_orgnum', 'alm_m5_lm_cell_nbank_mc_orgnum', 'alm_m5_lm_cell_nbank_ca_orgnum',
            'alm_m5_lm_cell_nbank_cf_orgnum', 'alm_m5_lm_cell_nbank_com_orgnum', 'alm_m5_lm_cell_nbank_oth_orgnum',
            'alm_m5_lm_cell_nbank_nsloan_orgnum', 'alm_m5_lm_cell_nbank_autofin_orgnum',
            'alm_m5_lm_cell_nbank_sloan_orgnum', 'alm_m5_lm_cell_nbank_cons_orgnum',
            'alm_m5_lm_cell_nbank_finlea_orgnum', 'alm_m5_lm_cell_nbank_else_orgnum', 'alm_m6_id_bank_selfnum',
            'alm_m6_id_bank_allnum', 'alm_m6_id_bank_tra_allnum', 'alm_m6_id_bank_ret_allnum', 'alm_m6_id_bank_orgnum',
            'alm_m6_id_bank_tra_orgnum', 'alm_m6_id_bank_ret_orgnum', 'alm_m6_id_nbank_selfnum',
            'alm_m6_id_nbank_allnum', 'alm_m6_id_nbank_p2p_allnum', 'alm_m6_id_nbank_mc_allnum',
            'alm_m6_id_nbank_ca_allnum', 'alm_m6_id_nbank_cf_allnum', 'alm_m6_id_nbank_com_allnum',
            'alm_m6_id_nbank_oth_allnum', 'alm_m6_id_nbank_nsloan_allnum', 'alm_m6_id_nbank_autofin_allnum',
            'alm_m6_id_nbank_sloan_allnum', 'alm_m6_id_nbank_cons_allnum', 'alm_m6_id_nbank_finlea_allnum',
            'alm_m6_id_nbank_else_allnum', 'alm_m6_id_nbank_orgnum', 'alm_m6_id_nbank_p2p_orgnum',
            'alm_m6_id_nbank_mc_orgnum', 'alm_m6_id_nbank_ca_orgnum', 'alm_m6_id_nbank_cf_orgnum',
            'alm_m6_id_nbank_com_orgnum', 'alm_m6_id_nbank_oth_orgnum', 'alm_m6_id_nbank_nsloan_orgnum',
            'alm_m6_id_nbank_autofin_orgnum', 'alm_m6_id_nbank_sloan_orgnum', 'alm_m6_id_nbank_cons_orgnum',
            'alm_m6_id_nbank_finlea_orgnum', 'alm_m6_id_nbank_else_orgnum', 'alm_m6_cell_bank_selfnum',
            'alm_m6_cell_bank_allnum', 'alm_m6_cell_bank_tra_allnum', 'alm_m6_cell_bank_ret_allnum',
            'alm_m6_cell_bank_orgnum', 'alm_m6_cell_bank_tra_orgnum', 'alm_m6_cell_bank_ret_orgnum',
            'alm_m6_cell_nbank_selfnum', 'alm_m6_cell_nbank_allnum', 'alm_m6_cell_nbank_p2p_allnum',
            'alm_m6_cell_nbank_mc_allnum', 'alm_m6_cell_nbank_ca_allnum', 'alm_m6_cell_nbank_cf_allnum',
            'alm_m6_cell_nbank_com_allnum', 'alm_m6_cell_nbank_oth_allnum', 'alm_m6_cell_nbank_nsloan_allnum',
            'alm_m6_cell_nbank_autofin_allnum', 'alm_m6_cell_nbank_sloan_allnum', 'alm_m6_cell_nbank_cons_allnum',
            'alm_m6_cell_nbank_finlea_allnum', 'alm_m6_cell_nbank_else_allnum', 'alm_m6_cell_nbank_orgnum',
            'alm_m6_cell_nbank_p2p_orgnum', 'alm_m6_cell_nbank_mc_orgnum', 'alm_m6_cell_nbank_ca_orgnum',
            'alm_m6_cell_nbank_cf_orgnum', 'alm_m6_cell_nbank_com_orgnum', 'alm_m6_cell_nbank_oth_orgnum',
            'alm_m6_cell_nbank_nsloan_orgnum', 'alm_m6_cell_nbank_autofin_orgnum', 'alm_m6_cell_nbank_sloan_orgnum',
            'alm_m6_cell_nbank_cons_orgnum', 'alm_m6_cell_nbank_finlea_orgnum', 'alm_m6_cell_nbank_else_orgnum',
            'alm_m6_lm_cell_bank_selfnum', 'alm_m6_lm_cell_bank_allnum', 'alm_m6_lm_cell_bank_tra_allnum',
            'alm_m6_lm_cell_bank_ret_allnum', 'alm_m6_lm_cell_bank_orgnum', 'alm_m6_lm_cell_bank_tra_orgnum',
            'alm_m6_lm_cell_bank_ret_orgnum', 'alm_m6_lm_cell_nbank_selfnum', 'alm_m6_lm_cell_nbank_allnum',
            'alm_m6_lm_cell_nbank_p2p_allnum', 'alm_m6_lm_cell_nbank_mc_allnum', 'alm_m6_lm_cell_nbank_ca_allnum',
            'alm_m6_lm_cell_nbank_cf_allnum', 'alm_m6_lm_cell_nbank_com_allnum', 'alm_m6_lm_cell_nbank_oth_allnum',
            'alm_m6_lm_cell_nbank_nsloan_allnum', 'alm_m6_lm_cell_nbank_autofin_allnum',
            'alm_m6_lm_cell_nbank_sloan_allnum', 'alm_m6_lm_cell_nbank_cons_allnum',
            'alm_m6_lm_cell_nbank_finlea_allnum', 'alm_m6_lm_cell_nbank_else_allnum', 'alm_m6_lm_cell_nbank_orgnum',
            'alm_m6_lm_cell_nbank_p2p_orgnum', 'alm_m6_lm_cell_nbank_mc_orgnum', 'alm_m6_lm_cell_nbank_ca_orgnum',
            'alm_m6_lm_cell_nbank_cf_orgnum', 'alm_m6_lm_cell_nbank_com_orgnum', 'alm_m6_lm_cell_nbank_oth_orgnum',
            'alm_m6_lm_cell_nbank_nsloan_orgnum', 'alm_m6_lm_cell_nbank_autofin_orgnum',
            'alm_m6_lm_cell_nbank_sloan_orgnum', 'alm_m6_lm_cell_nbank_cons_orgnum',
            'alm_m6_lm_cell_nbank_finlea_orgnum', 'alm_m6_lm_cell_nbank_else_orgnum', 'alm_m7_id_bank_selfnum',
            'alm_m7_id_bank_allnum', 'alm_m7_id_bank_tra_allnum', 'alm_m7_id_bank_ret_allnum', 'alm_m7_id_bank_orgnum',
            'alm_m7_id_bank_tra_orgnum', 'alm_m7_id_bank_ret_orgnum', 'alm_m7_id_nbank_selfnum',
            'alm_m7_id_nbank_allnum', 'alm_m7_id_nbank_p2p_allnum', 'alm_m7_id_nbank_mc_allnum',
            'alm_m7_id_nbank_ca_allnum', 'alm_m7_id_nbank_cf_allnum', 'alm_m7_id_nbank_com_allnum',
            'alm_m7_id_nbank_oth_allnum', 'alm_m7_id_nbank_nsloan_allnum', 'alm_m7_id_nbank_autofin_allnum',
            'alm_m7_id_nbank_sloan_allnum', 'alm_m7_id_nbank_cons_allnum', 'alm_m7_id_nbank_finlea_allnum',
            'alm_m7_id_nbank_else_allnum', 'alm_m7_id_nbank_orgnum', 'alm_m7_id_nbank_p2p_orgnum',
            'alm_m7_id_nbank_mc_orgnum', 'alm_m7_id_nbank_ca_orgnum', 'alm_m7_id_nbank_cf_orgnum',
            'alm_m7_id_nbank_com_orgnum', 'alm_m7_id_nbank_oth_orgnum', 'alm_m7_id_nbank_nsloan_orgnum',
            'alm_m7_id_nbank_autofin_orgnum', 'alm_m7_id_nbank_sloan_orgnum', 'alm_m7_id_nbank_cons_orgnum',
            'alm_m7_id_nbank_finlea_orgnum', 'alm_m7_id_nbank_else_orgnum', 'alm_m7_cell_bank_selfnum',
            'alm_m7_cell_bank_allnum', 'alm_m7_cell_bank_tra_allnum', 'alm_m7_cell_bank_ret_allnum',
            'alm_m7_cell_bank_orgnum', 'alm_m7_cell_bank_tra_orgnum', 'alm_m7_cell_bank_ret_orgnum',
            'alm_m7_cell_nbank_selfnum', 'alm_m7_cell_nbank_allnum', 'alm_m7_cell_nbank_p2p_allnum',
            'alm_m7_cell_nbank_mc_allnum', 'alm_m7_cell_nbank_ca_allnum', 'alm_m7_cell_nbank_cf_allnum',
            'alm_m7_cell_nbank_com_allnum', 'alm_m7_cell_nbank_oth_allnum', 'alm_m7_cell_nbank_nsloan_allnum',
            'alm_m7_cell_nbank_autofin_allnum', 'alm_m7_cell_nbank_sloan_allnum', 'alm_m7_cell_nbank_cons_allnum',
            'alm_m7_cell_nbank_finlea_allnum', 'alm_m7_cell_nbank_else_allnum', 'alm_m7_cell_nbank_orgnum',
            'alm_m7_cell_nbank_p2p_orgnum', 'alm_m7_cell_nbank_mc_orgnum', 'alm_m7_cell_nbank_ca_orgnum',
            'alm_m7_cell_nbank_cf_orgnum', 'alm_m7_cell_nbank_com_orgnum', 'alm_m7_cell_nbank_oth_orgnum',
            'alm_m7_cell_nbank_nsloan_orgnum', 'alm_m7_cell_nbank_autofin_orgnum', 'alm_m7_cell_nbank_sloan_orgnum',
            'alm_m7_cell_nbank_cons_orgnum', 'alm_m7_cell_nbank_finlea_orgnum', 'alm_m7_cell_nbank_else_orgnum',
            'alm_m7_lm_cell_bank_selfnum', 'alm_m7_lm_cell_bank_allnum', 'alm_m7_lm_cell_bank_tra_allnum',
            'alm_m7_lm_cell_bank_ret_allnum', 'alm_m7_lm_cell_bank_orgnum', 'alm_m7_lm_cell_bank_tra_orgnum',
            'alm_m7_lm_cell_bank_ret_orgnum', 'alm_m7_lm_cell_nbank_selfnum', 'alm_m7_lm_cell_nbank_allnum',
            'alm_m7_lm_cell_nbank_p2p_allnum', 'alm_m7_lm_cell_nbank_mc_allnum', 'alm_m7_lm_cell_nbank_ca_allnum',
            'alm_m7_lm_cell_nbank_cf_allnum', 'alm_m7_lm_cell_nbank_com_allnum', 'alm_m7_lm_cell_nbank_oth_allnum',
            'alm_m7_lm_cell_nbank_nsloan_allnum', 'alm_m7_lm_cell_nbank_autofin_allnum',
            'alm_m7_lm_cell_nbank_sloan_allnum', 'alm_m7_lm_cell_nbank_cons_allnum',
            'alm_m7_lm_cell_nbank_finlea_allnum', 'alm_m7_lm_cell_nbank_else_allnum', 'alm_m7_lm_cell_nbank_orgnum',
            'alm_m7_lm_cell_nbank_p2p_orgnum', 'alm_m7_lm_cell_nbank_mc_orgnum', 'alm_m7_lm_cell_nbank_ca_orgnum',
            'alm_m7_lm_cell_nbank_cf_orgnum', 'alm_m7_lm_cell_nbank_com_orgnum', 'alm_m7_lm_cell_nbank_oth_orgnum',
            'alm_m7_lm_cell_nbank_nsloan_orgnum', 'alm_m7_lm_cell_nbank_autofin_orgnum',
            'alm_m7_lm_cell_nbank_sloan_orgnum', 'alm_m7_lm_cell_nbank_cons_orgnum',
            'alm_m7_lm_cell_nbank_finlea_orgnum', 'alm_m7_lm_cell_nbank_else_orgnum', 'alm_m8_id_bank_selfnum',
            'alm_m8_id_bank_allnum', 'alm_m8_id_bank_tra_allnum', 'alm_m8_id_bank_ret_allnum', 'alm_m8_id_bank_orgnum',
            'alm_m8_id_bank_tra_orgnum', 'alm_m8_id_bank_ret_orgnum', 'alm_m8_id_nbank_selfnum',
            'alm_m8_id_nbank_allnum', 'alm_m8_id_nbank_p2p_allnum', 'alm_m8_id_nbank_mc_allnum',
            'alm_m8_id_nbank_ca_allnum', 'alm_m8_id_nbank_cf_allnum', 'alm_m8_id_nbank_com_allnum',
            'alm_m8_id_nbank_oth_allnum', 'alm_m8_id_nbank_nsloan_allnum', 'alm_m8_id_nbank_autofin_allnum',
            'alm_m8_id_nbank_sloan_allnum', 'alm_m8_id_nbank_cons_allnum', 'alm_m8_id_nbank_finlea_allnum',
            'alm_m8_id_nbank_else_allnum', 'alm_m8_id_nbank_orgnum', 'alm_m8_id_nbank_p2p_orgnum',
            'alm_m8_id_nbank_mc_orgnum', 'alm_m8_id_nbank_ca_orgnum', 'alm_m8_id_nbank_cf_orgnum',
            'alm_m8_id_nbank_com_orgnum', 'alm_m8_id_nbank_oth_orgnum', 'alm_m8_id_nbank_nsloan_orgnum',
            'alm_m8_id_nbank_autofin_orgnum', 'alm_m8_id_nbank_sloan_orgnum', 'alm_m8_id_nbank_cons_orgnum',
            'alm_m8_id_nbank_finlea_orgnum', 'alm_m8_id_nbank_else_orgnum', 'alm_m8_cell_bank_selfnum',
            'alm_m8_cell_bank_allnum', 'alm_m8_cell_bank_tra_allnum', 'alm_m8_cell_bank_ret_allnum',
            'alm_m8_cell_bank_orgnum', 'alm_m8_cell_bank_tra_orgnum', 'alm_m8_cell_bank_ret_orgnum',
            'alm_m8_cell_nbank_selfnum', 'alm_m8_cell_nbank_allnum', 'alm_m8_cell_nbank_p2p_allnum',
            'alm_m8_cell_nbank_mc_allnum', 'alm_m8_cell_nbank_ca_allnum', 'alm_m8_cell_nbank_cf_allnum',
            'alm_m8_cell_nbank_com_allnum', 'alm_m8_cell_nbank_oth_allnum', 'alm_m8_cell_nbank_nsloan_allnum',
            'alm_m8_cell_nbank_autofin_allnum', 'alm_m8_cell_nbank_sloan_allnum', 'alm_m8_cell_nbank_cons_allnum',
            'alm_m8_cell_nbank_finlea_allnum', 'alm_m8_cell_nbank_else_allnum', 'alm_m8_cell_nbank_orgnum',
            'alm_m8_cell_nbank_p2p_orgnum', 'alm_m8_cell_nbank_mc_orgnum', 'alm_m8_cell_nbank_ca_orgnum',
            'alm_m8_cell_nbank_cf_orgnum', 'alm_m8_cell_nbank_com_orgnum', 'alm_m8_cell_nbank_oth_orgnum',
            'alm_m8_cell_nbank_nsloan_orgnum', 'alm_m8_cell_nbank_autofin_orgnum', 'alm_m8_cell_nbank_sloan_orgnum',
            'alm_m8_cell_nbank_cons_orgnum', 'alm_m8_cell_nbank_finlea_orgnum', 'alm_m8_cell_nbank_else_orgnum',
            'alm_m8_lm_cell_bank_selfnum', 'alm_m8_lm_cell_bank_allnum', 'alm_m8_lm_cell_bank_tra_allnum',
            'alm_m8_lm_cell_bank_ret_allnum', 'alm_m8_lm_cell_bank_orgnum', 'alm_m8_lm_cell_bank_tra_orgnum',
            'alm_m8_lm_cell_bank_ret_orgnum', 'alm_m8_lm_cell_nbank_selfnum', 'alm_m8_lm_cell_nbank_allnum',
            'alm_m8_lm_cell_nbank_p2p_allnum', 'alm_m8_lm_cell_nbank_mc_allnum', 'alm_m8_lm_cell_nbank_ca_allnum',
            'alm_m8_lm_cell_nbank_cf_allnum', 'alm_m8_lm_cell_nbank_com_allnum', 'alm_m8_lm_cell_nbank_oth_allnum',
            'alm_m8_lm_cell_nbank_nsloan_allnum', 'alm_m8_lm_cell_nbank_autofin_allnum',
            'alm_m8_lm_cell_nbank_sloan_allnum', 'alm_m8_lm_cell_nbank_cons_allnum',
            'alm_m8_lm_cell_nbank_finlea_allnum', 'alm_m8_lm_cell_nbank_else_allnum', 'alm_m8_lm_cell_nbank_orgnum',
            'alm_m8_lm_cell_nbank_p2p_orgnum', 'alm_m8_lm_cell_nbank_mc_orgnum', 'alm_m8_lm_cell_nbank_ca_orgnum',
            'alm_m8_lm_cell_nbank_cf_orgnum', 'alm_m8_lm_cell_nbank_com_orgnum', 'alm_m8_lm_cell_nbank_oth_orgnum',
            'alm_m8_lm_cell_nbank_nsloan_orgnum', 'alm_m8_lm_cell_nbank_autofin_orgnum',
            'alm_m8_lm_cell_nbank_sloan_orgnum', 'alm_m8_lm_cell_nbank_cons_orgnum',
            'alm_m8_lm_cell_nbank_finlea_orgnum', 'alm_m8_lm_cell_nbank_else_orgnum', 'alm_m9_id_bank_selfnum',
            'alm_m9_id_bank_allnum', 'alm_m9_id_bank_tra_allnum', 'alm_m9_id_bank_ret_allnum', 'alm_m9_id_bank_orgnum',
            'alm_m9_id_bank_tra_orgnum', 'alm_m9_id_bank_ret_orgnum', 'alm_m9_id_nbank_selfnum',
            'alm_m9_id_nbank_allnum', 'alm_m9_id_nbank_p2p_allnum', 'alm_m9_id_nbank_mc_allnum',
            'alm_m9_id_nbank_ca_allnum', 'alm_m9_id_nbank_cf_allnum', 'alm_m9_id_nbank_com_allnum',
            'alm_m9_id_nbank_oth_allnum', 'alm_m9_id_nbank_nsloan_allnum', 'alm_m9_id_nbank_autofin_allnum',
            'alm_m9_id_nbank_sloan_allnum', 'alm_m9_id_nbank_cons_allnum', 'alm_m9_id_nbank_finlea_allnum',
            'alm_m9_id_nbank_else_allnum', 'alm_m9_id_nbank_orgnum', 'alm_m9_id_nbank_p2p_orgnum',
            'alm_m9_id_nbank_mc_orgnum', 'alm_m9_id_nbank_ca_orgnum', 'alm_m9_id_nbank_cf_orgnum',
            'alm_m9_id_nbank_com_orgnum', 'alm_m9_id_nbank_oth_orgnum', 'alm_m9_id_nbank_nsloan_orgnum',
            'alm_m9_id_nbank_autofin_orgnum', 'alm_m9_id_nbank_sloan_orgnum', 'alm_m9_id_nbank_cons_orgnum',
            'alm_m9_id_nbank_finlea_orgnum', 'alm_m9_id_nbank_else_orgnum', 'alm_m9_cell_bank_selfnum',
            'alm_m9_cell_bank_allnum', 'alm_m9_cell_bank_tra_allnum', 'alm_m9_cell_bank_ret_allnum',
            'alm_m9_cell_bank_orgnum', 'alm_m9_cell_bank_tra_orgnum', 'alm_m9_cell_bank_ret_orgnum',
            'alm_m9_cell_nbank_selfnum', 'alm_m9_cell_nbank_allnum', 'alm_m9_cell_nbank_p2p_allnum',
            'alm_m9_cell_nbank_mc_allnum', 'alm_m9_cell_nbank_ca_allnum', 'alm_m9_cell_nbank_cf_allnum',
            'alm_m9_cell_nbank_com_allnum', 'alm_m9_cell_nbank_oth_allnum', 'alm_m9_cell_nbank_nsloan_allnum',
            'alm_m9_cell_nbank_autofin_allnum', 'alm_m9_cell_nbank_sloan_allnum', 'alm_m9_cell_nbank_cons_allnum',
            'alm_m9_cell_nbank_finlea_allnum', 'alm_m9_cell_nbank_else_allnum', 'alm_m9_cell_nbank_orgnum',
            'alm_m9_cell_nbank_p2p_orgnum', 'alm_m9_cell_nbank_mc_orgnum', 'alm_m9_cell_nbank_ca_orgnum',
            'alm_m9_cell_nbank_cf_orgnum', 'alm_m9_cell_nbank_com_orgnum', 'alm_m9_cell_nbank_oth_orgnum',
            'alm_m9_cell_nbank_nsloan_orgnum', 'alm_m9_cell_nbank_autofin_orgnum', 'alm_m9_cell_nbank_sloan_orgnum',
            'alm_m9_cell_nbank_cons_orgnum', 'alm_m9_cell_nbank_finlea_orgnum', 'alm_m9_cell_nbank_else_orgnum',
            'alm_m9_lm_cell_bank_selfnum', 'alm_m9_lm_cell_bank_allnum', 'alm_m9_lm_cell_bank_tra_allnum',
            'alm_m9_lm_cell_bank_ret_allnum', 'alm_m9_lm_cell_bank_orgnum', 'alm_m9_lm_cell_bank_tra_orgnum',
            'alm_m9_lm_cell_bank_ret_orgnum', 'alm_m9_lm_cell_nbank_selfnum', 'alm_m9_lm_cell_nbank_allnum',
            'alm_m9_lm_cell_nbank_p2p_allnum', 'alm_m9_lm_cell_nbank_mc_allnum', 'alm_m9_lm_cell_nbank_ca_allnum',
            'alm_m9_lm_cell_nbank_cf_allnum', 'alm_m9_lm_cell_nbank_com_allnum', 'alm_m9_lm_cell_nbank_oth_allnum',
            'alm_m9_lm_cell_nbank_nsloan_allnum', 'alm_m9_lm_cell_nbank_autofin_allnum',
            'alm_m9_lm_cell_nbank_sloan_allnum', 'alm_m9_lm_cell_nbank_cons_allnum',
            'alm_m9_lm_cell_nbank_finlea_allnum', 'alm_m9_lm_cell_nbank_else_allnum', 'alm_m9_lm_cell_nbank_orgnum',
            'alm_m9_lm_cell_nbank_p2p_orgnum', 'alm_m9_lm_cell_nbank_mc_orgnum', 'alm_m9_lm_cell_nbank_ca_orgnum',
            'alm_m9_lm_cell_nbank_cf_orgnum', 'alm_m9_lm_cell_nbank_com_orgnum', 'alm_m9_lm_cell_nbank_oth_orgnum',
            'alm_m9_lm_cell_nbank_nsloan_orgnum', 'alm_m9_lm_cell_nbank_autofin_orgnum',
            'alm_m9_lm_cell_nbank_sloan_orgnum', 'alm_m9_lm_cell_nbank_cons_orgnum',
            'alm_m9_lm_cell_nbank_finlea_orgnum', 'alm_m9_lm_cell_nbank_else_orgnum', 'alm_m10_id_bank_selfnum',
            'alm_m10_id_bank_allnum', 'alm_m10_id_bank_tra_allnum', 'alm_m10_id_bank_ret_allnum',
            'alm_m10_id_bank_orgnum', 'alm_m10_id_bank_tra_orgnum', 'alm_m10_id_bank_ret_orgnum',
            'alm_m10_id_nbank_selfnum', 'alm_m10_id_nbank_allnum', 'alm_m10_id_nbank_p2p_allnum',
            'alm_m10_id_nbank_mc_allnum', 'alm_m10_id_nbank_ca_allnum', 'alm_m10_id_nbank_cf_allnum',
            'alm_m10_id_nbank_com_allnum', 'alm_m10_id_nbank_oth_allnum', 'alm_m10_id_nbank_nsloan_allnum',
            'alm_m10_id_nbank_autofin_allnum', 'alm_m10_id_nbank_sloan_allnum', 'alm_m10_id_nbank_cons_allnum',
            'alm_m10_id_nbank_finlea_allnum', 'alm_m10_id_nbank_else_allnum', 'alm_m10_id_nbank_orgnum',
            'alm_m10_id_nbank_p2p_orgnum', 'alm_m10_id_nbank_mc_orgnum', 'alm_m10_id_nbank_ca_orgnum',
            'alm_m10_id_nbank_cf_orgnum', 'alm_m10_id_nbank_com_orgnum', 'alm_m10_id_nbank_oth_orgnum',
            'alm_m10_id_nbank_nsloan_orgnum', 'alm_m10_id_nbank_autofin_orgnum', 'alm_m10_id_nbank_sloan_orgnum',
            'alm_m10_id_nbank_cons_orgnum', 'alm_m10_id_nbank_finlea_orgnum', 'alm_m10_id_nbank_else_orgnum',
            'alm_m10_cell_bank_selfnum', 'alm_m10_cell_bank_allnum', 'alm_m10_cell_bank_tra_allnum',
            'alm_m10_cell_bank_ret_allnum', 'alm_m10_cell_bank_orgnum', 'alm_m10_cell_bank_tra_orgnum',
            'alm_m10_cell_bank_ret_orgnum', 'alm_m10_cell_nbank_selfnum', 'alm_m10_cell_nbank_allnum',
            'alm_m10_cell_nbank_p2p_allnum', 'alm_m10_cell_nbank_mc_allnum', 'alm_m10_cell_nbank_ca_allnum',
            'alm_m10_cell_nbank_cf_allnum', 'alm_m10_cell_nbank_com_allnum', 'alm_m10_cell_nbank_oth_allnum',
            'alm_m10_cell_nbank_nsloan_allnum', 'alm_m10_cell_nbank_autofin_allnum', 'alm_m10_cell_nbank_sloan_allnum',
            'alm_m10_cell_nbank_cons_allnum', 'alm_m10_cell_nbank_finlea_allnum', 'alm_m10_cell_nbank_else_allnum',
            'alm_m10_cell_nbank_orgnum', 'alm_m10_cell_nbank_p2p_orgnum', 'alm_m10_cell_nbank_mc_orgnum',
            'alm_m10_cell_nbank_ca_orgnum', 'alm_m10_cell_nbank_cf_orgnum', 'alm_m10_cell_nbank_com_orgnum',
            'alm_m10_cell_nbank_oth_orgnum', 'alm_m10_cell_nbank_nsloan_orgnum', 'alm_m10_cell_nbank_autofin_orgnum',
            'alm_m10_cell_nbank_sloan_orgnum', 'alm_m10_cell_nbank_cons_orgnum', 'alm_m10_cell_nbank_finlea_orgnum',
            'alm_m10_cell_nbank_else_orgnum', 'alm_m10_lm_cell_bank_selfnum', 'alm_m10_lm_cell_bank_allnum',
            'alm_m10_lm_cell_bank_tra_allnum', 'alm_m10_lm_cell_bank_ret_allnum', 'alm_m10_lm_cell_bank_orgnum',
            'alm_m10_lm_cell_bank_tra_orgnum', 'alm_m10_lm_cell_bank_ret_orgnum', 'alm_m10_lm_cell_nbank_selfnum',
            'alm_m10_lm_cell_nbank_allnum', 'alm_m10_lm_cell_nbank_p2p_allnum', 'alm_m10_lm_cell_nbank_mc_allnum',
            'alm_m10_lm_cell_nbank_ca_allnum', 'alm_m10_lm_cell_nbank_cf_allnum', 'alm_m10_lm_cell_nbank_com_allnum',
            'alm_m10_lm_cell_nbank_oth_allnum', 'alm_m10_lm_cell_nbank_nsloan_allnum',
            'alm_m10_lm_cell_nbank_autofin_allnum', 'alm_m10_lm_cell_nbank_sloan_allnum',
            'alm_m10_lm_cell_nbank_cons_allnum', 'alm_m10_lm_cell_nbank_finlea_allnum',
            'alm_m10_lm_cell_nbank_else_allnum', 'alm_m10_lm_cell_nbank_orgnum', 'alm_m10_lm_cell_nbank_p2p_orgnum',
            'alm_m10_lm_cell_nbank_mc_orgnum', 'alm_m10_lm_cell_nbank_ca_orgnum', 'alm_m10_lm_cell_nbank_cf_orgnum',
            'alm_m10_lm_cell_nbank_com_orgnum', 'alm_m10_lm_cell_nbank_oth_orgnum',
            'alm_m10_lm_cell_nbank_nsloan_orgnum', 'alm_m10_lm_cell_nbank_autofin_orgnum',
            'alm_m10_lm_cell_nbank_sloan_orgnum', 'alm_m10_lm_cell_nbank_cons_orgnum',
            'alm_m10_lm_cell_nbank_finlea_orgnum', 'alm_m10_lm_cell_nbank_else_orgnum', 'alm_m11_id_bank_selfnum',
            'alm_m11_id_bank_allnum', 'alm_m11_id_bank_tra_allnum', 'alm_m11_id_bank_ret_allnum',
            'alm_m11_id_bank_orgnum', 'alm_m11_id_bank_tra_orgnum', 'alm_m11_id_bank_ret_orgnum',
            'alm_m11_id_nbank_selfnum', 'alm_m11_id_nbank_allnum', 'alm_m11_id_nbank_p2p_allnum',
            'alm_m11_id_nbank_mc_allnum', 'alm_m11_id_nbank_ca_allnum', 'alm_m11_id_nbank_cf_allnum',
            'alm_m11_id_nbank_com_allnum', 'alm_m11_id_nbank_oth_allnum', 'alm_m11_id_nbank_nsloan_allnum',
            'alm_m11_id_nbank_autofin_allnum', 'alm_m11_id_nbank_sloan_allnum', 'alm_m11_id_nbank_cons_allnum',
            'alm_m11_id_nbank_finlea_allnum', 'alm_m11_id_nbank_else_allnum', 'alm_m11_id_nbank_orgnum',
            'alm_m11_id_nbank_p2p_orgnum', 'alm_m11_id_nbank_mc_orgnum', 'alm_m11_id_nbank_ca_orgnum',
            'alm_m11_id_nbank_cf_orgnum', 'alm_m11_id_nbank_com_orgnum', 'alm_m11_id_nbank_oth_orgnum',
            'alm_m11_id_nbank_nsloan_orgnum', 'alm_m11_id_nbank_autofin_orgnum', 'alm_m11_id_nbank_sloan_orgnum',
            'alm_m11_id_nbank_cons_orgnum', 'alm_m11_id_nbank_finlea_orgnum', 'alm_m11_id_nbank_else_orgnum',
            'alm_m11_cell_bank_selfnum', 'alm_m11_cell_bank_allnum', 'alm_m11_cell_bank_tra_allnum',
            'alm_m11_cell_bank_ret_allnum', 'alm_m11_cell_bank_orgnum', 'alm_m11_cell_bank_tra_orgnum',
            'alm_m11_cell_bank_ret_orgnum', 'alm_m11_cell_nbank_selfnum', 'alm_m11_cell_nbank_allnum',
            'alm_m11_cell_nbank_p2p_allnum', 'alm_m11_cell_nbank_mc_allnum', 'alm_m11_cell_nbank_ca_allnum',
            'alm_m11_cell_nbank_cf_allnum', 'alm_m11_cell_nbank_com_allnum', 'alm_m11_cell_nbank_oth_allnum',
            'alm_m11_cell_nbank_nsloan_allnum', 'alm_m11_cell_nbank_autofin_allnum', 'alm_m11_cell_nbank_sloan_allnum',
            'alm_m11_cell_nbank_cons_allnum', 'alm_m11_cell_nbank_finlea_allnum', 'alm_m11_cell_nbank_else_allnum',
            'alm_m11_cell_nbank_orgnum', 'alm_m11_cell_nbank_p2p_orgnum', 'alm_m11_cell_nbank_mc_orgnum',
            'alm_m11_cell_nbank_ca_orgnum', 'alm_m11_cell_nbank_cf_orgnum', 'alm_m11_cell_nbank_com_orgnum',
            'alm_m11_cell_nbank_oth_orgnum', 'alm_m11_cell_nbank_nsloan_orgnum', 'alm_m11_cell_nbank_autofin_orgnum',
            'alm_m11_cell_nbank_sloan_orgnum', 'alm_m11_cell_nbank_cons_orgnum', 'alm_m11_cell_nbank_finlea_orgnum',
            'alm_m11_cell_nbank_else_orgnum', 'alm_m11_lm_cell_bank_selfnum', 'alm_m11_lm_cell_bank_allnum',
            'alm_m11_lm_cell_bank_tra_allnum', 'alm_m11_lm_cell_bank_ret_allnum', 'alm_m11_lm_cell_bank_orgnum',
            'alm_m11_lm_cell_bank_tra_orgnum', 'alm_m11_lm_cell_bank_ret_orgnum', 'alm_m11_lm_cell_nbank_selfnum',
            'alm_m11_lm_cell_nbank_allnum', 'alm_m11_lm_cell_nbank_p2p_allnum', 'alm_m11_lm_cell_nbank_mc_allnum',
            'alm_m11_lm_cell_nbank_ca_allnum', 'alm_m11_lm_cell_nbank_cf_allnum', 'alm_m11_lm_cell_nbank_com_allnum',
            'alm_m11_lm_cell_nbank_oth_allnum', 'alm_m11_lm_cell_nbank_nsloan_allnum',
            'alm_m11_lm_cell_nbank_autofin_allnum', 'alm_m11_lm_cell_nbank_sloan_allnum',
            'alm_m11_lm_cell_nbank_cons_allnum', 'alm_m11_lm_cell_nbank_finlea_allnum',
            'alm_m11_lm_cell_nbank_else_allnum', 'alm_m11_lm_cell_nbank_orgnum', 'alm_m11_lm_cell_nbank_p2p_orgnum',
            'alm_m11_lm_cell_nbank_mc_orgnum', 'alm_m11_lm_cell_nbank_ca_orgnum', 'alm_m11_lm_cell_nbank_cf_orgnum',
            'alm_m11_lm_cell_nbank_com_orgnum', 'alm_m11_lm_cell_nbank_oth_orgnum',
            'alm_m11_lm_cell_nbank_nsloan_orgnum', 'alm_m11_lm_cell_nbank_autofin_orgnum',
            'alm_m11_lm_cell_nbank_sloan_orgnum', 'alm_m11_lm_cell_nbank_cons_orgnum',
            'alm_m11_lm_cell_nbank_finlea_orgnum', 'alm_m11_lm_cell_nbank_else_orgnum', 'alm_m12_id_bank_selfnum',
            'alm_m12_id_bank_allnum', 'alm_m12_id_bank_tra_allnum', 'alm_m12_id_bank_ret_allnum',
            'alm_m12_id_bank_orgnum', 'alm_m12_id_bank_tra_orgnum', 'alm_m12_id_bank_ret_orgnum',
            'alm_m12_id_nbank_selfnum', 'alm_m12_id_nbank_allnum', 'alm_m12_id_nbank_p2p_allnum',
            'alm_m12_id_nbank_mc_allnum', 'alm_m12_id_nbank_ca_allnum', 'alm_m12_id_nbank_cf_allnum',
            'alm_m12_id_nbank_com_allnum', 'alm_m12_id_nbank_oth_allnum', 'alm_m12_id_nbank_nsloan_allnum',
            'alm_m12_id_nbank_autofin_allnum', 'alm_m12_id_nbank_sloan_allnum', 'alm_m12_id_nbank_cons_allnum',
            'alm_m12_id_nbank_finlea_allnum', 'alm_m12_id_nbank_else_allnum', 'alm_m12_id_nbank_orgnum',
            'alm_m12_id_nbank_p2p_orgnum', 'alm_m12_id_nbank_mc_orgnum', 'alm_m12_id_nbank_ca_orgnum',
            'alm_m12_id_nbank_cf_orgnum', 'alm_m12_id_nbank_com_orgnum', 'alm_m12_id_nbank_oth_orgnum',
            'alm_m12_id_nbank_nsloan_orgnum', 'alm_m12_id_nbank_autofin_orgnum', 'alm_m12_id_nbank_sloan_orgnum',
            'alm_m12_id_nbank_cons_orgnum', 'alm_m12_id_nbank_finlea_orgnum', 'alm_m12_id_nbank_else_orgnum',
            'alm_m12_cell_bank_selfnum', 'alm_m12_cell_bank_allnum', 'alm_m12_cell_bank_tra_allnum',
            'alm_m12_cell_bank_ret_allnum', 'alm_m12_cell_bank_orgnum', 'alm_m12_cell_bank_tra_orgnum',
            'alm_m12_cell_bank_ret_orgnum', 'alm_m12_cell_nbank_selfnum', 'alm_m12_cell_nbank_allnum',
            'alm_m12_cell_nbank_p2p_allnum', 'alm_m12_cell_nbank_mc_allnum', 'alm_m12_cell_nbank_ca_allnum',
            'alm_m12_cell_nbank_cf_allnum', 'alm_m12_cell_nbank_com_allnum', 'alm_m12_cell_nbank_oth_allnum',
            'alm_m12_cell_nbank_nsloan_allnum', 'alm_m12_cell_nbank_autofin_allnum', 'alm_m12_cell_nbank_sloan_allnum',
            'alm_m12_cell_nbank_cons_allnum', 'alm_m12_cell_nbank_finlea_allnum', 'alm_m12_cell_nbank_else_allnum',
            'alm_m12_cell_nbank_orgnum', 'alm_m12_cell_nbank_p2p_orgnum', 'alm_m12_cell_nbank_mc_orgnum',
            'alm_m12_cell_nbank_ca_orgnum', 'alm_m12_cell_nbank_cf_orgnum', 'alm_m12_cell_nbank_com_orgnum',
            'alm_m12_cell_nbank_oth_orgnum', 'alm_m12_cell_nbank_nsloan_orgnum', 'alm_m12_cell_nbank_autofin_orgnum',
            'alm_m12_cell_nbank_sloan_orgnum', 'alm_m12_cell_nbank_cons_orgnum', 'alm_m12_cell_nbank_finlea_orgnum',
            'alm_m12_cell_nbank_else_orgnum', 'alm_m12_lm_cell_bank_selfnum', 'alm_m12_lm_cell_bank_allnum',
            'alm_m12_lm_cell_bank_tra_allnum', 'alm_m12_lm_cell_bank_ret_allnum', 'alm_m12_lm_cell_bank_orgnum',
            'alm_m12_lm_cell_bank_tra_orgnum', 'alm_m12_lm_cell_bank_ret_orgnum', 'alm_m12_lm_cell_nbank_selfnum',
            'alm_m12_lm_cell_nbank_allnum', 'alm_m12_lm_cell_nbank_p2p_allnum', 'alm_m12_lm_cell_nbank_mc_allnum',
            'alm_m12_lm_cell_nbank_ca_allnum', 'alm_m12_lm_cell_nbank_cf_allnum', 'alm_m12_lm_cell_nbank_com_allnum',
            'alm_m12_lm_cell_nbank_oth_allnum', 'alm_m12_lm_cell_nbank_nsloan_allnum',
            'alm_m12_lm_cell_nbank_autofin_allnum', 'alm_m12_lm_cell_nbank_sloan_allnum',
            'alm_m12_lm_cell_nbank_cons_allnum', 'alm_m12_lm_cell_nbank_finlea_allnum',
            'alm_m12_lm_cell_nbank_else_allnum', 'alm_m12_lm_cell_nbank_orgnum', 'alm_m12_lm_cell_nbank_p2p_orgnum',
            'alm_m12_lm_cell_nbank_mc_orgnum', 'alm_m12_lm_cell_nbank_ca_orgnum', 'alm_m12_lm_cell_nbank_cf_orgnum',
            'alm_m12_lm_cell_nbank_com_orgnum', 'alm_m12_lm_cell_nbank_oth_orgnum',
            'alm_m12_lm_cell_nbank_nsloan_orgnum', 'alm_m12_lm_cell_nbank_autofin_orgnum',
            'alm_m12_lm_cell_nbank_sloan_orgnum', 'alm_m12_lm_cell_nbank_cons_orgnum',
            'alm_m12_lm_cell_nbank_finlea_orgnum', 'alm_m12_lm_cell_nbank_else_orgnum']

br_loanbf_data2 = br_loanbf_data.loc[br_loanbf_data.result.notnull()].reset_index(drop = True)
br_loanbf_data2.shape

result_df = pd.DataFrame(columns=col_list)

#for i in range(df.shape[0]):
for i in range(br_loanbf_data2.shape[0]):
    a1 = json.loads(br_loanbf_data2.iloc[i, 4])
    sValue = {key: value for key, value in a1.items() if key in product_list.keys()}
    result_dict = {}
    for var in dict_generator(sValue):
        var[0] = product_list[var[0]]
        # print('_'.join(var[0:-1]), ':', var[-1])
        result_dict['_'.join(var[0:-1])] = var[-1]
        result_dict_new = {key: value for key, value in result_dict.items() if value != '{}'}
        result_dict_new2 = {key: value for key, value in result_dict_new.items() if key[-17:] not in ['_bank_else_allnum', '_bank_else_orgnum']}
    a2 = pd.DataFrame.from_dict(result_dict_new2, orient='index').T
    result_df = pd.concat([result_df, a2], ignore_index=True)

1959

a1 = json.loads(br_loanbf_data2.iloc[1, 4])
a1.keys()
#dict_keys(['InfoRelation', 'ApplyLoan_d', 'swift_number', 'Flag', 'ApplyLoanUsury', 'TotalLoan', 'Rule', 'code', 'ApplyLoanStr', 'ApplyLoanMon', 'RiskStrategy'])
sValue
#dict_keys(['InfoRelation', 'ApplyLoan_d', 'ApplyLoanUsury', 'TotalLoan', 'ApplyLoanStr', 'ApplyLoanMon']) 只保留需要用的key

br_loanbf_df = result_df.copy()
br_loanbf_df2.shape

br_loanbf_df2 = br_loanbf_data2[['order_no']].merge(br_loanbf_df, left_index = True, right_index = True)



# br_alu_sql = """
# select *
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,fst,id,inteday}' as alu_fst_id_inteday
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,fst,cell,inteday}' as alu_fst_cell_inteday
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,lst,id,inteday}' as alu_lst_id_inteday
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,lst,cell,inteday}' as alu_lst_cell_inteday
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,y1,id,allnum}' as alu_y1_id_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,y1,id,orgnum}' as alu_y1_id_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,y1,cell,allnum}' as alu_y1_cell_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,y1,cell,orgnum}' as alu_y1_cell_orgnum
#
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,d7,id,allnum}' as alu_d7_id_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,d7,id,orgnum}' as alu_d7_id_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,d7,cell,allnum}' as alu_d7_cell_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,d7,cell,orgnum}' as alu_d7_cell_orgnum
#
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,d15,id,allnum}' as alu_d15_id_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,d15,id,orgnum}' as alu_d15_id_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,d15,cell,allnum}' as alu_d15_cell_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,d15,cell,orgnum}' as alu_d15_cell_orgnum
#
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m1,id,allnum}' as alu_m1_id_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m1,id,orgnum}' as alu_m1_id_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m1,cell,allnum}' as alu_m1_cell_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m1,cell,orgnum}' as alu_m1_cell_orgnum
#
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,id,allnum}' as alu_m3_id_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,id,orgnum}' as alu_m3_id_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,id,tot_monnum}' as alu_m3_id_tot_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,id,avg_monnum}' as alu_m3_id_avg_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,id,max_monnum}' as alu_m3_id_max_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,id,min_monnum}' as alu_m3_id_min_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,cell,allnum}' as alu_m3_cell_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,cell,orgnum}' as alu_m3_cell_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,cell,tot_monnum}' as alu_m3_cell_tot_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,cell,avg_monnum}' as alu_m3_cell_avg_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,cell,max_monnum}' as alu_m3_cell_max_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m3,cell,min_monnum}' as alu_m3_cell_min_monnum
#
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,id,allnum}' as alu_m6_id_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,id,orgnum}' as alu_m6_id_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,id,tot_monnum}' as alu_m6_id_tot_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,id,avg_monnum}' as alu_m6_id_avg_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,id,max_monnum}' as alu_m6_id_max_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,id,min_monnum}' as alu_m6_id_min_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,cell,allnum}' as alu_m6_cell_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,cell,orgnum}' as alu_m6_cell_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,cell,tot_monnum}' as alu_m6_cell_tot_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,cell,avg_monnum}' as alu_m6_cell_avg_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,cell,max_monnum}' as alu_m6_cell_max_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m6,cell,min_monnum}' as alu_m6_cell_min_monnum
#
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,id,allnum}' as alu_m12_id_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,id,orgnum}' as alu_m12_id_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,id,tot_monnum}' as alu_m12_id_tot_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,id,avg_monnum}' as alu_m12_id_avg_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,id,max_monnum}' as alu_m12_id_max_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,id,min_monnum}' as alu_m12_id_min_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,cell,allnum}' as alu_m12_cell_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,cell,orgnum}' as alu_m12_cell_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,cell,tot_monnum}' as alu_m12_cell_tot_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,cell,avg_monnum}' as alu_m12_cell_avg_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,cell,max_monnum}' as alu_m12_cell_max_monnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoanUsury,m12,cell,min_monnum}' as alu_m12_cell_min_monnum
# from risk_oss_bairong_new_loan_before
# where thirdtype = 'BAIRONG_NEW_LOAN_BEFORE' and customerid = '1000006358'
# """
#
# br_ald_sql = """
# select *
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id_x_cell_num}'  as ald_id_x_cell_num
#
# --id--
# --pdl
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,pdl,allnum}'  as ald_id_pdl_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,pdl,orgnum}'  as ald_id_pdl_orgnum
# --caon
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,cano,allnum}'  as ald_id_caon_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,cano,orgnum}'  as ald_id_caon_orgnum
# --rel
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,rel,allnum}'  as ald_id_rel_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,rel,orgnum}'  as ald_id_rel_orgnum
# --caoff
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,caoff,allnum}'  as ald_id_caoff_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,caoff,orgnum}'  as ald_id_caoff_orgnum
# --coon
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,cooff,allnum}'  as ald_id_cooff_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,cooff,orgnum}'  as ald_id_cooff_orgnum
# --af
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,af,allnum}'  as ald_id_af_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,af,orgnum}'  as ald_id_af_orgnum
# --coon
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,coon,allnum}'  as ald_id_coon_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,coon,orgnum}'  as ald_id_coon_orgnum
# --oth
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,oth,allnum}'  as ald_id_oth_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,oth,orgnum}'  as ald_id_oth_orgnum
# --bank
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,bank,selfnum}'  as ald_id_bank_selfnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,bank,allnum}'  as ald_id_bank_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,bank,tra_allnum}'  as ald_id_bank_tra_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,bank,ret_allnum}'  as ald_id_bank_ret_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,bank,orgnum}'  as ald_id_bank_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,bank,tra_orgnum}'  as ald_id_bank_tra_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,bank,ret_orgnum}'  as ald_id_bank_ret_orgnum
# --nbank
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,selfnum}'  as ald_id_nbank_selfnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,allnum}'  as ald_id_nbank_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,tra_allnum}'  as ald_id_nbank_p2p_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,ret_allnum}'  as ald_id_nbank_mc_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,orgnum}'  as ald_id_nbank_ca_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,ca_on_allnum}'  as ald_id_nbank_ca_on_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,ca_off_allnum}'  as ald_id_nbank_ca_off_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,cf_allnum}'  as ald_id_nbank_cf_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,cf_on_allnum}'  as ald_id_nbank_cf_on_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,cf_off_allnum}'  as ald_id_nbank_cf_off_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,com_allnum}'  as ald_id_nbank_com_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,oth_allnum}'  as ald_id_nbank_oth_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,nsloan_allnum}'  as ald_id_nbank_nsloan_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,autofin_allnum}'  as ald_id_nbank_autofin_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,sloan_allnum}'  as ald_id_nbank_sloan_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,cons_allnum}'  as ald_id_nbank_cons_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,finlea_allnum}'  as ald_id_nbank_finlea_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,else_allnum}'  as ald_id_nbank_else_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,p2p_orgnum}'  as ald_id_nbank_p2p_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,mc_orgnum}'  as ald_id_nbank_mc_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,ca_orgnum}'  as ald_id_nbank_ca_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,ca_on_orgnum}'  as ald_id_nbank_ca_on_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,ca_off_orgnum}'  as ald_id_nbank_ca_off_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,cf_orgnum}'  as ald_id_nbank_cf_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,cf_on_orgnum}'  as ald_id_nbank_cf_on_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,cf_off_orgnum}'  as ald_id_nbank_cf_off_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,com_orgnum}'  as ald_id_nbank_com_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,oth_orgnum}'  as ald_id_nbank_oth_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,nsloan_orgnum}'  as ald_id_nbank_nsloan_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,autofin_orgnum}'  as ald_id_nbank_autofin_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,sloan_orgnum}'  as ald_id_nbank_sloan_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,cons_orgnum}'  as ald_id_nbank_cons_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,finlea_orgnum}'  as ald_id_nbank_finlea_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,id,nbank,else_orgnum}'  as ald_id_nbank_else_orgnum
#
# --cell--
# --pdl
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,pdl,allnum}'  as ald_cell_pdl_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,pdl,orgnum}'  as ald_cell_pdl_orgnum
# --caon
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,cano,allnum}'  as ald_cell_caon_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,cano,orgnum}'  as ald_cell_caon_orgnum
# --rel
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,rel,allnum}'  as ald_cell_rel_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,rel,orgnum}'  as ald_cell_rel_orgnum
# --caoff
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,caoff,allnum}'  as ald_cell_caoff_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,caoff,orgnum}'  as ald_cell_caoff_orgnum
# --coon
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,cooff,allnum}'  as ald_cell_cooff_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,cooff,orgnum}'  as ald_cell_cooff_orgnum
# --af
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,af,allnum}'  as ald_cell_af_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,af,orgnum}'  as ald_cell_af_orgnum
# --coon
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,coon,allnum}'  as ald_cell_coon_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,coon,orgnum}'  as ald_cell_coon_orgnum
# --oth
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,oth,allnum}'  as ald_cell_oth_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,oth,orgnum}'  as ald_cell_oth_orgnum
# --bank
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,bank,selfnum}'  as ald_cell_bank_selfnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,bank,allnum}'  as ald_cell_bank_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,bank,tra_allnum}'  as ald_cell_bank_tra_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,bank,ret_allnum}'  as ald_cell_bank_ret_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,bank,orgnum}'  as ald_cell_bank_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,bank,tra_orgnum}'  as ald_cell_bank_tra_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,bank,ret_orgnum}'  as ald_cell_bank_ret_orgnum
# --nbank
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,selfnum}'  as ald_cell_nbank_selfnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,allnum}'  as ald_cell_nbank_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,tra_allnum}'  as ald_cell_nbank_p2p_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,ret_allnum}'  as ald_cell_nbank_mc_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,orgnum}'  as ald_cell_nbank_ca_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,ca_on_allnum}'  as ald_cell_nbank_ca_on_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,ca_off_allnum}'  as ald_cell_nbank_ca_off_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,cf_allnum}'  as ald_cell_nbank_cf_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,cf_on_allnum}'  as ald_cell_nbank_cf_on_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,cf_off_allnum}'  as ald_cell_nbank_cf_off_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,com_allnum}'  as ald_cell_nbank_com_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,oth_allnum}'  as ald_cell_nbank_oth_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,nsloan_allnum}'  as ald_cell_nbank_nsloan_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,autofin_allnum}'  as ald_cell_nbank_autofin_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,sloan_allnum}'  as ald_cell_nbank_sloan_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,cons_allnum}'  as ald_cell_nbank_cons_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,finlea_allnum}'  as ald_cell_nbank_finlea_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,else_allnum}'  as ald_cell_nbank_else_allnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,p2p_orgnum}'  as ald_cell_nbank_p2p_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,mc_orgnum}'  as ald_cell_nbank_mc_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,ca_orgnum}'  as ald_cell_nbank_ca_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,ca_on_orgnum}'  as ald_cell_nbank_ca_on_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,ca_off_orgnum}'  as ald_cell_nbank_ca_off_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,cf_orgnum}'  as ald_cell_nbank_cf_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,cf_on_orgnum}'  as ald_cell_nbank_cf_on_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,cf_off_orgnum}'  as ald_cell_nbank_cf_off_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,com_orgnum}'  as ald_cell_nbank_com_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,oth_orgnum}'  as ald_cell_nbank_oth_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,nsloan_orgnum}'  as ald_cell_nbank_nsloan_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,autofin_orgnum}'  as ald_cell_nbank_autofin_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,sloan_orgnum}'  as ald_cell_nbank_sloan_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,cons_orgnum}'  as ald_cell_nbank_cons_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,finlea_orgnum}'  as ald_cell_nbank_finlea_orgnum
# , cast(oss::json #>> '{result}' as json)::json #>>'{ApplyLoan_d,cell,nbank,else_orgnum}'  as ald_cell_nbank_else_orgnum
# from risk_oss_bairong_new_loan_before
# where thirdtype = 'BAIRONG_NEW_LOAN_BEFORE' --and customerid = '1000006358'
# """
#
#
# br_ir_sql = """
# select *
# --16 var
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,allmatch_days}' as ir_allmatch_days
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,cell_x_name_cnt}' as ir_cell_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,id_inlistwith_cell}' as ir_id_inlistwith_cell
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,id_x_cell_notmat_days}' as ir_id_x_cell_notmat_days
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,id_x_cell_lastchg_days}' as ir_id_x_cell_lastchg_days
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,id_x_name_cnt}' as ir_id_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,id_x_mail_cnt}' as ir_id_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,cell_x_mail_cnt}' as ir_cell_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,cell_x_id_cnt}' as ir_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,cell_is_reabnormal}' as ir_cell_is_reabnormal
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,cell_inlistwith_id}' as ir_cell_inlistwith_id
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,cell_x_id_lastchg_days}' as ir_cell_x_id_lastchg_days
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,id_x_cell_cnt}' as ir_id_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,id_is_reabnormal}' as ir_id_is_reabnormal
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,cell_x_id_notmat_days}' as ir_cell_x_id_notmat_days
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,mail_is_reabnormal}' as ir_mail_is_reabnormal
#
# --m1
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,id_x_cell_cnt}' as ir_m1_id_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,id_x_mail_cnt}' as ir_m1_id_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,id_x_name_cnt}' as ir_m1_id_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,cell_x_id_cnt}' as ir_m1_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,cell_x_mail_cnt}' as ir_m1_cell_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,cell_x_name_cnt}' as ir_m1_cell_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,id_x_tel_home_cnt}' as ir_m1_id_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,id_x_home_addr_cnt}' as ir_m1_id_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,id_x_biz_addr_cnt}' as ir_m1_id_x_biz_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,cell_x_tel_home_cnt}' as ir_m1_cell_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,cell_x_home_addr_cnt}' as ir_m1_cell_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,cell_x_biz_addr_cnt}' as ir_m1_cell_x_biz_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,linkman_cell_x_id_cnt}' as ir_m1_linkman_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,linkman_cell_x_cell_cnt}' as ir_m1_linkman_cell_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,linkman_cell_x_tel_home_cnt}' as ir_m1_linkman_cell_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,tel_home_x_cell_cnt}' as ir_m1_tel_home_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,tel_home_x_id_cnt}' as ir_m1_tel_home_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,home_addr_x_cell_cnt}' as ir_m1_home_addr_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,home_addr_x_id_cnt}' as ir_m1_home_addr_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,tel_home_x_home_addr_cnt}' as ir_m1_tel_home_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m1,home_addr_x_tel_home_cnt}' as ir_m1_home_addr_x_tel_home_cnt
# --m3
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,id_x_cell_cnt}' as ir_m3_id_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,id_x_mail_cnt}' as ir_m3_id_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,id_x_name_cnt}' as ir_m3_id_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,cell_x_id_cnt}' as ir_m3_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,cell_x_mail_cnt}' as ir_m3_cell_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,cell_x_name_cnt}' as ir_m3_cell_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,id_x_tel_home_cnt}' as ir_m3_id_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,id_x_home_addr_cnt}' as ir_m3_id_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,id_x_biz_addr_cnt}' as ir_m3_id_x_biz_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,cell_x_tel_home_cnt}' as ir_m3_cell_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,cell_x_home_addr_cnt}' as ir_m3_cell_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,cell_x_biz_addr_cnt}' as ir_m3_cell_x_biz_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,linkman_cell_x_id_cnt}' as ir_m3_linkman_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,linkman_cell_x_cell_cnt}' as ir_m3_linkman_cell_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,linkman_cell_x_tel_home_cnt}' as ir_m3_linkman_cell_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,tel_home_x_cell_cnt}' as ir_m3_tel_home_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,tel_home_x_id_cnt}' as ir_m3_tel_home_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,home_addr_x_cell_cnt}' as ir_m3_home_addr_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,home_addr_x_id_cnt}' as ir_m3_home_addr_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,tel_home_x_home_addr_cnt}' as ir_m3_tel_home_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m3,home_addr_x_tel_home_cnt}' as ir_m3_home_addr_x_tel_home_cnt
# --m6
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,id_x_cell_cnt}' as ir_m6_id_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,id_x_mail_cnt}' as ir_m6_id_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,id_x_name_cnt}' as ir_m6_id_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,cell_x_id_cnt}' as ir_m6_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,cell_x_mail_cnt}' as ir_m6_cell_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,cell_x_name_cnt}' as ir_m6_cell_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,id_x_tel_home_cnt}' as ir_m6_id_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,id_x_home_addr_cnt}' as ir_m6_id_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,id_x_biz_addr_cnt}' as ir_m6_id_x_biz_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,cell_x_tel_home_cnt}' as ir_m6_cell_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,cell_x_home_addr_cnt}' as ir_m6_cell_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,cell_x_biz_addr_cnt}' as ir_m6_cell_x_biz_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,linkman_cell_x_id_cnt}' as ir_m6_linkman_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,linkman_cell_x_cell_cnt}' as ir_m6_linkman_cell_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,linkman_cell_x_tel_home_cnt}' as ir_m6_linkman_cell_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,tel_home_x_cell_cnt}' as ir_m6_tel_home_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,tel_home_x_id_cnt}' as ir_m6_tel_home_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,home_addr_x_cell_cnt}' as ir_m6_home_addr_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,home_addr_x_id_cnt}' as ir_m6_home_addr_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,tel_home_x_home_addr_cnt}' as ir_m6_tel_home_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m6,home_addr_x_tel_home_cnt}' as ir_m6_home_addr_x_tel_home_cnt
# --m12
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,id_x_cell_cnt}' as ir_m12_id_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,id_x_mail_cnt}' as ir_m12_id_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,id_x_name_cnt}' as ir_m12_id_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,cell_x_id_cnt}' as ir_m12_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,cell_x_mail_cnt}' as ir_m12_cell_x_mail_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,cell_x_name_cnt}' as ir_m12_cell_x_name_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,id_x_tel_home_cnt}' as ir_m12_id_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,id_x_home_addr_cnt}' as ir_m12_id_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,id_x_biz_addr_cnt}' as ir_m12_id_x_biz_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,cell_x_tel_home_cnt}' as ir_m12_cell_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,cell_x_home_addr_cnt}' as ir_m12_cell_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,cell_x_biz_addr_cnt}' as ir_m12_cell_x_biz_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,linkman_cell_x_id_cnt}' as ir_m12_linkman_cell_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,linkman_cell_x_cell_cnt}' as ir_m12_linkman_cell_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,linkman_cell_x_tel_home_cnt}' as ir_m12_linkman_cell_x_tel_home_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,tel_home_x_cell_cnt}' as ir_m12_tel_home_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,tel_home_x_id_cnt}' as ir_m12_tel_home_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,home_addr_x_cell_cnt}' as ir_m12_home_addr_x_cell_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,home_addr_x_id_cnt}' as ir_m12_home_addr_x_id_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,tel_home_x_home_addr_cnt}' as ir_m12_tel_home_x_home_addr_cnt
# , cast(oss::json #>> '{result}' as json)::json #>>'{InfoRelation,m12,home_addr_x_tel_home_cnt}' as ir_m12_home_addr_x_tel_home_cnt
# from risk_oss_bairong_new_loan_before
# where customerid = '157523458572420099'
# """
