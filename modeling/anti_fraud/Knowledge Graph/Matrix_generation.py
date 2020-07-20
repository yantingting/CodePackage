import pandas as pd
import numpy as np
import os

# order_no ->> order_id
"""
basic_info = pd.read_csv('./import/basic_info.csv')
basic_info1 = basic_info[~basic_info['idcardno'].isnull()].reset_index().drop('index', axis=1).reset_index()
basic_info1.rename(columns={'index': 'order_id'}, inplace=True)
basic_info1['order_id'] = basic_info1['order_id']+1
basic_info1.to_csv('./import/basic_info1.csv', index=False)
"""

# one-path
"""
gps_0_r = pd.read_csv('./import/gps_0_r.csv')
gps = gps_0_r.merge(gps_0_r, on=':END_ID(gps_0-ID)', how='left')
gps = gps[gps[':START_ID(order_no-ID)_x'] != gps[':START_ID(order_no-ID)_y']].reset_index().drop('index', axis=1)
"""


def matrix_generation_1_path(order_id, relation_list, save_dir, file_dir='./import/'):

    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    if not os.path.exists(save_dir + '/matrix(1-path)'):
        os.mkdir(save_dir + '/matrix(1-path)')

    files = os.listdir(file_dir)
    files_csv = list(filter(lambda x: x[:-6] in relation_list and x[-6:] == '_r.csv', files))

    # files_csv.remove('gps_0_r.csv')
    # files_csv.remove('gps_1_r.csv')

    for csv in files_csv:
        file_name = csv[:-6]
        
        dat1 = pd.read_csv(file_dir+csv)
        dat1 = dat1.iloc[:, [0, -1]]
        print(csv + ' begin.')
        if ':START_ID(order_no-ID)' not in dat1.columns:
            continue
        else:
            dat2 = pd.merge(order_id, dat1, how='inner',
                            left_on='order_no:ID(order_no-ID)',
                            right_on=':START_ID(order_no-ID)')\
                .drop(['order_no:ID(order_no-ID)', ':START_ID(order_no-ID)'], axis=1)

            dat2.columns = ['order_id', ':END_ID']

            dat_merge = pd.merge(dat2, dat2, how='left', on=':END_ID')
            dat_merge = dat_merge[dat_merge['order_id_x'] != dat_merge['order_id_y']]\
                .reset_index().drop(['index', ':END_ID'], axis=1) \
                .rename(columns={'order_id_x': ':START_ID(order_no-ID)', 'order_id_y': ':END_ID(order_no-ID)'})

            del dat1, dat2

            dat_merge.to_csv(save_dir + '/matrix(1-path)/'+file_name+'.csv', index=False)
            del dat_merge
            print(csv + ' done.')


# two-path
def matrix_generation_2_path(basic_info, order_no, relation_list, save_dir=None, file_dir='./import/'):
    result = pd.DataFrame(columns=[':START_ID(order_no-ID)', ':END_ID(order_no-ID)'])
    if save_dir is not None:
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        
        if not os.path.exists(save_dir + '/matrix(2-path)'):
            os.mkdir(save_dir + '/matrix(2-path)')

    order_id = basic_info[basic_info['order_no:ID(order_no-ID)'] == order_no].reset_index(drop=True).iloc[0, 0]

    files = os.listdir(file_dir)
    files_csv = list(filter(lambda x: x[:-6] in relation_list and x[-6:] == '_r.csv', files))

    # files_csv.remove('gps_0_r.csv')
    # files_csv.remove('gps_1_r.csv')

    for csv1 in files_csv:
        file_name1 = csv1[:-6]
        for csv2 in files_csv:
            file_name2 = csv2[:-6]

            if file_name1 == file_name2:
                continue

            dat1 = pd.read_csv(file_dir + csv1, dtype={':START_ID(order_no-ID)': str})
            dat1 = dat1.iloc[:, [0, -1]]
            dat2 = pd.read_csv(file_dir + csv2, dtype={':START_ID(order_no-ID)': str})
            dat2 = dat2.iloc[:, [0, -1]]

            print(file_name1 + '_' + file_name2 + ' begin.')
            # if file_name1 == 'referphone':
            #     dat1.rename(columns={':END_ID(cellphone-ID)': ':END_ID(referphone-ID)'}, inplace=True)
            # if file_name2 == 'referphone':
            #     dat2.rename(columns={':END_ID(cellphone-ID)': ':END_ID(referphone-ID)'}, inplace=True)

            dat1_id = pd.merge(basic_info[['order_id', 'order_no:ID(order_no-ID)']], dat1,
                               how='inner', left_on='order_no:ID(order_no-ID)', right_on=':START_ID(order_no-ID)')\
                        .drop(['order_no:ID(order_no-ID)', ':START_ID(order_no-ID)'], axis=1)
            dat1_id.columns = ['order_id', ':END_ID(' + file_name1 + '-ID)']

            dat_merge = pd.merge(dat1_id[dat1_id.order_id == order_id], dat1_id, on=':END_ID(' + file_name1 + '-ID)', how='inner')
            if not dat_merge.empty:
                dat_merge = dat_merge[dat_merge['order_id_x'] != dat_merge['order_id_y']].reset_index(drop=True)
                # dat_merge = dat_merge[dat_merge.order_id_x.isin()]
                # dat_merge = pd.concat([dat_merge, dat_merge.rename(columns={'order_id_y': 'order_id_x', 'order_id_x': 'order_id_y'})], ignore_index=True)

                dat2_id = pd.merge(basic_info[['order_id', 'order_no:ID(order_no-ID)']], dat2, how='inner',
                                   left_on='order_no:ID(order_no-ID)',
                                   right_on=':START_ID(order_no-ID)')\
                    .drop(['order_no:ID(order_no-ID)', ':START_ID(order_no-ID)'], axis=1)
                dat2_id.columns = ['order_id', ':END_ID(' + file_name2 + '-ID)']

                dat_merge2 = pd.merge(dat2_id[dat2_id.order_id.isin(list(dat_merge.order_id_y.unique()))], dat2_id,
                                      on=':END_ID(' + file_name2 + '-ID)', how='inner', suffixes=("_m", "_n"))
                dat_merge2 = dat_merge2[dat_merge2['order_id_m'] != dat_merge2['order_id_n']].reset_index(drop=True)

                dat = pd.merge(dat_merge, dat_merge2, left_on='order_id_y', right_on='order_id_m')\
                        .drop(['order_id_y', 'order_id_m'], axis=1)\
                        .rename(columns={'order_id_x': ':START_ID(order_no-ID)', 'order_id_n': ':END_ID(order_no-ID)'})
                dat = dat[dat[':START_ID(order_no-ID)'] != dat[':END_ID(order_no-ID)']].reset_index(drop=True)
                if save_dir is not None:
                    dat.to_csv(save_dir + '/matrix(2-path)/' + file_name1 + '_' + file_name2 + '.csv', index=False)
                result = pd.concat([result, dat[[':START_ID(order_no-ID)', ':END_ID(order_no-ID)']]], ignore_index=True)
                result.drop_duplicates(inplace=True)
                del([dat1, dat2, dat_merge, dat_merge2, dat1_id, dat2_id])
                print(file_name1 + '_' + file_name2 + ' done.')
            else:
                continue

    return result
