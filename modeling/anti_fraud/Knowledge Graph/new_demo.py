import pandas as pd
import os
import sys

sys.path.append(r'/Users/wuxiongbin/Desktop/GACD_test/')
from lga import LGA
from Matrix_generation import matrix_generation_1_path, matrix_generation_2_path
from Matrix_weighted import matrix_weighted
from label_result import label_result
from network_stat import network_stat
import matplotlib

matplotlib.use('TkAgg')
from datetime import datetime
from datetime import timedelta

work_dir = '/Users/wuxiongbin/Desktop/反欺诈/图谱/'

base_info = pd.read_csv(work_dir + 'test2/basic_info.csv', dtype={'order_no:ID(order_no-ID)': str})
# base_info = base_info.reset_index().rename(columns={'index': 'order_id'})
# base_info['order_id'] = base_info['order_id'] + 1
order_id = base_info[['order_id', 'order_no:ID(order_no-ID)']]

weight = pd.read_excel(work_dir + 'relation_type.xlsx')
weight_dict = {}
for row in range(weight.shape[0]):
    weight_dict[weight.iloc[row, 0]] = weight.iloc[row, 1]

del weight

# matrix_generation_1_path(order_id=order_id, relation_list=list(weight_dict.keys()),
#                          save_dir=work_dir + '/test2/', file_dir=work_dir+'/import/')
#
# matrix_result_sum = matrix_weighted(dir=work_dir + '/test2/', weight_dict=weight_dict)
matrix_result_sum = pd.read_csv(work_dir + '/test2/matrix_weighted/matrix_result_sum.csv')

test_df = base_info[base_info['business_id'].isin(['xjbk', 'rong360'])].reset_index(drop=True)
test_df = test_df[~test_df['flag:int'].isin([-9, -2])].reset_index(drop=True)

# row = test_df.iloc[0, :]

stats = pd.DataFrame()
for index, row in test_df.iterrows():
    print(index)
    base_sub = base_info[base_info.created_time <= row['created_time']].reset_index(drop=True)

    if not base_sub.empty:
        cnt_stats = {}
        order_no = row['order_no:ID(order_no-ID)']
        order_id = row['order_id']

        matrix_result_sum_sub = matrix_result_sum[matrix_result_sum[':START_ID(order_no-ID)'] == order_id].reset_index(drop=True)
        matrix_result_sum_sub = matrix_result_sum_sub[matrix_result_sum_sub[':END_ID(order_no-ID)'].isin(list(base_sub.order_id.unique()))].reset_index(drop=True)
        community = matrix_result_sum_sub.drop('weight', axis=1)
        two_path_result = matrix_generation_2_path(basic_info=base_sub, order_no=order_no,
                                                   relation_list=list(weight_dict.keys()),
                                                   save_dir=None, file_dir=work_dir+'/import/')
        community = pd.concat([community, two_path_result], axis=0, ignore_index=True)\
            .drop_duplicates().reset_index(drop=True)
        community = pd.merge(community, base_sub,
                             left_on=':END_ID(order_no-ID)',
                             right_on='order_id',
                             how='left')
        cnt_stats['cnt_order:int'] = community.shape[0]

        if cnt_stats['cnt_order:int'] != 0:
            cnt_stats['cnt_effective:int'] = community[~community['flag:int'].isin([-9, -2])].shape[0]
            cnt_stats['cnt_reject:int'] = community[community['flag_1:int'] == -9].shape[0]
            cnt_stats['cnt_male:int'] = community[community['idcardgender'] == '男'].shape[0]

            cnt_stats['reject_rate:double'] = cnt_stats['cnt_reject:int'] / cnt_stats['cnt_order:int']
            cnt_stats['effective_rate:double'] = cnt_stats['cnt_effective:int'] / cnt_stats['cnt_order:int']
            cnt_stats['male_rate:double'] = cnt_stats['cnt_male:int'] / cnt_stats['cnt_order:int']

            cnt_stats['cnt_effective_due_flag1:int'] = community[community['flag_1:int'].isin([0, 1])].shape[0]

            if cnt_stats['cnt_effective_due_flag1:int'] != 0:
                cnt_stats['cnt_bad_flag1:int'] = community[community['flag_1:int'] == 1].shape[0]
                cnt_stats['bad_rate_flag1:double'] = cnt_stats['cnt_bad_flag1:int'] / cnt_stats[
                    'cnt_effective_due_flag1:int']
            else:
                cnt_stats['cnt_bad_flag1:int'] = -1
                cnt_stats['bad_rate_flag1:double'] = -1

            cnt_stats['cnt_effective_due_flag2:int'] = community[community['flag_2:int'].isin([0, 1])].shape[0]

            if cnt_stats['cnt_effective_due_flag2:int'] != 0:
                cnt_stats['cnt_bad_flag2:int'] = community[community['flag_2:int'] == 1].shape[0]
                cnt_stats['bad_rate_flag2:double'] = cnt_stats['cnt_bad_flag2:int'] / cnt_stats[
                    'cnt_effective_due_flag2:int']
            else:
                cnt_stats['cnt_bad_flag2:int'] = -1
                cnt_stats['bad_rate_flag2:double'] = -1

        else:
            cnt_stats['cnt_effective:int'] = -1
            cnt_stats['cnt_reject:int'] = -1
            cnt_stats['cnt_male:int'] = -1

            cnt_stats['reject_rate:double'] = -1
            cnt_stats['effective_rate:double'] = -1
            cnt_stats['male_rate:double'] = -1

            cnt_stats['cnt_effective_due_flag1:int'] = -1
            cnt_stats['cnt_bad_flag1:int'] = -1
            cnt_stats['bad_rate_flag1:double'] = -1

            cnt_stats['cnt_effective_due_flag2:int'] = -1
            cnt_stats['cnt_bad_flag2:int'] = -1
            cnt_stats['bad_rate_flag2:double'] = -1

        if not matrix_result_sum_sub.empty:
            matrix_result_sum_sub = pd.concat([matrix_result_sum_sub, matrix_result_sum_sub.rename(columns={':END_ID(order_no-ID)': ':START_ID(order_no-ID)', ':START_ID(order_no-ID)': ':END_ID(order_no-ID)'})], axis=0, ignore_index=True)
            label_dat, Q = LGA(data=matrix_result_sum_sub, L=100, pop_size=80, p1=0.8, save_dir=None)
            base_sub = pd.merge(base_sub, label_dat, on='order_id', how='left')

            base_sub['label:int'].fillna(0, inplace=True)
            label_max = base_sub['label:int'].max()
            cnt = 1
            label_new = []
            for i in range(base_sub.shape[0]):
                if base_sub['label:int'][i] == 0:
                    label_new.append(label_max + cnt)
                    cnt += 1
                else:
                    label_new.append(base_sub['label:int'][i])
            base_sub['label:int'] = label_new

            label = base_sub[base_sub['order_id'] == order_id].reset_index(drop=True).iloc[0, 0]
            community_group = pd.merge(community, base_sub[['order_id', 'label:int']], how='left', on='order_id')
            community_group = community_group[community_group['label:int'] == label].reset_index(drop=True)

            net_stats = network_stat(order_id, label, base_sub, matrix_result_sum_sub, save_dir=None)

            cnt_stats.update(net_stats)

            cnt_stats['cnt_order_group:int'] = community_group.shape[0]

            if cnt_stats['cnt_order_group:int'] != 0:
                cnt_stats['cnt_effective_group:int'] = community_group[~community_group['flag:int'].isin([-9, -2])].shape[0]
                cnt_stats['cnt_reject_group:int'] = community_group[community_group['flag_1:int'] == -9].shape[0]
                cnt_stats['cnt_male_group:int'] = community_group[community_group['idcardgender'] == '男'].shape[0]

                cnt_stats['reject_rate_group:double'] = cnt_stats['cnt_reject_group:int'] / cnt_stats['cnt_order_group:int']
                cnt_stats['effective_rate_group:double'] = cnt_stats['cnt_effective_group:int'] / cnt_stats['cnt_order_group:int']
                cnt_stats['male_rate_group:double'] = cnt_stats['cnt_male_group:int'] / cnt_stats['cnt_order_group:int']

                cnt_stats['cnt_effective_due_flag1_group:int'] = community_group[community_group['flag_1:int'].isin([0, 1])].shape[0]

                if cnt_stats['cnt_effective_due_flag1_group:int'] != 0:
                    cnt_stats['cnt_bad_flag1_group:int'] = community_group[community_group['flag_1:int'] == 1].shape[0]
                    cnt_stats['bad_rate_flag1_group:double'] = cnt_stats['cnt_bad_flag1_group:int'] / cnt_stats[
                        'cnt_effective_due_flag1_group:int']
                else:
                    cnt_stats['cnt_bad_flag1_group:int'] = -1
                    cnt_stats['bad_rate_flag1_group:double'] = -1

                cnt_stats['cnt_effective_due_flag2_group:int'] = \
                community_group[community_group['flag_2:int'].isin([0, 1])].shape[0]

                if cnt_stats['cnt_effective_due_flag2_group:int'] != 0:
                    cnt_stats['cnt_bad_flag2_group:int'] = community_group[community_group['flag_2:int'] == 1].shape[0]
                    cnt_stats['bad_rate_flag2_group:double'] = cnt_stats['cnt_bad_flag2_group:int'] / cnt_stats[
                        'cnt_effective_due_flag2_group:int']
                else:
                    cnt_stats['cnt_bad_flag2_group:int'] = -1
                    cnt_stats['bad_rate_flag2_group:double'] = -1

            else:
                cnt_stats['cnt_effective_group:int'] = -1
                cnt_stats['cnt_reject_group:int'] = -1
                cnt_stats['cnt_male_group:int'] = -1

                cnt_stats['reject_rate_group:double'] = -1
                cnt_stats['effective_rate_group:double'] = -1
                cnt_stats['male_rate_group:double'] = -1

                cnt_stats['cnt_effective_due_flag1_group:int'] = -1
                cnt_stats['cnt_bad_flag1_group:int'] = -1
                cnt_stats['bad_rate_flag1_group:double'] = -1

                cnt_stats['cnt_effective_due_flag2_group:int'] = -1
                cnt_stats['cnt_bad_flag2_group:int'] = -1
                cnt_stats['bad_rate_flag2_group:double'] = -1

        cnt_stats_df = pd.DataFrame(data=list(cnt_stats.values()), index=list(cnt_stats.keys()), columns=[str(order_id)])
        stats = pd.concat([stats, cnt_stats_df], axis=1)
    else:
        continue
