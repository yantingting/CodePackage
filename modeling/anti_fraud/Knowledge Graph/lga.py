import GACD_interface
import numpy as np
import pandas as pd
import time
import os

"""
dir = './sim/'
data = pd.read_csv(dir + 'matrix_result_sum.csv', usecols=[0,1,2])
L=10
pop_size=2
p1=0.7
"""


def LGA(data, L, pop_size, p1, save_dir=None):
    order_id = data[':START_ID(order_no-ID)'].unique()
    order_dict = {}
    cnt = 1
    for i in order_id:
        order_dict[i] = cnt
        cnt += 1
    """
    start = np.array([0]*data.shape[0], dtype=float)
    end = np.array([0]*data.shape[0], dtype=float)

    for i in range(data.shape[0]):
        start[i] = order_dict[data[':START_ID(order_no-ID)'][i]]
        end[i] = order_dict[data[':END_ID(order_no-ID)'][i]]
        
    data['start'] = start
    data['end'] = end
    """

    data['start'] = data[':START_ID(order_no-ID)'].apply(lambda x: order_dict[x])
    data['end'] = data[':END_ID(order_no-ID)'].apply(lambda x: order_dict[x])
    # 补足缺失列
    if data.shape[1] == 4:
        data = pd.concat([data, pd.DataFrame([1]*data.shape[0], columns=['weight'])], axis=1)

    # 准备W数据
    nval = data.shape[0]
    # n_cols = data_convert.shape[1]
    # size_w = nval * n_cols
    b_W1 = np.array(list(data['end']) + list(data['start']) + list(data['weight']), dtype=np.double)

    # 计算
    t1 = time.time()
    print(time.localtime(t1))
    Q, label, x_1, y_1 = GACD_interface.GACD(b_W1, nval, np.double(pop_size), np.double(L), np.double(p1))
    t2 = time.time()
    print('  TIME: %.4fs ' % (t2 - t1))

    label_dat = pd.DataFrame({'order_id': order_id.astype(int), 'label:int': label.astype(int)})
    
    if save_dir is not None:
        label_dat.to_csv(save_dir + 'label_dat.csv', index=False)

    return label_dat, Q
