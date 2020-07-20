import pandas as pd
import os


def matrix_weighted(dir, weight_dict):

    matrix_dict = {}

    if not os.path.exists(dir):
        os.makedirs(dir)

    if not os.path.exists(dir+'/matrix_weighted'):
        os.makedirs(dir+'/matrix_weighted')

    files1 = os.listdir(dir + '/matrix(1-path)')
    # files2 = os.listdir(dir + '/matrix(2-path)')

    for key in list(weight_dict.keys()):

        if (key + '.csv') in files1:
            dat = pd.read_csv(dir + '/matrix(1-path)/' + key + '.csv')
            print(key)
        else:
            print(key)
            continue
        """
        else:
            dat = pd.read_csv(dir + '/matrix(2-path)/' + key + '.csv')
            """
        # dat = pd.read_csv(dir + '/matrix(1-path)/' + key + '.csv')
        n_rows = dat.shape[0]

        dat['weight'] = pd.Series([weight_dict[key]]*n_rows)

        dat['relation'] = pd.Series([key]*n_rows)
    # print(dat.head())
        matrix_dict[key] = dat
        dat.to_csv(dir+'/matrix_weighted/' + key + '_w.csv', index=False)
        del dat

    matrix_result = pd.DataFrame()
    for key in matrix_dict:
        matrix_result = pd.concat([matrix_result,
                                   matrix_dict[key].loc[:, [':START_ID(order_no-ID)', ':END_ID(order_no-ID)', 'weight']]],
                                  ignore_index=True, axis=0)
        print(key)

    matrix_result_sum = matrix_result['weight'].groupby([matrix_result[':START_ID(order_no-ID)'],
                                                  matrix_result[':END_ID(order_no-ID)']]).sum().reset_index()
    matrix_result_sum.to_csv(dir+'/matrix_weighted/matrix_result_sum.csv', index=False)

    return matrix_result_sum
