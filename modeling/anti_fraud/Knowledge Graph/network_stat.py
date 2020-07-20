import pandas as pd
import numpy as np
import networkx as nx

def network_stat(order_id, label, basic_info, matrix_result_sum, save_dir=None):
    # basic_info = pd.read_csv('./test7/basic_info.csv')
    # basic_info.rename(columns={'label:int': 'label'}, inplace=True)
    # result = pd.read_excel(dir + 'result.xlsx')
    # matrix_result_sum = pd.read_csv('./test7/matrix_result_sum.csv')#.sort_values(by='weight', ascending=False)

    label_weight = pd.merge(matrix_result_sum, basic_info.loc[:, ['order_id', 'label:int']],
                            how='left', left_on=':START_ID(order_no-ID)', right_on='order_id')
    # label_weight = pd.merge(label_weight, basic_info.loc[:, ['order_id', 'label:int']], how='left', left_on=':END_ID(order_no-ID)', right_on='order_id')
# xxx2.drop(['order_id_x', 'order_id_y'], inplace=True, axis=1)

    label_weight = label_weight[label_weight['label:int'] == label]
    # label_weight.drop(['order_id_x', 'order_id_y'], axis=1, inplace=True)
    print('network stats begin!')

    # avg_length = []
    # avg_clustering = []

    G = nx.DiGraph()
    # label2_weight = label_weight[label_weight['label:int_x'] == i+1].reset_index().drop('index', axis=1)
    edge_list = []
    if label_weight.shape[0] == 0:
        avg_length = -1
        avg_clustering = -1
        stats = {'avg_length:double': avg_length, 'avg_clustering:double': avg_clustering,
                 'degree:int': -1, 'betweenness:double': -1,
                 'eigenvector:double': -1, 'closeness:double': -1,
                 'degree_centrality:double': -1, 'clustering:double': -1}
    else:
        for j in range(label_weight.shape[0]):
            edge_list.append(tuple(label_weight.iloc[j, 0:3]))

        G.add_weighted_edges_from(edge_list)

        avg_length = nx.average_shortest_path_length(G)
        avg_clustering = nx.average_clustering(G)

        # temp = pd.DataFrame({'order_id': list(dict(G.degree).keys()),
        #                          'degree:int': list(dict(G.degree).values())})
        degree_df = dict(G.degree)[order_id]

        # temp1 = pd.DataFrame({'order_id': list(nx.betweenness_centrality(G).keys()),
        #                           'betweenness:double': list(nx.betweenness_centrality(G).values())})
        betweenness_df = nx.betweenness_centrality(G)[order_id]

        # temp2 = pd.DataFrame({'order_id': list(nx.eigenvector_centrality(G, max_iter=500, tol=1.0e-4).keys()),
        #                           'eigenvector:double': list(nx.eigenvector_centrality(G, max_iter=500, tol=1.0e-4).values())})
        eigenvector_df = (nx.eigenvector_centrality(G, max_iter=500, tol=1.0e-4))[order_id]

        # temp3 = pd.DataFrame({'order_id': list(nx.closeness_centrality(G).keys()),
        #                           'closeness:double': list(nx.closeness_centrality(G).values())})
        closeness_df = nx.closeness_centrality(G)[order_id]

        # temp4 = pd.DataFrame({'order_id': list(nx.degree_centrality(G).keys()),
        #                           'degree_centrality:double': list(nx.degree_centrality(G).values())})
        degree_centrality_df = nx.degree_centrality(G)[order_id]

        # temp5 = pd.DataFrame({'order_id': list(nx.clustering(G).keys()),
        #                           'clustering:double': list(nx.clustering(G).values())})
        clustering_df = nx.clustering(G)[order_id]

        # del temp, temp1, temp2, temp3, temp4, temp5
        # print(i+1)
        stats = {'avg_length:double': avg_length, 'avg_clustering:double': avg_clustering,
                 'degree:int': degree_df, 'betweenness:double': betweenness_df,
                 'eigenvector:double': eigenvector_df, 'closeness:double': closeness_df,
                 'degree_centrality:double': degree_centrality_df, 'clustering:double': clustering_df}

    print('network stats done!')
    if save_dir is not None:
        stats_df = pd.DataFrame(data=list(stats.values()), index=list(stats.keys()), columns=[str(order_id)])
        stats_df.to_csv(save_dir + str(order_id) + '_network_stats.csv')
    return stats

    # result['avg_length:double'] = avg_length
    # result['avg_clustering:double'] = avg_clustering

    # result.to_excel(save_dir + 'result.xlsx', index=False)
    # del avg_length, avg_clustering
    #
    # basic_info = pd.merge(basic_info, result.iloc[:, [0, -2, -1]], how='left', on='label:int')
    # basic_info = pd.merge(basic_info, degree_df, how='left', on='order_id')
    # basic_info = pd.merge(basic_info, betweenness_df, how='left', on='order_id')
    # basic_info = pd.merge(basic_info, eigenvector_df, how='left', on='order_id')
    # basic_info = pd.merge(basic_info, closeness_df, how='left', on='order_id')
    # basic_info = pd.merge(basic_info, degree_centrality_df, how='left', on='order_id')
    # basic_info = pd.merge(basic_info, clustering_df, how='left', on='order_id')
    # basic_info['degree:int'].fillna(0, inplace=True)
    # # basic_info = pd.merge(basic_info, result.iloc[:, [0,3]], how='left', on='label:int')
    #
    # # basic_info.rename(columns={'credit_bad_rate': 'credit_bad_rate:double', 'cnt': 'cnt:int'}, inplace=True)
    # basic_info['label:int'] = basic_info['label:int'].astype(int)
    # del degree_df, degree_centrality_df, betweenness_df, closeness_df, clustering_df, eigenvector_df
    # basic_info.to_csv(save_dir + 'basic_info.csv', index=False)


