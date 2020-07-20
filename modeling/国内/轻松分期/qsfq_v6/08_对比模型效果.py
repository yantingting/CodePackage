"""
更新flag & 模型打分
"""

#tb的贷后表现

tb_perf = """
select order_no
, business_id
, effective_date
, fst_term_late_day
, fst_term_due_date
, lst_term_due_date
, late_day
, max_late_day
from t_loan_performance
where business_id in ('tb') and dt = '20190919' and effective_date between '2018-08-26' and '2019-01-31'
"""

tb_perf_0920 = get_df_from_pg(tb_perf)
tb_perf_0920 = tb_perf_0920.drop_duplicates()

"""
gy 子模型在test_new上的打分
"""
xy_gy_seq = list(pd.read_excel('D:/Model/201908_qsfq_model/02_result_gy/credit_model/features_in_model_xinyan.xlsx')['var'])
jxl_gy_seq = list(pd.read_excel('D:/Model/201908_qsfq_model/02_result_gy/credit_model/features_in_model_juxinli.xlsx')['var'])
td_gy_seq = ['tdidcntpartnerloan30d', 'tdidcntloancompanyloan30d','tdmcntloancompanyloan30d', 'tdidcntloancompanyloan90d']
td_gy_seq2 = ['r47580334_id_platform_ct', 'r47580334_id_industry_18_ct','r47580334_m_industry_18_ct', 'r47580344_id_industry_18_ct']

all_gy_seq = list(pd.read_excel('D:/Model/201908_qsfq_model/02_result_gy/credit_model/features_in_model_all.xlsx')['var'])

mymodel = xgb.Booster()
mymodel.load_model("D:/Model/201908_qsfq_model/02_result_gy/credit_model/dc_credit_xinyan.model")
mymodel.load_model("D:/Model/201908_qsfq_model/02_result_gy/credit_model/dc_credit_juxinli.model")
mymodel.load_model("D:/Model/201908_qsfq_model/02_result_gy/credit_model/dc_credit_tongdun.model")
mymodel.load_model("D:/Model/201908_qsfq_model/02_result_gy/credit_model/dc_credit_basic_3.model")

data_rescore = x_with_y2.loc[(x_with_y2.sample_set == 'test_new'),td_gy_seq2].rename(columns = dict(zip(td_gy_seq, td_gy_seq2)))
data_rescore = x_with_y2.loc[(x_with_y2.sample_set == 'test_new')][['education','age','industry','monthlyincome','positiontype','age_marital','idcity_jobcity','id_city_level','job_city_level']]

data_gy = data_rescore.merge(xy_data_scored, left_index  = True, right_on = 'order_no').merge(jxl_data_scored,on = 'order_no').merge(td_data_scored,  on = 'order_no')
data_gy.index = data_gy.order_no
data_gy2 = data_gy[all_gy_seq]
data_gy2[['jxl_score','xy_score','td_score']] = data_gy2[['jxl_score','xy_score','td_score']] .astype(float)

data_gy2 = DMatrix(data_gy2)
ypred = mymodel.predict(data_gy2)

score = [round(Prob2Score(value, 600, 20)) for value in ypred]

# xy_data_scored = xy_data_scored.rename(columns = {'score':'xy_score', 'y_pred': 'xy_pre'})
# jxl_data_scored = jxl_data_scored.rename(columns = {'score':'jxl_score', 'y_pred': 'jxl_pre'})
# td_data_scored = td_data_scored.rename(columns = {'score':'td_score', 'y_pred': 'td_pre'})
xy_data_scored = xy_data_scored.rename(columns = {'xy_score':'xy_score2', 'xy_pre': 'xy_score'})
jxl_data_scored = jxl_data_scored.rename(columns = {'jxl_score':'jxl_score2', 'jxl_pre': 'jxl_score'})
td_data_scored = td_data_scored.rename(columns = {'td_score':'td_score2', 'td_pre': 'td_score'})

data_scored  = pd.DataFrame([data_gy.index, score, ypred]).T.rename(columns = {0:'order_no',1:'score',2:'y_pred'})

data_scored.score = data_scored.score.astype(float)

data_scored.order_no = data_scored.order_no.astype(str)
data_scored2 = data_scored.merge(data_gy, left_on = 'order_no', right_index = True )
data_scored2['score_bin'] = pd.cut(data_scored2.score, bins = [-np.inf, 589, 603, 612, 623, 632, 640, 649, 661, 682, np.inf]).astype(str)
data_scored2['xy_score_bin'] = pd.cut(data_scored2.xy_score2, bins = [-np.inf, 597, 604, 605, 607, 609, 611, 615, 619, 623, np.inf]).astype(str)
data_scored2['td_score_bin'] = pd.cut(data_scored2.td_score2, bins = [-np.inf, 596, 600, 603, 607, 610, 612, 614, 616, np.inf]).astype(str)
data_scored2['jxl_score_bin'] = pd.cut(data_scored2.jxl_score2, bins = [-np.inf, 591, 600, 606, 609, 613, 617, 621, 625, 633, np.inf]).astype(str)



data_scored2.to_excel("D:/Model/201908_qsfq_model/02_result_gy/all_x_y_test_new.xlsx",index=False)
    #.to_excel("D:/Model/201908_qsfq_model/02_result_jxl/test_new_scored_81.xlsx",index=False)
data_scored.to_excel("D:/Model/201908_qsfq_model/02_result_xy/test_new_scored_17.xlsx",index=False)




data_rescore = x_with_y2.loc[(x_with_y2.sample_set == 'train')][xy_seq]
data_rescore = x_with_y2.loc[(x_with_y2.sample_set == 'test_new')][xy_seq]



"""
模型效果对比
"""
x_with_y_v5 = pd.read_excel(os.path.join(data_path, 'x_with_y_v5.xlsx'))

new_flag0923 = x_with_y_v5[['order_no','effective_date','fid7','flag_credit','perf_flag']]

model_result_train = pd.read_excel('D:/Model/201908_qsfq_model/模型效果对比.xlsx',sheet_name = 'data_scored_train')
model_result_test = pd.read_excel('D:/Model/201908_qsfq_model/模型效果对比.xlsx',sheet_name = 'data_scored_test')
model_result_test_new = pd.read_excel('D:/Model/201908_qsfq_model/模型效果对比.xlsx',sheet_name = 'data_scored_test_new')

model_result_test_new = model_result_test_new.merge(new_flag0923[['order_no','effective_date']], on = 'order_no')
bin_20_m = [400, 563, 574, 583, 590, 595,599,603,606,609, 613,616,620,624,629,633,639,646,653,663,750]
model_result_test_new['score_bin_20_m'] = pd.cut(model_result_test_new.score_m, bins = bin_20_m).astype(str)
model_result_test_new['score_bin_m'] = model_result_test_new['score_bin_m'].astype(str)

#合并
model_result_train['sample_set'] = 'train'
model_result_test['sample_set'] = 'test'
model_result_test_new['sample_set'] = 'Test_new'

model_result_m = pd.concat([model_result_train, model_result_test, model_result_test_new])
model_result_m['score_bin_m'] = pd.cut(model_result_m['score_m'], bins = [400, 574, 590, 599, 606, 613,620,629,639, 653,750]).astype(str)
model_result_m['score_bin_20_m'] = pd.cut(model_result_m['score_m'], bins = bin_20_m).astype(str)

#
# model_result_m['score_bin_20_m'] = model_result_m['score_bin_20_m'].astype(str)
# model_result_m['score_bin_m'] = model_result_m['score_bin_m'].astype(str)


model_result_m = new_flag0923.merge(model_result_m, on = 'order_no')
model_result_m.to_excel('D:/Model/201908_qsfq_model/model_compare.xlsx')

"""
黄海模型效果
"""
hh_train = pd.read_csv('D:/Model/201909_qsfq_refit_model/02_result_v3_hh/v4_credit_train_prob.csv')
hh_test = pd.read_excel('D:/Model/201909_qsfq_refit_model/02_result_v3_hh/v4_fraud&credit_test_pred_all.xlsx', sheet_name = 'credit')

hh_train_score = pd.DataFrame([round(Prob2Score(value, 600, 20)) for value in hh_train['prob_XGB']])
hh_train_score = pd.concat([hh_train,hh_train_score], axis = 1)
hh_train_score = hh_train_score.rename(columns = {0:'score_hh', 'prob_XGB':'y_pred_hh'})
hh_train_score['sample_set'] = 'train'

hh_test_score = pd.DataFrame([round(Prob2Score(value, 600, 20)) for value in hh_test['credit_prob']])
hh_test_score = pd.concat([hh_test,hh_test_score], axis = 1)
hh_test_score = hh_test_score.rename(columns = {0:'score_hh', 'credit_prob':'y_pred_hh'})
hh_test_score.shape
hh_test_score['sample_set'] = 'test'
hh_score = pd.concat([hh_train_score,hh_test_score])
hh_score['score_bin_hh'] = pd.cut(hh_score.score_hh, bins = [-np.inf, 591, 599, 606, 610, 614, 618, 621, 626, 631, np.inf]).astype(str)
pd.qcut(hh_score.loc[hh_score.sample_set == 'train']['score_hh'], q = 20, duplicates='drop', precision=0).value_counts().sort_index()

#hh_bin_20 = [-np.inf, 584, 590, 596, 599,602, 605, 608,610, 612, 613, 615, 617, 619,620, 622, 624, 627, 630, 634, np.inf] 按照test分
hh_bin_20 = [-np.inf, 585, 591, 595, 599,603, 606, 608,610, 612, 614, 616, 618, 619,621, 624, 626, 628, 631, 636, np.inf]

hh_score['score_bin_20_hh'] = pd.cut(hh_score.score_hh, bins =hh_bin_20).astype(str)

#
hh_score.to_excel('D:/Model/201909_qsfq_refit_model/02_result_v3_hh/hh_score_train_test.xlsx')

"""
gy模型效果
"""
gy_score = pd.read_excel('D:/Model/201908_qsfq_model/02_result_gy/data_all_result.xlsx').rename(columns = {'y_data_pred':'y_pred_gy','score_all':'score_gy'})
gy_score

gy_bin_20 = [-np.inf, 545, 557, 568, 578,586, 592, 598,604, 610, 615, 620, 626, 632,640, 646, 655, 664, 677, 693, np.inf]
#pd.qcut(gy_score.loc[gy_score.sample_set == 'train']['score_gy'], q = 20, duplicates='drop', precision=0).value_counts().sort_index()

gy_score_all = pd.concat([gy_score[['order_no','score_gy','y_pred_gy']], data_scored2[['order_no','score','y_pred']].rename(columns = {'score':'score_gy','y_pred':'y_pred_gy'})])gy_score_all
gy_score_all['score_bin_gy'] =  pd.cut(gy_score_all.score_gy, bins = [-np.inf, 558, 578, 592, 604, 615, 626, 638, 653, 675, np.inf]).astype(str)
gy_score_all['score_bin_20_gy'] =  pd.cut(gy_score_all.score_gy, bins = gy_bin_20).astype(str)

gy_score_all.to_excel('D:/Model/201908_qsfq_model/02_result_gy/score_all.xlsx')