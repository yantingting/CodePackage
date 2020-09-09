# coding: utf-8
"""
Python 3.6.0
uku日报
"""
import sys
import os

#sys.path.append("C:\\Users\\wangj\\Documents\\genie")
sys.path.append('/home/ops/repos/newgenie/')

import numpy as np
import pandas as pd
from importlib import reload

from utils3.data_io_utils import get_df_from_pg
import utils3.send_email_with_table as se
from utils3.send_email import mailtest
reload(se)

"""0.检查表是否是空---------------------------------------------------------------------"""
loan_check = get_df_from_pg("""SELECT * FROM public.dw_gocash_go_cash_loan_gocash_core_loan LIMIT 10""")

if loan_check.shape[0]<10:
    send_email_elart = 1
    print('数据底层表无数据')
else:
    send_email_elart = 0
    print('数据底层表有数据')



"""1.放款情况--------------------------------------------------------------------------"""

FANGKUAN_SQL = """
set enable_nestloop=off;

SELECT
	to_char(effective_date, 'YYYY-MM-DD') as date,
	count(distinct case when return_flag = 'false' and approved_period = 8 then id else null end) as new8fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 8 then id else null end) as old8fangkuanliang,
	count(distinct case when return_flag = 'false' and approved_period = 15 then id else null end) as new15fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 15 then id else null end) as old15fangkuanliang,
    count(distinct case when return_flag = 'false' and approved_period = 28 then id else null end) as new28fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 28 then id else null end) as old28fangkuanliang,
	--count(distinct case when return_flag = 'false' and approved_period = 22 then id else null end) as new22fangkuanliang,
	--count(distinct case when return_flag = 'true' and approved_period = 22 then id else null end) as old22fangkuanliang,
    --count(distinct case when return_flag = 'false' and approved_period = 29 then id else null end) as new29fangkuanliang,
	--count(distinct case when return_flag = 'true' and approved_period = 29 then id else null end) as old29fangkuanliang,
	count(distinct id) as totalfangkuanliang,
	round(sum(case when return_flag = 'false' and approved_period = 8 then approved_principal else 0 end)/ 2000)::int as new8hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 8 then approved_principal else 0 end)/ 2000)::int as old8hetongjine,
	round(sum(case when return_flag = 'false' and approved_period = 15 then approved_principal else 0 end)/ 2000)::int as new15hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 15 then approved_principal else 0 end)/ 2000)::int as old15hetongjine,
    round(sum(case when return_flag = 'false' and approved_period = 28 then approved_principal else 0 end)/ 2000)::int as new28hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 28 then approved_principal else 0 end)/ 2000)::int as old28hetongjine,
	--round(sum(case when return_flag = 'false' and approved_period = 22 then approved_principal else 0 end)/ 2000)::int as new22hetongjine,
	--round(sum(case when return_flag = 'true' and approved_period = 22 then approved_principal else 0 end)/ 2000)::int as old22hetongjine,
    --round(sum(case when return_flag = 'false' and approved_period = 29 then approved_principal else 0 end)/ 2000)::int as new29hetongjine,
	--round(sum(case when return_flag = 'true' and approved_period = 29 then approved_principal else 0 end)/ 2000)::int as old29hetongjine,
	round(sum(approved_principal / 2000))::int as totalhetongjine
from
	public.dw_gocash_go_cash_loan_gocash_core_loan
where
	effective_date >= current_date-7
	and effective_date <= current_date-1
	and loan_status not in ('DENIED',
	'RESCIND',
	'APPROVING',
	'CREATED')
group by
	date
union
select
	'合计' as date,
	count(distinct case when return_flag = 'false' and approved_period = 8 then id else null end) as new8fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 8 then id else null end) as old8fangkuanliang,
	count(distinct case when return_flag = 'false' and approved_period = 15 then id else null end) as new15fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 15 then id else null end) as old15fangkuanliang,
    count(distinct case when return_flag = 'false' and approved_period = 28 then id else null end) as new28fangkuanliang,
	count(distinct case when return_flag = 'true' and approved_period = 28 then id else null end) as old28fangkuanliang,
	--count(distinct case when return_flag = 'false' and approved_period = 22 then id else null end) as new22fangkuanliang,
	--count(distinct case when return_flag = 'true' and approved_period = 22 then id else null end) as old22fangkuanliang,
    --count(distinct case when return_flag = 'false' and approved_period = 29 then id else null end) as new29fangkuanliang,
	--count(distinct case when return_flag = 'true' and approved_period = 29 then id else null end) as old29fangkuanliang,
	count(distinct id) as totalfangkuanliang,
	round(sum(case when return_flag = 'false' and approved_period = 8 then approved_principal else 0 end)/ 2000)::int as new8hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 8 then approved_principal else 0 end)/ 2000)::int as old8hetongjine,
	round(sum(case when return_flag = 'false' and approved_period = 15 then approved_principal else 0 end)/ 2000)::int as new15hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 15 then approved_principal else 0 end)/ 2000)::int as old15hetongjine,
    round(sum(case when return_flag = 'false' and approved_period = 28 then approved_principal else 0 end)/ 2000)::int as new28hetongjine,
	round(sum(case when return_flag = 'true' and approved_period = 28 then approved_principal else 0 end)/ 2000)::int as old28hetongjine,
	--round(sum(case when return_flag = 'false' and approved_period = 22 then approved_principal else 0 end)/ 2000)::int as new22hetongjine,
	--round(sum(case when return_flag = 'true' and approved_period = 22 then approved_principal else 0 end)/ 2000)::int as old22hetongjine,
    --round(sum(case when return_flag = 'false' and approved_period = 29 then approved_principal else 0 end)/ 2000)::int as new29hetongjine,
	--round(sum(case when return_flag = 'true' and approved_period = 29 then approved_principal else 0 end)/ 2000)::int as old29hetongjine,
	round(sum(approved_principal / 2000))::int as totalhetongjine
from
	public.dw_gocash_go_cash_loan_gocash_core_loan
where
	effective_date >= current_date-7
	and effective_date <= current_date-1
	and loan_status not in ('DENIED',
	'RESCIND',
	'APPROVING',
	'CREATED')
group by
	date
order by
	date
"""

try:
    fangkuan_df = get_df_from_pg(FANGKUAN_SQL)
    fangkuan_df.head()
    fangkuan_df.rename(columns={'date':'放款日期','new8fangkuanliang':'新客户8日','old8fangkuanliang':'老客户8日','new15fangkuanliang':'新客户15日','old15fangkuanliang':'老客户15日','new28fangkuanliang':'linkaja新28日','old28fangkuanliang':'linkaja老28日','totalfangkuanliang':'合计',\
        'new8hetongjine':'新客户8日','old8hetongjine':'老客户8日','new15hetongjine':'新客户15日','old15hetongjine':'老客户15日','new28hetongjine':'linkaja新28日','old28hetongjine':'linkaja老28日','totalhetongjine':'合计'},inplace=True)
    the_col = [('','放款日期'),('放款件数','新客户8日'),('放款件数','老客户8日'),('放款件数','新客户15日'),('放款件数','老客户15日'),('放款件数','linkaja新28日'),('放款件数','linkaja老28日'),('放款件数','合计')\
        ,('合同金额','新客户8日'),('合同金额','老客户8日'),('合同金额','新客户15日'),('合同金额','老客户15日'),('合同金额','linkaja新28日'),('合同金额','linkaja老28日'),('合同金额','合计')]
    # fangkuan_df.rename(columns={'date':'放款日期','new8fangkuanliang':'新客户8日','old8fangkuanliang':'老客户8日','new15fangkuanliang':'新客户15日','old15fangkuanliang':'老客户15日','new22fangkuanliang':'新客户22日','old22fangkuanliang':'老客户22日','new29fangkuanliang':'新客户29日','old29fangkuanliang':'老客户29日','totalfangkuanliang':'合计',\
    #     'new8hetongjine':'新客户8日','old8hetongjine':'老客户8日','new15hetongjine':'新客户15日','old15hetongjine':'老客户15日','new22hetongjine':'新客户22日','old22hetongjine':'老客户22日','new29hetongjine':'新客户29日','old29hetongjine':'老客户29日','totalhetongjine':'合计'},inplace=True)
    # the_col = [('','放款日期'),('放款件数','新客户8日'),('放款件数','老客户8日'),('放款件数','新客户15日'),('放款件数','老客户15日'),('放款件数','新客户22日'),('放款件数','老客户22日'),('放款件数','新客户29日'),('放款件数','老客户29日'),('放款件数','合计')\
    #     ,('合同金额','新客户8日'),('合同金额','老客户8日'),('合同金额','新客户15日'),('合同金额','老客户15日'),('合同金额','新客户22日'),('合同金额','老客户22日'),('合同金额','新客户29日'),('合同金额','老客户29日'),('合同金额','合计')]
    fangkuan_df.columns=pd.MultiIndex.from_tuples(the_col)
except:
    fangkuan_df = pd.DataFrame()



if __name__ =="__main__":
    try:
        fangkuan_html = se.format_df(fangkuan_df)
        fangkuan_body = se.format_body(fangkuan_html,form_name='uku产品近7日放款情况')
    except:
        fangkuan_body = 'uku产品近7日放款情况（数据问题，暂时无法呈现）'

    if send_email_elart == 1:
        mailtest(["riskcontrol-business@huojintech.com"], ["jiangshanjiao@mintechai.com"], \
        '【印尼风控数据每日监测——底层数据问题_%s】'%pd.to_datetime('today').strftime('%Y-%m-%d'), 'select * 检查\n\nloan表：%d条\n'%(loan_check.shape[0]))
    else:
        None


    html_msg= "<html>" + fangkuan_body + "</html>"
    #mailtest(["riskcontrol-business@huojintech.com"],["wangjinwei@huojintech.com"], \
    #'【印尼风控数据每日监测_%s】'%pd.to_datetime('today').strftime('%Y-%m-%d'), "测试发送html", html_msg=html_msg,\
	#	cc=["jiangshanjiao@huojintech.com"])
    mailtest(["riskcontrol-business@huojintech.com"],["nianminhuan@huojintech.com","derek@ukuindo.com","liuyi@huojintech.com"], \
    '【印尼风控数据每日监测_%s】'%pd.to_datetime('today').strftime('%Y-%m-%d'), "测试发送html", html_msg=html_msg, \
    cc=["zhaozhonglin@mintechai.com","baozhe@huojintech.com","ganyiling@huojintech.com","liuman@mintechai.com","zhuran@mintechai.com","wangjinwei@mintechai.com","xuchao@huojintech.com","zhangji@huojintech.com","jindi@huojintech.com","renfei@huojintech.com","riskcontrol@huojintech.com"])

	#"18600230546@163.com"该邮箱收件人邮箱地址有问题
