import numpy as np
import pandas as pd

sys.path.append('/Users/Mint/Desktop/repos/genie')
import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *

"""
2020/1/13 固定表现期重新计算flag
"""

#计算用户在什么时候会展期
extendday_sql = """
select effective_month, day_diff, count(id)
from (
select id
--, customer_id
--, apply_time
, substring(effective_date :: varchar,1,7 ) as effective_month
--, approved_period
, due_date
--, extend_times
--, effective_date -1 + approved_period as first_due_date
, (effective_date -1 + approved_period) + (approved_period:: integer) * (extend_times) as new_due_date --假设到期当天展期计算的due_date
, abs(due_date - ((effective_date -1 + approved_period) + (approved_period:: integer) * (extend_times))) as day_diff
--, loan_status
from public.dw_gocash_go_cash_loan_gocash_core_loan
where return_flag = 'true' and effective_date between '2019-08-08' and '2019-12-30'
--and id in (
--374292691093651456,
--372513194341990400)
) a 
group by effective_month, day_diff
"""

extendday_data = get_df_from_pg(extendday_sql)
extendday_data.to_excel('D:/Model/202001_mvp_model/01_data/extendday.xlsx')

#97%的人会在放款日的前后3天内展期

hive_sql = """
SELECT id as loan_id
    , customer_id
    , apply_time
    , effective_date
    , paid_off_time
    , due_date
    , loan_status
    , extend_times
    , approved_period
    , cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date) as dt_date
    , date_add(date_sub(effective_date, 1), (approved_period+3)*5) as extend_due_date
    ,case when extend_times>4 then 0 
          when datediff(cast(paid_off_time as date), due_date) > 7 then 1 
          when loan_status='COLLECTION' and datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), due_date) <= 7 then -3 
          when loan_status='COLLECTION' and datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), due_date) > 7 then 1
          when extend_times<=4 and extend_times>0 and loan_status='FUNDED' then -2
          when datediff(cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date), effective_date) < approved_period and loan_status!='ADVANCE_PAIDOFF' then -1
          else 0 end as flag7
FROM dw.dw_gocash_go_cash_loan_gocash_core_loan 
--WHERE date_add(effective_date, (approved_period+3)*4) = cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date) 
WHERE date_add(date_sub(effective_date, 1), (approved_period+3)*5) = cast(substr(dt,1,4)||'-'||substr(dt, 5, 2)||'-'||substr(dt, 7, 2)  as date) 
      and return_flag = 'true' and effective_date between '2019-08-08' and '2019-11-30'
"""

flag_0113 = pd.read_csv('D:/Model/202001_mvp_model/01_data/flag_0808to1130.csv')
flag_0113.loan_id.nunique() #84807
flag_0113.customer_id.nunique() #37951

flag_0113.loc[(flag_0113.dt_date == '2020-01-12') & (flag_0113.effective_date == '2019-11-19')].to_excel('D:/Model/202001_mvp_model/01_data/data_check.xlsx')

#数据检查
flag_0113.loc[flag_0113.approved_period == 15]['effective_date'].max() #15期老客max effective_date = 2019-10-16
flag_0113.loc[flag_0113.effective_date <= '2019-10-16'].shape #81342/60 每天放款量为1378

flag_0113.loc[(flag_0113.effective_date >= '2019-10-15') & (flag_0113.effective_date <= '2019-11-30')].shape #3497/45 每天放款量为80单左右

flag_0113['effective_month'] = flag_0113.effective_date.apply(lambda x:str(x)[0:7])

flag_0113.loc[flag_0113.extend_times >4].shape #3%展期4次以上

flag_0113.loc[flag_0113.effective_date <= '2019-10-15'].shape #81342 和报表一致
flag_0113.loc[flag_0113.effective_date <= '2019-10-15']['flag7'].mean() #15.8%

#上传至pg
flag_0113.columns

SQL_CREATE_TABLE = """
CREATE TABLE temp_uku_mvpmodelflag(
    loan_id VARCHAR,
    customer_id VARCHAR,
    apply_time VARCHAR,
    effective_date VARCHAR,
    paid_off_time VARCHAR,
    due_date VARCHAR,
    loan_status VARCHAR,
    extend_times FLOAT,
    approved_period FLOAT,
    dt_date VARCHAR,
    extend_due_date VARCHAR,
    flag7 FLOAT    
)
"""

upload_df_to_pg(SQL_CREATE_TABLE)

flag_0113.loan_id = flag_0113.loan_id.astype(str)
flag_0113.customer_id = flag_0113.customer_id.astype(str)

insert = """
INSERT INTO temp_uku_mvpmodelflag
 VALUES
{% for var in var_list %}
{{ var }},
{% endfor %}
"""

var_list = []

for cols, rows in flag_0113.iterrows():
    c = tuple(rows)
    var_list.append(c)

insert_sql = Template(insert).render(var_list=var_list)[:-2]
insert_sql = insert_sql.replace('\n\n','')
insert_sql = insert_sql.replace('\n','')

upload_df_to_pg(insert_sql)

flag_0113.loc[(flag_0113.effective_date < '2019-09-17'),'sample_set'] = 'app_before'
flag_0113.loc[(flag_0113.effective_date >= '2019-09-17') & (flag_0113.effective_date <= '2019-10-07'),'sample_set'] = 'train'
flag_0113.loc[(flag_0113.effective_date > '2019-10-07') & (flag_0113.effective_date <= '2019-10-16'),'sample_set'] = 'test'
flag_0113.loc[(flag_0113.effective_date > '2019-10-16'),'sample_set'] = 'only_8'


