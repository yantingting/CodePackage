import pandas as pd
import psycopg2
def get_df_from_pg(SQL):
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
    cur = conn.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    df = pd.DataFrame(rows,columns=[i.name for i in cur.description])
    df.columns = [i.split('.')[1] if len(i.split('.'))>1 else i for i in df.columns.tolist()]
    return df

## 提取变量
apply_sql = """
select t0.order_no
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC022' as YLZC022
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC478' as YLZC478
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC214' as YLZC214
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC297' as YLZC297
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC215' as YLZC215
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC013' as YLZC013
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC035' as YLZC035
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC037' as YLZC037
, json_array_elements(cast(t2.oss ::json #>> '{data}' as json)) ::json ->> 'YLZC053' as YLZC053
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY004' as TSJY004
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY042' as TSJY042
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY041' as TSJY041
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY003' as TSJY003
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY006' as TSJY006
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY045' as TSJY045
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY049' as TSJY049
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY043' as TSJY043
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY009' as TSJY009
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY047' as TSJY047
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY012' as TSJY012
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY033' as TSJY033
, json_array_elements(cast(t3.oss ::json #>> '{data}' as json)) ::json ->> 'TSJY020' as TSJY020
, json_array_elements(cast(t1.oss ::json #>> '{data}' as json)) ::json ->> 'RMS002' as RMS002
from t_loan_performance t0 
left join dc_yinlianloanbeforescore_application t1 on t0.order_no = t1.order_no and t1.oss<>''
left join dc_yinlianoverallscore_application t2 on t0.order_no = t2.order_no and t2.oss<>''
left join dc_yinlianspecialexchangeimage_application t3 on t0.order_no = t3.order_no and t3.oss<>''
where t0.dt='20191107' and t0.business_id in ('tb', 'xjbk', 'rong360') and t0.effective_date between '2019-09-30' and '2019-10-23' 

"""
apply_data = get_df_from_pg(apply_sql)

apply_data.head()
apply_data.shape
apply_data.order_no.nunique() 

## 变量名小写改为大写
col_names = apply_data.columns.tolist()
col_upper = ([s.upper() for s in col_names])
apply_data.columns = col_upper
apply_data.rename(columns={'ORDER_NO': 'order_no'}, inplace=True) 
apply_data.columns