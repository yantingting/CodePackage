
import pandas as pd
import numpy as np
import numbers

from utils3.send_email import mailtest



# 添加图片附件
# 添加附件就是加上一个MIMEBase，从本地读取一个图片:
# with open('/Users/ruoshui/Desktop/杂/数据成本.png', 'rb') as f:
#     # 设置附件的MIME和文件名，这里是png类型:
#     mime = MIMEBase('image', 'png', filename='test.png')
#     # 加上必要的头信息:
#     mime.add_header('Content-Disposition', 'attachment', filename='test.png')
#     mime.add_header('Content-ID', '<0>')
#     mime.add_header('X-Attachment-Id', '0')
#     # 把附件的内容读进来:
#     mime.set_payload(f.read())
#     # 用Base64编码:
#     encoders.encode_base64(mime)
#     # 添加到MIMEMultipart:
#     msg.attach(mime)
#
# msg.attach(MIMEText('<html><body><h1>Hello</h1>' +
#         '<p><img src="cid:0"></p>' +
#         '</body></html>', 'html', 'utf-8'))


def format_df(df):
    # Construct a mask of which columns are numeric
    numeric_col_mask = df.dtypes.apply(lambda d: issubclass(np.dtype(d).type, numbers.Number))
    # Dict used to center the table headers
    d = dict(selector="th",
        props=[('text-align', 'center')])
    # Style
    df_html = df.style.set_properties(subset=df.columns[numeric_col_mask], # right-align the numeric columns and set their width
                            **{'width':'10em', 'text-align':'right'})\
            .set_properties(subset=df.columns[~numeric_col_mask], # left-align the non-numeric columns and set their width
                            **{'width':'10em', 'text-align':'center'})\
            .format(lambda x: '{:,}'.format(x) if isinstance(x, (int,np.integer)) else '{:,.2f}'.format(x), # format the numeric values
                    subset=pd.IndexSlice[:,df.columns[numeric_col_mask]])\
            .set_table_styles([{'selector': 'tr:nth-of-type(odd)',
              'props': [('background', '#eee')]},
             {'selector': 'tr:nth-of-type(even)',
              'props': [('background', 'white')]},
             {'selector': 'th',
              'props': [('background', '#606060'),
                        ('color', 'white'),
                        ('font-family', 'verdana')]},
             {'selector': 'td',
              'props': [('font-family', 'verdana')]},
            ]).hide_index().render() # center the header
    return df_html



head = \
    """
    <head>
        <meta charset="utf-8">
        <STYLE TYPE="text/css" MEDIA=screen>

            table.dataframe {
                border-collapse: collapse;
                border: 2px solid #a19da2;
                /*居中显示整个表格*/
                margin: auto;
            }

            table.dataframe thead {
                border: 2px solid #91c6e1;
                background: #f1f1f1;
                padding: 10px 10px 10px 10px;
                color: #333333;
            }

            table.dataframe tbody {
                border: 2px solid #91c6e1;
                padding: 10px 10px 10px 10px;
            }

            table.dataframe tr {

            }

            table.dataframe th {
                vertical-align: top;
                font-size: 14px;
                padding: 10px 10px 10px 10px;
                color: #105de3;
                font-family: arial;
                text-align: center;
            }

            table.dataframe td {
                text-align: center;
                padding: 10px 10px 10px 10px;
            }

            body {
                font-family: 宋体;
            }

            h1 {
                color: #5db446
            }

            div.header h2 {
                color: #0002e3;
                font-family: 黑体;
            }

            div.content h2 {
                text-align: center;
                font-size: 28px;
                text-shadow: 2px 2px 1px #de4040;
                color: #fff;
                font-weight: bold;
                background-color: #008eb7;
                line-height: 1.5;
                margin: 20px 0;
                box-shadow: 10px 10px 5px #888888;
                border-radius: 5px;
            }

            h3 {
                font-size: 22px;
                background-color: rgba(0, 2, 227, 0.71);
                text-shadow: 2px 2px 1px #de4040;
                color: rgba(239, 241, 234, 0.99);
                line-height: 1.5;
            }

            h4 {
                color: #e10092;
                font-family: 楷体;
                font-size: 20px;
                text-align: center;
            }

            td img {
                /*width: 60px;*/
                max-width: 300px;
                max-height: 300px;
            }

        </STYLE>
    </head>
    """

def format_body(df_html,form_name=None):
    # 构造模板的附件（100）
    body = \
            """
            <body>

            <hr>

            <div class="content">
                <!--正文内容-->
                <h3>{form_name}</h3>

                <div>
                    {df_html}

                </div>
                <hr>

            </div>
            </body>
            """.format(form_name=form_name, df_html=df_html)
    return body



if __name__ =="__main__":
    df_html = format_df(df)
    the_body = format_body(df_html)
    html_msg= "<html>" + the_body + "</html>"
    mailtest(["riskcontrol@huojintech.com"], ["jiaoruoshui@mintechai.com"], '【印尼风控数据每日监测_%s】'%pd.to_datetime('today').strftime('%Y-%m-%d'), "测试发送html", html_msg=html_msg)
