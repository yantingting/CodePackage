import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
import configparser
import sys
from email.mime.application import MIMEApplication

conf = configparser.ConfigParser()
DATABASE_CONFIGURE_FILE = os.path.join(os.path.split(os.path.realpath(__file__))[0],'mysql.cfg')
print('open mysql.cfg')
print(DATABASE_CONFIGURE_FILE)
print('open path')
print(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
with open(DATABASE_CONFIGURE_FILE, 'r') as cfgfile:
    conf.readfp(cfgfile)

def mailtest_report(from_addr,to,subject,content,attfile=None,login_user=conf.get('email','EMAIL_USER'), login_password=conf.get('email', 'EMAIL_PASSWORD'), html_msg=None, cc=list(),fig=None):
    #创建一个带附件的实例
    msg = MIMEMultipart()
    #添加邮件内容
    #注意，要指定邮件内容的编码为utf-8，否则中文会有乱码
    if html_msg is None:
        text_msg = MIMEText(content,'plain','gbk')
        msg.attach(text_msg)
    else:
        html_msg = MIMEText(html_msg, "html", "utf-8")
        msg.attach(html_msg)

    if fig is not None:
        #添加附件就是加上一个MIMEBase，从本地读取一个图片:
        with open(fig, 'rb') as f:
            # 设置附件的MIME和文件名，这里是png类型:
            mime = MIMEBase('image', 'png', filename='test.png')
            # 加上必要的头信息:
            mime.add_header('Content-Disposition', 'attachment', filename='test.png')
            mime.add_header('Content-ID', '<0>')
            mime.add_header('X-Attachment-Id', '0')
            # 把附件的内容读进来:
            mime.set_payload(f.read())
            # 用Base64编码:
            encoders.encode_base64(mime)
            # 添加到MIMEMultipart:
            msg.attach(mime)

            msg.attach(MIMEText('<html><body>%s<h1></h1>'%content +
                                '<p><img src="cid:0"></p>' +
                                '</body></html>', 'html', 'utf-8'))

    if attfile is not None:
        #构造附件
        #注意：传入的参数attfile为unicode，否则带中文的目录或名称的文件读不出来
        #      basename 为文件名称，由于传入的参数attfile为unicode编码，此处的basename也为unicode编码
        basename = os.path.basename(attfile)
        #注意：指定att的编码方式为gb2312
        att = MIMEText(open(attfile, 'rb').read(), 'base64', 'utf-8')
        att["Content-Type"] = 'application/octet-stream'
        #注意：此处basename要转换为gb2312编码，否则中文会有乱码。
        #      特别，此处的basename为unicode编码，所以可以用basename.encode('gb2312')
        #            如果basename为utf-8编码，要用basename.decode('utf-8').encode('gb2312')
        att.add_header('Content-Disposition', 'attachment',filename=('gbk', '', basename)) # basename若包含中文则需要转换为gbk编码
        msg.attach(att)
    #加邮件头
    msg['to'] = ", ".join(to)
    msg['from'] = ", ".join(from_addr)
    msg['Cc']=','.join(cc)
    to_addr = to + cc
    #主题指定utf-8编码，否则中文会有乱码
    msg['subject'] = Header(subject, 'utf-8')
    #发送邮件
    print('start send')
    print(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
    server = smtplib.SMTP_SSL("smtp.exmail.qq.com",port=465, timeout=30)
    # server.login(conf.get('email','EMAIL_USER'),conf.get('email', 'EMAIL_PASSWORD'))
    print(login_user)
    print(login_password)
    server.login(login_user,login_password)
    server.sendmail(msg['from'], to_addr, msg.as_string()) # 收件人在这里必须是list
    server.close


def mailtest(from_addr,to,subject,content,attfile=None,login_user=conf.get('email','EMAIL_USER'), login_password=conf.get('email', 'EMAIL_PASSWORD'), html_msg=None, cc=list(),fig=None):
    print('start sending email')
    #创建一个带附件的实例
    msg = MIMEMultipart()
    #添加邮件内容
    #注意，要指定邮件内容的编码为utf-8，否则中文会有乱码
    if html_msg is None:
        text_msg = MIMEText(content,'plain','gbk')
        msg.attach(text_msg)
    else:
        html_msg = MIMEText(html_msg, "html", "utf-8")
        msg.attach(html_msg)

    if fig is not None:
        #添加附件就是加上一个MIMEBase，从本地读取一个图片:
        with open(fig, 'rb') as f:
            # 设置附件的MIME和文件名，这里是png类型:
            mime = MIMEBase('image', 'png', filename='test.png')
            # 加上必要的头信息:
            mime.add_header('Content-Disposition', 'attachment', filename='test.png')
            mime.add_header('Content-ID', '<0>')
            mime.add_header('X-Attachment-Id', '0')
            # 把附件的内容读进来:
            mime.set_payload(f.read())
            # 用Base64编码:
            encoders.encode_base64(mime)
            # 添加到MIMEMultipart:
            msg.attach(mime)

            msg.attach(MIMEText('<html><body>%s<h1></h1>'%content +
                                '<p><img src="cid:0"></p>' +
                                '</body></html>', 'html', 'utf-8'))

    if attfile is not None:
        #构造附件
        #注意：传入的参数attfile为unicode，否则带中文的目录或名称的文件读不出来
        #      basename 为文件名称，由于传入的参数attfile为unicode编码，此处的basename也为unicode编码
        basename = os.path.basename(attfile)
        #注意：指定att的编码方式为gb2312
        att = MIMEText(open(attfile, 'rb').read(), 'base64', 'utf-8')
        att["Content-Type"] = 'application/octet-stream'
        #注意：此处basename要转换为gb2312编码，否则中文会有乱码。
        #      特别，此处的basename为unicode编码，所以可以用basename.encode('gb2312')
        #            如果basename为utf-8编码，要用basename.decode('utf-8').encode('gb2312')
        #att.add_header('Content-Disposition', 'attachment',filename=('gbk', '', basename)) # basename若包含中文则需要转换为gbk编码
        att.add_header('Content-Disposition', 'attachment', filename=basename)
        msg.attach(att)
    #加邮件头
    msg['to'] = ", ".join(to)
    msg['from'] = ", ".join(from_addr)
    msg['Cc']=','.join(cc)
    to_addr = to + cc
    #主题指定utf-8编码，否则中文会有乱码
    msg['subject'] = Header(subject, 'utf-8')
    #发送邮件
    print('start send')
    print(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
    server = smtplib.SMTP_SSL("smtp.exmail.qq.com",port=465, timeout=30)
    # server.login(conf.get('email','EMAIL_USER'),conf.get('email', 'EMAIL_PASSWORD'))
    print(login_user)
    print(login_password)
    server.login(login_user,login_password)
    server.sendmail(msg['from'], to_addr, msg.as_string()) # 收件人在这里必须是list
    server.close

if  __name__ =="__main__":
    mailtest(["riskcontrol-business@huojintech.com"], ["jiangshanjiao@mintechai.com"], "测试发送邮件邮件主题", "测试发送附件", "/home/ops/repos/newgenie/utils3/mysql.cfg")
