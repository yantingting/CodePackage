# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 18:10:20 2019

@author: yuexin
"""
import os
import sys
import logging
import codecs
import argparse
from functools import reduce
from imp import reload
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from jinja2 import Template
from sklearn.model_selection import train_test_split
sys.path.append('D:/_Tools/genie')

import utils3.misc_utils as mu
import utils3.metrics as mt
import utils3.summary_statistics as ss
import utils3.feature_selection as fs
from utils3.data_io_utils import *


import os
import time
import pandas as pd
import psycopg2

filepath_out = 'D:/Model Development/201910 MVP/01 Data/raw data/'

usename = "postgres"
password = "Mintq2019"
db = "risk_dm"
host = "192.168.2.19"
port = "5432"

conn = psycopg2.connect(database=db, user=usename, password=password, host=host, port=port)

sql_gy = '''
select t0.order_no
,coalesce(cast(t1.oss ::json ->> 'result' as json) ::json #>>'{data,score}', t1.oss::json#>>'{data,score}')as gy_xiaoescore
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601025}', t2.oss::json#>>'{data,SXB0601025}')as SXB0601025
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601024}', t2.oss::json#>>'{data,SXB0601024}')as SXB0601024
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601023}', t2.oss::json#>>'{data,SXB0601023}')as SXB0601023
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601022}', t2.oss::json#>>'{data,SXB0601022}')as SXB0601022
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601079}', t2.oss::json#>>'{data,SXA1601079}')as SXA1601079
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601021}', t2.oss::json#>>'{data,SXB0601021}')as SXB0601021
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601020}', t2.oss::json#>>'{data,SXB0601020}')as SXB0601020
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,score}', t2.oss::json#>>'{data,score}')as score
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4203001}', t2.oss::json#>>'{data,SXX4203001}')as SXX4203001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5603001}', t2.oss::json#>>'{data,SXX5603001}')as SXX5603001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1103001}', t2.oss::json#>>'{data,SXX1103001}')as SXX1103001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601070}', t2.oss::json#>>'{data,SXA1601070}')as SXA1601070
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601075}', t2.oss::json#>>'{data,SXA1601075}')as SXA1601075
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2503001}', t2.oss::json#>>'{data,SXX2503001}')as SXX2503001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601076}', t2.oss::json#>>'{data,SXA1601076}')as SXA1601076
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601077}', t2.oss::json#>>'{data,SXA1601077}')as SXA1601077
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4604001}', t2.oss::json#>>'{data,SXX4604001}')as SXX4604001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601078}', t2.oss::json#>>'{data,SXA1601078}')as SXA1601078
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601071}', t2.oss::json#>>'{data,SXA1601071}')as SXA1601071
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601029}', t2.oss::json#>>'{data,SXB0601029}')as SXB0601029
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601028}', t2.oss::json#>>'{data,SXB0601028}')as SXB0601028
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601072}', t2.oss::json#>>'{data,SXA1601072}')as SXA1601072
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601027}', t2.oss::json#>>'{data,SXB0601027}')as SXB0601027
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601073}', t2.oss::json#>>'{data,SXA1601073}')as SXA1601073
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601074}', t2.oss::json#>>'{data,SXA1601074}')as SXA1601074
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601026}', t2.oss::json#>>'{data,SXB0601026}')as SXB0601026
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601014}', t2.oss::json#>>'{data,SXB0601014}')as SXB0601014
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601013}', t2.oss::json#>>'{data,SXB0601013}')as SXB0601013
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3003001}', t2.oss::json#>>'{data,SXX3003001}')as SXX3003001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601012}', t2.oss::json#>>'{data,SXB0601012}')as SXB0601012
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4403001}', t2.oss::json#>>'{data,SXX4403001}')as SXX4403001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601011}', t2.oss::json#>>'{data,SXB0601011}')as SXB0601011
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601010}', t2.oss::json#>>'{data,SXB0601010}')as SXB0601010
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601068}', t2.oss::json#>>'{data,SXA1601068}')as SXA1601068
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601069}', t2.oss::json#>>'{data,SXA1601069}')as SXA1601069
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXE0101001}', t2.oss::json#>>'{data,SXE0101001}')as SXE0101001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1704001}', t2.oss::json#>>'{data,SXX1704001}')as SXX1704001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXE0101004}', t2.oss::json#>>'{data,SXE0101004}')as SXE0101004
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601064}', t2.oss::json#>>'{data,SXA1601064}')as SXA1601064
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601065}', t2.oss::json#>>'{data,SXA1601065}')as SXA1601065
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXE0101002}', t2.oss::json#>>'{data,SXE0101002}')as SXE0101002
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601066}', t2.oss::json#>>'{data,SXA1601066}')as SXA1601066
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601067}', t2.oss::json#>>'{data,SXA1601067}')as SXA1601067
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601019}', t2.oss::json#>>'{data,SXB0601019}')as SXB0601019
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXE0101003}', t2.oss::json#>>'{data,SXE0101003}')as SXE0101003
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601018}', t2.oss::json#>>'{data,SXB0601018}')as SXB0601018
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601060}', t2.oss::json#>>'{data,SXA1601060}')as SXA1601060
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601061}', t2.oss::json#>>'{data,SXA1601061}')as SXA1601061
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601017}', t2.oss::json#>>'{data,SXB0601017}')as SXB0601017
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601016}', t2.oss::json#>>'{data,SXB0601016}')as SXB0601016
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601062}', t2.oss::json#>>'{data,SXA1601062}')as SXA1601062
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601063}', t2.oss::json#>>'{data,SXA1601063}')as SXA1601063
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601015}', t2.oss::json#>>'{data,SXB0601015}')as SXB0601015
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601003}', t2.oss::json#>>'{data,SXB0601003}')as SXB0601003
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6803001}', t2.oss::json#>>'{data,SXX6803001}')as SXX6803001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601002}', t2.oss::json#>>'{data,SXB0601002}')as SXB0601002
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601001}', t2.oss::json#>>'{data,SXB0601001}')as SXB0601001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601057}', t2.oss::json#>>'{data,SXA1601057}')as SXA1601057
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5804001}', t2.oss::json#>>'{data,SXX5804001}')as SXX5804001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601058}', t2.oss::json#>>'{data,SXA1601058}')as SXA1601058
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601059}', t2.oss::json#>>'{data,SXA1601059}')as SXA1601059
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1304001}', t2.oss::json#>>'{data,SXX1304001}')as SXX1304001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601053}', t2.oss::json#>>'{data,SXA1601053}')as SXA1601053
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601054}', t2.oss::json#>>'{data,SXA1601054}')as SXA1601054
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601055}', t2.oss::json#>>'{data,SXA1601055}')as SXA1601055
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601009}', t2.oss::json#>>'{data,SXB0601009}')as SXB0601009
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601056}', t2.oss::json#>>'{data,SXA1601056}')as SXA1601056
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601008}', t2.oss::json#>>'{data,SXB0601008}')as SXB0601008
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX7503001}', t2.oss::json#>>'{data,SXX7503001}')as SXX7503001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601007}', t2.oss::json#>>'{data,SXB0601007}')as SXB0601007
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601050}', t2.oss::json#>>'{data,SXA1601050}')as SXA1601050
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1904001}', t2.oss::json#>>'{data,SXX1904001}')as SXX1904001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601006}', t2.oss::json#>>'{data,SXB0601006}')as SXB0601006
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601051}', t2.oss::json#>>'{data,SXA1601051}')as SXA1601051
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601005}', t2.oss::json#>>'{data,SXB0601005}')as SXB0601005
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5104001}', t2.oss::json#>>'{data,SXX5104001}')as SXX5104001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601004}', t2.oss::json#>>'{data,SXB0601004}')as SXB0601004
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601052}', t2.oss::json#>>'{data,SXA1601052}')as SXA1601052
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601046}', t2.oss::json#>>'{data,SXA1601046}')as SXA1601046
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601047}', t2.oss::json#>>'{data,SXA1601047}')as SXA1601047
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601048}', t2.oss::json#>>'{data,SXA1601048}')as SXA1601048
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2904001}', t2.oss::json#>>'{data,SXX2904001}')as SXX2904001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601049}', t2.oss::json#>>'{data,SXA1601049}')as SXA1601049
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6104001}', t2.oss::json#>>'{data,SXX6104001}')as SXX6104001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601042}', t2.oss::json#>>'{data,SXA1601042}')as SXA1601042
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4003001}', t2.oss::json#>>'{data,SXX4003001}')as SXX4003001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601043}', t2.oss::json#>>'{data,SXA1601043}')as SXA1601043
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601044}', t2.oss::json#>>'{data,SXA1601044}')as SXA1601044
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4804001}', t2.oss::json#>>'{data,SXX4804001}')as SXX4804001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601045}', t2.oss::json#>>'{data,SXA1601045}')as SXA1601045
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6704001}', t2.oss::json#>>'{data,SXX6704001}')as SXX6704001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2703001}', t2.oss::json#>>'{data,SXX2703001}')as SXX2703001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601040}', t2.oss::json#>>'{data,SXA1601040}')as SXA1601040
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601041}', t2.oss::json#>>'{data,SXA1601041}')as SXA1601041
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601069}', t2.oss::json#>>'{data,SXB0601069}')as SXB0601069
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601068}', t2.oss::json#>>'{data,SXB0601068}')as SXB0601068
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601067}', t2.oss::json#>>'{data,SXB0601067}')as SXB0601067
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601066}', t2.oss::json#>>'{data,SXB0601066}')as SXB0601066
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601065}', t2.oss::json#>>'{data,SXB0601065}')as SXB0601065
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601064}', t2.oss::json#>>'{data,SXB0601064}')as SXB0601064
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601063}', t2.oss::json#>>'{data,SXB0601063}')as SXB0601063
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0804001}', t2.oss::json#>>'{data,SXX0804001}')as SXX0804001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4104001}', t2.oss::json#>>'{data,SXX4104001}')as SXX4104001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601062}', t2.oss::json#>>'{data,SXB0601062}')as SXB0601062
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601061}', t2.oss::json#>>'{data,SXB0601061}')as SXB0601061
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601060}', t2.oss::json#>>'{data,SXB0601060}')as SXB0601060
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1803001}', t2.oss::json#>>'{data,SXX1803001}')as SXX1803001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5504001}', t2.oss::json#>>'{data,SXX5504001}')as SXX5504001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0303001}', t2.oss::json#>>'{data,SXX0303001}')as SXX0303001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601058}', t2.oss::json#>>'{data,SXB0601058}')as SXB0601058
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4703001}', t2.oss::json#>>'{data,SXX4703001}')as SXX4703001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601057}', t2.oss::json#>>'{data,SXB0601057}')as SXB0601057
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601056}', t2.oss::json#>>'{data,SXB0601056}')as SXB0601056
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601055}', t2.oss::json#>>'{data,SXB0601055}')as SXB0601055
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601054}', t2.oss::json#>>'{data,SXB0601054}')as SXB0601054
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601053}', t2.oss::json#>>'{data,SXB0601053}')as SXB0601053
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601052}', t2.oss::json#>>'{data,SXB0601052}')as SXB0601052
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5704001}', t2.oss::json#>>'{data,SXX5704001}')as SXX5704001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601051}', t2.oss::json#>>'{data,SXB0601051}')as SXB0601051
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5203001}', t2.oss::json#>>'{data,SXX5203001}')as SXX5203001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601050}', t2.oss::json#>>'{data,SXB0601050}')as SXB0601050
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5003001}', t2.oss::json#>>'{data,SXX5003001}')as SXX5003001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX7703001}', t2.oss::json#>>'{data,SXX7703001}')as SXX7703001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6004001}', t2.oss::json#>>'{data,SXX6004001}')as SXX6004001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6603001}', t2.oss::json#>>'{data,SXX6603001}')as SXX6603001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602020}', t2.oss::json#>>'{data,SXB0602020}')as SXB0602020
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602021}', t2.oss::json#>>'{data,SXB0602021}')as SXB0602021
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602022}', t2.oss::json#>>'{data,SXB0602022}')as SXB0602022
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602023}', t2.oss::json#>>'{data,SXB0602023}')as SXB0602023
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602024}', t2.oss::json#>>'{data,SXB0602024}')as SXB0602024
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602025}', t2.oss::json#>>'{data,SXB0602025}')as SXB0602025
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601059}', t2.oss::json#>>'{data,SXB0601059}')as SXB0601059
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602016}', t2.oss::json#>>'{data,SXB0602016}')as SXB0602016
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601047}', t2.oss::json#>>'{data,SXB0601047}')as SXB0601047
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601046}', t2.oss::json#>>'{data,SXB0601046}')as SXB0601046
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602017}', t2.oss::json#>>'{data,SXB0602017}')as SXB0602017
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602018}', t2.oss::json#>>'{data,SXB0602018}')as SXB0602018
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601045}', t2.oss::json#>>'{data,SXB0601045}')as SXB0601045
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602019}', t2.oss::json#>>'{data,SXB0602019}')as SXB0602019
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601044}', t2.oss::json#>>'{data,SXB0601044}')as SXB0601044
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601043}', t2.oss::json#>>'{data,SXB0601043}')as SXB0601043
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601042}', t2.oss::json#>>'{data,SXB0601042}')as SXB0601042
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601041}', t2.oss::json#>>'{data,SXB0601041}')as SXB0601041
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601040}', t2.oss::json#>>'{data,SXB0601040}')as SXB0601040
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601090}', t2.oss::json#>>'{data,SXA1601090}')as SXA1601090
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601091}', t2.oss::json#>>'{data,SXA1601091}')as SXA1601091
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601092}', t2.oss::json#>>'{data,SXA1601092}')as SXA1601092
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2104001}', t2.oss::json#>>'{data,SXX2104001}')as SXX2104001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2203001}', t2.oss::json#>>'{data,SXX2203001}')as SXX2203001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601097}', t2.oss::json#>>'{data,SXA1601097}')as SXA1601097
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601098}', t2.oss::json#>>'{data,SXA1601098}')as SXA1601098
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601099}', t2.oss::json#>>'{data,SXA1601099}')as SXA1601099
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602010}', t2.oss::json#>>'{data,SXB0602010}')as SXB0602010
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602011}', t2.oss::json#>>'{data,SXB0602011}')as SXB0602011
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602012}', t2.oss::json#>>'{data,SXB0602012}')as SXB0602012
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601093}', t2.oss::json#>>'{data,SXA1601093}')as SXA1601093
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602013}', t2.oss::json#>>'{data,SXB0602013}')as SXB0602013
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601094}', t2.oss::json#>>'{data,SXA1601094}')as SXA1601094
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602014}', t2.oss::json#>>'{data,SXB0602014}')as SXB0602014
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601095}', t2.oss::json#>>'{data,SXA1601095}')as SXA1601095
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601049}', t2.oss::json#>>'{data,SXB0601049}')as SXB0601049
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601048}', t2.oss::json#>>'{data,SXB0601048}')as SXB0601048
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602015}', t2.oss::json#>>'{data,SXB0602015}')as SXB0602015
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601096}', t2.oss::json#>>'{data,SXA1601096}')as SXA1601096
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602005}', t2.oss::json#>>'{data,SXB0602005}')as SXB0602005
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601036}', t2.oss::json#>>'{data,SXB0601036}')as SXB0601036
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602006}', t2.oss::json#>>'{data,SXB0602006}')as SXB0602006
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601035}', t2.oss::json#>>'{data,SXB0601035}')as SXB0601035
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602007}', t2.oss::json#>>'{data,SXB0602007}')as SXB0602007
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601034}', t2.oss::json#>>'{data,SXB0601034}')as SXB0601034
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601033}', t2.oss::json#>>'{data,SXB0601033}')as SXB0601033
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602008}', t2.oss::json#>>'{data,SXB0602008}')as SXB0602008
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3704001}', t2.oss::json#>>'{data,SXX3704001}')as SXX3704001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601032}', t2.oss::json#>>'{data,SXB0601032}')as SXB0601032
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602009}', t2.oss::json#>>'{data,SXB0602009}')as SXB0602009
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1204001}', t2.oss::json#>>'{data,SXX1204001}')as SXX1204001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601031}', t2.oss::json#>>'{data,SXB0601031}')as SXB0601031
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601030}', t2.oss::json#>>'{data,SXB0601030}')as SXB0601030
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXC0501007}', t2.oss::json#>>'{data,SXC0501007}')as SXC0501007
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3104001}', t2.oss::json#>>'{data,SXX3104001}')as SXX3104001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXC0501006}', t2.oss::json#>>'{data,SXC0501006}')as SXC0501006
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXC0501005}', t2.oss::json#>>'{data,SXC0501005}')as SXC0501005
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXC0501004}', t2.oss::json#>>'{data,SXC0501004}')as SXC0501004
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXC0501008}', t2.oss::json#>>'{data,SXC0501008}')as SXC0501008
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3803001}', t2.oss::json#>>'{data,SXX3803001}')as SXX3803001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601080}', t2.oss::json#>>'{data,SXA1601080}')as SXA1601080
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601081}', t2.oss::json#>>'{data,SXA1601081}')as SXA1601081
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXC0501003}', t2.oss::json#>>'{data,SXC0501003}')as SXC0501003
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXC0501002}', t2.oss::json#>>'{data,SXC0501002}')as SXC0501002
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3203001}', t2.oss::json#>>'{data,SXX3203001}')as SXX3203001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXC0501001}', t2.oss::json#>>'{data,SXC0501001}')as SXC0501001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601086}', t2.oss::json#>>'{data,SXA1601086}')as SXA1601086
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601087}', t2.oss::json#>>'{data,SXA1601087}')as SXA1601087
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601088}', t2.oss::json#>>'{data,SXA1601088}')as SXA1601088
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX7003001}', t2.oss::json#>>'{data,SXX7003001}')as SXX7003001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601089}', t2.oss::json#>>'{data,SXA1601089}')as SXA1601089
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601082}', t2.oss::json#>>'{data,SXA1601082}')as SXA1601082
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602001}', t2.oss::json#>>'{data,SXB0602001}')as SXB0602001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601083}', t2.oss::json#>>'{data,SXA1601083}')as SXA1601083
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601039}', t2.oss::json#>>'{data,SXB0601039}')as SXB0601039
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602002}', t2.oss::json#>>'{data,SXB0602002}')as SXB0602002
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601038}', t2.oss::json#>>'{data,SXB0601038}')as SXB0601038
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602003}', t2.oss::json#>>'{data,SXB0602003}')as SXB0602003
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601084}', t2.oss::json#>>'{data,SXA1601084}')as SXA1601084
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601037}', t2.oss::json#>>'{data,SXB0601037}')as SXB0601037
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601085}', t2.oss::json#>>'{data,SXA1601085}')as SXA1601085
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0602004}', t2.oss::json#>>'{data,SXB0602004}')as SXB0602004
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3304001}', t2.oss::json#>>'{data,SXX3304001}')as SXX3304001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1604001}', t2.oss::json#>>'{data,SXX1604001}')as SXX1604001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0204001}', t2.oss::json#>>'{data,SXX0204001}')as SXX0204001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0903001}', t2.oss::json#>>'{data,SXX0903001}')as SXX0903001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601105}', t2.oss::json#>>'{data,SXA1601105}')as SXA1601105
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601106}', t2.oss::json#>>'{data,SXA1601106}')as SXA1601106
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6204001}', t2.oss::json#>>'{data,SXX6204001}')as SXX6204001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX7203001}', t2.oss::json#>>'{data,SXX7203001}')as SXX7203001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601099}', t2.oss::json#>>'{data,SXB0601099}')as SXB0601099
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601098}', t2.oss::json#>>'{data,SXB0601098}')as SXB0601098
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601101}', t2.oss::json#>>'{data,SXA1601101}')as SXA1601101
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601102}', t2.oss::json#>>'{data,SXA1601102}')as SXA1601102
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601097}', t2.oss::json#>>'{data,SXB0601097}')as SXB0601097
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601103}', t2.oss::json#>>'{data,SXA1601103}')as SXA1601103
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601096}', t2.oss::json#>>'{data,SXB0601096}')as SXB0601096
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601095}', t2.oss::json#>>'{data,SXB0601095}')as SXB0601095
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601104}', t2.oss::json#>>'{data,SXA1601104}')as SXA1601104
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601094}', t2.oss::json#>>'{data,SXB0601094}')as SXB0601094
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601093}', t2.oss::json#>>'{data,SXB0601093}')as SXB0601093
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601092}', t2.oss::json#>>'{data,SXB0601092}')as SXB0601092
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601091}', t2.oss::json#>>'{data,SXB0601091}')as SXB0601091
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601090}', t2.oss::json#>>'{data,SXB0601090}')as SXB0601090
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX7604001}', t2.oss::json#>>'{data,SXX7604001}')as SXX7604001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2804001}', t2.oss::json#>>'{data,SXX2804001}')as SXX2804001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601100}', t2.oss::json#>>'{data,SXA1601100}')as SXA1601100
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601089}', t2.oss::json#>>'{data,SXB0601089}')as SXB0601089
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601088}', t2.oss::json#>>'{data,SXB0601088}')as SXB0601088
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601087}', t2.oss::json#>>'{data,SXB0601087}')as SXB0601087
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601086}', t2.oss::json#>>'{data,SXB0601086}')as SXB0601086
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601085}', t2.oss::json#>>'{data,SXB0601085}')as SXB0601085
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601084}', t2.oss::json#>>'{data,SXB0601084}')as SXB0601084
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601083}', t2.oss::json#>>'{data,SXB0601083}')as SXB0601083
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601082}', t2.oss::json#>>'{data,SXB0601082}')as SXB0601082
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601081}', t2.oss::json#>>'{data,SXB0601081}')as SXB0601081
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601080}', t2.oss::json#>>'{data,SXB0601080}')as SXB0601080
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3504001}', t2.oss::json#>>'{data,SXX3504001}')as SXX3504001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX7404001}', t2.oss::json#>>'{data,SXX7404001}')as SXX7404001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0404001}', t2.oss::json#>>'{data,SXX0404001}')as SXX0404001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1403001}', t2.oss::json#>>'{data,SXX1403001}')as SXX1403001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5903001}', t2.oss::json#>>'{data,SXX5903001}')as SXX5903001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2604001}', t2.oss::json#>>'{data,SXX2604001}')as SXX2604001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2304001}', t2.oss::json#>>'{data,SXX2304001}')as SXX2304001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601079}', t2.oss::json#>>'{data,SXB0601079}')as SXB0601079
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601078}', t2.oss::json#>>'{data,SXB0601078}')as SXB0601078
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601077}', t2.oss::json#>>'{data,SXB0601077}')as SXB0601077
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0503001}', t2.oss::json#>>'{data,SXX0503001}')as SXX0503001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601076}', t2.oss::json#>>'{data,SXB0601076}')as SXB0601076
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601075}', t2.oss::json#>>'{data,SXB0601075}')as SXB0601075
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601074}', t2.oss::json#>>'{data,SXB0601074}')as SXB0601074
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601073}', t2.oss::json#>>'{data,SXB0601073}')as SXB0601073
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601072}', t2.oss::json#>>'{data,SXB0601072}')as SXB0601072
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601071}', t2.oss::json#>>'{data,SXB0601071}')as SXB0601071
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601070}', t2.oss::json#>>'{data,SXB0601070}')as SXB0601070
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX650400}', t2.oss::json#>>'{data,SXX650400}')as SXX650400
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2403001}', t2.oss::json#>>'{data,SXX2403001}')as SXX2403001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4504001}', t2.oss::json#>>'{data,SXX4504001}')as SXX4504001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6404001}', t2.oss::json#>>'{data,SXX6404001}')as SXX6404001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601039}', t2.oss::json#>>'{data,SXA1601039}')as SXA1601039
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601101}', t2.oss::json#>>'{data,SXB0601101}')as SXB0601101
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXB0601100}', t2.oss::json#>>'{data,SXB0601100}')as SXB0601100
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601035}', t2.oss::json#>>'{data,SXA1601035}')as SXA1601035
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX2003001}', t2.oss::json#>>'{data,SXX2003001}')as SXX2003001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1602003}', t2.oss::json#>>'{data,SXA1602003}')as SXA1602003
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601036}', t2.oss::json#>>'{data,SXA1601036}')as SXA1601036
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601037}', t2.oss::json#>>'{data,SXA1601037}')as SXA1601037
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601038}', t2.oss::json#>>'{data,SXA1601038}')as SXA1601038
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3904001}', t2.oss::json#>>'{data,SXX3904001}')as SXX3904001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1004001}', t2.oss::json#>>'{data,SXX1004001}')as SXX1004001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3403001}', t2.oss::json#>>'{data,SXX3403001}')as SXX3403001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601031}', t2.oss::json#>>'{data,SXA1601031}')as SXA1601031
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6904001}', t2.oss::json#>>'{data,SXX6904001}')as SXX6904001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601032}', t2.oss::json#>>'{data,SXA1601032}')as SXA1601032
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601033}', t2.oss::json#>>'{data,SXA1601033}')as SXA1601033
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1602002}', t2.oss::json#>>'{data,SXA1602002}')as SXA1602002
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1602001}', t2.oss::json#>>'{data,SXA1602001}')as SXA1602001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601034}', t2.oss::json#>>'{data,SXA1601034}')as SXA1601034
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601030}', t2.oss::json#>>'{data,SXA1601030}')as SXA1601030
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601028}', t2.oss::json#>>'{data,SXA1601028}')as SXA1601028
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601029}', t2.oss::json#>>'{data,SXA1601029}')as SXA1601029
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601024}', t2.oss::json#>>'{data,SXA1601024}')as SXA1601024
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX3603001}', t2.oss::json#>>'{data,SXX3603001}')as SXX3603001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601025}', t2.oss::json#>>'{data,SXA1601025}')as SXA1601025
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601026}', t2.oss::json#>>'{data,SXA1601026}')as SXA1601026
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601027}', t2.oss::json#>>'{data,SXA1601027}')as SXA1601027
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX7304001}', t2.oss::json#>>'{data,SXX7304001}')as SXX7304001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX7104001}', t2.oss::json#>>'{data,SXX7104001}')as SXX7104001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX6303001}', t2.oss::json#>>'{data,SXX6303001}')as SXX6303001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601020}', t2.oss::json#>>'{data,SXA1601020}')as SXA1601020
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0103001}', t2.oss::json#>>'{data,SXX0103001}')as SXX0103001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601021}', t2.oss::json#>>'{data,SXA1601021}')as SXA1601021
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601022}', t2.oss::json#>>'{data,SXA1601022}')as SXA1601022
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601023}', t2.oss::json#>>'{data,SXA1601023}')as SXA1601023
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0604001}', t2.oss::json#>>'{data,SXX0604001}')as SXX0604001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601017}', t2.oss::json#>>'{data,SXA1601017}')as SXA1601017
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601018}', t2.oss::json#>>'{data,SXA1601018}')as SXA1601018
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601019}', t2.oss::json#>>'{data,SXA1601019}')as SXA1601019
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5403001}', t2.oss::json#>>'{data,SXX5403001}')as SXX5403001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601013}', t2.oss::json#>>'{data,SXA1601013}')as SXA1601013
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601014}', t2.oss::json#>>'{data,SXA1601014}')as SXA1601014
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601015}', t2.oss::json#>>'{data,SXA1601015}')as SXA1601015
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601016}', t2.oss::json#>>'{data,SXA1601016}')as SXA1601016
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX0703001}', t2.oss::json#>>'{data,SXX0703001}')as SXX0703001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4904001}', t2.oss::json#>>'{data,SXX4904001}')as SXX4904001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601010}', t2.oss::json#>>'{data,SXA1601010}')as SXA1601010
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601011}', t2.oss::json#>>'{data,SXA1601011}')as SXA1601011
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601012}', t2.oss::json#>>'{data,SXA1601012}')as SXA1601012
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX4304001}', t2.oss::json#>>'{data,SXX4304001}')as SXX4304001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601006}', t2.oss::json#>>'{data,SXA1601006}')as SXA1601006
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601007}', t2.oss::json#>>'{data,SXA1601007}')as SXA1601007
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601008}', t2.oss::json#>>'{data,SXA1601008}')as SXA1601008
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601009}', t2.oss::json#>>'{data,SXA1601009}')as SXA1601009
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601002}', t2.oss::json#>>'{data,SXA1601002}')as SXA1601002
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601003}', t2.oss::json#>>'{data,SXA1601003}')as SXA1601003
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601004}', t2.oss::json#>>'{data,SXA1601004}')as SXA1601004
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601005}', t2.oss::json#>>'{data,SXA1601005}')as SXA1601005
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX1504001}', t2.oss::json#>>'{data,SXX1504001}')as SXX1504001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXX5304001}', t2.oss::json#>>'{data,SXX5304001}')as SXX5304001
,coalesce(cast(t2.oss ::json ->> 'result' as json) ::json #>>'{data,SXA1601001}', t2.oss::json#>>'{data,SXA1601001}')as SXA1601001
from t_loan_performance t0 
left join dc_guoyuxiaoescore_application t1 on t0.order_no = t1.order_no and t1.oss<>''
left join dc_guoyuscore_application t2 on t0.order_no = t2.order_no and t2.oss<>''
--left join bk_guoyuscore_application t3 on t0.order_no = t1.order_no and t1.oss<>''
--left join bk_guoyuxiaoescore_application t4 on t0.order_no = t2.order_no and t2.oss<>''
where t0.dt='20191020' and t0.business_id in ('tb', 'rong360') and t0.effective_date between '2019-09-23' and '2019-09-30' 
'''
conn.rollback()
cur = conn.cursor()
cur.execute(sql_gy)
raw = pd.DataFrame(cur.fetchall(),  columns = [elt[0] for elt in cur.description] )
r_gy = raw.copy()  
print(r_gy.shape)
print(r_gy.order_no.nunique())

r_gy.to_excel('D:/Model Development/201910 MVP/01 Data/raw data/r_gy.xlsx')

r_gy = pd.read_excel('D:/Model Development/201910 MVP/01 Data/raw data/r_gy.xlsx')

r_gy_var = r_gy.drop(['Unnamed: 0', 'gy_xiaoescore'], axis=1)
r_gy_var['sample_set_gy'] = 'online'
r_gy_var.order_no = r_gy_var.order_no.astype(str)

''' 回溯数据 '''
r_gy_huisu = pd.read_excel('D:/Model Development/201910 MVP/01 Data/副本国誉-10.22回溯_整理后.xlsx', sheet_name='分期小额评分')
r_gy_huisu_f = r_gy_huisu.drop(['effective_date',	'business_id',	'created_time', 'cell_phone', 'id_card_no',	'name'], axis=1)
r_gy_huisu_f.columns = map(str.lower, r_gy_huisu_f.columns)
r_gy_huisu_f['sample_set_gy'] = 'huisu'
print(r_gy_huisu_f.shape)
print(r_gy_huisu_f.order_no.nunique())

'''合并线上回溯 '''
r_gy_all = r_gy_var.append(r_gy_huisu_f, ignore_index = True)
print(r_gy_all.shape)
print(r_gy_all.order_no.nunique())

'''Dedup '''
r_gy_all.sort_values("sample_set_gy", inplace=True) 
r_gy_f = r_gy_all.drop_duplicates(subset = 'order_no', keep = 'first', inplace= False)

r_gy_f.groupby('sample_set_gy').size()
r_gy_all.groupby('sample_set_gy').size()


''' To float '''
var_to_float = list(r_gy_f.iloc[:,2:].columns)
for x in var_to_float:
    r_gy_f[x] = r_gy_f[x].astype(float)
print(r_gy_f.dtypes)

''' 删除常数值变量 '''
var = list(r_gy_f.iloc[:,2:].columns)
for x in var:
    if r_gy_f[x].std()<=.001:
        print(r_gy_f[x].std(), x) 

r_gy_f =  r_gy_f.drop(['sxx0103001', 'sxx1103001', 'sxx2703001', 'sxx4003001', 'sxx4804001'], axis=1)

'''检查分布 '''
var = list(r_gy_f.iloc[:,2:].columns)
test = (r_gy_f.groupby('sample_set_gy')[var].agg(['mean','min', 'median', 'max'])).T
test2 = (r_gy_f.groupby('sample_set_gy')[var].quantile([0, 0.25, 0.5, 0.75, 0.95, 1])).T

''' 检查缺失比例'''
r_gy_f = r_base_f[(r_base_f['cp_sampleset']!='train') & (r_base_f['cp_sampleset']!='test')].merge(r_gy_f, how='left', on='order_no')

r_gy_f = r_gy_f.fillna(-1)
r_gy_f = r_gy_f.replace([-9995, -9996, -9997, -9998, -9999, -99998, -99999, -999],[-1,  -1, -1, -1, -1, -1, -1, -1])

sum(r_gy_f[r_gy_f['sample_set_gy']=='online'].score==-1)/len(r_gy_f[r_gy_f['sample_set_gy']=='online'])  #0.12284069097888675

sum(r_gy_f[r_gy_f['sample_set_gy']!='online'].score==-1)/len(r_gy_f[r_gy_f['sample_set_gy']!='online'])  #0.009522435435002164

        

'''保存最终数据 '''
r_gy_f.to_excel('D:/Model Development/201910 MVP/01 Data/raw data/r_gy_f.xlsx')


