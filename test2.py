#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : test2.py
@Time    : 2020-09-17 21:06
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import sys
line1 = input()
line2 = sys.stdin.readline()

print(len(line1),len(line2))
print(line1, line2)
[i for i in line2]
[i for i in line1]

for line in  sys.stdin:
    i = line.split('\t')
    print(i)





import pandas as pd

# my_dict = {'b':'c','a':'b',  'c':'d', 'd':'e', 's':'t', 't':'m'}

temp = pd.read_excel(r'/Users/yantingting/Desktop/text.xlsx')
temp = temp[~temp['property2'].isna()]
temp['property2'] = temp['property2'].astype(str)
temp['id'] = temp['id'].astype(str)
my_dict = dict(zip(temp['id'], temp['property2']))
my_dict
def convert_dict(my_dict):
    all_keys = my_dict.keys()
    all_keys_1 = [value for value in all_keys]
    all_values = my_dict.values()
    all_values_1 = [value for value in all_values]
    my_dict_1 = {}
    mylist_temp = []
    for k,v in my_dict.items():
        if k in all_values_1:
            continue
        else:
            temp1 = v
            while v in all_keys_1:
                temp1 = ','.join([temp1, my_dict.get(v)])
                v = my_dict.get(v)
            my_dict_1[k] = temp1


    return my_dict_1, mylist_temp


dict1, mylist_temp = convert_dict(my_dict)
# print(mylist_temp)
# dict1
# pd.DataFrame([dict1]).T

df = pd.DataFrame.from_dict(dict1, orient='index', columns = ['his_name_list'])
df = df.reset_index().rename(columns = {'index':'name'})






def search_used_name(root, used_name, lines):
    child_counter = 0
    for line in lines:
        if root == line[1]:
            child_counter += 1
            used_name.append(line[0])
            used_name = search_used_name(line[0], used_name, lines)

            if child_counter == 0:
                return used_name

    return used_name

lines = my_dict
for line in lines:
    if not line[1]:
        used_name = search_used_name(line[0], [], lines)
        print(line[0],used_name)