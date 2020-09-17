#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : test2.py
@Time    : 2020-09-17 21:06
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""


from openpyxl import load_workbook


def search_used_name(root, used_name, lines):
    child_counter = 0
    for line in lines:
        if line[35].value == root:  # 如果property2 和 root相同，表示这行记录对应的id为 root的曾用id
            child_counter += 1
            used_name.append(line[4].value)
            used_name = search_used_name(line[4].value, used_name, lines)
    if child_counter == 0:
        return used_name
    return used_name


wb = load_workbook(r'/Users/yantingting/Desktop/text.xlsx')
ws = wb['Sheet1']

for line in ws.rows:
    # id
    row_child = line[4].value
    # property2
    row_parent = line[35].value
    if not row_parent:  # 如果property2是空，表示当前id为最新id
        used_name = search_used_name(row_child, [], ws.rows)  #递归搜索当前id的曾用名，返回当前id的所有曾用id
        if used_name:  # 如果当前id有曾用id，则打印
            print(row_child, used_name)



