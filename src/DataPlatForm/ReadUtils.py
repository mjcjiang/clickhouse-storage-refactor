#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:40:09 2021
#-------------------------------------------------------------------------------
from DataPlatForm.BasicUtils import *
from DataPlatForm.configs import *
from clickhouse_driver import Client
import DataPlatForm.AccessControl as control

import pandas as pd
import os
from multiprocessing import Pool

#------------------------------------------------------------------------------
# 查询单个表格中的数据，返回一个dataframe
#------------------------------------------------------------------------------
def __read_single_table(database, table, fields):
    client = control.get_session_client()

    all_valid_fileds = get_2d_table_columns_list(client, database, table)
    #用户输入的属性列表为空，那么直接获取所有属性
    if len(fields) == 0:
        fields = all_valid_fileds
    else:
        diff_fields = set(fields) - set(all_valid_fields)
        if len(diff_fields) > 0:
            raise ValueError("[%s] not valid fields in table. check it out" % list(diff_fields))

    #去重
    fields = list(set(fields))
    
    #构建查询SQL语句
    select_query = "select indexer,"
    for field in fields:
        select_query += field + ","
    select_query = select_query[0:-1]
    select_query += " from " + database + "." + table
    
    res = client.execute(select_query)
    if len(res) > 0:
        indexes = [x[0] for x in res]
        data = [x[1:] for x in res]
        return pd.DataFrame(data, index=indexes, columns=fields)
    else:
        return pd.DataFrame()

#------------------------------------------------------------------------------
# 获取raw数据库中一张表的所有日期列表
#------------------------------------------------------------------------------
def __get_table_date_list(client, database, name):
    show_tables_query = "show tables in " + database
    all_tables = [x[0] for x in client.execute(show_tables_query)]
    dates = []
    for table in all_tables:
        if table.startswith(name) and len(table) == (len(name) + len('_2005_01_01')):
            dates.append(table[len(name)+1:])
    return [x.replace('_', '-') for x in dates]

#------------------------------------------------------------------------------
# 二维数据读取通用内部接口
# name: 数据名
# start_date: 开始日期
# end_date: 结束日期
# columns: 读取表中哪几列
#------------------------------------------------------------------------------
def read_raw_data(database, name, start_date, end_date, fields=[]):
    client = control.get_session_client()

    valid_dates = __get_table_date_list(client, database, name)
    if len(valid_dates) == 0:
        print("[WARN]: no [%s_YYYY_MM_DD] in %s database" % (name, database))
        return

    ret_dict = True
    
    #用户没有填写开始和截至日期, 默认返回最后一天数据【dataframe】
    if start_date == None and end_date == None:
        start_date = valid_dates[-1]
        end_date = valid_dates[-1]
        ret_dict = False
    
    #用户只填写结束日期[dataframe]
    if start_date is None and end_date is not None:
        start_date = end_date
        ret_dict = False

    #用户只填写开始日期[dataframe]
    if start_date is not None and end_date is None:
        end_date = start_date
        ret_dict = False
        
    #校验日期
    check_date_range(start_date, end_date)
    date_lst = gen_date_list(start_date, end_date)

    valid_dates = []
    for date in date_lst:
        table_name = name + "_" + trans_date_string(date)
        if is_table_exist(client, database, table_name):
            valid_dates.append(date)
            
    frame_dict = {}
    with Pool(processes=(os.cpu_count() // parallel_level)) as p:
        res_dict = {}
        for date in valid_dates:
            table = name + "_" + trans_date_string(date)
            res = p.apply_async(__read_single_table, (database, table, fields))
            res_dict[date] = res

        for date, res in res_dict.items():
            frame_dict[date] = res.get()

    if ret_dict:
        return frame_dict
    else:
        return frame_dict[start_date]

#------------------------------------------------------------------------------
# 根据股票列表和日期范围生成dataframe
#------------------------------------------------------------------------------
def __get_sub_dataframe(name, database, symbol_list, start_date, end_date):
    client = control.get_session_client()

    #000001.SH转化成000001SH
    new_slist = [('S' + x.replace(".", "") + ',') for x in symbol_list]

    symbols_str = ""
    for s in new_slist:
        symbols_str += s
    symbols_str = symbols_str[:-1]

    table_name = gen_real_table_name(name)
    select_query = "select timestamp," + symbols_str + " from " + database + "." + table_name
    select_query +=  " where timestamp <= " + "\'" + end_date + "\'"
    select_query += " and timestamp >= " + "\'" + start_date + "\'"
    
    res = client.execute(select_query)

    #transform selected result into small dataframe 
    date_list = [x[0] for x in res]
    data = [x[1:] for x in res]
    dataframe = pd.DataFrame(data, index=date_list, columns=symbol_list)
    
    return dataframe.transpose()

#------------------------------------------------------------------------------
# 并发生成dataframe
#------------------------------------------------------------------------------
def read_factor_data(database, name, start_date = "", end_date = "", symbols = []):
    #用户输入的列表为空，默认返回所有的股票数据
    client = control.get_session_client()

    #查看因子表是否存在
    if not is_table_exist(client, database, name):
        raise ValueError("%s table not exist in database(%s)" % (name, database))

    #如果开始日期为空，默认用表中最早的时间
    if start_date == "":
        start_date = get_first_row_timestamp(client, database, name)

    #如果结束日期为空，默认用表中最迟的时间
    if end_date == "":
        end_date = get_last_row_timestamp(client, database, name)
    
    #检查日期有效性性
    check_date_range(start_date, end_date)
    if len(symbols) == 0:
        symbols = get_factor_columns(database, name)

    #进程池中得进程数量
    pool_size = os.cpu_count() // parallel_level
    #并发单位（单个进程处理得列数）
    parallel_size = len(symbols) // pool_size + 1
        
    with Pool(processes=pool_size) as p:
        block_num = len(symbols) // parallel_size
        remain_num = len(symbols) % parallel_size

        #处理“整块”数据
        res_list = []
        for i in range(block_num):
            res = p.apply_async(__get_sub_dataframe, (name, database, symbols[i * parallel_size: (i+1) * parallel_size], start_date, end_date))
            res_list.append(res)

        #处理“剩余”数据
        if remain_num > 0:
            res = p.apply_async(__get_sub_dataframe, (name, database, symbols[block_num * parallel_size:], start_date, end_date))
            res_list.append(res)

        #合成最终的dataframe
        frame_list = []
        for res in res_list:
            frame_list.append(res.get())

        if len(frame_list) > 0:
            res = pd.concat(frame_list, axis=0)
            cols = list(res.columns.values)
            cols.sort()
            return res[cols]
        else:
            return pd.DataFrame()

#------------------------------------------------------------------------------
# 读取indexes数据
#------------------------------------------------------------------------------
def get_factor_indexes(database, name):
    client = control.get_session_client()
    table_name = gen_real_table_name(name)
    select_query = "select timestamp from " + database + "." + table_name
    res = client.execute(select_query)
    return [x[0] for x in res]

#------------------------------------------------------------------------------
# 返回表中的所有列名（股票列表）
#------------------------------------------------------------------------------
def get_factor_columns(database, name):
    client = control.get_session_client()
    table_name = gen_real_table_name(name)

    desc_query = "desc " + database + "." + table_name
    res = client.execute(desc_query)
    tmp = [x[0][1:] for x in res[1:]]

    #000001SZ转成000001.SZ
    res = []
    for x in tmp:
        m = re.match("([0-9]+)([a-zA-Z]+)", x)
        if m:
            res.append(m.group(1) + "." + m.group(2))
        else:
            res.append(x)
    return res