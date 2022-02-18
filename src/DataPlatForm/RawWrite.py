#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:39:14 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.WriteUtils as wutils

from DataPlatForm.BasicUtils import *
import DataPlatForm.AccessControl as control
from clickhouse_driver import Client
import os

import grpc
import DataPlatForm.service_pb2 as service_pb2
import DataPlatForm.service_pb2_grpc as service_pb2_grpc

#------------------------------------------------------------------------------
# 二维frame数据写入接口
#------------------------------------------------------------------------------
def write_frame(data, name, date):
    wutils.write_raw_data(data, raw_frame_db, name, date)
    
#------------------------------------------------------------------------------
# frame数据删除接口
#------------------------------------------------------------------------------
def delete_frame(name, date):
    wutils.delete_raw_data(raw_frame_db, name, date)

#------------------------------------------------------------------------------
# 列表数据写入接口
# data: list
# name: 列表名称, raw中所有列表数据存放在一个统一的数据库中，一个name是一张表
#------------------------------------------------------------------------------
def write_list(data, name):
    client = control.get_session_client()

    #如果list数据库还不存在，创建list数据库
    create_database_query = "CREATE DATABASE IF NOT EXISTS " + raw_list_db
    client.execute(create_database_query)

    #user insert empty data, exist
    if len(data) == 0:
        return

    #如果用户传入的list是一个异质列表（其中有不同类型的元素），报错
    if not is_homogeneous_list(data):
        raise ValueError("Not homogeneous list write(Different data types in list), check your list")
    
    #如果name数据表还不存在，创建之
    column_name = 'value'
    if isinstance(data[0], str):
        column_type = 'String'
    elif isinstance(data[0], int):
        column_type = 'Int64'
    elif isinstance(data[0], float):
        column_type = 'Float64'
    else:
        raise ValueError("data type not String, Int Or Float!")
    
    create_table_query = "CREATE TABLE IF NOT EXISTS " + raw_list_db + "." + name
    create_table_query += " ("
    create_table_query += column_name + " " + column_type
    create_table_query += ") "
    create_table_query += " ENGINE = MergeTree() order by " + column_name
    client.execute(create_table_query)

    #将数据插入到数据表中
    insert_query = "insert into " + raw_list_db + "." + name + " "
    insert_query += "(" + column_name+ ") values "
    data_str = ""
    for x in data:
        data_str += "("
        if type(x) == str:
            data_str += "\'" + x + "\'"
        else:
            data_str += str(x)
        data_str += ")" + ","
    data_str = data_str[0:-1]
    insert_query += data_str
    
    client.execute(insert_query)

#------------------------------------------------------------------------------
# 列表数据删除接口
# name: 列表名称, raw中所有列表数据存放在一个统一的数据库中，一个name是一张表
#------------------------------------------------------------------------------
def delete_list(name):
    client = control.get_session_client()
    
    if not is_table_exist(client, raw_list_db, name):
        raise ValueError("%s not exist in database[%s]" % (name, raw_list_db))

    delete_query = "drop table " + raw_list_db + "." + name
    client.execute(delete_query)