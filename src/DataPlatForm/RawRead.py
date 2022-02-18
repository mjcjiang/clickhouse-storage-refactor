#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:40:09 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.ReadUtils as rutils
from DataPlatForm.BasicUtils import *
from clickhouse_driver import Client
import DataPlatForm.AccessControl as control

import grpc
import DataPlatForm.service_pb2 as service_pb2
import DataPlatForm.service_pb2_grpc as service_pb2_grpc

#------------------------------------------------------------------------------
# frame数据读取接口
#------------------------------------------------------------------------------
def read_frame(name, start_date=None, end_date=None, fields=[]):
    return rutils.read_raw_data(raw_frame_db, name, start_date, end_date, fields)

#------------------------------------------------------------------------------
# 读取列表内容
# name: 列表名
#------------------------------------------------------------------------------
def read_list(name):
    client = control.get_session_client()
    
    if not is_table_exist(client, raw_list_db, name):
        raise ValueError("%s not in %s database!" % (name, raw_list_db))

    select_query = "select * from " + raw_list_db + "." + name
    res = client.execute(select_query)

    return [x[0] for x in res]
