#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:38:41 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.ReadUtils as rutils
import DataPlatForm.service_pb2 as pb2

#------------------------------------------------------------------------------
# 读取label数据
#------------------------------------------------------------------------------
def read_label(name, start_date="", end_date="", symbols=[]):
    return rutils.read_factor_data(label_db, name, start_date, end_date, symbols)

#------------------------------------------------------------------------------
# 返回因子数据表的日期索引列
#------------------------------------------------------------------------------
def get_label_dates(name):
    return rutils.get_factor_indexes(label_db, name)

#------------------------------------------------------------------------------
# 返回label表中的所有列名（股票列表）
#------------------------------------------------------------------------------
def get_label_symbols(name):
    return rutils.get_factor_columns(label_db, name)

