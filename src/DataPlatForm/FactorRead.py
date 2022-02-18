#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:38:41 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.ReadUtils as rutils
import DataPlatForm.service_pb2 as pb2

#------------------------------------------------------------------------------
# 读取factor数据
#------------------------------------------------------------------------------
def read_factor(name, start_date="", end_date="", symbols=[]):
    return rutils.read_factor_data(factor_db, name, start_date, end_date, symbols)

#------------------------------------------------------------------------------
# 返回因子数据表的日期索引列
#------------------------------------------------------------------------------
def get_factor_dates(name):
    return rutils.get_factor_indexes(factor_db, name)

#------------------------------------------------------------------------------
# 返回factor表中的所有列名（股票列表）
#------------------------------------------------------------------------------
def get_factor_symbols(name):
    return rutils.get_factor_columns(factor_db, name)
