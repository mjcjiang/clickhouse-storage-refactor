#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:38:51 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.WriteUtils as wutils
import DataPlatForm.service_pb2 as pb2

#------------------------------------------------------------------------------
# 写入factor数据到clickhouse数据库中
#------------------------------------------------------------------------------
def write_factor(data, name):
    wutils.write_factor_data(data, factor_db, name)
    
#------------------------------------------------------------------------------
# 删除factor表中指定日期之间的数据
#------------------------------------------------------------------------------
def delete_factor(name, start_date, end_date):
    wutils.delete_factor_data(factor_db, name, start_date, end_date)


