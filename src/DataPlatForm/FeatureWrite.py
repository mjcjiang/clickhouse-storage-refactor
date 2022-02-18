#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:39:14 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.WriteUtils as wutils
import DataPlatForm.service_pb2 as pb2

#------------------------------------------------------------------------------
# 写入指定日期的feature数据
#------------------------------------------------------------------------------
def write_feature(data, name, date):
    wutils.write_raw_data(data, raw_feature_db, name, date)

#------------------------------------------------------------------------------
# 删除指定日期的feature数据
#------------------------------------------------------------------------------
def delete_feature(name, date):
    wutils.delete_raw_data(raw_feature_db, name, date)
