#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:40:09 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.ReadUtils as rutils
import DataPlatForm.service_pb2 as pb2

#------------------------------------------------------------------------------
# feature 数据读取
#------------------------------------------------------------------------------
def read_feature(name, start_date=None, end_date=None, fields=[]):
    return rutils.read_raw_data(raw_feature_db, name, start_date, end_date, fields)
