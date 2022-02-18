#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:38:41 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.ReadUtils as rutils

#------------------------------------------------------------------------------
# 读取高级factor数据
#------------------------------------------------------------------------------
def read_alphafactor(name, start_date="", end_date="", symbols=[]):
    return rutils.read_factor_data(alpha_db, name, start_date, end_date, symbols)

#------------------------------------------------------------------------------
# 返回alpha因子数据表的日期索引
#------------------------------------------------------------------------------
def get_alphafactor_dates(name):
    return rutils.get_factor_indexes(alpha_db, name)

#------------------------------------------------------------------------------
# 返回alphafactor表中的所有列名（股票列表）
#------------------------------------------------------------------------------
def get_alphafactor_symbols(name):
    rutils.get_factor_columns(alpha_db, name)

