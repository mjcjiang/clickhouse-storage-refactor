#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:38:51 2021
#-------------------------------------------------------------------------------
from DataPlatForm.configs import *
import DataPlatForm.WriteUtils as wutils

#------------------------------------------------------------------------------
# 写入高阶因子数据
#------------------------------------------------------------------------------
def write_alphafactor(data, name):
    wutils.write_factor_data(data, alpha_db, name)

#------------------------------------------------------------------------------
# 删除alphafactor表中指定日期之间的数据
#------------------------------------------------------------------------------
def delete_alphafactor(name, start_date, end_date):
    wutils.delete_factor_data(alpha_db, name, start_date, end_date)
