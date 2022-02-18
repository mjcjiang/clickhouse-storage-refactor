# DataPlatForm
DataPlatForm is a thin client which can read(write) clickhouse data in a blazing fast speed! It transforms column strored data
to pandas.DataFrame seamlessly, make data analyst's life easier!

## 1. 软件包的构建和安装:
### 1.1 构建:
安装python包构建工具:
``` bash
python -m pip install --upgrade build
```

在pyproject.toml同目录下，运行下面命令:
``` bash
python -m build
```
构建成功后，在dist目录下会生成两个包文件；用户可以选择其中任意一个进行安装。

### 1.2 安装:
将安装包拷贝到目标机器，运行下面命令安装：
``` bash
pip install --force-reinstall rdsp-client-ck-0.0.3.tar.gz -i https://pypi.tuna.tsinghua.edu.cn/simple
```
安装过程中，会自动获取关联包。

## 2.使用样例:
### 2.1 用户登陆
``` python
import DataPlatForm.AccessControl as control

control.set_user_and_password("guest", "Life123")
```
### 2.1 alpha 因子数据的写入、读取和删除:
``` python
import DataPlatForm.AlphaWrite as awrite
import DataPlatForm.AlphaRead as aread
import DataPlatForm.BasicUtils as butils

from clickhouse_driver import Client
import time

if __name__ == "__main__":
    # generate a simulative factor data frame, size: 4000 * 4000
    symbols = butils.gen_symbol_list(4000)
    frame = butils.gen_factor_dataframe(symbols, "2005-01-01", 4000)

    # write frame into clickhouse database
    alpha_name = "test_alpha"
    awrite.write_alphafactor(frame, alpha_name)

    # read full frame from clickhouse database
    start_time = time.time()
    print(aread.read_alphafactor(alpha_name))
    print("full read cost: ", time.time() - start_time)
    print("\n")

    # read frame after a specific date
    print(aread.read_alphafactor(alpha_name, "2015-01-10"))
    print("\n")

    # read frame in a specific date range
    start_time = time.time()
    print(aread.read_alphafactor(alpha_name, "2005-01-10", "2012-02-10"))
    print("sub read cost: ", time.time() - start_time)
    print("\n")

    # delete frame data in a specific date range and read again
    awrite.delete_alphafactor(alpha_name, "2015-01-02", "2016-02-10")
    
    time.sleep(5)
    print(aread.read_alphafactor(alpha_name))
    print("\n")
```

写入和读取出的alpha因子frame样例如下:  
```
            2005-01-01  2005-01-02  2005-01-03  2005-01-04  2005-01-05  2005-01-06  2005-01-07  2005-01-08  ...  2015-12-07  2015-12-08  2015-12-09  2015-12-10  2015-12-11  2015-12-12  2015-12-13  2015-12-14  
000001.SZ    0.535220    0.205988    0.335961    0.834852    0.112003    0.919941    0.875636    0.887549  ...    0.649830    0.696530    0.045682    0.880277    0.075432    0.631742    0.959705    0.220630  
000002.SZ    0.938244    0.530874    0.079325    0.356512    0.042381    0.375716    0.395381    0.961783  ...    0.092060    0.831129    0.019920    0.660466    0.437825    0.109623    0.848383    0.227238  
000003.SZ    0.206504    0.839733    0.780990    0.851193    0.899428    0.616933    0.093183    0.913690  ...    0.641113    0.610197    0.794826    0.661464    0.124016    0.714615    0.267652    0.434545  
000004.SZ    0.422335    0.517828    0.312185    0.488226    0.757833    0.623301    0.546232    0.456417  ...    0.200252    0.327144    0.343255    0.152591    0.163533    0.110011    0.995652    0.588894  
000005.SZ    0.583304    0.056870    0.904808    0.264128    0.943056    0.562986    0.274213    0.032516  ...    0.330358    0.747506    0.078752    0.940141    0.184873    0.456735    0.478904    0.760676  
...               ...         ...         ...         ...         ...         ...         ...         ...  ...         ...         ...         ...         ...         ...         ...         ...         ...  
003996.SZ    0.421185    0.952605    0.587954    0.996448    0.210763    0.530705    0.250641    0.761918  ...    0.845469    0.575943    0.762343    0.186279    0.183688    0.906844    0.513097    0.943115  
003997.SZ    0.136105    0.977461    0.597265    0.688368    0.883986    0.067450    0.587556    0.062752  ...    0.656445    0.071368    0.171395    0.701333    0.039717    0.078252    0.524880    0.407103  
003998.SZ    0.474554    0.394323    0.700195    0.109733    0.750469    0.557397    0.255609    0.815489  ...    0.944505    0.188605    0.995399    0.905324    0.042364    0.232546    0.327405    0.355310  
003999.SZ    0.757346    0.819448    0.020039    0.324620    0.130320    0.438880    0.686235    0.435685  ...    0.976843    0.318599    0.505389    0.137305    0.628306    0.371370    0.970112    0.285573  
004000.SZ    0.407143    0.099597    0.083911    0.302204    0.786280    0.511379    0.667941    0.846188  ...    0.759713    0.689910    0.470835    0.723051    0.508540    0.332280    0.428221    0.640774  
```
index是股票列表，columns是日期列表

## 3. API使用说明
### 3.1 alpha因子相关接口:
#### alpha因子写入
AlphaWrite.write_alphafactor(data, name)
> data: 因子矩阵
> name: 因子名

#### alpha因子删除
AlphaWrite.delete_alphafactor(name, start_date, end_date)
> name: 因子名
> start_date: 开始日期
> end_date: 结束日期

#### alpha因子读取
AlphaRead.read_alphafactor(name, start_date="", end_date="", symbols=[])
> name: 因子名
> start_date: 开始日期
> end_date: 结束日期
> symbols: 股票列[默认为空时,读取所有股票的因子数据]

#### alpha因子日期索引获取
AlphaRead.get_alphafactor_dates(name)
> name: 因子名

#### alpha因子股票列表获取
AlphaRead.get_alphafactor_symbols(name)
> name: 因子名

### 3.2 普通因子相关接口
#### 写入factor数据到clickhouse数据库中
FactorWrite.def write_factor(data, name)
> data: 因子矩阵
> name: 因子名称
    
#### 删除factor表中指定日期之间的数据
FactorWrite.delete_factor(name, start_date, end_date)
> data: 因子矩阵
> start_date: 开始日期
> end_date: 结束日期

#### 读取factor数据
FactorRead.read_factor(name, start_date="", end_date="", symbols=[])
> name: 因子名
> start_date: 开始日期
> end_date: 结束日期
> symbols: 股票列[默认为空时,读取所有股票的因子数据]

#### 返回因子数据表的日期索引列
FactorRead.get_factor_dates(name)
> name: 因子名

#### 返回factor表中的所有列名（股票列表）
FactorRead.get_factor_symbols(name)
> name: 因子名

### 3.3 Feature数据写入和读取相关接口
#### 写入指定日期的feature数据
FeatureWrite.write_feature(data, name, date)
> data: Feature数据
> name: Feature名称
> date: Feature日期

#### 删除指定日期的feature数据
FeatureWrite.delete_feature(name, date)
> name: Feature名称
> date: Feature日期

#### 读取指定日期范围内的feature数据
FeatureRead.read_feature(name, start_date=None, end_date=None, fields=[])
> name: 因子名
> start_date: 开始日期
> end_date: 结束日期
> fields: 股票列[默认为空时,读取所有股票的因子数据]

### 3.4 Label数据写入和读取接口
#### 写入label数据到clickhouse数据库中
LabelWrite.write_label(data, name)
> data: Label数据
> name: Label名称

#### 删除label表中指定日期之间的数据
LabelWrite.delete_label(name, start_date, end_date)
> name: label名
> start_date: 开始日期
> end_date: 结束日期

#### 读取label数据
LabelRead.read_label(name, start_date="", end_date="", symbols=[])
> name: nabel名
> start_date: 开始日期
> end_date: 结束日期
> symbols: 股票列[默认为空时,读取所有股票的因子数据]

#### 返回因子数据表的日期索引列
LabelRead.get_label_dates(name)
> name: label名

#### 返回label表中的所有列名（股票列表）
LabelRead.get_label_symbols(name)
> name: label名

### 3.5 基础数据写入和读取接口
#### 二维frame基础数据写入
RawWrite.write_frame(data, name, date)
> data: 二维数据
> name: 数据名
> date: 写入日期

#### 二维frame基础数据删除
RawWrite.delete_frame(name, date):
> name: 数据名
> date: 删除日期

#### 二维frame基础数据读取
RawRead.read_frame(name, start_date=None, end_date=None, fields=[])
> name: nabel名
> start_date: 开始日期
> end_date: 结束日期
> fields: 属性列表[默认为空时,读取所有属性]

#### 列表数据写入
RawWrite.write_list(data, name)
> data: 列表数据
> name: 列表数据名

#### 列表数据删除
RawWrite.delete_list(name)
> name: 列表数据名

#### 列表数据读取
RawRead.read_list(name)
> name: 列表数据名

### 3.6 用户权限设置接口
#### 创建用户
AccessControl.create_user(user_name, password, cloned_user="")  
> user_name: 新用户名  
> password: 新用户密码  
> cloned_user: 如果这个字段为一个已存在的用户，直接复制这个用户的权限为新创建用户的权限

#### 删除用户
AccessControl.delete_user(user_name)  
> user_name: 被删除的用户名

#### 查看某用户权限
AccessControl.get_user_permissions(user_name)  
> user_name: 用户名

#### 设置某用户因子数据权限
AccessControl.grant_factor_permission_to_user(user_name, permission, factor)  
> user_name: 用户名  
> permission: 权限（select，alter，insert，drop），一次只能设置其中的一个  
> factor: 因子名
  1. ""和"*" 表示针对整个数据库设置权限
  2. "table_*" 表示针对数据库中所有以table_开头的table设置权限
  3. "table_12" 表示针对数据库中名为table_12的表设置权限

#### 回收某用户因子数据权限
AccessControl.revoke_factor_permission_to_user(user_name, permission, factor)  
> user_name: 用户名  
> permission: 权限（select，alter，insert，drop），一次只能设置其中的一个  
> factor: 因子名
  1. ""和"*" 表示针对整个数据库回收权限
  2. "table_*" 表示针对数据库中所有以table_开头的table回收权限
  3. "table_12" 表示针对数据库中名为table_12的表回收权限

#### 设置某用户二维数据权限
AccessControl.grant_2d_permission_to_user(user_name, permission, name)  
> user_name: 用户名  
> permission: 权限（select，alter，insert，drop），一次只能设置其中的一个  
> name: 二维数据名
  1. ""和"*" 表示针对整个数据库设置权限
  2. "table_*" 表示针对数据库中所有以table_开头的table设置权限
  3. "table_12" 表示针对数据库中名为table_12的表设置权限

#### 回收某用户二维数据权限
AccessControl.revoke_2d_permission_to_user(user_name, permission, name)  
> user_name: 用户名  
> permission: 权限（select，alter，insert，drop），一次只能设置其中的一个  
> name: 二维数据名
  1. ""和"*" 表示针对整个数据库回收权限
  2. "table_*" 表示针对数据库中所有以table_开头的table回收权限
  3. "table_12" 表示针对数据库中名为table_12的表回收权限

#### 设置某用户列表数据权限
AccessControl.grant_list_permission_to_user(user_name, permission, name)  
> user_name: 用户名  
> permission: 权限（select，alter，insert，drop），一次只能设置其中的一个  
> name: 列表数据名
  1. ""和"*" 表示针对整个数据库设置权限
  2. "table_*" 表示针对数据库中所有以table_开头的table设置权限
  3. "table_12" 表示针对数据库中名为table_12的表设置权限

#### 回收某用户列表数据权限
AccessControl.revoke_list_permission_to_user(user_name, permission, name)  
> user_name: 用户名  
> permission: 权限（select，alter，insert，drop），一次只能设置其中的一个  
> name: 列表数据名
  1. ""和"*" 表示针对整个数据库回收权限
  2. "table_*" 表示针对数据库中所有以table_开头的table回收权限
  3. "table_12" 表示针对数据库中名为table_12的表回收权限

## 4. 性能测试
### 4.1 因子数据读取测试
4000 * 4000 的float64因子矩阵，读取时间在1s左右  
（221机器上测试，带宽10Gbps/s）
### 4.2 二维数据写入测试
以jq_post数据表为例(表结构如下)：
```
   "jq_post": [('timestamp', 'FixedString(19)'),
                ('avg', 'Float64'),
                ('close', 'Float64'),
                ('high', 'Float64'),
                ('low', 'Float64'),
                ('open', 'Float64'),
                ('money', 'Float64'),
                ('volume', 'Float64'),
                ('volume_ratio', 'Float64')]
```
插入100w行数据耗时：7.114301681518555  
（221机器上测试，带宽10Gbps/s）

## 5. FAQ & Bug Fixing:
### 5.1 插入大表存在用户内存限制：
默认users.xml中:  
max_memory_usage = 10000000000  
最大内存使用量是10G,再内存够用的情况下，上调这个阈值即可；

### 5.2 insert empty values problem:
``` bash
insert into alpha_database.FC0000010008 (timestamp,S000001SZ,S000002SZ,S000003SZ,S000004SZ,S000005SZ,S000006SZ,S000007SZ,S000008SZ,S000009SZ,S000010SZ) values
```
clickhouse 在执行上述语句时，不会及时退出，范围会在等待一段时间之后报错

### 5.3 config.xml配置问题:
<listen_host>::</listen_host> 可以让clickhouse在ipv4和ipv6地址上监听连接.

## 6. 待解决问题:
1. 迁移到集群模式时: 
* get_first_row_timestamp(client, database_name, table_name) 
* get_last_row_timestamp(client, database_name, table_name)
这两个函数的逻辑要修改；

## 7. 项目时间节点：

工作时间段|完成任务列表
----------|---------
2021-11-01-2021-11-14|Clickhouse基础知识探索，并发编程知识探索，建立知识储备
2021-11-15-2021-11-19|因子数据插入，读取和删除代码编写，用例测试
2021-11-20-2021-11-24|raw数据插入，读取和删除代码编写，用例测试
2021-11-25-2021-11-28|所有代码集成测试，代码优化，raw数据删除异步延迟状况优化，第一版本交付业务部门使用
2021-12-01-2021-12-05|添加权限控制模块
2021-12-09-2021-12-10|解决“小表扩展大表”性能下降问题
2021-12-11-2021-12-22|配合用户测试和bug修复
2022-01-01-2022-01-04|加入权限管理
2022-01-04-2022-01-07|修复raw数据写入并发生成插入数据bug
2022-02-17-2022-02-18|仲阳重构版本完成
