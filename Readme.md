<center><font size=7>aibox说明文档</font></center>

[TOC]



# 整体概述

​		代码文件介绍如下：

1. dbutils.py：数据库交互
2. hdfsutils.py：HDFS交互，支持Kerberos认证
3. Ipynb_importer.py：用于支持调用notebook文件中的函数
4. conf：配置文件
5. demo：示例代码
6. data：示例数据
7. fonts：中文字体，防止运行环境缺少中文字体



# dbutils

## 功能描述

​		与数据库进行交互，目前支持如下数据库：

1. hive：支持通过Pyhive、Impyla、Pyodbc进行连接访问。
2. mysql
3. greenplum



## 环境准备

```shell
# 安装gcc是防止安装sasl时编译报错
yum install gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64

pip install pyhive==0.6.5
pip install thrift==0.16.0
pip install impyla==0.18.0
pip install sasl==0.3.1
```



## 使用方法

​		根据实际环境修改配置文件：./conf/dbutils.conf。代码示例见：./demo/dbutils.ipynb。



# hdfsutils

## 功能描述

​		与HDFS进行文件交互操作。



## 环境准备

```shell
pip install hdfs==2.7.0  
pip install requests_kerberos==0.14.0
```



## 使用方法

​		根据实际环境修改/替换配置文件：./conf/hdfsutils.conf、./conf/hdfs.keytab。代码示例见：./demo/hdfsutils.ipynb。

​		备注：hdfs.keytab文件需要从Transwarp Manager -> Guardian页面获取。



# 调用notebook文件

​		示例代码见：./demo/call_notebook.ipynb