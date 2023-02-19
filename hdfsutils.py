import subprocess
from hdfs.ext.kerberos import KerberosClient
import os
import configparser


FILE_BASE_PATH=__file__

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_active_namenode(namenode1, namenode2, tdh_principal):
    '''
    功能：获取active namenode结点
    参数：namenode1 - nm1的地址
          namenode2 - nm2的地址
          tdh_principal - keytab文件对应的Kerberos principal
    返回：active namenode的地址
    '''
    try:
        #status = subprocess.call(['kinit -kt ./hdfs.keytab  ' + tdh_principal], shell=True)
        hdfs_client = KerberosClient(namenode1, principal=tdh_principal)
        tmp_dir_status = hdfs_client.status('/')
        active_namenode = namenode1
#         print('active nm1:', namenode1)
    except:
        active_namenode = namenode2
#         print('active nm2:', namenode2)
    return active_namenode

def get_hdfs_client(active_namenode, tdh_principal):
    '''
    功能：获取hdfs client
    参数：active_namenode - active namenode对象
          tdh_principal - keytab文件对应的Kerberos principal
    返回：KerberosClient对象
    '''
    hdfs_client = KerberosClient(active_namenode, principal=tdh_principal)
    return hdfs_client

def hdfs_ls(path, hdfs_client):
    '''
    功能：ls
    参数：path - hdfs路径
          hdfs_client - KerberosClient对象
    返回：目录文件列表
    '''
    return hdfs_client.list(path)

def hdfs_put(target_path, source_file, hdfs_client):
    '''
    功能：put
    参数：target_path - hdfs路径
          source_file - 本地待上传文件
          hdfs_client - KerberosClient对象
    返回：无
    '''
    hdfs_client.upload(target_path, source_file)
    
def hdfs_download(source_file, target_path, hdfs_client):
    '''
    功能：download
    参数：source_file - hdfs待下载文件
          target_path - 存储下载文件的目标路径
          hdfs_client - KerberosClient对象
    返回：无
    '''
    hdfs_client.download(source_file, target_path)
    

def hdfs_delete(target_file, hdfs_client):
    '''
    功能：delete
    参数：target_file - hdfs路径上待删除的文件
          hdfs_client - KerberosClient对象
    返回：无
    '''
    hdfs_client.delete(target_file)
    

def hdfs_mkdir(path, hdfs_client):
    '''
    功能：mkdir
    参数：path - hdfs文件路径
          hdfs_client - KerberosClient对象
    返回：无
    '''
    hdfs_client.makedirs(path)
    
    
def init_hdfs():
    '''
    功能：初始化hdfs环境
    参数：无
    返回：无
    '''
    cf = configparser.ConfigParser()
    cf.read(os.path.join(BASE_DIR, './conf/hdfsutils.conf'))
    namenode1 = cf.get('hdfs', 'namenode1')
    namenode2 = cf.get('hdfs', 'namenode2')
    tdh_principal = cf.get('hdfs', 'tdh_principal')
    print('tdh_principal:', tdh_principal)
    hdfs_keytab_path = os.path.join(BASE_DIR, './conf/hdfs.keytab')
    
    print('hdfs_keytab_path:', hdfs_keytab_path)

    # kinit    
    status = subprocess.call(['kinit -kt ' + hdfs_keytab_path + ' ' + tdh_principal], shell=True)

    print('kinti status:', 'kinit success.' if status == 0 else 'kinit fail!!!')

    active_namenode = get_active_namenode(namenode1, namenode2, tdh_principal)
    hdfs_client = get_hdfs_client(active_namenode, tdh_principal)
    print('active nm:', active_namenode)
    
    return hdfs_client



 

# if __name__ == '__main__':
#     cf = configparser.ConfigParser()
#     cf.read(os.path.join(BASE_DIR, 'conf', 'hdfsutils.conf'))
#     namenode1 = cf.get('hdfs', 'namenode1')
#     namenode2 = cf.get('hdfs', 'namenode2')
#     tdh_principal = cf.get('hdfs', 'tdh_principal')
    
#     active_namenode = get_active_namenode(namenode1, namenode2)
#     hdfs_client = get_hdfs_client(active_namenode, tdh_principal)
    
#     hdfs_put('/tmp/jinan_subway', './hdfs.keytab')
#     print(hdfs_ls('/tmp/jinan_subway'))
#     hdfs_download('/tmp/jinan_subway/hdfs.keytab', './tmp')
#     hdfs_delete('/tmp/jinan_subway/hdfs.keytab')
#     print('active_namenode:', active_namenode)