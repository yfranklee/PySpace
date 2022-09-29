# -*- coding:gbk -*-
import os
import csv
import pymysql
from cn.cv import file_utils as fu
from cn.cv import local_config as cf
import requests

"""
���ڴ������ݱ��ʱ��ʹ��
must read me
ʹ�ýű�ǰ��Ҫ��ǰ���ô�����Ķ�Ӧע���ֵ䣬���Ӧregion�ĸ��������ֶμ����ͺ�ע�͵�csv�ļ�
"""


# ����������ɣ������������Ϣ���뵽TBLS��
def gen_table_sql(table_info=dict(), table_region=dict()):
    sqls = []
    for k, v in table_info.items():
        tbl_type = "2"
        tbl_state = "1"
        tbl_partition = "0"
        tbl_phoenix = "1"
        # k = "tmp_"+k
        owner_name = "'franklee'"
        msg = "insert into `TBLS` " + "(`db_id`,`tbl_name`,`tbl_name_cn`,`tbl_type`,`tbl_desc`,`tbl_state`,`tbl_partition`,`tbl_phoenix`,`pre_partition`,`pre_partition_type`,`owner_name`) values( ";
        msg = msg + "6,'" +k + "','" + v + "'," + tbl_type + ",'" + v + "'," + tbl_state + "," + tbl_partition + "," + tbl_phoenix + "," \
              + str(table_region[k]) + ",0," + owner_name + ");"
        # print(k)
        # inser_table(msg);
        sqls.append(msg)
    return sqls


# --------------------���´���field������
def query_db():
    db = pymysql.connect(host='39.106.67.4', port=3306, user='root', password='cv123456', database='meta_data',
                         charset='utf8')
    cursor = db.cursor()
    cursor.execute("select tbl_name,tbl_id from TBLS where db_id=6")
    lists = cursor.fetchall()  # ���շ��ص�ֵ
    db.commit()
    dicts = dict()
    for lst in lists:
        dicts[lst[0]] = lst[1]
    return dicts


# �����ֶ�����sql ��������뵽�ֶα���
def gen_table_filed_sql(file_path="../../resources/", tb_dic=dict()):
    # print(tb_dic)
    sql_list = []
    fel_datas = {"int": 1, "string": 2, "bigint": 3, "double": 4, "decimal": 5, "timestamp": 6, "date": 7, "array": 8,
                 "map": 9, "struct": 10}
    fel_type = 0
    for tbnm in os.listdir(file_path):
        with open(file_path + tbnm, 'r', encoding='utf-8') as f:
            print(file_path + tbnm)
            reader = csv.reader(f)
            for line in reader:
                col_name, data_type, comment = line
                if col_name == 'col_name' or col_name == 'row_key' or col_name == 'update_time_sys':
                    continue
                # print(col_name,comment)
                sql = "insert into `FELS`(tbl_id,fel_name,fel_name_cn,fel_type,fel_data,fel_desc,owner_name) values("
                tbl_id = tb_dic[tbnm.split(".")[0]]
                data_type = str(data_type).lower()
                fel_data = fel_datas[data_type]
                sql = sql + str(tbl_id) + ",'" + col_name + "','" + comment + "'," + str(fel_type) + "," + str(
                    fel_data) + ",'" + comment + "','" + "frankleee');"
                sql_list.append(sql)
    return sql_list


def inser_table(sql):
    db = pymysql.connect(host='39.106.67.4', port=3306, user='root', password='cv123456', database='meta_data',
                         charset='utf8')
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()


# �����ɵ����ݲ������
def inser_fields(sqls=[]):
    db = pymysql.connect(host='39.106.67.4', port=3306, user='root', password='cv123456', database='meta_data',
                         charset='utf8')
    cursor = db.cursor()
    for sql in sqls:
        # print(sql)
        cursor.execute(sql)
        print(sql)
        db.commit()


def request_batch_build_table():
    for k, v in cf.get_tmp_table_region().items():
        db = pymysql.connect(host='39.106.67.4', port=3306, user='root', password='cv123456', database='meta_data',
                             charset='utf8')
        cursor = db.cursor()
        sql = "select tbl_id from TBLS where db_id=6 and tbl_name ='" + k+"'"
        cursor.execute(sql)
        lists = cursor.fetchall()  # ���շ��ص�ֵ
        print(lists)
        db.commit()
        # dicts = dict()
        for tbl_id in lists:
            # print(k+">>"+">>"+str(tbl_id[0]) + " prepareing")
            # ret = requests.get("https://www.baidu.com").text
            ret = requests.get("http://39.106.67.4:29099/addTable?tbl_id=" + str(tbl_id[0])).text
            print(k+">>"+str(ret))


if __name__ == '__main__':
    file_path = '../../resources/'
    lst1 = gen_table_sql(cf.get_tmp_table_info(), cf.get_tmp_table_region())
    # print(lst1)
    # inser_fields(lst1)

    lst2 = gen_table_filed_sql(file_path, query_db())
    # print(lst2)
    inser_fields(lst2)
    # fu.move_file(file_path, "../../master_ed/")
    request_batch_build_table()


