"""
MySQL建表操作
"""
import os
import pymysql
from pymysql import Error
import struct
import json


# 连接数据库
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = pymysql.connect(
            host=host_name, user=user_name, password=user_password, database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


# 创建数据表
def create_table(connection, create_table_sql, table_name):
    cursor = connection.cursor()  # 创建游标对象
    try:
        cursor.execute(create_table_sql)
        print("Table {} created successfully".format(table_name))
    except Error as e:
        print(f"The error '{e}' occurred")


def create_HEAD_FILE(connection):
    # 从JSON文件读取表结构
    with open("mysql.json", "r") as file:
        table_structure = json.load(file)
    # 构建SQL建表语句
    create_table_sql = "CREATE TABLE IF NOT EXISTS head_file_info ("
    create_table_sql += "id INT AUTO_INCREMENT PRIMARY KEY, "  # 添加自增主键列
    create_table_sql += "file_name VARCHAR(50), "  # 添加file_name列
    for column, attrs in table_structure["HEAD_FILE"].items():
        column_type = attrs["type"]
        if column_type == "VARCHAR":
            column_type += f"({attrs['b_length']})"

        create_table_sql += f"{column} {column_type}, "
    # 添加 date_time 列
    create_table_sql += "date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    create_table_sql += ");"
    # create_table_sql = create_table_sql.rstrip(", ") + ");"

    create_table(connection, create_table_sql, "head_file_info")


def create_HF_PRPD(connection):
    # 从JSON文件读取表结构
    with open("mysql.json", "r") as file:
        table_structure = json.load(file)["HF_PRPD"]
    # 构建SQL建表语句
    create_table_sql = "CREATE TABLE IF NOT EXISTS HF_prpd_info ("
    create_table_sql += "id INT AUTO_INCREMENT PRIMARY KEY, "  # 添加自增主键列
    create_table_sql += "file_name VARCHAR(50), "  # 添加file_name列
    for column, attrs in table_structure["Info"].items():
        column_type = attrs["type"]
        if column_type == "VARCHAR":
            column_type += f"({attrs['b_length']})"

        create_table_sql += f"{column} {column_type}, "
    # create_table_sql = create_table_sql.rstrip(", ") + ");"
    # 添加 date_time 列
    create_table_sql += "date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    create_table_sql += ");"
    create_table(connection, create_table_sql, "HF_prpd_info")


def create_HF_PRPS(connection):
    # 从JSON文件读取表结构
    with open("mysql.json", "r") as file:
        table_structure = json.load(file)["HF_PRPS"]
    # 构建SQL建表语句
    create_table_sql = "CREATE TABLE IF NOT EXISTS HF_prps_info ("
    create_table_sql += "id INT AUTO_INCREMENT PRIMARY KEY, "  # 添加自增主键列
    create_table_sql += "file_name VARCHAR(50), "  # 添加file_name列
    for column, attrs in table_structure["Info"].items():
        column_type = attrs["type"]
        if column_type == "VARCHAR":
            column_type += f"({attrs['b_length']})"

        create_table_sql += f"{column} {column_type}, "
    # create_table_sql = create_table_sql.rstrip(", ") + ");"
    # 添加 date_time 列
    create_table_sql += "date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    create_table_sql += ");"
    create_table(connection, create_table_sql, "HF_prps_info")


def create_HF_PULSE_WAVEFORM(connection):
    # 创建高频脉冲波形信息表
    # 从JSON文件读取表结构
    with open("mysql.json", "r") as file:
        table_structure = json.load(file)["HF_PULSE_WAVEFORM"]
    # 构建SQL建表语句
    create_table_sql = "CREATE TABLE IF NOT EXISTS HF_pulse_waveform_info ("
    create_table_sql += "id INT AUTO_INCREMENT PRIMARY KEY, "  # 添加自增主键列
    create_table_sql += "file_name VARCHAR(50), "  # 添加file_name列
    for column, attrs in table_structure["Info"].items():
        column_type = attrs["type"]
        if column_type == "VARCHAR":
            column_type += f"({attrs['b_length']})"

        create_table_sql += f"{column} {column_type}, "
    # create_table_sql = create_table_sql.rstrip(", ") + ");"
    # 添加 date_time 列
    create_table_sql += "date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    create_table_sql += ");"
    create_table(connection, create_table_sql, "HF_pulse_waveform_info")


def main():
    # 打开文件并加载JSON数据
    with open("../package.json", "r") as file:
        db_config = json.load(file)["database"]
    # Database credentials
    host_name = db_config["host_name"]
    user_name = db_config["user_name"]
    user_password = db_config["user_password"]
    db_name = db_config["db_name"]
    # 连接数据库
    connection = create_connection(host_name, user_name, user_password, db_name)
    create_HEAD_FILE(connection=connection)
    create_HF_PRPD(connection=connection)
    create_HF_PRPS(connection=connection)
    create_HF_PULSE_WAVEFORM(connection=connection)

    if connection:
        connection.close()


if __name__ == "__main__":
    main()
