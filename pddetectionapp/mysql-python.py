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


# SQL for creating tables
create_sampinfo_table = """
CREATE TABLE IF NOT EXISTS sample_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sensor_type INT,
    device_type INT,
    sampling_rate FLOAT,
    sampling_length INT,
    discharge_type INT
)
"""

create_sampdata_table = """
CREATE TABLE IF NOT EXISTS sample_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_info_id INT,
    max_peak FLOAT,
    phase FLOAT,
    freq FLOAT,
    tim FLOAT,
    waveform BLOB
)
"""

create_pdresults_table = """
CREATE TABLE IF NOT EXISTS pd_result (
    id INT AUTO_INCREMENT PRIMARY KEY,
    start_index INT,
    end_index INT,
    detect_result INT
)
"""


def alter_table_add_timestamp_column(connection):
    with connection.cursor() as cursor:
        # 检查 sample_info 表中是否存在 data_time 列
        cursor.execute("SHOW COLUMNS FROM sample_info LIKE 'data_time'")
        result = cursor.fetchone()
        if not result:
            cursor.execute(
                "ALTER TABLE sample_info ADD COLUMN data_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )

        # 检查 sample_data 表中是否存在 data_time 列
        cursor.execute("SHOW COLUMNS FROM sample_data LIKE 'data_time'")
        result = cursor.fetchone()
        if not result:
            cursor.execute(
                "ALTER TABLE sample_data ADD COLUMN data_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )

        connection.commit()


def insert_sample_info(
    connection, sensor_type, device_type, sampling_rate, sampling_length, discharge_type
):
    with connection.cursor() as cursor:
        # SQL执行语句
        sql = """
            INSERT INTO sample_info (sensor_type, device_type, sampling_rate, sampling_length, discharge_type)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (sensor_type, device_type, sampling_rate, sampling_length, discharge_type),
        )
        connection.commit()
        print("Data insert successfully")
        return cursor.lastrowid


def insert_sample_data(
    connection, sample_info_id, max_peak, phase, other1, other2, waveform
):
    with connection.cursor() as cursor:
        sql = """
            INSERT INTO sample_data (sample_info_id, max_peak, phase, freq, tim, waveform)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql, (sample_info_id, max_peak, phase, other1, other2, json.dumps(waveform))
        )
        connection.commit()


def read_and_store_data(file_path, connection):
    with open(file_path, "rb") as file:
        # 以下是数据读取部分
        data = file.read(4)
        sensor_type = struct.unpack("i", data)[0]
        data = file.read(4)
        device_type = struct.unpack("i", data)[0]
        data = file.read(4)
        sampling_rate = struct.unpack("f", data)[0]
        data = file.read(4)
        sampling_length = struct.unpack("i", data)[0]
        data = file.read(4)
        discharge_type = struct.unpack("i", data)[0]

        # # 连接到数据库并插入数据
        sample_info_id = insert_sample_info(
            connection,
            sensor_type,
            device_type,
            sampling_rate,
            sampling_length,
            discharge_type,
        )

        # 向后移动48个字节
        file.seek(44, 1)

        for i in range(1000):
            # 最大幅值
            max_peak = struct.unpack("f", file.read(4))[0]
            # 相位
            phase = struct.unpack("f", file.read(4))[0]
            # 其他1
            freq = struct.unpack("f", file.read(4))[0]
            # 其他2
            tim = struct.unpack("f", file.read(4))[0]

            waveform = [
                struct.unpack("f", file.read(4))[0] for _ in range(sampling_length)
            ]
            insert_sample_data(
                connection, sample_info_id, max_peak, phase, freq, tim, waveform
            )


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

    # Create tables
    create_table(connection, create_sampinfo_table, "sample_info")
    create_table(connection, create_sampdata_table, "sample_data")
    create_table(connection, create_pdresults_table, "pd_result")
    alter_table_add_timestamp_column(connection)  # 保存数据时间

    # dir_path = 'E:\Data\data0824\高频局放数据20230824_001\电缆\巡检异常数据'  # 实验室GIS模拟放电装置数据20231024
    # file_names = os.listdir(dir_path)  # 文件名字
    # dirname = dir_path.split('\\')
    # files = [os.path.join(dir_path, file) for file in os.listdir(dir_path)]  # 文件地址

    # for i in range(20):
    #     read_and_store_data(files[i], connection)
    #     print('完成第{}组数据读取'.format(i))

    # Close the connection
    if connection:
        connection.close()


if __name__ == "__main__":
    main()
