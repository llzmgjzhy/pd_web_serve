# your_app/consumers.py
import struct
import pandas as pd
import json
from channels.generic.websocket import AsyncWebsocketConsumer
import mysql.connector
import re
from mysql.connector import Error

# 打开文件并加载JSON数据
with open("package.json", "r") as file:
    db_config = json.load(file)["database"]


class MyConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_buffer = bytearray()  # 初始化一个字节数组作为数据缓冲区
        self.filename = None

    async def connect(self):  # 当WebSocket连接建立时，该方法将被调用
        await self.accept()

    async def disconnect(self, close_code):  # 当WebSocket连接关闭时，该方法将被调用
        pass

    async def receive(self, text_data=None, bytes_data=None):
        # 当接收到WebSocket消息时，该方法将被调用
        if text_data:
            # 处理文本数据
            await self.handle_text_data(text_data)
        elif bytes_data:
            # 处理字节数据
            await self.handle_bytes_data(bytes_data)

    async def handle_text_data(self, text_data):
        if text_data == "END_OF_DATA":
            print("此数据文件数据接收完成")
            await self.process_complete_data(self.data_buffer)
            self.data_buffer = bytearray()  # 重置缓冲区
        elif text_data == "SEND_NEW_DATA":
            print("准备接收新的数据文件")
            self.data_buffer = bytearray()  # 初始化一个字节数组作为缓冲区域
        else:
            try:
                # 尝试解析JSON数据
                data = json.loads(text_data)
                # 检查是否包含特定的键
                if "file_name" in data:
                    self.filename = data["file_name"]
                    print(f"文件名为: {self.filename}")
                # else:
                #     print("JSON数据不包含'name of file'键")
            except json.JSONDecodeError:
                # 处理普通文本数据
                print("接收到文本数据:", text_data)

    async def handle_bytes_data(self, bytes_data):
        # 处理接收到的字节数据
        print("收到字节数据")
        print("字节数据的字节数:", len(bytes_data))

        self.data_buffer.extend(bytes_data)
        # await save_head_file_info_mysql(self, bytes_data)

        # await self.send(">>>>>> 服务器端已收到数据 <<<<<<")

    async def process_complete_data(self, data):
        print("处理完整的数据，数据大小:", len(data))
        # 解析头部信息
        map_quantity = int.from_bytes(data[286:288], "little")  # 图谱数量
        print("图谱数量：", map_quantity)
        # 初始化指针，跳过头部
        pointer = 512
        head_file_data = data[:pointer]
        await save_head_file_info_mysql(self, head_file_data)  # 保存头文件信息

        # 循环解析每个图谱数据
        for _ in range(map_quantity):
            if pointer + 4 > len(data):
                raise ValueError("数据长度不足以包含图谱大小信息")

            # 读取图谱类型
            map_type = data[pointer]
            print("图谱类型:", hex(map_type))

            # 读取图谱大小
            map_size = int.from_bytes(data[pointer + 1 : pointer + 5], "little")

            if pointer + map_size > len(data):
                raise ValueError("数据长度不足以包含完整的图谱数据")

            # 读取图谱数据
            map_data = data[pointer : pointer + map_size]
            pointer += map_size  # 移动指针
            # 根据图谱类型处理数据
            if map_type == 0x11:
                # 处理高频PRPD图
                await process_hf_prpd_map(self, map_data)

            elif map_type == 0x12:
                # 处理高频PRPS图
                await process_hf_prps_map(self, map_data)
            elif map_type == 0x13:
                # 处理高频脉冲波形图
                await process_hf_pulse_waveform_map(self, map_data)
            else:
                print("未知的图谱类型")

        print("所有图谱数据解析完毕")


async def process_hf_prpd_map(self, map_data):
    # 处理高频PRPD图的函数
    print("处理高频PRPD图，数据长度:", len(map_data))
    await save_hf_prpd_info_mysql(self, map_data)


async def process_hf_prps_map(self, map_data):
    # 处理高频PRPS图的函数
    print("处理高频PRPS图，数据长度:", len(map_data))
    await save_hf_prps_info_mysql(self, map_data)


async def process_hf_pulse_waveform_map(self, map_data):
    # 处理高频PRPS图的函数
    print("处理高频PRPS图，数据长度:", len(map_data))
    await save_hf_pulse_waveform_info_mysql(self, map_data)


from .mysql_connect import DatabaseConnection


async def save_head_file_info_mysql(self, data):
    print("将头文件信息数据存到mysql表里")
    table_name = "head_file_info"
    try:
        with DatabaseConnection() as db_conn:  # 连接数据库
            if db_conn.is_connected():
                cursor = db_conn.cursor()
                # 按照JSON定义分割数据
                with open("websocketapp/mysql.json", "r") as file:
                    table_structure = json.load(file)
                columns = ["file_name"]
                insert_data = [self.filename]
                for column, attrs in table_structure["HEAD_FILE"].items():
                    # print("column值为{}".format(column))
                    pos_left = attrs["index"][0]
                    pos_right = attrs["index"][1]
                    byte_data = data[pos_left : pos_right + 1]
                    # print(byte_data)
                    if column in ["version_num", "instrument_version_number"]:
                        # 版本号转化
                        version_parts = [str(int(b)) for b in byte_data]
                        version_str = ".".join(version_parts)
                        columns.append(column)
                        insert_data.append(version_str)
                        # print(version_str)
                    elif attrs["type"] == "VARCHAR":
                        coding = attrs.get("coding", "UTF-8")  # 假设默认编码为UTF-8
                        try:
                            if coding == "UNICODE":
                                # 使用UTF-8解码
                                try:
                                    decoded_string = byte_data.decode("utf-16")
                                    columns.append(column)
                                    insert_data.append(decoded_string)
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                            elif coding == "ASCII":
                                try:
                                    decoded_string = byte_data.decode("ascii")
                                    columns.append(column)
                                    insert_data.append(decoded_string)
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                            else:
                                # 如果没有特定的编码方式，直接转换成字符串
                                try:
                                    decoded_string = byte_data.decode()  # 默认使用UTF-8解码
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                        except UnicodeDecodeError:
                            decoded_string = "解码错误"
                    elif attrs["type"] == "FLOAT":
                        float_num = struct.unpack("f", byte_data)[0]
                        # print(float_num)
                        columns.append(column)
                        insert_data.append(float_num)
                    elif attrs["type"] in ["INT", "BIGINT"]:
                        int_num_small = int.from_bytes(byte_data, "little")
                        # print(int_num_small)
                        columns.append(column)
                        insert_data.append(int_num_small)
                    else:
                        print("数据类型存在错误")
                for i, value in enumerate(insert_data):
                    if pd.isna(value):
                        insert_data[i] = None  # 将NaN转换为None，这在SQL中对应于NULL
                # 构建插入数据的SQL语句
                insert_sql = f"INSERT INTO {table_name} ({','.join(columns)})VALUES({','.join(['%s']*len(columns))})"
                # 执行一次性插入操作
                cursor.execute(insert_sql, insert_data)

            db_conn.commit()
            print("HEAD_FILE_INFO inserted successfully")
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")


from .save_mysql import get_data_storage_method


async def save_hf_prpd_info_mysql(self, data):
    print("将prpd数据存到mysql表里")
    table_name = "hf_prpd_info"
    try:
        with DatabaseConnection() as db_conn:
            if db_conn.is_connected():
                cursor = db_conn.cursor()
                # 按照JSON定义分割数据
                with open("websocketapp/mysql.json", "r") as file:
                    table_structure = json.load(file)["HF_PRPD"]
                columns = ["file_name"]
                insert_data = [self.filename]
                for column, attrs in table_structure["Info"].items():
                    # print("column值为{}".format(column))
                    pos_left = attrs["index"][0]
                    pos_right = attrs["index"][1]
                    byte_data = data[pos_left : pos_right + 1]
                    # print(byte_data)
                    if column in ["discharge_type_probability"]:
                        # 放电概率
                        discharge_probabilities = struct.unpack("8B", byte_data)
                        non_zero_dict = {
                            index: value
                            for index, value in enumerate(discharge_probabilities)
                            if value != 0
                        }
                        # 将字典转换为字符串
                        non_zero_str = json.dumps(non_zero_dict)
                        columns.append(column)
                        insert_data.append(non_zero_str)
                    elif attrs["type"] == "VARCHAR":
                        coding = attrs.get("coding", "UTF-8")  # 假设默认编码为UTF-8
                        try:
                            if coding == "UNICODE":
                                # 使用UTF-8解码
                                try:
                                    decoded_string = byte_data.decode("utf-16")
                                    columns.append(column)
                                    insert_data.append(decoded_string)
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                            elif coding == "ASCII":
                                try:
                                    decoded_string = byte_data.decode("ascii")
                                    columns.append(column)
                                    insert_data.append(decoded_string)
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                            else:
                                # 如果没有特定的编码方式，直接转换成字符串
                                try:
                                    decoded_string = byte_data.decode()  # 默认使用UTF-8解码
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                        except UnicodeDecodeError:
                            decoded_string = "解码错误"

                    elif attrs["type"] == "FLOAT":
                        float_num = struct.unpack("f", byte_data)[0]
                        # print(float_num)
                        columns.append(column)
                        insert_data.append(float_num)
                    elif attrs["type"] in ["INT", "BIGINT"]:
                        int_num_small = int.from_bytes(byte_data, "little")
                        # print(int_num_small)
                        columns.append(column)
                        insert_data.append(int_num_small)
                    else:
                        print("数据类型存在错误")
                for i, value in enumerate(insert_data):
                    if pd.isna(value):
                        insert_data[i] = None  # 将NaN转换为None，这在SQL中对应于NULL
                # 构建插入数据的SQL语句
                insert_sql = f"INSERT INTO {table_name} ({','.join(columns)})VALUES({','.join(['%s']*len(columns))})"
                # 执行一次性插入操作
                cursor.execute(insert_sql, insert_data)
                db_conn.commit()

                # 建立数据表
                t = data[336:337]  # 存储数据类型t
                k = get_data_storage_method(t)  # 字节数
                print("k的值为：{}".format(k))
                m = int.from_bytes(data[355:359], "little")  # 相位窗数m
                n = int.from_bytes(data[359:363], "little")  # 量化幅值n
                sample_table_name = "hf_prpd_sampledata"  # + self.filename
                columns_sql = ", ".join([f"`col_{i}` INT" for i in range(1, m + 1)])
                create_table_sql = f"CREATE TABLE IF NOT EXISTS `{sample_table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, FILE_NAME VARCHAR(50), {columns_sql})"
                cursor.execute(create_table_sql)
                db_conn.commit()

                # 数据解析和插入

                data_start_index = 512  # 假设数据从此索引开始
                parsed_data = []
                for _ in range(n):
                    # 读取可变部分
                    variable_part = data[data_start_index : data_start_index + m * k]

                    # 将每两个字节的可变部分转换为整数
                    parsed_data = [
                        int.from_bytes(variable_part[i : i + k], "little")
                        for i in range(0, len(variable_part), k)
                    ]

                    # 更新下一组数据的起始索引
                    data_start_index += k * m

                    columns_sql = ", ".join([f"`col_{i}`" for i in range(1, m + 1)])
                    columns_sql = "file_name, " + columns_sql
                    # 在 parsed_data 中添加 filename 值
                    parsed_data.insert(0, self.filename)
                    # 修改 INSERT INTO 语句以包含 float_num 和 variable_part_int_list
                    insert_query = f"INSERT INTO `{sample_table_name}` ({columns_sql}) VALUES ({','.join(['%s']*(m+1))})"
                    # 使用 executemany 来执行批量插入
                    cursor.execute(insert_query, parsed_data)
                    db_conn.commit()
                    parsed_data = []  # 重置数据列表
                # while data_start_index < len(data):
                #     if len(parsed_data) < m:
                #         # 按照k字节读取并解析数据
                #         row_data = int.from_bytes(
                #             data[data_start_index : data_start_index + k], "little"
                #         )
                #         parsed_data.append(row_data)
                #         data_start_index += k
                #     else:
                #         columns_sql = ", ".join([f"`col_{i}`" for i in range(1, m + 1)])
                #         columns_sql = "file_name, " + columns_sql
                #         # 在 parsed_data 中添加 filename 值
                #         parsed_data.insert(0, self.filename)
                #         # 批量插入
                #         insert_query = f"INSERT INTO `{sample_table_name}` ({columns_sql}) VALUES ({','.join(['%s']*(m+1))})"
                #         cursor.execute(insert_query, parsed_data)
                #         db_conn.commit()
                #         parsed_data = []  # 重置数据列表

            db_conn.commit()
            print("HF-PRPD inserted successfully")
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")


async def save_hf_prps_info_mysql(self, data):
    print("将hf-prps数据存到mysql表里")
    table_name = "hf_prps_info"
    try:
        with DatabaseConnection() as db_conn:
            if db_conn.is_connected():
                cursor = db_conn.cursor()
                # 按照JSON定义分割数据
                with open("websocketapp/mysql.json", "r") as file:
                    table_structure = json.load(file)["HF_PRPS"]
                columns = ["file_name"]
                insert_data = [self.filename]
                for column, attrs in table_structure["Info"].items():
                    # print("column值为{}".format(column))
                    pos_left = attrs["index"][0]
                    pos_right = attrs["index"][1]
                    byte_data = data[pos_left : pos_right + 1]
                    # print(byte_data)
                    if column in ["discharge_type_probability"]:
                        # 放电概率
                        discharge_probabilities = struct.unpack("8B", byte_data)
                        non_zero_dict = {
                            index: value
                            for index, value in enumerate(discharge_probabilities)
                            if value != 0
                        }
                        non_zero_str = json.dumps(non_zero_dict)
                        columns.append(column)
                        insert_data.append(non_zero_str)
                    elif attrs["type"] == "VARCHAR":
                        coding = attrs.get("coding", "UTF-8")  # 假设默认编码为UTF-8
                        try:
                            if coding == "UNICODE":
                                # 使用UTF-8解码
                                try:
                                    decoded_string = byte_data.decode("utf-16")
                                    columns.append(column)
                                    insert_data.append(decoded_string)
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                            elif coding == "ASCII":
                                try:
                                    decoded_string = byte_data.decode("ascii")
                                    columns.append(column)
                                    insert_data.append(decoded_string)
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                            else:
                                # 如果没有特定的编码方式，直接转换成字符串
                                try:
                                    decoded_string = byte_data.decode()  # 默认使用UTF-8解码
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                        except UnicodeDecodeError:
                            decoded_string = "解码错误"

                    elif attrs["type"] == "FLOAT":
                        float_num = struct.unpack("f", byte_data)[0]
                        # print(float_num)
                        columns.append(column)
                        insert_data.append(float_num)
                    elif attrs["type"] in ["INT", "BIGINT"]:
                        int_num_small = int.from_bytes(byte_data, "little")
                        # print(int_num_small)
                        columns.append(column)
                        insert_data.append(int_num_small)
                    else:
                        print("数据类型存在错误")
                for i, value in enumerate(insert_data):
                    if pd.isna(value):
                        insert_data[i] = None  # 将NaN转换为None，这在SQL中对应于NULL
                # 构建插入数据的SQL语句
                insert_sql = f"INSERT INTO {table_name} ({','.join(columns)})VALUES({','.join(['%s']*len(columns))})"
                # 执行一次性插入操作
                cursor.execute(insert_sql, insert_data)
                db_conn.commit()
                print("prps表信息插入成功")

                # 建表
                t = data[336:337]  # 存储数据类型t
                k = get_data_storage_method(t)  # 字节数
                print("k的值为：{}".format(k))
                m = int.from_bytes(data[355:359], "little")  # 相位窗数m
                p = int.from_bytes(data[363:367], "little")  # 工频周期数p

                sample_table_name = "hf_prps_sampledata"  # + self.filename
                columns_sql = ", ".join([f"`col_{i}` INT" for i in range(1, m + 1)])
                create_table_sql = f"CREATE TABLE IF NOT EXISTS `{sample_table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, file_name VARCHAR(50), {columns_sql})"
                cursor.execute(create_table_sql)
                db_conn.commit()

                # 数据解析和插入

                data_start_index = 512  # 假设数据从此索引开始
                parsed_data = []
                for _ in range(p):
                    # 读取可变部分
                    variable_part = data[data_start_index : data_start_index + m * k]

                    # 将每两个字节的可变部分转换为整数
                    parsed_data = [
                        struct.unpack("<f", variable_part[i : i + k])[0]
                        for i in range(0, len(variable_part), k)
                    ]
                    # print(len(parsed_data))
                    # 更新下一组数据的起始索引
                    data_start_index += k * m

                    columns_sql = ", ".join([f"`col_{i}`" for i in range(1, m + 1)])
                    columns_sql = "file_name, " + columns_sql
                    # 在 parsed_data 中添加 filename 值
                    parsed_data.insert(0, self.filename)
                    # 修改 INSERT INTO 语句以包含 float_num 和 variable_part_int_list
                    insert_query = f"INSERT INTO `{sample_table_name}` ({columns_sql}) VALUES ({','.join(['%s']*(m+1))})"
                    # 使用 executemany 来执行批量插入
                    cursor.execute(insert_query, parsed_data)
                    db_conn.commit()
                    parsed_data = []  # 重置数据列表

            print("HF-PRPS inserted successfully")
            # except:
            #     print("报错")
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")


async def save_hf_pulse_waveform_info_mysql(self, data):
    print("将hf-pulse-waveform数据存到mysql表里")
    table_name = "hf_pulse_waveform_info"
    try:
        with DatabaseConnection() as db_conn:
            if db_conn.is_connected():
                cursor = db_conn.cursor()
                # 按照JSON定义分割数据
                with open("websocketapp/mysql.json", "r") as file:
                    table_structure = json.load(file)["HF_PULSE_WAVEFORM"]
                columns = ["file_name"]
                insert_data = [self.filename]
                for column, attrs in table_structure["Info"].items():
                    # print("column值为{}".format(column))
                    pos_left = attrs["index"][0]
                    pos_right = attrs["index"][1]
                    byte_data = data[pos_left : pos_right + 1]
                    # print(byte_data)
                    if column in ["discharge_type_probability"]:
                        # 放电概率
                        discharge_probabilities = struct.unpack("8B", byte_data)
                        non_zero_dict = {
                            index: value
                            for index, value in enumerate(discharge_probabilities)
                            if value != 0
                        }
                        non_zero_str = json.dumps(non_zero_dict)
                        columns.append(column)
                        insert_data.append(non_zero_str)
                    elif attrs["type"] == "VARCHAR":
                        coding = attrs.get("coding", "UTF-8")  # 假设默认编码为UTF-8
                        try:
                            if coding == "UNICODE":
                                # 使用UTF-16解码
                                try:
                                    decoded_string = byte_data.decode("utf-16")
                                    columns.append(column)
                                    insert_data.append(decoded_string)
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                            elif coding == "ASCII":
                                try:
                                    decoded_string = byte_data.decode("ascii")
                                    columns.append(column)
                                    insert_data.append(decoded_string)
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                            else:
                                # 如果没有特定的编码方式，直接转换成字符串
                                try:
                                    decoded_string = byte_data.decode()  # 默认使用UTF-8解码
                                except UnicodeDecodeError:
                                    decoded_string = "解码错误"
                        except UnicodeDecodeError:
                            decoded_string = "解码错误"

                    elif attrs["type"] == "FLOAT":
                        float_num = struct.unpack("f", byte_data)[0]
                        # print(float_num)
                        columns.append(column)
                        insert_data.append(float_num)
                    elif attrs["type"] in ["INT", "BIGINT"]:
                        int_num_small = int.from_bytes(byte_data, "little")
                        # print(int_num_small)
                        columns.append(column)
                        insert_data.append(int_num_small)
                    else:
                        print("数据类型存在错误")
                        print("column值为{}".format(column))
                        print(byte_data)
                for i, value in enumerate(insert_data):
                    if pd.isna(value):
                        insert_data[i] = None  # 将NaN转换为None，这在SQL中对应于NULL
                # 构建插入数据的SQL语句
                insert_sql = f"INSERT INTO {table_name} ({','.join(columns)})VALUES({','.join(['%s']*len(columns))})"
                # 执行一次性插入操作
                cursor.execute(insert_sql, insert_data)
                db_conn.commit()

                # 建数据表
                t = data[336:337]  # 存储数据类型t
                k = get_data_storage_method(t)  # 字节数
                print("k的值为：{}".format(k))
                n = int.from_bytes(data[355:359], "little")  # n数据点数
                m = int.from_bytes(data[359:363], "little")  # m脉冲个数
                q = int(n / m)
                print("q的值为：{}".format(q))
                sample_table_name = "hf_pulse_waveform_sampledata"  # + self.filename
                columns_sql = ", ".join([f"`coll_{i}` INT" for i in range(1, q + 1)])
                create_table_sql = f"CREATE TABLE IF NOT EXISTS `{sample_table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, file_name VARCHAR(50), start_time FLOAT, {columns_sql})"
                cursor.execute(create_table_sql)
                db_conn.commit()

                # 数据解析和插入
                data_start_index = 512  # 假设数据从此索引开始
                fixed_size = 4  # 固定字节
                pulsed_data = []
                for _ in range(m):
                    # 读取固定部分
                    fixed_part = data[data_start_index : data_start_index + fixed_size]
                    float_num = struct.unpack("<f", fixed_part)[0]
                    # 读取可变部分
                    variable_part = data[
                        data_start_index
                        + fixed_size : data_start_index
                        + fixed_size
                        + k * q
                    ]

                    # 将每两个字节的可变部分转换为整数
                    variable_part_int_list = [
                        struct.unpack("<f", variable_part[i : i + k])[0]
                        for i in range(0, len(variable_part), k)
                    ]

                    # 更新下一组数据的起始索引
                    data_start_index += k * q + fixed_size

                    columns_sql = ", ".join([f"`coll_{i}`" for i in range(1, q + 1)])
                    columns_sql = "file_name, " + columns_sql
                    # 在 parsed_data 中添加 filename 值
                    variable_part_int_list.insert(0, self.filename)
                    # 修改 INSERT INTO 语句以包含 float_num 和 variable_part_int_list
                    insert_query = f"INSERT INTO `{sample_table_name}` (`start_time`, {columns_sql}) VALUES (%s, {','.join(['%s']*(q+1))})"
                    # 使用 executemany 来执行批量插入
                    cursor.execute(insert_query, [float_num] + variable_part_int_list)
                    db_conn.commit()

        # db_conn.commit()
        print("HF-PULSE-WAVEFORM inserted successfully")
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")


async def copy_and_increment_last_number(connection, table_name, columns_to_increment):
    try:
        # 查询上一行数据
        select_query = f"SELECT * FROM {table_name} ORDER BY CAST(SUBSTRING_INDEX(id, '-', -1) AS UNSIGNED) DESC LIMIT 1;"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(select_query)
        previous_row = cursor.fetchone()

        if previous_row:
            # 获取上一行的所有列的数据
            original_data = {key: previous_row[key] for key in previous_row}

            # 对指定列进行增量操作
            for column in columns_to_increment:
                if (
                    column in original_data
                    and isinstance(original_data[column], str)
                    and re.search(r"-(\d+)$", original_data[column])
                ):
                    original_number = int(
                        re.search(r"-(\d+)$", original_data[column]).group(1)
                    )
                    new_number = original_number + 1
                    original_data[column] = re.sub(
                        r"-(\d+)$", f"-{new_number}", original_data[column]
                    )

            # 插入新行
            insert_query = f"INSERT INTO {table_name} ({', '.join(original_data.keys())}) VALUES ({', '.join(['%s'] * len(original_data))})"
            cursor.execute(insert_query, tuple(original_data.values()))
            connection.commit()

            print("成功复制并插入新行！")

    except Exception as e:
        print(f"发生错误：{str(e)}")

    finally:
        cursor.close()
