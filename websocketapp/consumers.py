# your_app/consumers.py

import json
from channels.generic.websocket import AsyncWebsocketConsumer
import mysql.connector
import re

# 打开文件并加载JSON数据
with open("package.json", "r") as file:
    db_config = json.load(file)["database"]


class MyConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.Flag = True
    async def connect(self):  # 当WebSocket连接建立时，该方法将被调用
        await self.accept()

    async def disconnect(self, close_code):  # 当WebSocket连接关闭时，该方法将被调用
        pass

    async def receive(self, text_data):  # 当接收到WebSocket消息时，该方法将被调用
        try:
            text_data_json = json.loads(text_data)
            await self.save_to_mysql(text_data_json)
            # message = text_data_json.get('sensor_type', 'No message key in JSON')
        except json.JSONDecodeError:
            print("注意: 接收的数据格式不是JSON类型，而是{}类型".format(type(text_data)))
            if text_data:
                print("接收到的数据:", repr(text_data))
            else:
                print("接收到的数据为空或不可打印")

        await self.send(">>>>>> 服务器端已收到数据 <<<<<<")

    async def save_to_mysql(self, data):
        
        connection = mysql.connector.connect(
        host=db_config["host_name"],
        database=db_config["db_name"],
        user=db_config["user_name"],
        password=db_config["user_password"],
        )
        if self.Flag:
        # 指定要进行增量操作的列
            columns_to_increment = ["id", "path", "children_name", "children_meta_title"]  # 替换为你实际的列名
            # 调用复制并插入的函数
            await copy_and_increment_last_number(connection, "dynamic_routes", columns_to_increment)
            print('???添加数据流信息到MySQL中???')
            self.Flag = False
    
        print("----- 将数据存到MySQL中 -----")

        # connection = mysql.connector.connect(
        #     host=db_config["host_name"],
        #     database=db_config["db_name"],
        #     user=db_config["user_name"],
        #     password=db_config["user_password"],
        # )

        cursor = connection.cursor()

        if "sensor_type" in data:
            print("存到sample_info表里.............")
            file_name = data["file_name"]
            sensor_type = data["sensor_type"]
            device_type = data["device_type"]
            sampling_rate = data["sampling_rate"]
            pulse_count = data["pulse_count"]
            sampling_length = data["sampling_length"]
            discharge_type = data["discharge_type"]
            Date_created = data["Date_created"]
            self.sample_info_id = insert_sample_info(
                connection,
                file_name,
                sensor_type,
                device_type,
                sampling_rate,
                pulse_count,
                sampling_length,
                discharge_type,
                Date_created
            )

            # connection.commit()
            # cursor.close()
            # connection.close()

        elif "max_peak" in data:
            if self.sample_info_id is None:
                print("错误: 未接收到 sensor_type 数据")
                return
            max_peak = data["max_peak"]
            phase = data["phase"]
            freq = data["freq"]
            tim = data["tim"]
            waveform = data["waveform"]
            insert_sample_data(
                connection, 
                self.sample_info_id, 
                max_peak, 
                phase, 
                freq, 
                tim, 
                waveform
            )

        else:
            print("Mysql数据存储出现错误")
        connection.commit()
        cursor.close()
        connection.close()


def insert_sample_info(
    connection, file_name, sensor_type, device_type, sampling_rate, pulse_count,
    sampling_length, discharge_type, Date_created 
    ):
    with connection.cursor() as cursor:
        # SQL执行语句
        sql = """
            INSERT INTO sample_info (file_name, sensor_type, device_type, sampling_rate, pulse_count, sampling_length, discharge_type, Date_created)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (file_name, sensor_type, device_type, sampling_rate, pulse_count, sampling_length, discharge_type, Date_created),
        )
        connection.commit()
        print("Data information insert successfully")
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
        print("Data Sample insert successfully")


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
                if column in original_data and isinstance(original_data[column], str) and re.search(r'-(\d+)$', original_data[column]):
                    original_number = int(re.search(r'-(\d+)$', original_data[column]).group(1))
                    new_number = original_number + 1
                    original_data[column] = re.sub(r'-(\d+)$', f'-{new_number}', original_data[column])

            # 插入新行
            insert_query = f"INSERT INTO {table_name} ({', '.join(original_data.keys())}) VALUES ({', '.join(['%s'] * len(original_data))})"
            cursor.execute(insert_query, tuple(original_data.values()))
            connection.commit()

            print("成功复制并插入新行！")

    except Exception as e:
        print(f"发生错误：{str(e)}")

    finally:
        cursor.close()