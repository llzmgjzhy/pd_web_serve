# your_app/consumers.py

import json
from channels.generic.websocket import AsyncWebsocketConsumer
import mysql.connector


class MyConsumer(AsyncWebsocketConsumer):
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
            print('报错: 数据格式不是JSON类型，而是{}类型'.format(type(text_data)))
            if text_data:
                print('接收到的数据:', repr(text_data))
            else:
                print('接收到的数据为空或不可打印')

        # 打印接收到的消息
        # print(f"Received message from client: {message}")

        # 存储数据到MySQL
        # self.save_to_mysql(sensor_type)
        # 发送消息到 WebSocket
        await self.send('>>>>>> 服务器端已收到数据 <<<<<<')

    async def save_to_mysql(self, data):
        print('----- 将数据存到MySQL中 -----')

        connection = mysql.connector.connect(
            host='localhost',
            database='pulsedata',
            user='root',
            password='123456'
        )
        cursor = connection.cursor()

        if 'sensor_type' in data:
            print('存到sample_info表里')
            sensor_type = data["sensor_type"]
            device_type = data["device_type"]
            sampling_rate = data["sampling_rate"]
            sampling_length = data["sampling_length"]
            discharge_type = data["discharge_type"]
            sample_info_id = insert_sample_info(connection, sensor_type, device_type, sampling_rate, sampling_length, discharge_type)

            connection.commit()
            cursor.close()
            connection.close()

        elif 'max_peak' in data:
            max_peak = data["max_peak"]
            phase = data["phase"]
            freq = data["freq"]
            tim = data["tim"]
            waveform = data["waveform"]
            insert_sample_data(connection, sample_info_id, max_peak, phase, freq, tim, waveform)

            connection.commit()
            cursor.close()
            connection.close()
        else:
            print('Mysql数据存储出现错误')


def insert_sample_info(connection, sensor_type, device_type, sampling_rate, sampling_length,
                       discharge_type):
    with connection.cursor() as cursor:
        # SQL执行语句
        sql = """
            INSERT INTO sample_info (sensor_type, device_type, sampling_rate, sampling_length, discharge_type)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (sensor_type, device_type, sampling_rate, sampling_length, discharge_type))
        connection.commit()
        print("Data information insert successfully")
        return cursor.lastrowid


def insert_sample_data(connection, sample_info_id, max_peak, phase, other1, other2, waveform):
    with connection.cursor() as cursor:
        sql = """
            INSERT INTO sample_data (sample_info_id, max_peak, phase, freq, tim, waveform)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (sample_info_id, max_peak, phase, other1, other2, json.dumps(waveform)))
        connection.commit()
        print("Data Sample insert successfully")
