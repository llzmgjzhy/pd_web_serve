# your_app/consumers.py

import json
from channels.generic.websocket import AsyncWebsocketConsumer
import mysql.connector
import asyncio
import aiomysql
from .pddetect import PD_detect

class PDConsumer(AsyncWebsocketConsumer):
    async def connect(self):  # 当WebSocket连接建立时，该方法将被调用
        await self.accept()
        await read_mysql()

    async def disconnect(self, close_code):  # 当WebSocket连接关闭时，该方法将被调用
        pass

    async def receive(self, text_data):  # 当接收到WebSocket消息时，该方法将被调用
        pass

        await self.send('>>>>>> 服务器端已收到数据 <<<<<<')

    # 连接数据库的函数
async def create_connection():
    return await aiomysql.connect(
        host='localhost',
        user=       'root',
        password='123456',
        db='pulsedata',
        loop=asyncio.get_event_loop()
    )

# 查询数据总行数的函数
async def get_total_rows(connection):
    async with connection.cursor() as cursor:
        await cursor.execute("SELECT COUNT(*) FROM sample_data")
        result = await cursor.fetchone()
        total_rows = result[0]
    connection.close()
    return total_rows

# 读取数据的函数
async def read_data(current_row, batch_size):
    connection = await create_connection()
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(f"SELECT max_peak, freq, tim FROM sample_data LIMIT {current_row}, {batch_size}")
            rows = await cursor.fetchall()
            return rows
    finally:
        connection.close()

# 实时监测数据表并输出新数据的函数
async def monitor_table(batch_size):
    current_row = 0
    while True:
        connection = await create_connection()
        try:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM sample_data")
                result = await cursor.fetchone()
                new_total_rows = result[0]
                if new_total_rows >= current_row + batch_size:
                    rows = await read_data(current_row, batch_size)
                    print(rows)
                    PD_detect(rows)
                    current_row += batch_size
            await asyncio.sleep(5)
        finally:
            connection.close()

async def read_mysql():
    # 连接数据库
    connection = await create_connection()
    # print(1)
    # 查询数据总行数
    total_rows = await get_total_rows(connection)

    # 定义每次读取的行数
    batch_size = 100

    # 启动监测任务
    monitor_task = asyncio.create_task(monitor_table(batch_size))

    # 读取数据
    current_row = 0
    while current_row < total_rows:
        rows = await read_data(current_row, batch_size)
        # print(rows)
        PD_detect(rows)
        current_row += batch_size

    # 等待监测任务结束
    await monitor_task