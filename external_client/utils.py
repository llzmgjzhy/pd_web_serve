import json
import time
import asyncio
import os
import struct
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from termcolor import colored

async def test_condition(web_client):
    '''
    旨在与一个 WebSocket 客户端实例（web_client）协作，定期检查一个特定条件，并在条件满足时发送消息到 WebSocket 服务器
    :param web_client:
    :return:
    '''
    while True:
        if time.localtime().tm_sec % 7 == 0:  # 计算当前秒数除以 7 的余数
            await web_client._send('Seven seconds')
        await asyncio.sleep(1)

async def monitor_directory(path, web_client):  # 监测文件夹
    known_files = set(os.listdir(path))
    while True:
        current_files = set(os.listdir(path))
        new_files = current_files - known_files
        if new_files:
            await web_client._send("New data generation")
            known_files = current_files

            await send_file_data(os.path.join(path, next(iter(new_files))), web_client)
        await asyncio.sleep(1)  # 每秒检查一次

async def initial_files_data(folder_path, web_client):       # 初始文件数据

    if web_client.initial_files_sent == False:
        print('********** 指定路径文件夹里已存有数据 **********')
        print('************** 开始发送原有数据 **************')
        for filename in os.listdir(folder_path):                 # 文件夹路径下的所有文件名
            file_path = os.path.join(folder_path, filename)      # 所有文件的绝对路径
            if os.path.isfile(file_path):                        # 判断文件是否存在
                if file_path.endswith(".dat"):                   # 文件名结尾是否为.dat
                    await send_inital_file_data(file_path, web_client)  # 对文件进行发送
            print('原始文件名为:{}'.format(filename))
        print('================ 完成初始数据的发送 ================')
        await web_client.mark_initial_files_sent()  # 标记初始文件已发送, 更改标志位

async def send_file_data(file_path, web_client):
    with open(file_path, 'rb') as file:
        # 以下是数据读取部分
        data = file.read(4)
        sensor_type = struct.unpack('i', data)[0]
        data = file.read(4)
        device_type = struct.unpack('i', data)[0]
        data = file.read(4)
        sampling_rate = struct.unpack('f', data)[0]
        data = file.read(4)
        sampling_length = struct.unpack('i', data)[0]
        data = file.read(4)
        discharge_type = struct.unpack('i', data)[0]
        sampinfo = {
            "sensor_type": sensor_type,
            "device_type": device_type,
            "sampling_rate":sampling_rate,
            "sampling_length": sampling_length,
            "discharge_type": discharge_type,
        }
        # 向后移动44个字节
        file.seek(44, 1)
        await web_client._send(sampinfo)

        for i in range(100):
            max_peak = struct.unpack('f', file.read(4))[0]
            phase = struct.unpack('f', file.read(4))[0]
            freq = struct.unpack('f', file.read(4))[0]
            tim = struct.unpack('f', file.read(4))[0]
            waveform = [struct.unpack('f', file.read(4))[0] for _ in range(sampling_length)]

            measurement = {
                "max_peak": max_peak,
                "phase": phase,
                "freq": freq,
                "tim": tim,
                "waveform": waveform
            }
            await web_client._send(measurement)
        print('====== {}数据集发送完成 ======'.format(file_path))


async def send_inital_file_data(file_path, web_client):
    with open(file_path, 'rb') as file:
        # 以下是数据读取部分
        data = file.read(4)
        sensor_type = struct.unpack('i', data)[0]
        data = file.read(4)
        device_type = struct.unpack('i', data)[0]
        data = file.read(4)
        sampling_rate = struct.unpack('f', data)[0]
        data = file.read(4)
        sampling_length = struct.unpack('i', data)[0]
        data = file.read(4)
        discharge_type = struct.unpack('i', data)[0]
        sampinfo = {
            "sensor_type": sensor_type,
            "device_type": device_type,
            "sampling_rate":sampling_rate,
            "sampling_length": sampling_length,
            "discharge_type": discharge_type,
        }
        # 向后移动44个字节
        file.seek(44, 1)
        await web_client._send(sampinfo)

        for i in range(10):
            max_peak = struct.unpack('f', file.read(4))[0]
            phase = struct.unpack('f', file.read(4))[0]
            freq = struct.unpack('f', file.read(4))[0]
            tim = struct.unpack('f', file.read(4))[0]
            waveform = [struct.unpack('f', file.read(4))[0] for _ in range(sampling_length)]

            measurement = {
                "max_peak": max_peak,
                "phase": phase,
                "freq": freq,
                "tim": tim,
                "waveform": waveform
            }
            await web_client._send(measurement)


class MyHandler(FileSystemEventHandler):
    def __init__(self, webclient, loop):
        self.webclient = webclient  # webclient object
        self.loop = loop
    def on_created(self, event,):
        if event.src_path.endswith(".dat"):
            print(f"New data {event.src_path} has been created!")
            print('************* 发送新生成的数据文件 **************')
            asyncio.run_coroutine_threadsafe(self.handle_created(event), self.loop)
        else:
            # print(event.src_path)
            pass

    async def handle_created(self, event):
        # 放置原来on_created里的异步操作
        await send_file_data(event.src_path, self.webclient)

    # async def on_deleted(self, event):
    #     print(f"{event.src_path} has been deleted!")
    #
    # async def on_modified(self, event):
    #     print(f"{event.src_path} has been modified!")
    #
    # async def on_moved(self, event):
    #     print(f"{event.src_path} was moved to {event.dest_path}")

async def start_monitoring(path, webclient):
    loop = asyncio.get_event_loop()  # 获取主线程的事件循环
    event_handler = MyHandler(webclient, loop)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        await asyncio.Future()
    finally:
        observer.stop()
        observer.join()
