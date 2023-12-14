import time
from datetime import datetime
import asyncio
import os
import struct
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# async def monitor_directory(path, web_client):  # 监测文件夹
#     known_files = set(os.listdir(path))
#     while True:
#         current_files = set(os.listdir(path))
#         new_files = current_files - known_files
#         if new_files:
#             await web_client._send("New data generation")
#             known_files = current_files

#             await send_file_data(os.path.join(path, next(iter(new_files))), web_client)
#         await asyncio.sleep(1)  # 每秒检查一次


async def initial_files_data(folder_path, web_client):  # 初始文件数据
    if web_client.initial_files_sent == False:
        print("********** 指定路径文件夹里已存有数据 **********")
        print("************** 开始发送原有数据 **************")
        for filename in os.listdir(folder_path):  # 文件夹路径下的所有文件名
            file_path = os.path.join(folder_path, filename)  # 所有文件的绝对路径
            if os.path.isfile(file_path):  # 判断文件是否存在
                if file_path.endswith(".dat"):  # 文件名结尾是否为.dat
                    creation_time = os.stat(file_path).st_atime  # 文件的创建时间
                    # 将时间戳转换为可读的日期-时间格式
                    correct_datetime = datetime.fromtimestamp(creation_time).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    await send_inital_file_data(
                        file_path, filename, correct_datetime, web_client
                    )  # 对文件进行发送
            print("原始文件名为:{}".format(filename))
        print("================ 完成初始数据的发送 ================")
        await web_client.mark_initial_files_sent()  # 标记初始文件已发送, 更改标志位


async def send_file_data(file_path, file_name, create_time, web_client):
    file_size = os.path.getsize(file_path)
    sample_data_size = int((file_size - 64) / 4016)
    try:
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
            sampinfo = {
                "file_name": file_name,
                "sensor_type": sensor_type,
                "device_type": device_type,
                "sampling_rate": sampling_rate,
                "pulse_count": sample_data_size,
                "sampling_length": sampling_length,
                "discharge_type": discharge_type,
                "Date_created": create_time,
            }

            # 向后移动44个字节
            file.seek(44, 1)
            await web_client._send(sampinfo)

            for i in range(sample_data_size):
                max_peak = struct.unpack("f", file.read(4))[0]
                phase = struct.unpack("f", file.read(4))[0]
                freq = struct.unpack("f", file.read(4))[0]
                tim = struct.unpack("f", file.read(4))[0]
                waveform = [
                    struct.unpack("f", file.read(4))[0] for _ in range(sampling_length)
                ]

                measurement = {
                    "max_peak": max_peak,
                    "phase": phase,
                    "freq": freq,
                    "tim": tim,
                    "waveform": waveform,
                }
                await web_client._send(measurement)
        print("====== {}数据集发送完成 ======".format(file_path))
    except Exception as e:
        print(f"在读取文件 {file_path} 时出现错误: {e}")


async def send_inital_file_data(file_path, file_name, create_time, web_client):
    file_size = os.path.getsize(file_path)
    sample_data_size = int((file_size - 64) / 4016)
    try:
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
            sampinfo = {
                "file_name": file_name,
                "sensor_type": sensor_type,
                "device_type": device_type,
                "sampling_rate": sampling_rate,
                "pulse_count": sample_data_size,
                "sampling_length": sampling_length,
                "discharge_type": discharge_type,
                "Date_created": create_time,
            }

            # 向后移动44个字节
            file.seek(44, 1)
            await web_client._send(sampinfo)

            for i in range(sample_data_size):
                max_peak = struct.unpack("f", file.read(4))[0]
                phase = struct.unpack("f", file.read(4))[0]
                freq = struct.unpack("f", file.read(4))[0]
                tim = struct.unpack("f", file.read(4))[0]
                waveform = [
                    struct.unpack("f", file.read(4))[0] for _ in range(sampling_length)
                ]

                measurement = {
                    "max_peak": max_peak,
                    "phase": phase,
                    "freq": freq,
                    "tim": tim,
                    "waveform": waveform,
                }
                await web_client._send(measurement)
    except Exception as e:
        print(f"在读取文件 {file_path} 时出现错误: {e}")
    # 这里可以添加重试逻辑或者只是简单地打印错误


class MyHandler(FileSystemEventHandler):
    def __init__(self, webclient, loop):
        self.webclient = webclient  # webclient object
        self.loop = loop

    def on_created(self, event):
        if event.src_path.endswith(".dat"):
            print(f"New data {event.src_path} has been created!")
            print("************* 发送新生成的数据文件 **************")
            # 从完整的文件路径中提取文件名
            file_name = os.path.basename(event.src_path)
            creation_time = os.stat(event.src_path).st_atime  # 文件的创建时间
            # 将时间戳转换为可读的日期-时间格式
            correct_datetime = datetime.fromtimestamp(creation_time).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            asyncio.run_coroutine_threadsafe(
                self.handle_created(event, file_name, correct_datetime),
                self.loop,
            )

    async def handle_created(self, event, file_name, create_time):
        # 检查文件是否准备好读取，如果准备好，则发送文件数据
        try:
            if await self.wait_for_file_ready(event.src_path):
                await send_file_data(
                    event.src_path,
                    file_name,
                    create_time,
                    self.webclient,
                )
            else:
                print(f"文件 {event.src_path} 未准备就绪。")
        except Exception as e:
            print(f"处理文件 {event.src_path} 时出错: {e}")

    async def wait_for_file_ready(self, file_path, timeout=30, check_interval=1):
        """
        异步等待文件准备就绪。

        :param file_path: 文件的路径。
        :param timeout: 等待文件就绪的超时时间（秒）。
        :param check_interval: 检查文件状态的间隔时间（秒）。
        :return: 文件是否就绪。
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not os.path.exists(file_path):
                await asyncio.sleep(check_interval)
                continue

            try:
                with open(file_path, "rb") as file:
                    return True  # 文件已准备就绪
            except IOError:
                # 文件可能正在写入中，稍后重试
                print("文件可能正在写入中，稍后重试")
                await asyncio.sleep(check_interval)

        return False  # 超时，文件未就绪

    # async def on_deleted(self, event):
    #     print(f"{event.src_path} has been deleted!")
    #
    # async def on_modified(self, event):
    #     print(f"{event.src_path} has been modified!")
    #
    # async def on_moved(self, event):
    #     print(f"{event.src_path} was moved to {event.dest_path}")


async def start_monitoring(path, webclient):
    loop = asyncio.get_running_loop()  # 获取主线程的事件循环
    event_handler = MyHandler(webclient, loop)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        await asyncio.Future()
    finally:
        observer.stop()
        observer.join()
