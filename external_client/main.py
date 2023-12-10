import asyncio
from modules import WsClient
from utils import initial_files_data, start_monitoring
import argparse

URL = "ws://127.0.0.1:8000/ws/chat/"
DIRECTORY_PATH = "E:\\项目程序\\test"  # 替换要监视的文件夹路径


async def main():
    web_client = WsClient(URL)  # 创建 WsClient 类的实例，用于管理 WebSocket 连接

    await web_client.one_handle()
    await initial_files_data(DIRECTORY_PATH, web_client)
    # 定义其他异步任务
    await asyncio.gather(
        web_client.handler(),
        start_monitoring(DIRECTORY_PATH, web_client))
    # asyncio.gather(*tasks)：将多个异步任务聚合在一起，以便它们可以并发运行。

if __name__ == "__main__":
    print('***********客户端开始运行***********')
    asyncio.run(main())
