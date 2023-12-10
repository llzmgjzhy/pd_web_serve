import asyncio
from modules import WsClient
from utils import initial_files_data, start_monitoring
import argparse
import os

parser = argparse.ArgumentParser(description="客户端发送数据")
parser.add_argument(
    "--dir_path", type=str, default="E:\\Project program\\test", help="需要监视的文件夹路径"
)
URL = "ws://127.0.0.1:8000/chat/"

args = parser.parse_args()


async def main():
    if not os.path.exists(args.dir_path):
        print(f"给定的路径不存在: {args.dir_path}")
        return
    web_client = WsClient(URL)  # 创建 WsClient 类的实例，用于管理 WebSocket 连接

    await web_client.one_handle()
    await initial_files_data(args.dir_path, web_client)
    # 定义其他异步任务
    await asyncio.gather(
        web_client.handler(), start_monitoring(argparse.dir_path, web_client)
    )


if __name__ == "__main__":
    print("***********客户端开始运行***********")
    asyncio.run(main())
