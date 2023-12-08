from django.urls import re_path
# from channels.routing import ProtocolTypeRouter, URLRouter
from realtime_data.consumers import Data_Consumer
from websocketapp.consumers import MyConsumer  # 用于处理WebSocket请求的消费类
from pddetectionapp.consumers import PDConsumer

# websocket_urlpattern = ProtocolTypeRouter({
#     "websocket": URLRouter([
#         re_path(r"ws/chat/$", MyConsumer.as_asgi()),
#         re_path(r"ws/pddect", PDConsumer.as_asgi()),
#         re_path(r"ws/vue", Data_Consumer.as_asgi())]),
# })

websocket_urlpatterns = [
        re_path(r"chat/$", MyConsumer.as_asgi()),
        re_path(r"pddect/", PDConsumer.as_asgi()),
        re_path(r"vue/", Data_Consumer.as_asgi())]
