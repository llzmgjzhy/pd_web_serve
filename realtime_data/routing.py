from django.urls import re_path
from .consumers import Data_Consumer

websocket_urlpatterns = [
    re_path(r'vue/$',Data_Consumer.as_asgi())
]

