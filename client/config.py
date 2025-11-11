# client/config.py
import os

class Config:
    BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:5000")  # 本地后端
    TIMEOUT = 10  # 请求超时
    TOKEN_PATH = os.getenv("TOKEN_PATH", "./.token_cache.json")
