# client/api/base.py
import requests
from client.config import Config

class BaseAPI:
    def __init__(self, base_url=None, token=None):
        self.base_url = base_url or Config.BASE_URL
        self.session = requests.Session()
        self.token = token

        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def set_token(self, token):
        """更新 token"""
        self.token = token
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def request(self, method, path, **kwargs):
        """封装统一请求逻辑"""
        url = f"{self.base_url}{path}"
        try:
            resp = self.session.request(method, url, timeout=Config.TIMEOUT, **kwargs)
            data = resp.json()
        except Exception as e:
            raise RuntimeError(f"HTTP error: {e}")

        if data.get("code") != 0:
            raise RuntimeError(f"API Error: {data}")

        return data.get("data")
