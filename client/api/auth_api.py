# client/api/auth_api.py
import json
import os
from client.api.base import BaseAPI
from client.config import Config

class AuthAPI(BaseAPI):
    """统一的认证与用户信息接口"""

    # ---------- 身份认证 ----------
    def register(self, username, password):
        return self.request("POST", "/auth/register", json={"username": username, "password": password})

    def login(self, username, password):
        data = self.request("POST", "/auth/login", json={"username": username, "password": password})
        token = data["token"]
        self.set_token(token)
        self._save_token(token)
        return token

    def logout(self):
        return self.request("POST", "/auth/logout")

    # ---------- 用户资料 ----------
    def profile(self):
        return self.request("GET", "/auth/profile")

    def change_password(self, old_pwd, new_pwd):
        return self.request("POST", "/auth/change_password", json={
            "old_password": old_pwd,
            "new_password": new_pwd
        })

    def delete_account(self):
        """如果后端支持账户注销"""
        return self.request("POST", "/auth/delete_account")

    # ---------- 本地缓存 ----------
    def _save_token(self, token):
        os.makedirs(os.path.dirname(Config.TOKEN_PATH), exist_ok=True)
        with open(Config.TOKEN_PATH, "w") as f:
            json.dump({"token": token}, f)

    def load_token(self):
        if os.path.exists(Config.TOKEN_PATH):
            with open(Config.TOKEN_PATH, "r") as f:
                data = json.load(f)
                token = data.get("token")
                if token:
                    self.set_token(token)
