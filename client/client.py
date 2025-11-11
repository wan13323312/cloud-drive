# client/client.py
from client.api.auth_api import AuthAPI
from client.api.file_api import FileAPI
from client.config import Config
from client.sync.sync_manager import SyncManager
from client.sync.watcher import FolderWatcher
import argparse
import os

class Client:
    """统一客户端"""
    def __init__(self, base_url=None):
        self.base_url = base_url or Config.BASE_URL
        self.auth = AuthAPI(self.base_url)
        self.file = FileAPI(self.base_url)

    def login(self, username, password):
        token = self.auth.login(username, password)
        self._sync_token(token)
        return token

    def _sync_token(self, token):
        """同步 token 到所有模块"""
        self.file.set_token(token)


def main():
    parser = argparse.ArgumentParser(description="Cloud Drive Client")
    parser.add_argument("--base-url", default=Config.BASE_URL, help="Backend API base url")
    parser.add_argument("--username", required=True, help="Username")
    parser.add_argument("--password", required=True, help="Password")
    parser.add_argument("--path", required=True, help="Local folder to sync")
    args = parser.parse_args()

    sync_path = os.path.abspath(args.path)
    if not os.path.isdir(sync_path):
        raise SystemExit(f"路径不存在或不是目录: {sync_path}")

    cli = Client(base_url=args.base_url)
    cli.login(args.username, args.password)

    sync_manager = SyncManager(sync_path, cli.file)

    # 初始同步（本地 -> 云端）
    sync_manager.initial_sync()

    # 启动文件夹监听（阻塞，Ctrl+C 退出）
    watcher = FolderWatcher(sync_path, sync_manager)
    print(f"[客户端] 开始监听目录: {sync_path}，按 Ctrl+C 退出")
    watcher.start()


if __name__ == "__main__":
    main()
