"""
优化版客户端 - 支持网络流量优化
"""
from client.api.auth_api import AuthAPI
from client.api.file_api import FileAPI
from client.sync.sync_manager import SyncManager
from client.sync.watcher import FolderWatcher
import argparse
import os


class OptimizedClient:
    """优化版客户端，支持压缩、去重等网络流量优化"""
    
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
    parser = argparse.ArgumentParser(description="Optimized Cloud Drive Client")
    parser.add_argument("--base-url", default=Config.BASE_URL, help="Backend API base url")
    parser.add_argument("--username", required=True, help="Username")
    parser.add_argument("--password", required=True, help="Password")
    parser.add_argument("--path", required=True, help="Local folder to sync")
    parser.add_argument("--compression", action="store_true", default=True, help="Enable compression")
    parser.add_argument("--file-dedup", action="store_true", default=True, help="Enable file-level deduplication")
    parser.add_argument("--chunk-dedup", action="store_true", default=True, help="Enable chunk-level deduplication")
    args = parser.parse_args()
    
    sync_path = os.path.abspath(args.path)
    if not os.path.isdir(sync_path):
        raise SystemExit(f"路径不存在或不是目录: {sync_path}")
    
    # 创建优化客户端
    cli = OptimizedClient(base_url=args.base_url)
    cli.login(args.username, args.password)
    
    # 配置优化选项
    cli.file.enable_compression = args.compression
    cli.file.enable_dedup = args.file_dedup
    cli.file.enable_chunk_dedup = args.chunk_dedup
    
    print(f"[优化客户端] 网络优化配置:")
    print(f"  - 压缩: {'启用' if args.compression else '禁用'}")
    print(f"  - 文件级去重: {'启用' if args.file_dedup else '禁用'}")
    print(f"  - 块级去重: {'启用' if args.chunk_dedup else '禁用'}")
    
    # 创建优化同步管理器
    sync_manager = SyncManager(sync_path, cli.file)
    
    # 初始同步（本地 -> 云端）
    sync_manager.initial_sync()
    
    # 启动文件夹监听（阻塞，Ctrl+C 退出）
    watcher = FolderWatcher(sync_path, sync_manager)
    print(f"[优化客户端] 开始监听目录: {sync_path}，按 Ctrl+C 退出")
    watcher.start()


if __name__ == "__main__":
    main()
