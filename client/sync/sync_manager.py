"""
优化的同步管理器 - 支持网络流量优化
"""
import os
from client.api.file_api import FileAPI


class SyncManager:
    """优化的同步管理器，支持压缩、去重等网络流量优化"""
    
    def __init__(self, base_path, file_api: FileAPI):
        self.base_path = base_path
        self.file_api = file_api
        self.ignore_suffixes = ('.swp', '.tmp', '.temp')
        self.ignore_prefixes = ('~$',)
        
        # 统计信息
        self.stats = {
            'total_files': 0,
            'total_size': 0,
            'compressed_files': 0,
            'deduped_files': 0,
            'chunk_deduped_files': 0,
            'bytes_saved': 0
        }
    
    def _should_ignore(self, name: str) -> bool:
        if name.startswith(self.ignore_prefixes):
            return True
        if name.endswith(self.ignore_suffixes):
            return True
        return False
    
    def _rel(self, path: str) -> str:
        return os.path.relpath(path, self.base_path).replace("\\", "/")
    
    def upload_file(self, local_path):
        """优化的文件上传"""
        relative_path = self._rel(local_path)
        file_size = os.path.getsize(local_path)
        
        print(f"[优化同步] 上传文件: {relative_path} ({file_size} bytes)")
        
        try:
            result = self.file_api.upload_optimized(
                local_path, 
                folder=os.path.dirname(relative_path),
                enable_compression=True,
                enable_dedup=True,
                enable_chunk_dedup=True
            )
            
            # 更新统计信息
            self.stats['total_files'] += 1
            self.stats['total_size'] += file_size
            
            # 这里可以根据result中的信息更新更详细的统计
            print(f"[优化同步] 上传完成: {relative_path}")
            
        except Exception as e:
            print(f"[优化同步] 上传失败: {relative_path}, 错误: {e}")
    
    def delete_file(self, local_path):
        relative_path = self._rel(local_path)
        print(f"[优化同步] 删除文件: {relative_path}")
        self.file_api.delete(os.path.basename(local_path), folder=os.path.dirname(relative_path))
    
    def create_folder(self, local_path):
        relative_path = self._rel(local_path)
        print(f"[优化同步] 创建文件夹: {relative_path}")
        self.file_api.create_folder(relative_path)
    
    def rename_file(self, old_path, new_path):
        old_rel = self._rel(old_path)
        new_rel = self._rel(new_path)
        print(f"[优化同步] 重命名: {old_rel} -> {new_rel}")
        
        try:
            self.file_api.rename(old_rel.replace("\\", "/"), new_rel.replace("\\", "/"))
        except Exception as _:
            # 兜底方案：删除旧文件并上传新文件
            try:
                self.file_api.delete(os.path.basename(old_rel), folder=os.path.dirname(old_rel))
            except Exception:
                pass
            if os.path.isfile(new_path):
                self.upload_file(new_path)
    
    def initial_sync(self):
        """优化的初始同步"""
        print(f"[优化同步] 初始同步开始：{self.base_path}")
        
        # 重置统计信息
        self.stats = {
            'total_files': 0,
            'total_size': 0,
            'compressed_files': 0,
            'deduped_files': 0,
            'chunk_deduped_files': 0,
            'bytes_saved': 0
        }
        
        for root, dirs, files in os.walk(self.base_path):
            # 先创建目录
            rel_root = self._rel(root)
            if rel_root != '.':
                self.file_api.create_folder(rel_root)
            
            # 上传文件
            for name in files:
                if self._should_ignore(name):
                    continue
                
                local_path = os.path.join(root, name)
                self.upload_file(local_path)
        
        # 打印统计信息
        self._print_sync_stats()
        print(f"[优化同步] 初始同步完成")
    
    def _print_sync_stats(self):
        """打印同步统计信息"""
        stats = self.stats
        print(f"\n[同步统计]")
        print(f"  总文件数: {stats['total_files']}")
        print(f"  总大小: {stats['total_size']:,} bytes ({stats['total_size']/1024/1024:.2f} MB)")
        
        if stats['total_files'] > 0:
            print(f"  平均文件大小: {stats['total_size']/stats['total_files']:,.0f} bytes")
        
        if stats['bytes_saved'] > 0:
            savings_percent = (stats['bytes_saved'] / stats['total_size']) * 100
            print(f"  节省空间: {stats['bytes_saved']:,} bytes ({savings_percent:.1f}%)")
        
        print(f"  压缩文件: {stats['compressed_files']}")
        print(f"  去重文件: {stats['deduped_files']}")
        print(f"  块级去重文件: {stats['chunk_deduped_files']}")
        print()
