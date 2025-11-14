"""
优化的文件API - 支持压缩、去重、块级上传
"""
import os
import json
from typing import Dict, List, Optional
from client.api.base import BaseAPI
from client.utils.compression import CompressionUtils
from client.utils.hash_utils import HashUtils


class FileAPI(BaseAPI):
    """文件API，支持压缩、去重等网络流量优化"""
    
    def __init__(self, base_url=None, token=None):
        super().__init__(base_url, token)
        self.compression = CompressionUtils()
        self.hash_utils = HashUtils()
    
    def upload_optimized(self, filepath: str, folder: str = "", enable_compression: bool = True, 
                        enable_dedup: bool = True, enable_chunk_dedup: bool = False) -> Dict:
        """
        优化的文件上传
        
        Args:
            filepath: 本地文件路径
            folder: 目标文件夹
            enable_compression: 启用压缩
            enable_dedup: 启用文件级去重
            enable_chunk_dedup: 启用块级去重
        """
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)
        file_ext = os.path.splitext(filename)[1]
        
        print(f"[上传优化] 开始上传文件: {filename} ({file_size} bytes)")
        
        # 1. 文件级去重检查
        if enable_dedup:
            file_hash = self.hash_utils.calculate_file_hash(filepath)
            print(f"[上传优化] 文件哈希: {file_hash}")
            
            # 检查服务器是否已有此文件
            if self._check_file_exists(file_hash):
                print(f"[上传优化] 文件已存在，创建引用")
                return self._create_file_reference(file_hash, filename, folder)
        
        # 2. 块级去重上传
        if enable_chunk_dedup and file_size > 4 * 1024 * 1024:  # 大于4MB才使用块级去重
            return self._upload_with_chunk_dedup(filepath, folder, enable_compression)
        
        # 3. 普通上传（可选压缩）
        return self._upload_with_compression(filepath, folder, enable_compression)
    
    def _check_file_exists(self, file_hash: str) -> bool:
        """检查服务器是否已有此文件"""
        try:
            result = self.request("GET", "/file/check_hash", params={"hash": file_hash})
            return result.get("exists", False)
        except:
            return False
    
    def _create_file_reference(self, file_hash: str, filename: str, folder: str) -> Dict:
        """为已存在的文件创建引用"""
        return self.request("POST", "/file/create_reference", json={
            "hash": file_hash,
            "filename": filename,
            "folder": folder
        })
    
    def _upload_with_compression(self, filepath: str, folder: str, enable_compression: bool) -> Dict:
        """带压缩的普通上传"""
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)
        file_ext = os.path.splitext(filename)[1]
        
        with open(filepath, "rb") as f:
            file_data = f.read()
        
        # 决定是否压缩
        should_compress = (enable_compression and 
                          self.compression.should_compress(file_size, file_ext))
        
        if should_compress:
            # 估算压缩比
            compression_ratio = self.compression.estimate_compression_ratio(file_data)
            if compression_ratio < 0.9:  # 压缩效果好于10%才压缩
                print(f"[上传优化] 压缩文件 (预估压缩比: {compression_ratio:.2f})")
                compressed_data = self.compression.compress_zlib(file_data)
                
                files = {
                    "file": (filename, compressed_data),
                    "folder": (None, folder),
                    "compressed": (None, "true")
                }
                print(f"[上传优化] 压缩后大小: {len(compressed_data)} bytes (节省 {len(file_data) - len(compressed_data)} bytes)")
            else:
                print(f"[上传优化] 压缩效果不佳，使用原始文件")
                files = {"file": (filename, file_data), "folder": (None, folder)}
        else:
            files = {"file": (filename, file_data), "folder": (None, folder)}
        
        return self.request("POST", "/file/upload", files=files)
    
    def _upload_with_chunk_dedup(self, filepath: str, folder: str, enable_compression: bool) -> Dict:
        """块级去重上传"""
        filename = os.path.basename(filepath)
        print(f"[上传优化] 使用块级去重上传: {filename}")
        
        # 1. 分割文件为块
        chunks = self.hash_utils.split_file_to_chunks(filepath)
        print(f"[上传优化] 文件分为 {len(chunks)} 个块")
        
        # 2. 批量查询哪些块已存在
        chunk_hashes = [chunk['hash'] for chunk in chunks]
        existing_chunks = self._check_chunks_exist(chunk_hashes)
        
        # 3. 只上传不存在的块
        missing_chunks = []
        for chunk in chunks:
            if chunk['hash'] not in existing_chunks:
                missing_chunks.append(chunk)
        
        print(f"[上传优化] 需要上传 {len(missing_chunks)} 个新块 (去重了 {len(chunks) - len(missing_chunks)} 个块)")
        
        # 4. 上传缺失的块
        for chunk in missing_chunks:
            self._upload_chunk(chunk, enable_compression)
        
        # 5. 创建文件元数据
        file_metadata = {
            'filename': filename,
            'folder': folder,
            'total_size': sum(chunk['size'] for chunk in chunks),
            'chunks': [
                {
                    'hash': chunk['hash'],
                    'index': chunk['index'],
                    'offset': chunk['offset'],
                    'size': chunk['size']
                }
                for chunk in chunks
            ]
        }
        
        return self.request("POST", "/file/assemble_from_chunks", json=file_metadata)
    
    def _check_chunks_exist(self, chunk_hashes: List[str]) -> set:
        """批量检查哪些块已存在"""
        try:
            result = self.request("POST", "/file/check_chunks", json={"hashes": chunk_hashes})
            return set(result.get("existing_hashes", []))
        except:
            return set()
    
    def _upload_chunk(self, chunk: Dict, enable_compression: bool):
        """上传单个数据块"""
        chunk_data = chunk['data']
        
        # 可选压缩
        if enable_compression and len(chunk_data) > 1024:  # 大于1KB才考虑压缩
            compression_ratio = self.compression.estimate_compression_ratio(chunk_data)
            if compression_ratio < 0.9:
                chunk_data = self.compression.compress_zlib(chunk_data)
                compressed = True
            else:
                compressed = False
        else:
            compressed = False
        
        files = {
            "chunk": (chunk['hash'], chunk_data),
            "hash": (None, chunk['hash']),
            "size": (None, str(chunk['size'])),
            "compressed": (None, str(compressed).lower())
        }
        
        self.request("POST", "/file/upload_chunk", files=files)
    
    def download_optimized(self, filename: str, save_path: str, folder: str = "") -> str:
        """优化的文件下载（支持解压）"""
        params = {"filename": filename}
        if folder:
            params["folder"] = folder
        
        # 获取文件信息
        file_info = self.request("GET", "/file/info", params=params)
        is_compressed = file_info.get("compressed", False)
        
        # 下载文件
        res = self.session.get(f"{self.base_url}/file/download", params=params, stream=True)
        if res.status_code != 200:
            raise RuntimeError(f"Download failed: {res.text}")
        
        file_data = res.content
        
        # 如果是压缩文件，解压
        if is_compressed:
            print(f"[下载优化] 解压文件: {filename}")
            file_data = self.compression.decompress_zlib(file_data)
        
        # 保存文件
        with open(save_path, "wb") as f:
            f.write(file_data)
        
        print(f"[下载优化] 文件已保存: {save_path}")
        return save_path
    
    # 兼容原有API
    def upload(self, filepath: str, folder: str = ""):
        """兼容原有上传接口"""
        return self.upload_optimized(filepath, folder, 
                                   enable_compression=True, 
                                   enable_dedup=True, 
                                   enable_chunk_dedup=True)
    
    def download(self, filename: str, save_path: str, folder: str = ""):
        """兼容原有下载接口"""
        return self.download_optimized(filename, save_path, folder)
