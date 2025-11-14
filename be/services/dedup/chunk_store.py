import os
import hashlib
from typing import List, Dict, Optional, Tuple, BinaryIO
from utils.compress import compress_for_storage, decompress_from_storage
from config import Config


class DatabaseChunkStore:
    """
    数据库驱动的块级去重存储系统
    - 将文件分割成固定大小的数据块
    - 对每个数据块进行去重存储
    - 支持文件的分块上传和组装下载
    - 提供块级引用计数管理
    """
    
    DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB默认块大小
    
    def __init__(self, storage_root: str = "./uploads", chunk_size: int = None):
        self.storage_root = storage_root
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
        self.chunks_dir = os.path.join(self.storage_root, ".chunks")
        os.makedirs(self.chunks_dir, exist_ok=True)
        
        # 延迟导入避免循环依赖
        from models.chunk import Chunk, FileChunkMapping
        self.Chunk = Chunk
        self.FileChunkMapping = FileChunkMapping
    
    # -------- 文件分块算法 --------
    def split_file_to_chunks(self, file_data: bytes) -> List[Dict]:
        """
        将文件数据分割成固定大小的数据块
        
        Args:
            file_data: 文件二进制数据
            
        Returns:
            List[Dict]: 块信息列表 [{'data': bytes, 'hash': str, 'index': int, 'offset': int, 'size': int}, ...]
        """
        chunks = []
        offset = 0
        chunk_index = 0
        
        while offset < len(file_data):
            # 计算当前块的大小
            current_chunk_size = min(self.chunk_size, len(file_data) - offset)
            
            # 提取块数据
            chunk_data = file_data[offset:offset + current_chunk_size]
            
            # 计算块哈希
            chunk_hash = self._calculate_chunk_hash(chunk_data)
            
            chunks.append({
                'data': chunk_data,
                'hash': chunk_hash,
                'index': chunk_index,
                'offset': offset,
                'size': current_chunk_size
            })
            
            offset += current_chunk_size
            chunk_index += 1
        
        return chunks
    
    def split_file_stream_to_chunks(self, file_stream: BinaryIO) -> List[Dict]:
        """
        从文件流中分割数据块（适用于大文件）
        
        Args:
            file_stream: 文件流对象
            
        Returns:
            List[Dict]: 块信息列表
        """
        chunks = []
        offset = 0
        chunk_index = 0
        
        while True:
            # 读取一个块的数据
            chunk_data = file_stream.read(self.chunk_size)
            if not chunk_data:
                break
            
            # 计算块哈希
            chunk_hash = self._calculate_chunk_hash(chunk_data)
            
            chunks.append({
                'data': chunk_data,
                'hash': chunk_hash,
                'index': chunk_index,
                'offset': offset,
                'size': len(chunk_data)
            })
            
            offset += len(chunk_data)
            chunk_index += 1
        
        return chunks
    
    def _calculate_chunk_hash(self, chunk_data: bytes) -> str:
        """计算数据块的SHA256哈希值"""
        return hashlib.sha256(chunk_data).hexdigest()
    
    def _calculate_file_hash(self, chunks: List[Dict]) -> str:
        """根据所有块的哈希计算文件的整体哈希"""
        hasher = hashlib.sha256()
        for chunk in sorted(chunks, key=lambda x: x['index']):
            hasher.update(chunk['hash'].encode('utf-8'))
        return hasher.hexdigest()
    
    # -------- 块存储路径管理 --------
    def _get_chunk_storage_path(self, chunk_hash: str) -> str:
        """获取数据块的存储路径"""
        # 使用哈希的前两位作为子目录，避免单个目录文件过多
        subdir = chunk_hash[:2]
        chunk_dir = os.path.join(self.chunks_dir, subdir)
        os.makedirs(chunk_dir, exist_ok=True)
        return os.path.join(chunk_dir, chunk_hash)
    
    # -------- 块级去重逻辑 --------
    def store_chunk(self, chunk_data: bytes, chunk_hash: str) -> Tuple[bool, str]:
        """
        存储数据块（如果不存在的话）
        
        Args:
            chunk_data: 块数据
            chunk_hash: 块哈希
            
        Returns:
            Tuple[bool, str]: (是否为新块, 存储路径)
        """
        storage_path = self._get_chunk_storage_path(chunk_hash)
        
        # 检查块是否已存在
        if self.Chunk.exists(chunk_hash):
            # 块已存在，只需增加引用计数
            self.Chunk.increment_ref(chunk_hash)
            return False, storage_path
        
        # 块不存在，需要存储
        try:
            # 压缩并存储块数据
            compressed_data = compress_for_storage(
                chunk_data, 
                enabled=getattr(Config, "ENABLE_COMPRESSION", True)
            )
            
            with open(storage_path, "wb") as f:
                f.write(compressed_data)
            
            # 在数据库中记录块信息
            self.Chunk.increment_ref(
                chunk_hash=chunk_hash,
                chunk_size=len(chunk_data),
                storage_path=storage_path,
                compressed_size=len(compressed_data)
            )
            
            return True, storage_path
            
        except Exception as e:
            # 如果存储失败，清理可能创建的文件
            if os.path.exists(storage_path):
                os.remove(storage_path)
            raise e
    
    def read_chunk(self, chunk_hash: str) -> Optional[bytes]:
        """
        读取数据块
        
        Args:
            chunk_hash: 块哈希
            
        Returns:
            Optional[bytes]: 块数据，如果不存在则返回None
        """
        chunk = self.Chunk.query.filter_by(chunk_hash=chunk_hash).first()
        if not chunk:
            return None
        
        storage_path = chunk.storage_path
        if not os.path.exists(storage_path):
            return None
        
        try:
            with open(storage_path, "rb") as f:
                compressed_data = f.read()
            
            # 解压缩数据
            chunk_data = decompress_from_storage(
                compressed_data,
                enabled=getattr(Config, "ENABLE_COMPRESSION", True)
            )
            
            return chunk_data
            
        except Exception:
            return None
    
    def delete_chunk(self, chunk_hash: str) -> bool:
        """
        删除数据块（减少引用计数，如果为0则物理删除）
        
        Args:
            chunk_hash: 块哈希
            
        Returns:
            bool: 是否实际删除了块文件
        """
        result = self.Chunk.decrement_ref(chunk_hash)
        if isinstance(result, tuple):
            ref_count, storage_path = result
            if ref_count == 0 and storage_path:
                # 引用计数为0，删除物理文件
                try:
                    if os.path.exists(storage_path):
                        os.remove(storage_path)
                    return True
                except Exception:
                    pass
        
        return False
    
    # -------- 文件级操作 --------
    def store_file(self, file_data: bytes) -> Dict:
        """
        存储文件（分块去重）
        
        Args:
            file_data: 文件二进制数据
            
        Returns:
            Dict: 文件存储信息 {'file_hash': str, 'total_size': int, 'chunk_count': int, 'new_chunks': int}
        """
        # 分割文件为数据块
        chunks = self.split_file_to_chunks(file_data)
        
        # 计算文件整体哈希
        file_hash = self._calculate_file_hash(chunks)
        
        # 存储每个数据块
        new_chunks_count = 0
        chunk_mappings = []
        
        for chunk in chunks:
            is_new_chunk, storage_path = self.store_chunk(chunk['data'], chunk['hash'])
            if is_new_chunk:
                new_chunks_count += 1
            
            # 记录块映射信息
            chunk_mappings.append({
                'chunk_hash': chunk['hash'],
                'chunk_index': chunk['index'],
                'chunk_offset': chunk['offset'],
                'chunk_size': chunk['size']
            })
        
        # 创建文件-块映射关系
        self.FileChunkMapping.create_mapping(file_hash, chunk_mappings)
        
        return {
            'file_hash': file_hash,
            'total_size': len(file_data),
            'chunk_count': len(chunks),
            'new_chunks': new_chunks_count
        }
    
    def store_file_stream(self, file_stream: BinaryIO) -> Dict:
        """
        从文件流存储文件（适用于大文件）
        
        Args:
            file_stream: 文件流对象
            
        Returns:
            Dict: 文件存储信息
        """
        # 分割文件流为数据块
        chunks = self.split_file_stream_to_chunks(file_stream)
        
        # 计算文件整体哈希
        file_hash = self._calculate_file_hash(chunks)
        
        # 存储每个数据块
        new_chunks_count = 0
        chunk_mappings = []
        total_size = 0
        
        for chunk in chunks:
            is_new_chunk, storage_path = self.store_chunk(chunk['data'], chunk['hash'])
            if is_new_chunk:
                new_chunks_count += 1
            
            total_size += chunk['size']
            
            # 记录块映射信息
            chunk_mappings.append({
                'chunk_hash': chunk['hash'],
                'chunk_index': chunk['index'],
                'chunk_offset': chunk['offset'],
                'chunk_size': chunk['size']
            })
        
        # 创建文件-块映射关系
        self.FileChunkMapping.create_mapping(file_hash, chunk_mappings)
        
        return {
            'file_hash': file_hash,
            'total_size': total_size,
            'chunk_count': len(chunks),
            'new_chunks': new_chunks_count
        }
    
    # -------- 文件组装功能 --------
    def read_file(self, file_hash: str) -> Optional[bytes]:
        """
        读取并组装文件
        
        Args:
            file_hash: 文件哈希
            
        Returns:
            Optional[bytes]: 文件数据，如果不存在则返回None
        """
        # 获取文件的块映射信息
        chunk_mappings = self.FileChunkMapping.get_file_chunks(file_hash)
        if not chunk_mappings:
            return None
        
        # 按顺序读取并组装数据块
        file_data = bytearray()
        
        for mapping in chunk_mappings:
            chunk_data = self.read_chunk(mapping.chunk_hash)
            if chunk_data is None:
                # 如果任何一个块读取失败，整个文件读取失败
                return None
            
            # 验证块大小
            if len(chunk_data) != mapping.chunk_size:
                return None
            
            file_data.extend(chunk_data)
        
        return bytes(file_data)
    
    def delete_file(self, file_hash: str) -> Dict:
        """
        删除文件（减少所有相关块的引用计数）
        
        Args:
            file_hash: 文件哈希
            
        Returns:
            Dict: 删除统计信息 {'deleted_chunks': int, 'remaining_chunks': int}
        """
        # 获取文件使用的所有块
        chunk_hashes = self.FileChunkMapping.delete_file_mapping(file_hash)
        
        # 减少每个块的引用计数
        deleted_chunks = 0
        remaining_chunks = 0
        
        for chunk_hash in chunk_hashes:
            if self.delete_chunk(chunk_hash):
                deleted_chunks += 1
            else:
                remaining_chunks += 1
        
        return {
            'deleted_chunks': deleted_chunks,
            'remaining_chunks': remaining_chunks
        }
    
    def get_file_info(self, file_hash: str) -> Optional[Dict]:
        """获取文件信息"""
        return self.FileChunkMapping.get_file_info(file_hash)
    
    def file_exists(self, file_hash: str) -> bool:
        """检查文件是否存在"""
        return len(self.FileChunkMapping.get_file_chunks(file_hash)) > 0
    
    # -------- 统计和维护功能 --------
    def get_storage_stats(self) -> Dict:
        """获取存储统计信息"""
        chunk_stats = self.Chunk.get_storage_stats()
        
        # 获取文件统计
        file_count = self.FileChunkMapping.query.with_entities(
            self.FileChunkMapping.file_hash
        ).distinct().count()
        
        return {
            **chunk_stats,
            'total_files': file_count,
            'avg_chunks_per_file': chunk_stats['total_refs'] / file_count if file_count > 0 else 0
        }
    
    def cleanup_orphaned_chunks(self) -> int:
        """清理孤立的数据块文件（数据库中没有记录的文件）"""
        if not os.path.exists(self.chunks_dir):
            return 0
        
        cleaned_count = 0
        
        # 遍历所有子目录
        for subdir in os.listdir(self.chunks_dir):
            subdir_path = os.path.join(self.chunks_dir, subdir)
            if not os.path.isdir(subdir_path):
                continue
            
            # 遍历子目录中的文件
            for filename in os.listdir(subdir_path):
                file_path = os.path.join(subdir_path, filename)
                
                # 检查是否是有效的哈希文件名
                if len(filename) == 64 and all(c in '0123456789abcdef' for c in filename.lower()):
                    if not self.Chunk.exists(filename):
                        # 数据库中没有记录，删除孤立文件
                        try:
                            os.remove(file_path)
                            cleaned_count += 1
                        except FileNotFoundError:
                            pass
        
        return cleaned_count
    
    # -------- 兼容性接口（用于替换md5_store） --------
    def ensure_blob(self, data: bytes) -> str:
        """
        兼容md5_store的接口：存储数据并返回标识符
        
        Args:
            data: 文件数据
            
        Returns:
            str: 文件哈希标识符
        """
        result = self.store_file(data)
        return result['file_hash']
    
    def read_blob(self, file_hash: str) -> Optional[bytes]:
        """
        兼容md5_store的接口：读取数据
        
        Args:
            file_hash: 文件哈希
            
        Returns:
            Optional[bytes]: 文件数据
        """
        return self.read_file(file_hash)
    
    def inc_ref(self, file_hash: str, blob_size: int = None) -> int:
        """
        兼容md5_store的接口：增加引用计数
        注意：在块存储中，这个操作比较复杂，因为需要对所有块增加引用
        """
        chunk_mappings = self.FileChunkMapping.get_file_chunks(file_hash)
        if not chunk_mappings:
            return 0
        
        # 对所有块增加引用计数
        for mapping in chunk_mappings:
            self.Chunk.increment_ref(mapping.chunk_hash)
        
        return len(chunk_mappings)  # 返回块数量作为引用计数
    
    def dec_ref(self, file_hash: str) -> int:
        """
        兼容md5_store的接口：减少引用计数
        """
        result = self.delete_file(file_hash)
        return result['remaining_chunks']
    
    def exists_ref(self, file_hash: str) -> bool:
        """
        兼容md5_store的接口：检查是否存在
        """
        return self.file_exists(file_hash)


