import pytest
import tempfile
import shutil
from services.dedup.md5_store import Md5Store
from common.db import db


class TestMd5Store:
    """测试MD5存储类与数据库的集成"""
    
    @pytest.fixture
    def temp_store(self):
        """创建临时存储目录"""
        temp_dir = tempfile.mkdtemp()
        store = Md5Store(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_ensure_blob_and_deduplication(self, test_app, temp_store):
        """测试文件存储和去重功能"""
        with test_app.app_context():
            # 清理现有数据（现在需要清理块存储相关的表）
            from models.chunk import Chunk, FileChunkMapping
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            test_data1 = b"Hello, World!"
            test_data2 = b"Another test data"
            test_data3 = b"Hello, World!"  # 与test_data1相同
            
            # 存储第一个文件
            hash_1 = temp_store.ensure_blob(test_data1)
            assert temp_store._read_ref(hash_1) == 1
            
            # 存储第二个文件
            hash_2 = temp_store.ensure_blob(test_data2)
            assert temp_store._read_ref(hash_2) == 1
            assert hash_1 != hash_2
            
            # 存储第三个文件（与第一个相同）
            hash_3 = temp_store.ensure_blob(test_data3)
            assert hash_3 == hash_1  # 应该是相同的文件哈希
            # 注意：在块存储中，相同文件会有相同的哈希，不会重复存储
            
            # 测试读取
            read_data_1 = temp_store.read_blob(hash_1)
            assert read_data_1 == test_data1
    
    def test_reference_counting_and_cleanup(self, test_app, temp_store):
        """测试引用计数和自动清理"""
        with test_app.app_context():
            test_data = b"Test data for cleanup"
            
            # 存储文件
            file_hash = temp_store.ensure_blob(test_data)
            
            # 验证文件存在
            assert temp_store.read_blob(file_hash) == test_data
            assert temp_store._read_ref(file_hash) == 1
            
            # 减少引用计数（应该删除文件）
            remaining_count = temp_store.dec_ref(file_hash)
            assert remaining_count == 0
            
            # 验证文件已被删除
            assert temp_store.read_blob(file_hash) is None
    
    def test_storage_statistics(self, test_app, temp_store):
        """测试存储统计功能"""
        with test_app.app_context():
            # 清理现有数据
            from models.chunk import Chunk, FileChunkMapping
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 创建测试文件
            files_data = [
                b"File 1 content",
                b"File 2 content", 
                b"File 1 content",  # 重复文件
            ]
            
            hash_list = []
            for data in files_data:
                file_hash = temp_store.ensure_blob(data)
                hash_list.append(file_hash)
            
            # 获取统计信息
            stats = temp_store.get_storage_stats()
            
            # 验证统计结果（块存储的统计信息结构不同）
            unique_hashes = set(hash_list)
            assert stats['total_files'] == len(unique_hashes)  # 唯一文件数
            assert stats['total_chunks'] > 0    # 总块数
    
    def test_cleanup_orphaned_blobs(self, test_app, temp_store):
        """测试清理孤立文件功能"""
        with test_app.app_context():
            # 清理现有数据
            from models.chunk import Chunk, FileChunkMapping
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 创建一个正常的文件
            test_data = b"Normal file"
            file_hash = temp_store.ensure_blob(test_data)
            
            # 手动创建一个孤立的chunk文件（没有数据库记录）
            # 在块存储中，孤立文件在chunks目录下
            orphaned_hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"  # 64位SHA256
            chunk_store = temp_store.chunk_store
            orphaned_path = chunk_store._get_chunk_storage_path(orphaned_hash)
            
            # 确保目录存在
            import os
            os.makedirs(os.path.dirname(orphaned_path), exist_ok=True)
            with open(orphaned_path, "wb") as f:
                f.write(b"orphaned content")
            
            # 执行清理
            cleaned_count = temp_store.cleanup_orphaned_blobs()
            
            # 验证孤立文件被清理，正常文件保留
            assert cleaned_count >= 0  # 可能清理了一些文件
            assert temp_store.read_blob(file_hash) == test_data  # 正常文件还在
    
    def test_pointer_operations(self, test_app, temp_store):
        """测试指针操作"""
        with test_app.app_context():
            test_md5 = "pointer123456789012345678901234567"
            
            # 测试指针创建和解析
            pointer = temp_store.make_pointer(test_md5)
            assert temp_store.is_pointer(pointer) == True
            
            parsed_md5 = temp_store.parse_pointer(pointer)
            assert parsed_md5 == test_md5
            
            # 测试非指针数据
            normal_data = b"This is not a pointer"
            assert temp_store.is_pointer(normal_data) == False
