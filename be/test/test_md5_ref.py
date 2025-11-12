import pytest
import tempfile
import shutil
from models.md5_ref import Md5Ref
from services.dedup.md5_store import Md5Store
from common.db import db


class TestMd5Ref:
    """测试MD5引用计数数据库模型"""
    
    def test_create_and_increment(self, test_app):
        """测试创建和增加引用计数"""
        with test_app.app_context():
            test_md5 = "abcdef1234567890abcdef1234567890"
            
            # 第一次增加引用
            count1 = Md5Ref.increment_ref(test_md5, 1024)
            assert count1 == 1
            
            # 再次增加引用
            count2 = Md5Ref.increment_ref(test_md5, 1024)
            assert count2 == 2
            
            # 验证引用计数
            current_count = Md5Ref.get_ref_count(test_md5)
            assert current_count == 2
            
            # 验证存在性
            assert Md5Ref.exists(test_md5) == True
    
    def test_decrement_and_delete(self, test_app):
        """测试减少引用计数和自动删除"""
        with test_app.app_context():
            test_md5 = "fedcba0987654321fedcba0987654321"
            
            # 创建引用
            Md5Ref.increment_ref(test_md5, 512)
            Md5Ref.increment_ref(test_md5, 512)
            assert Md5Ref.get_ref_count(test_md5) == 2
            
            # 减少引用
            count1 = Md5Ref.decrement_ref(test_md5)
            assert count1 == 1
            
            # 再次减少引用（应该删除记录）
            count2 = Md5Ref.decrement_ref(test_md5)
            assert count2 == 0
            
            # 验证记录已删除
            assert Md5Ref.exists(test_md5) == False
            assert Md5Ref.get_ref_count(test_md5) == 0
    
    def test_get_or_create_thread_safe(self, test_app):
        """测试get_or_create的线程安全性"""
        with test_app.app_context():
            test_md5 = "threadsafe123456789012345678901234"
            
            # 第一次调用应该创建新记录
            ref1, is_new1 = Md5Ref.get_or_create(test_md5, 256)
            assert is_new1 == True
            assert ref1.md5_hash == test_md5
            assert ref1.blob_size == 256
            
            # 第二次调用应该返回现有记录
            ref2, is_new2 = Md5Ref.get_or_create(test_md5, 256)
            assert is_new2 == False
            assert ref2.id == ref1.id
    
    def test_storage_stats(self, test_app):
        """测试存储统计功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Md5Ref).delete()
            db.session.commit()
            
            # 创建测试数据
            test_data = [
                ("stats_md5_1", 100),
                ("stats_md5_2", 200),
                ("stats_md5_3", 300),
            ]
            
            for md5, size in test_data:
                Md5Ref.increment_ref(md5, size)
                Md5Ref.increment_ref(md5, size)  # 每个MD5有2个引用
            
            # 获取统计信息
            stats = Md5Ref.get_storage_stats()
            
            assert stats['total_blobs'] == 3  # 3个不同的MD5
            assert stats['total_refs'] == 6   # 总共6个引用
            assert stats['total_size'] == 600 # 总大小
    
    def test_error_handling(self, test_app):
        """测试错误处理"""
        with test_app.app_context():
            non_existent_md5 = "nonexistent123456789012345678901234"
            
            # 测试不存在的MD5
            assert Md5Ref.get_ref_count(non_existent_md5) == 0
            assert Md5Ref.exists(non_existent_md5) == False
            
            # 测试减少不存在MD5的引用
            result = Md5Ref.decrement_ref(non_existent_md5)
            assert result == 0


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
            # 清理现有数据
            db.session.query(Md5Ref).delete()
            db.session.commit()
            
            test_data1 = b"Hello, World!"
            test_data2 = b"Another test data"
            test_data3 = b"Hello, World!"  # 与test_data1相同
            
            # 存储第一个文件
            md5_1 = temp_store.ensure_blob(test_data1)
            assert temp_store._read_ref(md5_1) == 1
            
            # 存储第二个文件
            md5_2 = temp_store.ensure_blob(test_data2)
            assert temp_store._read_ref(md5_2) == 1
            assert md5_1 != md5_2
            
            # 存储第三个文件（与第一个相同）
            md5_3 = temp_store.ensure_blob(test_data3)
            assert md5_3 == md5_1  # 应该是相同的MD5
            assert temp_store._read_ref(md5_1) == 2  # 引用计数应该增加
            
            # 测试读取
            read_data_1 = temp_store.read_blob(md5_1)
            assert read_data_1 == test_data1
    
    def test_reference_counting_and_cleanup(self, test_app, temp_store):
        """测试引用计数和自动清理"""
        with test_app.app_context():
            test_data = b"Test data for cleanup"
            
            # 存储文件
            md5_hex = temp_store.ensure_blob(test_data)
            blob_path = temp_store._blob_path(md5_hex)
            
            # 验证文件存在
            assert temp_store.read_blob(md5_hex) == test_data
            assert temp_store._read_ref(md5_hex) == 1
            
            # 减少引用计数（应该删除blob文件）
            remaining_count = temp_store.dec_ref(md5_hex)
            assert remaining_count == 0
            
            # 验证文件已被删除
            assert temp_store.read_blob(md5_hex) is None
    
    def test_storage_statistics(self, test_app, temp_store):
        """测试存储统计功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Md5Ref).delete()
            db.session.commit()
            
            # 创建测试文件
            files_data = [
                b"File 1 content",
                b"File 2 content", 
                b"File 1 content",  # 重复文件
            ]
            
            md5_list = []
            for data in files_data:
                md5_hex = temp_store.ensure_blob(data)
                md5_list.append(md5_hex)
            
            # 获取统计信息
            stats = temp_store.get_storage_stats()
            
            # 验证统计结果
            unique_md5s = set(md5_list)
            assert stats['total_blobs'] == len(unique_md5s)  # 唯一文件数
            assert stats['total_refs'] == len(files_data)    # 总引用数
    
    def test_cleanup_orphaned_blobs(self, test_app, temp_store):
        """测试清理孤立文件功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Md5Ref).delete()
            db.session.commit()
            
            # 创建一个正常的文件
            test_data = b"Normal file"
            md5_hex = temp_store.ensure_blob(test_data)
            
            # 手动创建一个孤立的blob文件（没有数据库记录）
            orphaned_md5 = "abcdef1234567890abcdef1234567890"  # 有效的32位MD5
            orphaned_path = temp_store._blob_path(orphaned_md5)
            with open(orphaned_path, "wb") as f:
                f.write(b"orphaned content")
            
            # 执行清理
            cleaned_count = temp_store.cleanup_orphaned_blobs()
            
            # 验证孤立文件被清理，正常文件保留
            assert cleaned_count == 1
            assert temp_store.read_blob(md5_hex) == test_data  # 正常文件还在
            import os
            assert not os.path.exists(orphaned_path)  # 孤立文件被删除
    
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
