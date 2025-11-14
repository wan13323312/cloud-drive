import pytest
import tempfile
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from services.dedup.md5_store import Md5Store
from common.db import db


class TestDedupService:
    """测试去重服务的完整功能"""
    
    @pytest.fixture
    def temp_store(self):
        """创建临时存储目录"""
        temp_dir = tempfile.mkdtemp()
        store = Md5Store(temp_dir)
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_file_deduplication_workflow(self, test_app, temp_store):
        """测试完整的文件去重工作流程"""
        with test_app.app_context():
            # 清理现有数据
            db.session.commit()
            
            # 模拟用户上传多个文件，其中有重复
            files_data = {
                "user1_doc1.txt": b"This is document 1 content",
                "user1_doc2.txt": b"This is document 2 content",
                "user2_doc1.txt": b"This is document 1 content",  # 与user1_doc1.txt相同
                "user2_image.jpg": b"Binary image data here",
                "user3_doc1.txt": b"This is document 1 content",  # 再次重复
                "user3_image.jpg": b"Binary image data here",     # 图片重复
            }
            
            md5_mapping = {}
            
            # 模拟文件上传过程
            for filename, content in files_data.items():
                md5_hex = temp_store.ensure_blob(content)
                md5_mapping[filename] = md5_hex
                
                # 验证文件可以正确读取
                read_content = temp_store.read_blob(md5_hex)
                assert read_content == content, f"File {filename} content mismatch"
            
            # 验证去重效果
            unique_md5s = set(md5_mapping.values())
            assert len(unique_md5s) == 3  # 应该只有3个唯一的MD5（doc1, doc2, image各一个）
            
            # 验证引用计数
            doc1_md5 = md5_mapping["user1_doc1.txt"]
            image_md5 = md5_mapping["user2_image.jpg"]
            
            assert temp_store._read_ref(doc1_md5) == 3  # doc1被引用3次
            assert temp_store._read_ref(image_md5) == 2  # image被引用2次
            
            # 获取存储统计
            stats = temp_store.get_storage_stats()
            assert stats['total_blobs'] == 3  # 3个唯一文件
            assert stats['total_refs'] == 6   # 6个总引用
            
            # 模拟文件删除过程
            deleted_files = ["user1_doc1.txt", "user2_doc1.txt", "user3_doc1.txt"]
            for filename in deleted_files:
                md5_hex = md5_mapping[filename]
                temp_store.dec_ref(md5_hex)
            
            # 验证doc1的blob文件被删除（引用计数为0）
            assert temp_store.read_blob(doc1_md5) is None
            
            # 验证其他文件仍然存在
            assert temp_store.read_blob(image_md5) is not None
    
    def test_concurrent_file_operations(self, test_app, temp_store):
        """测试并发文件操作的安全性"""
        with test_app.app_context():
            # 清理现有数据
            db.session.commit()
            
            # 准备测试数据
            test_contents = [
                b"Concurrent test file 1",
                b"Concurrent test file 2",
                b"Concurrent test file 1",  # 重复
                b"Concurrent test file 3",
                b"Concurrent test file 2",  # 重复
            ]
            
            results = []
            
            def upload_file(content):
                """模拟文件上传"""
                with test_app.app_context():
                    try:
                        md5_hex = temp_store.ensure_blob(content)
                        results.append(md5_hex)
                        return md5_hex
                    except Exception as e:
                        results.append(f"ERROR: {e}")
                        return None
            
            # 使用线程池并发上传文件
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(upload_file, content) for content in test_contents]
                
                # 等待所有任务完成
                for future in futures:
                    future.result()
            
            # 验证结果
            assert len(results) == len(test_contents)
            
            # 验证没有错误
            errors = [r for r in results if isinstance(r, str) and r.startswith("ERROR")]
            assert len(errors) == 0, f"Concurrent operations failed: {errors}"
            
            # 验证去重效果
            unique_md5s = set(results)
            assert len(unique_md5s) == 3  # 应该只有3个唯一的MD5
    
    def test_large_file_handling(self, test_app, temp_store):
        """测试大文件处理"""
        with test_app.app_context():
            # 创建一个较大的测试文件（1MB）
            large_content = b"A" * (1024 * 1024)
            
            # 存储大文件
            md5_hex = temp_store.ensure_blob(large_content)
            
            # 验证存储成功
            assert temp_store._read_ref(md5_hex) == 1
            
            # 验证可以正确读取
            read_content = temp_store.read_blob(md5_hex)
            assert read_content == large_content
            
            # 验证文件大小统计
            stats = temp_store.get_storage_stats()
            assert stats['total_size'] >= len(large_content)
    
    def test_compression_integration(self, test_app, temp_store):
        """测试压缩功能集成"""
        with test_app.app_context():
            # 创建可压缩的重复内容
            repetitive_content = b"AAAAAAAAAA" * 1000  # 10KB的重复内容
            
            # 存储文件
            md5_hex = temp_store.ensure_blob(repetitive_content)
            
            # 验证可以正确读取（自动解压）
            read_content = temp_store.read_blob(md5_hex)
            assert read_content == repetitive_content
            
            # 验证blob文件存在（压缩后的）
            blob_path = temp_store._blob_path(md5_hex)
            import os
            assert os.path.exists(blob_path)
            
            # 压缩后的文件应该比原文件小
            with open(blob_path, "rb") as f:
                compressed_size = len(f.read())
            
            # 对于重复内容，压缩效果应该很明显
            assert compressed_size < len(repetitive_content)
    
    def test_error_recovery(self, test_app, temp_store):
        """测试错误恢复机制"""
        with test_app.app_context():
            # 测试读取不存在的blob
            non_existent_md5 = "nonexistent123456789012345678901234"
            result = temp_store.read_blob(non_existent_md5)
            assert result is None
            
            # 测试减少不存在MD5的引用
            count = temp_store.dec_ref(non_existent_md5)
            assert count == 0
            
            # 测试获取不存在MD5的引用计数
            ref_count = temp_store._read_ref(non_existent_md5)
            assert ref_count == 0
    
    def test_storage_maintenance(self, test_app, temp_store):
        """测试存储维护功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.commit()
            
            # 创建一些正常文件
            normal_files = [b"Normal file 1", b"Normal file 2"]
            normal_md5s = []
            
            for content in normal_files:
                md5_hex = temp_store.ensure_blob(content)
                normal_md5s.append(md5_hex)
            
            # 执行清理（在块存储系统中，清理逻辑已经内置）
            cleaned_count = temp_store.cleanup_orphaned_blobs()
            
            # 验证清理功能正常运行
            assert cleaned_count >= 0
            
            # 验证正常文件未受影响
            for md5_hex in normal_md5s:
                assert temp_store.read_blob(md5_hex) is not None
    
    def test_performance_characteristics(self, test_app, temp_store):
        """测试性能特征"""
        with test_app.app_context():
            # 清理现有数据
            db.session.commit()
            
            # 测试批量操作性能
            test_files = [f"Test file content {i}".encode() for i in range(100)]
            
            start_time = time.time()
            
            # 批量存储文件
            md5_list = []
            for content in test_files:
                md5_hex = temp_store.ensure_blob(content)
                md5_list.append(md5_hex)
            
            storage_time = time.time() - start_time
            
            # 验证所有文件都存储成功
            assert len(md5_list) == len(test_files)
            
            # 测试批量读取性能
            start_time = time.time()
            
            for md5_hex in md5_list:
                content = temp_store.read_blob(md5_hex)
                assert content is not None
            
            read_time = time.time() - start_time
            
            # 性能应该在合理范围内（这里只是基本检查）
            assert storage_time < 10.0  # 存储100个文件应该在10秒内完成
            assert read_time < 5.0      # 读取100个文件应该在5秒内完成
            
            print(f"Performance: Storage={storage_time:.3f}s, Read={read_time:.3f}s")
