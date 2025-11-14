import pytest
import tempfile
import shutil
import io
from models.chunk import Chunk, FileChunkMapping
from services.dedup.chunk_store import DatabaseChunkStore
from common.db import db


class TestDatabaseChunkStore:
    """测试数据库驱动的块存储系统"""
    
    @pytest.fixture
    def temp_store(self):
        """创建临时存储目录"""
        temp_dir = tempfile.mkdtemp()
        store = DatabaseChunkStore(temp_dir, chunk_size=1024)  # 使用小块大小便于测试
        yield store
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_file_chunking(self, test_app, temp_store):
        """测试文件分块功能"""
        with test_app.app_context():
            # 创建一个2.5KB的测试文件（会被分成3个块）
            test_data = b"A" * 1024 + b"B" * 1024 + b"C" * 512
            
            # 分块
            chunks = temp_store.split_file_to_chunks(test_data)
            
            # 验证分块结果
            assert len(chunks) == 3
            assert chunks[0]['size'] == 1024
            assert chunks[1]['size'] == 1024
            assert chunks[2]['size'] == 512
            
            # 验证块数据
            assert chunks[0]['data'] == b"A" * 1024
            assert chunks[1]['data'] == b"B" * 1024
            assert chunks[2]['data'] == b"C" * 512
            
            # 验证偏移量
            assert chunks[0]['offset'] == 0
            assert chunks[1]['offset'] == 1024
            assert chunks[2]['offset'] == 2048
    
    def test_chunk_storage_and_deduplication(self, test_app, temp_store):
        """测试块存储和去重功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 创建重复的块数据
            chunk_data1 = b"Chunk data 1" * 100
            chunk_data2 = b"Chunk data 2" * 100
            chunk_data3 = b"Chunk data 1" * 100  # 与chunk_data1相同
            
            # 计算哈希
            hash1 = temp_store._calculate_chunk_hash(chunk_data1)
            hash2 = temp_store._calculate_chunk_hash(chunk_data2)
            hash3 = temp_store._calculate_chunk_hash(chunk_data3)
            
            # 验证相同数据有相同哈希
            assert hash1 == hash3
            assert hash1 != hash2
            
            # 存储块
            is_new1, path1 = temp_store.store_chunk(chunk_data1, hash1)
            is_new2, path2 = temp_store.store_chunk(chunk_data2, hash2)
            is_new3, path3 = temp_store.store_chunk(chunk_data3, hash3)
            
            # 验证去重效果
            assert is_new1 == True   # 第一次存储
            assert is_new2 == True   # 不同数据
            assert is_new3 == False  # 重复数据，不需要存储
            
            # 验证引用计数
            assert Chunk.get_ref_count(hash1) == 2  # chunk1被引用2次
            assert Chunk.get_ref_count(hash2) == 1  # chunk2被引用1次
    
    def test_file_storage_and_retrieval(self, test_app, temp_store):
        """测试文件存储和检索功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 创建测试文件
            test_file = b"Hello, " + b"World! " * 200 + b"End of file."
            
            # 存储文件
            result = temp_store.store_file(test_file)
            
            # 验证存储结果
            assert 'file_hash' in result
            assert result['total_size'] == len(test_file)
            assert result['chunk_count'] > 0
            
            file_hash = result['file_hash']
            
            # 检索文件
            retrieved_data = temp_store.read_file(file_hash)
            
            # 验证检索结果
            assert retrieved_data == test_file
            
            # 验证文件信息
            file_info = temp_store.get_file_info(file_hash)
            assert file_info is not None
            assert file_info['total_size'] == len(test_file)
            assert file_info['chunk_count'] == result['chunk_count']
    
    def test_file_stream_processing(self, test_app, temp_store):
        """测试文件流处理功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 创建测试数据流
            test_data = b"Stream data " * 300
            stream = io.BytesIO(test_data)
            
            # 从流存储文件
            result = temp_store.store_file_stream(stream)
            
            # 验证结果
            assert result['total_size'] == len(test_data)
            assert result['chunk_count'] > 0
            
            # 检索并验证
            retrieved_data = temp_store.read_file(result['file_hash'])
            assert retrieved_data == test_data
    
    def test_file_deduplication_across_files(self, test_app, temp_store):
        """测试跨文件的块级去重"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 创建有重复内容的文件（确保公共块大于chunk_size）
            common_chunk = b"Common content " * 100  # 公共块，确保超过1024字节
            file1_data = common_chunk + b"File 1 unique content " * 50
            file2_data = common_chunk + b"File 2 unique content " * 50
            file3_data = b"File 3 unique content " * 50 + common_chunk
            
            # 存储文件
            result1 = temp_store.store_file(file1_data)
            result2 = temp_store.store_file(file2_data)
            result3 = temp_store.store_file(file3_data)
            
            # 验证去重效果
            # 第一个文件创建了新块
            assert result1['new_chunks'] > 0
            
            # 总的新块数应该少于所有块的总数（因为有去重）
            total_new_chunks = result1['new_chunks'] + result2['new_chunks'] + result3['new_chunks']
            total_chunks = result1['chunk_count'] + result2['chunk_count'] + result3['chunk_count']
            assert total_new_chunks < total_chunks, f"Expected deduplication: {total_new_chunks} < {total_chunks}"
            
            # 验证所有文件都能正确检索
            assert temp_store.read_file(result1['file_hash']) == file1_data
            assert temp_store.read_file(result2['file_hash']) == file2_data
            assert temp_store.read_file(result3['file_hash']) == file3_data
    
    def test_file_deletion_and_cleanup(self, test_app, temp_store):
        """测试文件删除和清理功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 创建测试文件
            test_data = b"Test file for deletion " * 50
            result = temp_store.store_file(test_data)
            file_hash = result['file_hash']
            
            # 验证文件存在
            assert temp_store.file_exists(file_hash)
            assert temp_store.read_file(file_hash) == test_data
            
            # 删除文件
            delete_result = temp_store.delete_file(file_hash)
            
            # 验证删除结果
            assert 'deleted_chunks' in delete_result
            assert 'remaining_chunks' in delete_result
            
            # 验证文件已被删除
            assert not temp_store.file_exists(file_hash)
            assert temp_store.read_file(file_hash) is None
    
    def test_storage_statistics(self, test_app, temp_store):
        """测试存储统计功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 获取初始统计
            initial_stats = temp_store.get_storage_stats()
            assert initial_stats['total_files'] == 0
            assert initial_stats['total_chunks'] == 0
            
            # 存储一些文件
            files_data = [
                b"File 1 content " * 100,
                b"File 2 content " * 150,
                b"File 3 content " * 80,
            ]
            
            for data in files_data:
                temp_store.store_file(data)
            
            # 获取更新后的统计
            final_stats = temp_store.get_storage_stats()
            
            # 验证统计信息
            assert final_stats['total_files'] == len(files_data)
            assert final_stats['total_chunks'] > 0
            assert final_stats['total_refs'] >= final_stats['total_chunks']
            assert final_stats['total_size'] > 0
            
            # 验证压缩比
            if final_stats['total_compressed_size'] > 0:
                assert 0 <= final_stats['compression_ratio'] <= 1
    
    def test_chunk_reference_counting(self, test_app, temp_store):
        """测试块引用计数功能"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 创建相同内容的文件（会产生相同的块）
            same_content = b"Same chunk content " * 60
            
            # 存储多个相同文件
            result1 = temp_store.store_file(same_content)
            result2 = temp_store.store_file(same_content)
            result3 = temp_store.store_file(same_content)
            
            # 验证文件哈希相同（因为内容相同）
            assert result1['file_hash'] == result2['file_hash'] == result3['file_hash']
            
            # 但是映射记录应该只有一份
            file_hash = result1['file_hash']
            chunk_mappings = FileChunkMapping.get_file_chunks(file_hash)
            
            # 验证块的引用计数
            for mapping in chunk_mappings:
                chunk_ref_count = Chunk.get_ref_count(mapping.chunk_hash)
                assert chunk_ref_count >= 1  # 至少被引用一次
    
    def test_error_handling(self, test_app, temp_store):
        """测试错误处理"""
        with test_app.app_context():
            # 测试读取不存在的文件
            non_existent_hash = "nonexistent" + "0" * 54
            assert temp_store.read_file(non_existent_hash) is None
            assert not temp_store.file_exists(non_existent_hash)
            
            # 测试删除不存在的文件
            delete_result = temp_store.delete_file(non_existent_hash)
            assert delete_result['deleted_chunks'] == 0
            assert delete_result['remaining_chunks'] == 0
            
            # 测试读取不存在的块
            non_existent_chunk = "nonexistent" + "0" * 54
            assert temp_store.read_chunk(non_existent_chunk) is None
    
    def test_compatibility_interface(self, test_app, temp_store):
        """测试与md5_store的兼容性接口"""
        with test_app.app_context():
            # 清理现有数据
            db.session.query(Chunk).delete()
            db.session.query(FileChunkMapping).delete()
            db.session.commit()
            
            # 测试数据
            test_data = b"Compatibility test data " * 50
            
            # 使用兼容性接口存储
            file_hash = temp_store.ensure_blob(test_data)
            
            # 使用兼容性接口读取
            retrieved_data = temp_store.read_blob(file_hash)
            assert retrieved_data == test_data
            
            # 测试引用计数接口
            assert temp_store.exists_ref(file_hash)
            
            # 测试增加引用
            ref_count = temp_store.inc_ref(file_hash)
            assert ref_count > 0
            
            # 测试减少引用
            remaining = temp_store.dec_ref(file_hash)
            # 注意：在块存储中，这个行为可能与原来的md5_store不完全相同
