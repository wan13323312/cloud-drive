from models.base import BaseModel
from common.db import db
from sqlalchemy import func, Index


class Chunk(BaseModel):
    """数据块表 - 存储文件的数据块信息"""
    __tablename__ = 'chunks'

    chunk_hash = db.Column(db.String(64), unique=True, nullable=False, index=True)  # SHA256哈希
    chunk_size = db.Column(db.Integer, nullable=False)  # 块大小
    ref_count = db.Column(db.Integer, default=1, nullable=False)  # 引用计数
    storage_path = db.Column(db.String(512), nullable=False)  # 存储路径
    compressed_size = db.Column(db.Integer, nullable=True)  # 压缩后大小

    @classmethod
    def get_or_create(cls, chunk_hash: str, chunk_size: int, storage_path: str, compressed_size: int = None):
        """获取或创建数据块记录（线程安全）"""
        chunk = cls.query.filter_by(chunk_hash=chunk_hash).first()
        if chunk:
            return chunk, False  # 已存在
        else:
            try:
                chunk = cls(
                    chunk_hash=chunk_hash,
                    chunk_size=chunk_size,
                    ref_count=0,
                    storage_path=storage_path,
                    compressed_size=compressed_size
                )
                db.session.add(chunk)
                db.session.flush()  # 立即执行INSERT，可能触发UNIQUE约束错误
                return chunk, True  # 新创建
            except Exception as e:
                # 如果发生UNIQUE约束错误，说明其他线程已经创建了记录
                db.session.rollback()
                chunk = cls.query.filter_by(chunk_hash=chunk_hash).first()
                if chunk:
                    return chunk, False  # 其他线程创建的记录
                else:
                    # 如果还是找不到，重新抛出异常
                    raise e

    @classmethod
    def increment_ref(cls, chunk_hash: str, chunk_size: int = None, storage_path: str = None, compressed_size: int = None, commit: bool = True):
        """增加引用计数"""
        chunk, is_new = cls.get_or_create(chunk_hash, chunk_size, storage_path, compressed_size)
        chunk.ref_count += 1
        if commit:
            db.session.commit()
        return chunk.ref_count

    @classmethod
    def decrement_ref(cls, chunk_hash: str):
        """减少引用计数，返回新的计数值，如果为0则删除记录"""
        chunk = cls.query.filter_by(chunk_hash=chunk_hash).first()
        if not chunk:
            return 0
        
        chunk.ref_count = max(0, chunk.ref_count - 1)
        
        if chunk.ref_count == 0:
            # 引用计数为0时删除记录
            storage_path = chunk.storage_path
            db.session.delete(chunk)
            db.session.commit()
            return 0, storage_path  # 返回存储路径以便删除文件
        else:
            db.session.commit()
            return chunk.ref_count, None

    @classmethod
    def get_ref_count(cls, chunk_hash: str):
        """获取引用计数"""
        chunk = cls.query.filter_by(chunk_hash=chunk_hash).first()
        return chunk.ref_count if chunk else 0

    @classmethod
    def exists(cls, chunk_hash: str):
        """检查数据块是否存在"""
        return cls.query.filter_by(chunk_hash=chunk_hash).first() is not None

    @classmethod
    def get_storage_stats(cls):
        """获取存储统计信息"""
        result = db.session.query(
            func.count(cls.id).label('total_chunks'),
            func.sum(cls.ref_count).label('total_refs'),
            func.sum(cls.chunk_size).label('total_size'),
            func.sum(cls.compressed_size).label('total_compressed_size')
        ).first()
        
        return {
            'total_chunks': result.total_chunks or 0,
            'total_refs': result.total_refs or 0,
            'total_size': result.total_size or 0,
            'total_compressed_size': result.total_compressed_size or 0,
            'compression_ratio': (result.total_compressed_size / result.total_size) if result.total_size else 0
        }

    def __repr__(self):
        return f'<Chunk {self.chunk_hash[:8]}... size={self.chunk_size} refs={self.ref_count}>'


class FileChunkMapping(BaseModel):
    """文件-数据块映射表 - 记录文件由哪些数据块组成"""
    __tablename__ = 'file_chunk_mappings'

    file_hash = db.Column(db.String(64), nullable=False, index=True)  # 文件的整体哈希
    chunk_hash = db.Column(db.String(64), nullable=False, index=True)  # 数据块哈希
    chunk_index = db.Column(db.Integer, nullable=False)  # 块在文件中的顺序
    chunk_offset = db.Column(db.BigInteger, nullable=False)  # 块在文件中的偏移量
    chunk_size = db.Column(db.Integer, nullable=False)  # 块大小

    # 创建复合索引以提高查询性能
    __table_args__ = (
        Index('idx_file_chunk', 'file_hash', 'chunk_index'),
        Index('idx_chunk_file', 'chunk_hash', 'file_hash'),
    )

    @classmethod
    def create_mapping(cls, file_hash: str, chunk_mappings: list):
        """创建文件-块映射关系
        
        Args:
            file_hash: 文件哈希
            chunk_mappings: 块映射列表 [{'chunk_hash': str, 'chunk_index': int, 'chunk_offset': int, 'chunk_size': int}, ...]
        """
        # 先删除现有映射
        cls.query.filter_by(file_hash=file_hash).delete()
        
        # 创建新映射
        for mapping in chunk_mappings:
            file_chunk = cls(
                file_hash=file_hash,
                chunk_hash=mapping['chunk_hash'],
                chunk_index=mapping['chunk_index'],
                chunk_offset=mapping['chunk_offset'],
                chunk_size=mapping['chunk_size']
            )
            db.session.add(file_chunk)
        
        db.session.commit()

    @classmethod
    def get_file_chunks(cls, file_hash: str):
        """获取文件的所有数据块信息，按顺序排列"""
        return cls.query.filter_by(file_hash=file_hash).order_by(cls.chunk_index).all()

    @classmethod
    def get_chunk_files(cls, chunk_hash: str):
        """获取使用某个数据块的所有文件"""
        return cls.query.filter_by(chunk_hash=chunk_hash).all()

    @classmethod
    def delete_file_mapping(cls, file_hash: str):
        """删除文件的所有块映射"""
        mappings = cls.query.filter_by(file_hash=file_hash).all()
        chunk_hashes = [m.chunk_hash for m in mappings]
        
        # 删除映射记录
        cls.query.filter_by(file_hash=file_hash).delete()
        db.session.commit()
        
        return chunk_hashes  # 返回需要减少引用计数的块哈希列表

    @classmethod
    def get_file_info(cls, file_hash: str):
        """获取文件信息摘要"""
        mappings = cls.get_file_chunks(file_hash)
        if not mappings:
            return None
        
        total_size = sum(m.chunk_size for m in mappings)
        chunk_count = len(mappings)
        
        return {
            'file_hash': file_hash,
            'total_size': total_size,
            'chunk_count': chunk_count,
            'chunks': [
                {
                    'chunk_hash': m.chunk_hash,
                    'chunk_index': m.chunk_index,
                    'chunk_offset': m.chunk_offset,
                    'chunk_size': m.chunk_size
                }
                for m in mappings
            ]
        }

    def __repr__(self):
        return f'<FileChunkMapping file={self.file_hash[:8]}... chunk={self.chunk_hash[:8]}... index={self.chunk_index}>'
