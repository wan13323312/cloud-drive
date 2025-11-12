from models.base import BaseModel
from common.db import db
from sqlalchemy import func


class Md5Ref(BaseModel):
    """MD5引用计数表 - 用于文件去重的引用计数管理"""
    __tablename__ = 'md5_refs'

    md5_hash = db.Column(db.String(32), unique=True, nullable=False, index=True)
    ref_count = db.Column(db.Integer, default=1, nullable=False)
    blob_size = db.Column(db.BigInteger, nullable=True)  # 存储原始文件大小，便于统计

    @classmethod
    def get_or_create(cls, md5_hash: str, blob_size: int = None):
        """获取或创建MD5引用记录（线程安全）"""
        ref = cls.query.filter_by(md5_hash=md5_hash).first()
        if ref:
            return ref, False  # 已存在
        else:
            try:
                ref = cls(md5_hash=md5_hash, ref_count=0, blob_size=blob_size)
                db.session.add(ref)
                db.session.flush()  # 立即执行INSERT，可能触发UNIQUE约束错误
                return ref, True  # 新创建
            except Exception as e:
                # 如果发生UNIQUE约束错误，说明其他线程已经创建了记录
                db.session.rollback()
                ref = cls.query.filter_by(md5_hash=md5_hash).first()
                if ref:
                    return ref, False  # 其他线程创建的记录
                else:
                    # 如果还是找不到，重新抛出异常
                    raise e

    @classmethod
    def increment_ref(cls, md5_hash: str, blob_size: int = None, commit: bool = True):
        """增加引用计数"""
        ref, is_new = cls.get_or_create(md5_hash, blob_size)
        ref.ref_count += 1
        if is_new and blob_size is not None:
            ref.blob_size = blob_size
        if commit:
            db.session.commit()
        return ref.ref_count

    @classmethod
    def decrement_ref(cls, md5_hash: str):
        """减少引用计数，返回新的计数值，如果为0则删除记录"""
        ref = cls.query.filter_by(md5_hash=md5_hash).first()
        if not ref:
            return 0
        
        ref.ref_count = max(0, ref.ref_count - 1)
        
        if ref.ref_count == 0:
            # 引用计数为0时删除记录
            db.session.delete(ref)
            db.session.commit()
            return 0
        else:
            db.session.commit()
            return ref.ref_count

    @classmethod
    def get_ref_count(cls, md5_hash: str):
        """获取引用计数"""
        ref = cls.query.filter_by(md5_hash=md5_hash).first()
        return ref.ref_count if ref else 0

    @classmethod
    def exists(cls, md5_hash: str):
        """检查MD5是否存在"""
        return cls.query.filter_by(md5_hash=md5_hash).first() is not None

    @classmethod
    def get_storage_stats(cls):
        """获取存储统计信息"""
        result = db.session.query(
            func.count(cls.id).label('total_blobs'),
            func.sum(cls.ref_count).label('total_refs'),
            func.sum(cls.blob_size).label('total_size')
        ).first()
        
        return {
            'total_blobs': result.total_blobs or 0,
            'total_refs': result.total_refs or 0,
            'total_size': result.total_size or 0
        }

    def __repr__(self):
        return f'<Md5Ref {self.md5_hash[:8]}... refs={self.ref_count}>'
