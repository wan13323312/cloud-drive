from models.base import BaseModel
from common.db import db

class File(BaseModel):
    __tablename__ = 'files'

    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    filename = db.Column(db.String(256))
    hash = db.Column(db.String(64))
    s3_key = db.Column(db.String(256))
    folder = db.Column(db.String(256), default='')  # 文件所属目录
