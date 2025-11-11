from models.base import BaseModel
from common.db import db

class User(BaseModel):
    __tablename__ = 'users'

    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(256), nullable=False)
