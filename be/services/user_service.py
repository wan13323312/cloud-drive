from models.user import User
from common.db import db
from werkzeug.security import generate_password_hash, check_password_hash

# JWT 黑名单（内存存储，重启失效）
jwt_blacklist = set()

class UserService:
    @staticmethod
    def register(username, password):
        if User.query.filter_by(username=username).first():
            return None, "用户名已存在"
        hashed = generate_password_hash(password)
        user = User(username=username, password=hashed)
        db.session.add(user)
        db.session.commit()
        return user, None

    @staticmethod
    def login(username, password):
        user = User.query.filter_by(username=username).first()
        if not user or not check_password_hash(user.password, password):
            return None
        return user

    @staticmethod
    def logout(jti):
        """将JWT ID加入黑名单"""
        jwt_blacklist.add(jti)
        return True

    @staticmethod
    def is_token_revoked(jti):
        return jti in jwt_blacklist

    @staticmethod
    def change_password(user_id, old_password, new_password):
        user = User.query.get(user_id)
        if not user or not check_password_hash(user.password, old_password):
            return False, "原密码错误"
        user.password = generate_password_hash(new_password)
        db.session.commit()
        return True, None

    @staticmethod
    def delete_account(user_id):
        user = User.query.get(user_id)
        if not user:
            return False
        db.session.delete(user)
        db.session.commit()
        return True

    @staticmethod
    def get_profile(user_id):
        user = User.query.get(user_id)
        if not user:
            return None
        return {"id": user.id, "username": user.username}
