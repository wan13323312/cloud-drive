from flask import Blueprint, request
from flask_jwt_extended import (
    create_access_token,
    jwt_required,
    get_jwt_identity,
    get_jwt
)
from services.user_service import UserService
from common.response import success, fail

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/register', methods=['POST'])
def register():
    data = request.json
    user, err = UserService.register(data['username'], data['password'])
    if err:
        return fail(err)
    return success({"user_id": user.id, "username": user.username})

@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.json
    user = UserService.login(data['username'], data['password'])
    if not user:
        return fail("用户名或密码错误")
    # ⚡ 改成字符串 identity
    token = create_access_token(identity=str(user.id))
    return success({"token": token})


@auth_bp.route('/profile', methods=['GET'])
@jwt_required()
def profile():
    user_id = int(get_jwt_identity())  # ⚡ 转回整数
    info = UserService.get_profile(user_id)
    if not info:
        return fail("用户不存在")
    return success(info)

@auth_bp.route('/change_password', methods=['POST'])
@jwt_required()
def change_password():
    user_id = int(get_jwt_identity())
    data = request.json
    ok, err = UserService.change_password(user_id, data['old_password'], data['new_password'])
    if not ok:
        return fail(err)
    return success({"msg": "密码修改成功"})

@auth_bp.route('/delete_account', methods=['POST'])
@jwt_required()
def delete_account():
    user_id = int(get_jwt_identity())
    UserService.delete_account(user_id)
    return success({"msg": "账号已删除"})

@auth_bp.route('/logout', methods=['POST'])
@jwt_required()
def logout():
    jti = get_jwt()["jti"]
    UserService.logout(jti)
    return success({"msg": "已登出"})
