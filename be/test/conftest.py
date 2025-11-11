import pytest
from flask import Flask
from flask_jwt_extended import JWTManager
from common.db import db
from services.user_service import UserService
from app import create_app  # 假设你的 app 工厂在 app.py

@pytest.fixture(scope="module")
def test_app():
    app = create_app()
    app.config.update({
        "TESTING": True,
        "SQLALCHEMY_DATABASE_URI": "sqlite:///:memory:",
        "JWT_SECRET_KEY": "test-secret",
    })

    # 初始化 JWT
    jwt = JWTManager(app)

    # 黑名单回调
    @jwt.token_in_blocklist_loader
    def check_if_token_revoked(jwt_header, jwt_payload):
        return UserService.is_token_revoked(jwt_payload["jti"])

    with app.app_context():
        db.create_all()
        yield app
        db.drop_all()

@pytest.fixture(scope="module")
def client(test_app):
    return test_app.test_client()
