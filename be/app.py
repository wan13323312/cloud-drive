from flask import Flask
from common.db import db
from flask_jwt_extended import JWTManager
from routes.auth_routes import auth_bp
from routes.file_routes import file_bp
from routes.sync_routes import sync_bp
from services.user_service import UserService

def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config')

    db.init_app(app)
    jwt = JWTManager(app)

    # 检查 token 是否在黑名单
    @jwt.token_in_blocklist_loader
    def check_if_token_revoked(jwt_header, jwt_payload):
        jti = jwt_payload["jti"]
        return UserService.is_token_revoked(jti)

    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(file_bp, url_prefix='/file')
    app.register_blueprint(sync_bp, url_prefix='/sync')

    with app.app_context():
        db.create_all()

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)
