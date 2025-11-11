from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from services.sync_service import SyncService
from common.response import success

sync_bp = Blueprint('sync', __name__)

@sync_bp.route('/upload', methods=['POST'])
@jwt_required()
def upload_sync():
    user_id = get_jwt_identity()
    filepath = request.json.get('filepath')
    SyncService.sync_to_server(user_id, filepath)
    return success({"msg": "占位：文件同步到服务器"})

@sync_bp.route('/download', methods=['POST'])
@jwt_required()
def download_sync():
    user_id = get_jwt_identity()
    filepath = request.json.get('filepath')
    SyncService.sync_to_local(user_id, filepath)
    return success({"msg": "占位：文件同步到本地"})
