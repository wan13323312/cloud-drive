from flask import Blueprint, request, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
from services.file_service import FileService
from common.response import success, fail
import io

file_bp = Blueprint('file', __name__)

@file_bp.route('/upload', methods=['POST'])
@jwt_required()
def upload_file():
    user_id = get_jwt_identity()
    file_obj = request.files.get("file")
    folder = request.form.get("folder", "")
    if not file_obj:
        return fail("未上传文件")
    result = FileService.upload(user_id, file_obj, folder)
    return success(result)

@file_bp.route('/download', methods=['GET'])
@jwt_required()
def download_file():
    user_id = get_jwt_identity()
    filename = request.args.get("filename")
    folder = request.args.get("folder", "")
    result = FileService.download(user_id, filename, folder)
    if not result:
        return fail("文件不存在")
    return send_file(io.BytesIO(result["content"]), as_attachment=True, download_name=result["filename"])

@file_bp.route('/list', methods=['GET'])
@jwt_required()
def list_files():
    user_id = get_jwt_identity()
    folder = request.args.get("folder", "")
    files = FileService.list_files(user_id, folder)
    return success(files)

@file_bp.route('/delete', methods=['POST'])
@jwt_required()
def delete_file():
    user_id = get_jwt_identity()
    filename = request.json.get("filename")
    folder = request.json.get("folder", "")
    ok = FileService.delete_file(user_id, filename, folder)
    if not ok:
        return fail("文件不存在")
    return success({"filename": filename, "status": "删除成功"})

@file_bp.route('/create_folder', methods=['POST'])
@jwt_required()
def create_folder():
    user_id = get_jwt_identity()
    foldername = request.json.get("foldername")
    FileService.create_folder(user_id, foldername)
    return success({"foldername": foldername, "status": "创建成功"})

@file_bp.route('/rename', methods=['POST'])
@jwt_required()
def rename_file():
    user_id = get_jwt_identity()
    old_path = request.json.get("old_path")
    new_path = request.json.get("new_path")
    if not old_path or not new_path:
        return fail("参数错误：old_path/new_path 必填")
    ok = FileService.rename_file(user_id, old_path, new_path)
    if not ok:
        return fail("重命名失败，源文件不存在或目标无效")
    return success({"old_path": old_path, "new_path": new_path, "status": "重命名成功"})

@file_bp.route('/archive/create', methods=['POST'])
@jwt_required()
def create_archive():
    user_id = get_jwt_identity()
    folder = request.json.get("folder", "")
    archive_name = request.json.get("archive_name", "archive.zip")
    rel_path = FileService.create_archive(user_id, folder, archive_name)
    if not rel_path:
        return fail("创建压缩包失败，目录不存在或后端未实现")
    return success({"archive_path": rel_path})

@file_bp.route('/archive/extract', methods=['POST'])
@jwt_required()
def extract_archive():
    user_id = get_jwt_identity()
    archive_path = request.json.get("archive_path")
    dest_folder = request.json.get("dest_folder", "")
    if not archive_path:
        return fail("参数错误：archive_path 必填")
    ok = FileService.extract_archive(user_id, archive_path, dest_folder)
    if not ok:
        return fail("解压失败，压缩包不存在或后端未实现")
    return success({"dest_folder": dest_folder or "/", "status": "解压完成"})