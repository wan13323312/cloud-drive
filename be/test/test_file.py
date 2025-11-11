import pytest
import io
import uuid
import os
import shutil


# ------------------------------
# 模块级夹具：注册并登录一个用户，所有测试共用
# ------------------------------
@pytest.fixture(scope="module")
def auth_headers(client):
    """注册并登录随机用户，返回 JWT headers（整个模块共享）"""
    username = f"user_{uuid.uuid4().hex[:6]}"
    password = "123456"
    # 注册
    res = client.post("/auth/register", json={"username": username, "password": password})
    data = res.get_json()
    assert data["code"] == 0, f"Register failed: {data}"
    # 登录
    res = client.post("/auth/login", json={"username": username, "password": password})
    data = res.get_json()
    assert data["code"] == 0, f"Login failed: {data}"
    token = data["data"]["token"]
    return {"Authorization": f"Bearer {token}"}


# ------------------------------
# 模块级清理夹具：测试结束后删除该用户上传目录
# ------------------------------
@pytest.fixture(scope="module", autouse=True)
def clean_test_files(auth_headers, client):
    # 获取用户 ID
    res = client.get("/auth/profile", headers=auth_headers)
    user_id = res.get_json()["data"]["id"]
    user_upload_dir = f"./uploads/{user_id}"

    yield  # 等待测试执行完毕

    # 清理用户上传目录
    if os.path.exists(user_upload_dir):
        shutil.rmtree(user_upload_dir)
        print(f"清理测试残留：{user_upload_dir}")


# ------------------------------
# 文件相关测试
# ------------------------------
def test_upload_file(client, auth_headers):
    data = {
        "file": (io.BytesIO(b"hello world"), "hello.txt"),
        "folder": ""
    }
    res = client.post("/file/upload", headers=auth_headers, data=data, content_type="multipart/form-data")
    assert res.status_code == 200
    assert res.get_json()["code"] == 0


def test_list_files(client, auth_headers):
    res = client.get("/file/list", headers=auth_headers)
    files = res.get_json()["data"]
    assert any(f["filename"] == "hello.txt" for f in files)


def test_download_file(client, auth_headers):
    res = client.get("/file/download", headers=auth_headers, query_string={"filename": "hello.txt"})
    assert res.status_code == 200
    assert res.data == b"hello world"


def test_create_folder(client, auth_headers):
    res = client.post("/file/create_folder", headers=auth_headers, json={"foldername": "docs"})
    assert res.get_json()["code"] == 0 


def test_delete_file(client, auth_headers):
    res = client.post("/file/delete", headers=auth_headers, json={"filename": "hello.txt"})
    assert res.get_json()["code"] == 0


def test_list_files_after_delete(client, auth_headers):
    res = client.get("/file/list", headers=auth_headers)
    files = res.get_json()["data"]
    assert all(f["filename"] != "hello.txt" for f in files)


# ------------------------------
# 目录与重命名测试
# ------------------------------
def test_upload_into_folder(client, auth_headers):
    # 上传到 docs 子目录
    data = {
        "file": (io.BytesIO(b"doc content"), "doc1.txt"),
        "folder": "docs"
    }
    res = client.post("/file/upload", headers=auth_headers, data=data, content_type="multipart/form-data")
    assert res.status_code == 200
    assert res.get_json()["code"] == 0

    # 列出 docs 目录
    res = client.get("/file/list", headers=auth_headers, query_string={"folder": "docs"})
    assert res.status_code == 200
    files = res.get_json()["data"]
    assert any(f["filename"] == "doc1.txt" for f in files)


def test_rename_within_folder(client, auth_headers):
    # 在 docs 目录内重命名 doc1.txt -> doc2.txt
    res = client.post("/file/rename", headers=auth_headers, json={
        "old_path": "docs/doc1.txt",
        "new_path": "docs/doc2.txt"
    })
    data = res.get_json()
    assert res.status_code == 200
    assert data["code"] == 0

    # 校验重命名后的存在性
    res = client.get("/file/list", headers=auth_headers, query_string={"folder": "docs"})
    files = res.get_json()["data"]
    assert any(f["filename"] == "doc2.txt" for f in files)
    assert all(f["filename"] != "doc1.txt" for f in files)


def test_move_across_folders(client, auth_headers):
    # 创建目标目录 docs2
    res = client.post("/file/create_folder", headers=auth_headers, json={"foldername": "docs2"})
    assert res.get_json()["code"] == 0

    # 跨目录移动 docs/doc2.txt -> docs2/doc2.txt
    res = client.post("/file/rename", headers=auth_headers, json={
        "old_path": "docs/doc2.txt",
        "new_path": "docs2/doc2.txt"
    })
    data = res.get_json()
    assert res.status_code == 200
    assert data["code"] == 0

    # 校验新旧目录列表
    res = client.get("/file/list", headers=auth_headers, query_string={"folder": "docs"})
    files_docs = res.get_json()["data"]
    assert all(f["filename"] != "doc2.txt" for f in files_docs)

    res = client.get("/file/list", headers=auth_headers, query_string={"folder": "docs2"})
    files_docs2 = res.get_json()["data"]
    assert any(f["filename"] == "doc2.txt" for f in files_docs2)


def test_rename_not_found(client, auth_headers):
    # 重命名不存在的文件应失败
    res = client.post("/file/rename", headers=auth_headers, json={
        "old_path": "docs/not_exists.txt",
        "new_path": "docs/whatever.txt"
    })
    data = res.get_json()
    assert data["code"] != 0