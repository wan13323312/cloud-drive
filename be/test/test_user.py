import pytest
import uuid

@pytest.fixture
def auth_headers(client):
    """注册并登录一个随机用户，返回 JWT headers"""
    import uuid
    username = f"alice_{uuid.uuid4().hex[:6]}"
    password = "123456"
    client.post("/auth/register", json={"username": username, "password": password})
    res = client.post("/auth/login", json={"username": username, "password": password})
    data = res.get_json()
    assert data["code"] == 0, f"Login failed: {data}"
    token = data["data"]["token"]
    return {"Authorization": f"Bearer {token}"}


def test_register(client):
    res = client.post("/auth/register", json={"username": f"user_{uuid.uuid4().hex[:4]}", "password": "123456"})
    data = res.get_json()
    assert data["code"] == 0
    assert "user_id" in data["data"]

def test_login(client):
    username = f"user_{uuid.uuid4().hex[:4]}"
    password = "123456"
    client.post("/auth/register", json={"username": username, "password": password})
    res = client.post("/auth/login", json={"username": username, "password": password})
    data = res.get_json()
    assert data["code"] == 0
    assert "token" in data["data"]

def test_profile(client, auth_headers):
    res = client.get("/auth/profile", headers=auth_headers)
    assert res.status_code == 200
    data = res.get_json()
    assert data["code"] == 0
    assert "username" in data["data"]

def test_change_password(client, auth_headers):
    res = client.post("/auth/change_password", headers=auth_headers, json={
        "old_password": "123456",
        "new_password": "654321"
    })
    data = res.get_json()
    assert data["code"] == 0

def test_logout(client, auth_headers):
    res = client.post("/auth/logout", headers=auth_headers)
    data = res.get_json()
    assert data["code"] == 0

def test_delete_account(client):
    # 注册新用户
    username = f"user_{uuid.uuid4().hex[:4]}"
    password = "111111"
    client.post("/auth/register", json={"username": username, "password": password})
    res = client.post("/auth/login", json={"username": username, "password": password})
    token = res.get_json()["data"]["token"]
    headers = {"Authorization": f"Bearer {token}"}

    res = client.post("/auth/delete_account", headers=headers)
    data = res.get_json()
    assert data["code"] == 0
