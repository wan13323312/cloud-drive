from flask import jsonify

def success(data=None, msg="ok"):
    return jsonify({"code": 0, "msg": msg, "data": data or {}})

def fail(msg="error", code=1):
    return jsonify({"code": code, "msg": msg})
