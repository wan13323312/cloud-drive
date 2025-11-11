import os


class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'super-secret')
    JWT_SECRET_KEY = os.getenv('JWT_SECRET', 'jwt-secret')

    # SQLite数据库
    SQLALCHEMY_DATABASE_URI = 'sqlite:///cloud.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # 存储后端选择：local 或 s3
    STORAGE_BACKEND = os.getenv('STORAGE_BACKEND', 'local')
    # 是否启用入库压缩/出库解压
    ENABLE_COMPRESSION = os.getenv('ENABLE_COMPRESSION', 'true').lower() in ('1', 'true', 'yes')

    # S3 / MinIO
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY', 'test-key')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY', 'test-secret')
    S3_BUCKET = os.getenv('S3_BUCKET', 'cloud-drive-bucket')
    UPLOAD_TMP_DIR = './tmp_uploads'
