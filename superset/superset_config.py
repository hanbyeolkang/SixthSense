# superset_config.py
import os

# 보안키
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change_me")

# Superset 내부 메타DB (Postgres/MySQL/SQLite 등)
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SQLALCHEMY_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db"
)

# CORS 허용
ENABLE_CORS = True
CORS_OPTIONS = {"supports_credentials": True}

# 웹서버 포트 (기본값: 8088)
SUPERSET_WEBSERVER_PORT = 8088
