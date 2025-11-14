FROM apache/superset:3.0.3

# 시스템 패키지 설치를 위해 root 권한으로 전환
USER root

# 시스템 패키지 업데이트 및 psycopg2의 의존성(libpq-dev 등) 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python 패키지 설치
RUN pip install --no-cache-dir \
    psycopg2-binary \
    && rm -rf /var/lib/apt/lists/* # apt 캐시 정리 재확인

# Superset 실행 권한으로 돌아옴
USER superset
