FROM apache/airflow:2.7.3-python3.11

USER root

# 필요한 시스템 패키지 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Python 패키지 복사 및 설치
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt 설치
RUN pip install --no-cache-dir dbt-redshift==1.7.0