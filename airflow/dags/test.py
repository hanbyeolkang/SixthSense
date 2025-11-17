from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from pathlib import Path
import os
import sys


# dotenv 로드 추가
dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)


@task
def test():
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    REDSHIFT_USER = os.getenv('REDSHIFT_USER')
    KOBIS_API_KEY = os.getenv('KOBIS_API_KEY')
    SUPERSET_SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY')

    print("Hello, World!")
    print('aws: ' + AWS_ACCESS_KEY_ID[:3])
    print('s3: ' + S3_BUCKET_NAME[:3])
    print('redshift: ' + REDSHIFT_USER[:3])
    print('kobis: ' + KOBIS_API_KEY[:3])
    print('superest: ' + SUPERSET_SECRET_KEY[:3])


with DAG(
    dag_id="test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["test"]
) as dag:

    test()
