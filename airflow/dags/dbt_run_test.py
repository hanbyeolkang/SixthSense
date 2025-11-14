from __future__ import annotations # 에어플로우 2.0 권장
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import subprocess
import os
import sys

# dbt 프로젝트 설정
DBT_PROJECT_DIR = '/opt/airflow/dbt'
DBT_PROFILE = 'sixthsense_proj'


@task
def run_dbt_model():
    """
    dbt run 명령을 실행하여 'stg_sample' 모델을 빌드합니다.
    """

    # 환경 변수 확인
    if not os.getenv('REDSHIFT_HOST'):
        raise ValueError("REDSHIFT_HOST environment variable is not set. Check your Docker Compose file.")
        
    dbt_command = [
        "dbt",
        "run",
        "--profile", DBT_PROFILE,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
        "--select", "stg_sample" 
    ]
    
    # subprocess를 사용하여 명령어 실행
    try:
        result = subprocess.run(
            dbt_command,
            check=True, 
            capture_output=True,
            text=True
        )
        print("--- dbt STDOUT ---")
        print(result.stdout)
        print("--- dbt STDERR ---")
        print(result.stderr)
        
    except subprocess.CalledProcessError as e:
        raise


with DAG(
    dag_id="dbt_run_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "redshift", "test"]
) as dag:

    run_dbt_model()