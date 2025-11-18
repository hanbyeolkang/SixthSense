from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import subprocess
import os


# dbt 프로젝트 설정
DBT_PROJECT_DIR = '/opt/airflow/dbt'
DBT_PROFILE = 'sixthsense_proj'


@task(task_id="dbt_run_marts")
def dbt_run_marts():
    # 환경 변수 확인
    if not os.getenv('REDSHIFT_HOST'):
        raise ValueError("REDSHIFT_HOST environment variable is not set.")

    dbt_command = [
        "dbt",
        "run",
        "--profile", DBT_PROFILE,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR, 
        "--select", "marts"   # marts 폴더 안의 모든 모델 실행
    ]
    
    print(f"Executing dbt command: {' '.join(dbt_command)}")

    try:
        result = subprocess.run(
            dbt_command,
            check=True,
            capture_output=True,
            text=True
        )
        print(f"--- dbt STDOUT ---")
        print(result.stdout)
        print(f"--- dbt STDERR ---")
        print(result.stderr)
        
    except subprocess.CalledProcessError as e:
        print(f"dbt execution failed. Return code: {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise


with DAG(
    dag_id="dbt_run_marts",
    start_date=datetime(2025, 10, 1),
    schedule_interval="30 9 * * *",
    catchup=False,
    tags=["dbt", "marts"]
) as dag:

    dbt_run_marts()
