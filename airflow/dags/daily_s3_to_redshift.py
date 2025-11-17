from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from datetime import datetime, timedelta

# Airflow Variable에서 설정값 로드
DE7_SIXTHSENSE_BUCKET = Variable.get('DE7_SIXTHSENSE_BUCKET')
REDSHIFT_IAM_ROLE = Variable.get('REDSHIFT_IAM_ROLE')
REDSHIFT_CONN_ID = 'redshift_default'  # Connection ID

@dag(
    dag_id="daily_s3_to_redshift",
    start_date=pendulum.datetime(2025, 10, 1, tz="Asia/Seoul"),
    schedule_interval="10 8 * * *",  # 매일 오전 8:10 (KST) 실행 (S3 업로드 후)
    catchup=False,
    tags=["redshift", "s3", "kobis"]
)
def s3_to_redshift_pipeline():
    # 전날 날짜 계산 (어제)
    target_date = "{{ (data_interval_end - macros.timedelta(days=1)).format('YYYYMMDD') }}"
    
    @task
    def create_raw_data_schema():
        """Redshift에 raw_data 스키마가 없으면 생성"""
        try:
            redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
            
            create_schema_sql = """
            CREATE SCHEMA IF NOT EXISTS raw_data;
            """
            redshift_hook.run(create_schema_sql)
            print("✅ raw_data 스키마 생성 완료 (또는 이미 존재)")
        except Exception as e:
            print(f"❌ 스키마 생성 실패: {e}")
            raise
    
    @task
    def create_redshift_table_if_not_exists():
        """Redshift에 테이블이 없으면 생성"""
        try:
            redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS raw_data.daily_boxoffice (
                boxofficeType VARCHAR(50),
                showRange VARCHAR(20),
                rnum INTEGER,
                rank INTEGER,
                rankInten INTEGER,
                rankOldAndNew VARCHAR(10),
                openDt VARCHAR(8),
                salesAmt BIGINT,
                salesShare DECIMAL(5,1),
                salesInten BIGINT,
                salesChange DECIMAL(5,1),
                salesAcc BIGINT,
                audiCnt INTEGER,
                audiChange DECIMAL(5,1),
                scrnCnt INTEGER,
                load_dt VARCHAR(8)
            );
            """
            redshift_hook.run(create_table_sql)
            print("✅ Redshift 테이블 생성 완료 (또는 이미 존재)")
        except Exception as e:
            print(f"❌ 테이블 생성 실패: {e}")
            raise
    
    @task
    def load_s3_to_redshift(dt: str):
        """S3의 CSV 파일을 Redshift raw_data.daily_boxoffice 테이블에 COPY"""
        try:
            redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
            s3_path = f"s3://{DE7_SIXTHSENSE_BUCKET}/daily/daily_{dt}.csv"
            
            # S3에서 Redshift로 COPY
            copy_sql = f"""
            COPY raw_data.daily_boxoffice
            FROM '{s3_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            CSV
            IGNOREHEADER 1
            DELIMITER ','
            ENCODING UTF8;
            """
            
            redshift_hook.run(copy_sql)
            print(f"✅ Redshift 데이터 적재 성공: {s3_path}")
            print(f"✅ 테이블: raw_data.daily_boxoffice")
        except Exception as e:
            print(f"❌ Redshift 적재 실패: {e}")
            raise
    
    # Task 순서 정의
    create_raw_data_schema()
    create_redshift_table_if_not_exists()
    load_s3_to_redshift(target_date)

s3_to_redshift_pipeline()