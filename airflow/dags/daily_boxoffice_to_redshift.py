from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

# Airflow Variable에서 설정값 로드
DE7_SIXTHSENSE_BUCKET = Variable.get('DE7_SIXTHSENSE_BUCKET')
REDSHIFT_IAM_ROLE = Variable.get('REDSHIFT_IAM_ROLE')
REDSHIFT_CONN_ID = 'redshift_default'

SCHEMA_NAME = "raw_data"
TABLE_NAME = "daily_boxoffice"

@dag(
    dag_id="daily_boxoffice_to_redshift",
    start_date=pendulum.datetime(2025, 10, 1, tz="Asia/Seoul"),
    schedule_interval="10 8 * * *",
    catchup=False,
    tags=["daily_boxoffice", "redshift"]
)
def daily_s3_to_redshift_pipeline():
    # 실행일 날짜 그대로 사용
    target_date = "{{ data_interval_end.strftime('%Y%m%d') }}"
    
    @task
    def create_raw_data_schema():
        """Redshift에 raw_data 스키마가 없으면 생성"""
        try:
            hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
            
            create_schema_sql = """
            CREATE SCHEMA IF NOT EXISTS raw_data;
            """
            hook.run(create_schema_sql)
            print("raw_data 스키마 생성 완료 (또는 이미 존재)")
        except Exception as e:
            print(f"스키마 생성 실패: {e}")
            raise
    
    @task
    def create_redshift_table_if_not_exists():
        """Redshift에 테이블이 없으면 생성"""
        try:
            hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                boxofficeType VARCHAR(50),
                showRange VARCHAR(20),
                rnum INTEGER,
                rank INTEGER,
                rankInten INTEGER,
                rankOldAndNew VARCHAR(10),
                movieCd VARCHAR(20),
                movieNm VARCHAR(256),
                openDt VARCHAR(20),
                salesAmt BIGINT,
                salesShare DECIMAL(18,2),
                salesInten BIGINT,
                salesChange DECIMAL(18,2),
                salesAcc BIGINT,
                audiCnt INTEGER,
                audiInten INTEGER,
                audiChange DECIMAL(18,2),
                audiAcc BIGINT,
                scrnCnt INTEGER,
                showCnt INTEGER,
                load_dt VARCHAR(20)
            );
            """
            hook.run(create_table_sql)
            print("Redshift 테이블 생성 완료 (또는 이미 존재)")
        except Exception as e:
            print(f"테이블 생성 실패: {e}")
            raise
   
    @task
    def load_s3_to_redshift(dt: str):
        """S3 CSV → Redshift 적재"""
        
        if not dt:
            print("날짜 정보가 없어 Redshift 적재를 건너뜁니다.")
            return
        
        s3_path = f"s3://{DE7_SIXTHSENSE_BUCKET}/daily/daily_{dt}.csv"
        hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
        
        # 실제 실행되는 S3 경로 출력
        print(f"실제 COPY 대상 S3 파일: {s3_path}")
        
        # 기존 데이터 삭제 (중복 방지)
        delete_sql = f"DELETE FROM {SCHEMA_NAME}.{TABLE_NAME} WHERE load_dt = '{dt}';"
        
        copy_sql = f"""
        COPY {SCHEMA_NAME}.{TABLE_NAME}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
        """
        
        try:
            print(f"Running DELETE: {delete_sql}")
            hook.run(sql=delete_sql)
            
            print(f"Running COPY: {copy_sql}")
            hook.run(sql=copy_sql)
            
            print(f"Redshift 데이터 적재 성공: {s3_path}")
        except Exception as e:
            print(f"Redshift 적재 실패: {e}")
            
            print("Redshift COPY 상세 오류 확인:")
            error_sql = """
            SELECT *
            FROM sys_load_error_detail
            LIMIT 100;
            """
            try:
                error_rows = hook.get_records(error_sql)
                if error_rows:
                    for idx, row in enumerate(error_rows, 1):
                        print(f"   에러 {idx}: {row}")
                else:
                    print("   (조회된 에러 레코드 없음)")
            except Exception as err_query_e:
                print(f"   에러 상세 조회 실패: {err_query_e}")
                print(f"   원본 에러 메시지: {e}")
            
            raise

    
    schema_task = create_raw_data_schema()
    table_task = create_redshift_table_if_not_exists()
    
    schema_task >> table_task >> load_s3_to_redshift(target_date)

daily_s3_to_redshift_pipeline()
