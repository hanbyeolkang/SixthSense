from airflow.decorators import dag, task 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import pendulum
import io 
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

aws_conn_id='S3_CONN_ID'
date="20150101"
postgres_conn_id='redshift_default'
# weekGb="0" 


def gen_url(weekGb,dt=date ):
    base_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json"
    key=get_key()
    
    url=f"{base_url}?key={key}&targetDt={dt}&weekGb={weekGb}"
    return url

def get_key():
    key=Variable.get('MOVIE_API_KEY') #variable 등록 필요 
    return key

def req(dt=date):
    weekGb_val=0
    # “0” : 주간 (월~일), “1” : 주말 (금~일) , “2” : 주중 (월~목)
    url=gen_url(weekGb_val,dt)
    r=requests.get(url)
    if not r:
        print("호출 실패")
    code=r.status_code
    data=r.json()
    print(data)
    return code, data 


def get_weekly_df(load_dt=date) -> pd.DataFrame:
    code, data=req(load_dt)
    
    if ('boxOfficeResult' in data and 
    'weeklyBoxOfficeList' in data['boxOfficeResult'] and 
    'showRange' in data['boxOfficeResult']):
        
        movie_list=data['boxOfficeResult']['weeklyBoxOfficeList']
        if not movie_list:
            print("데이터 없음")
            return pd.DataFrame()
        df=pd.DataFrame(movie_list)
        df['showRange']=data['boxOfficeResult']['showRange']
        df['yearWeekTime']=data['boxOfficeResult']['yearWeekTime']
        df['boxofficeType'] = data['boxOfficeResult']['boxofficeType']
        return df
    else:
        return pd.DataFrame()


def save2df(load_dt=date):
    df=get_weekly_df(load_dt)
    df['load_dt']=load_dt #조회 날짜 
    
    return df

def transform(df):
    if df.empty:
        return df 

    # showRange → startRange, endRange
    split_df = df['showRange'].str.split('~', expand=True)
    df['startRange'] = pd.to_datetime(split_df[0], format='%Y%m%d', errors='coerce')
    df['endRange']   = pd.to_datetime(split_df[1], format='%Y%m%d', errors='coerce')

    # 날짜 변환
    df['openDt'] = pd.to_datetime(df['openDt'], errors='coerce')

    # 숫자 컬럼 daily와 동일하게 처리
    numeric_cols = [
        'rnum', 'rank', 'rankInten',
        'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc',
        'audiCnt', 'audiInten', 'audiChange', 'audiAcc',
        'scrnCnt', 'showCnt'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 최종 컬럼 세트(daily 구성 + weekly 전용 컬럼 포함)
    cols_order = [
        'boxofficeType', 'showRange', 'yearWeekTime',
        'rnum', 'rank', 'rankInten', 'rankOldAndNew',
        'movieCd', 'movieNm',
        'openDt',
        'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc',
        'audiCnt', 'audiInten', 'audiChange', 'audiAcc',
        'scrnCnt', 'showCnt',
        'startRange', 'endRange',
        'load_dt'
    ]

    # 누락 컬럼 보충
    for col in cols_order:
        if col not in df.columns:
            df[col] = None

    return df[cols_order]


######### dag 시작 
S3_BUCKET = Variable.get("DE7_SIXTHSENSE_BUCKET") 
TABLE_NAME = "weekly_boxoffice"
SCHEMA_NAME="raw_data"
REDSHIFT_IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE")

@dag(
    dag_id="weekly_boxoffice_to_s3_redshift",
    start_date=pendulum.datetime(2025,10,1,tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
    tags=["weekly_boxoffice", "s3", "redshift"]
)

def total_pipeline():

    @task 
    def extract_and_transform(dt: str) -> dict:

        df_with_load_dt = save2df(dt)
        df_transformed=transform(df_with_load_dt)
        
        #s3 hook 연결 
        s3_hook=S3Hook(aws_conn_id=aws_conn_id)
     

        csv_buffer = io.BytesIO()
        df_transformed.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        s3_key_staging = f'staging/weekly_{dt}.csv'

        s3_hook.load_bytes(
            bytes_data=csv_buffer.getvalue(),
            key=s3_key_staging,
            bucket_name= S3_BUCKET, #나중에 수정 
            replace=True
        )

        return {'staging_key': s3_key_staging, 'dt':dt}
    @task
    def load_staging_to_processed(upstream_data:dict):

        staging_key = upstream_data.get('staging_key')
        dt=upstream_data.get('dt')
        processed_key = f'weekly/weekly_{dt}.csv'
        s3_hook=S3Hook(aws_conn_id=aws_conn_id) #variable 설정 
        

        #s3 내에서 파일 복사 
        s3_hook.copy_object(
            source_bucket_key=staging_key,
            dest_bucket_key=processed_key,
            source_bucket_name= S3_BUCKET, 
            dest_bucket_name= S3_BUCKET 
            
        )

        #s3 원본 파일 삭제 
        s3_hook.delete_objects(
            bucket=S3_BUCKET, 
            keys=[staging_key]
        )
        return {'processed_key': processed_key, 'dt': upstream_data.get('dt')}
    
    @task
    def create_redshift_table():
        hook = PostgresHook(postgres_conn_id=postgres_conn_id) #variable 
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            boxofficeType VARCHAR(50),
            showRange VARCHAR(20),
            yearWeekTime VARCHAR(20),
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
            audiCnt BIGINT,
            audiInten BIGINT,
            audiChange DECIMAL(18,2),
            audiAcc BIGINT,
            scrnCnt INTEGER,
            showCnt INTEGER,
            startRange VARCHAR(20),
            endRange VARCHAR(20),
            load_dt VARCHAR(20)
        );
        """
        hook.run(sql=create_table_sql)

    @task
    def load_s3_to_redshift(upstream_data: dict):
        
        if not upstream_data: 
            print("Upstream 데이터가 없어 Redshift 적재를 건너뜁니다.")
            return

        processed_key = upstream_data.get('processed_key')
        dt = upstream_data.get('dt')
        s3_path = f"s3://{S3_BUCKET}/{processed_key}"
        
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        
        
        delete_sql = f"DELETE FROM {SCHEMA_NAME}.{TABLE_NAME} WHERE load_dt = '{dt}';"
        
        # S3 -> Redshift로 COPY
        copy_sql = f"""
        COPY {SCHEMA_NAME}.{TABLE_NAME}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
        """
        
        print(f"Running DELETE: {delete_sql}")
        hook.run(sql=delete_sql)
        print(f"Running COPY: {copy_sql}")
        hook.run(sql=copy_sql)


    staging_info = extract_and_transform(dt="{{ data_interval_end.strftime('%Y%m%d') }}")
    processed_info = load_staging_to_processed(staging_info)
 
    create_task = create_redshift_table()

    load_task = load_s3_to_redshift(processed_info)

    processed_info >> load_task
    create_task >> load_task

# DAG 실행
total_pipeline()