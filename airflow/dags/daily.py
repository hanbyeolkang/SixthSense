from airflow.decorators import dag, task 
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pendulum
import io 
import requests
import pandas as pd
from datetime import datetime, timedelta

# Airflow Variable에서 설정값 로드
KOBIS_API_KEY = Variable.get('MOVIE_API_KEY')
S3_BUCKET_NAME = Variable.get('DE7_SIXTHSENSE_BUCKET')
AWS_CONN_ID = 'S3_CONN_ID'

def gen_url(dt):
    base_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key=KOBIS_API_KEY
    url=f"{base_url}?key={key}&targetDt={dt}"
    return url

def get_key():
    return KOBIS_API_KEY

def req(dt):
    url=gen_url(dt)
    print(f"요청 URL: {url}")
    r=requests.get(url)
    if not r:
        print("호출 실패")
        return None, None
    code=r.status_code
    data=r.json()
    print(f"상태코드: {code}")
    print(f"응답 데이터: {data}")
    return code, data 

def get_daily_df(load_dt) -> pd.DataFrame:
    code, data=req(load_dt)
    
    if data is None:
        return pd.DataFrame()
    
    if ('boxOfficeResult' in data and 
    'dailyBoxOfficeList' in data['boxOfficeResult']):
        
        movie_list=data['boxOfficeResult']['dailyBoxOfficeList']
        if not movie_list:
            print("데이터 없음")
            return pd.DataFrame()
        df=pd.DataFrame(movie_list)
        
        # 상위 레벨의 정보 추가
        df['boxofficeType']=data['boxOfficeResult'].get('boxofficeType', '')
        df['showRange']=data['boxOfficeResult'].get('showRange', '')
        
        return df
    else:
        print("예상한 데이터 구조 없음")
        return pd.DataFrame()

def transform(df, load_dt):
    if df.empty:
        return df

    df['load_dt'] = pd.to_datetime(load_dt, format='%Y%m%d')

    # 날짜 변환
    df['openDt'] = pd.to_datetime(df['openDt'], errors='coerce')

    numeric_cols = [
        'rnum', 'rank', 'rankInten',
        'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc',
        'audiCnt', 'audiInten', 'audiChange', 'audiAcc',
        'scrnCnt', 'showCnt'
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    cols_to_select = [
        'boxofficeType', 'showRange',
        'rnum', 'rank', 'rankInten', 'rankOldAndNew',
        'movieCd', 'movieNm',
        'openDt',
        'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc',
        'audiCnt', 'audiInten', 'audiChange', 'audiAcc',
        'scrnCnt', 'showCnt',
        'load_dt'
    ]

    return df[cols_to_select]


## dag 
@dag(
    dag_id="daily_to_s3",
    start_date=pendulum.datetime(2025, 10, 1, tz="Asia/Seoul"),
    schedule_interval="@daily",
    catchup=False,
    tags=["daily", "s3"]
)
def total_pipeline():
    # 실행일 날짜 그대로 사용
    target_date = "{{ data_interval_end.strftime('%Y%m%d') }}"
    
    @task 
    def extract_and_transform(dt: str) -> dict:
        print(f"추출 시작: {dt}")
        df_raw=get_daily_df(dt)
        
        if df_raw.empty:
            print(f"데이터가 없습니다: {dt}")
            return {'staging_key': None, 'dt': dt, 'success': False}
        
        df_transformed=transform(df_raw, dt)
        
        try:
            # S3Hook 사용 (Airflow Connection 이용)
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            s3_key = f'staging/daily_{dt}.csv'
            csv_buffer = io.BytesIO()
            df_transformed.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            
            s3_hook.load_bytes(
                bytes_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True
            )
            print(f"Staging 저장 성공: {s3_key}")
            print(f"Bucket: {S3_BUCKET_NAME}, Key: {s3_key}")
            return {'staging_key': s3_key, 'dt': dt, 'success': True}
        except Exception as e:
            print(f"Staging 저장 실패: {e}")
            return {'staging_key': None, 'dt': dt, 'success': False}
    
    @task
    def load_staging_to_processed(upstream_data:dict):
        if not upstream_data.get('success'):
            print(f"이전 Task 실패로 건너뜁니다")
            return
        
        staging_key = upstream_data.get('staging_key')
        dt = upstream_data.get('dt')
        
        try:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            processed_key = f'daily/daily_{dt}.csv'
            
            # staging 파일을 processed로 복사
            s3_hook.copy_object(
                source_bucket_key=staging_key,
                dest_bucket_key=processed_key,
                source_bucket_name=S3_BUCKET_NAME,
                dest_bucket_name=S3_BUCKET_NAME
            )
            print(f"Processed 저장 성공: {processed_key}")
            
            # staging 파일 삭제
            s3_hook.delete_objects(
                bucket=S3_BUCKET_NAME,
                keys=[staging_key]
            )
            print(f"Staging 파일 삭제 완료: {staging_key}")
        except Exception as e:
            print(f"처리 중 오류: {e}")
    
    staging_info=extract_and_transform(target_date)
    load_staging_to_processed(staging_info)

total_pipeline()