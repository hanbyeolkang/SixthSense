from airflow.decorators import dag, task 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import pendulum
import io 
import requests
import pandas as pd
from datetime import datetime, timedelta

def gen_url(dt):
    base_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key="cfbd1e13aaf5a9e9666f23434e6bdc1c" 
    url=f"{base_url}?key={key}&targetDt={dt}"
    return url

def get_key():
    key="cfbd1e13aaf5a9e9666f23434e6bdc1c"  
    return key

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
    df['load_dt']=load_dt
    cols_to_select = ['boxofficeType', 'showRange', 'rnum', 'rank', 'rankInten', 'rankOldAndNew', 
                    'openDt', 'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc', 
                    'audiCnt', 'audiChange', 'scrnCnt', 'load_dt']
    df = df[cols_to_select]
    return df 

## dag 
@dag(
    dag_id="daily_to_s3",
    start_date=pendulum.datetime(2025, 10, 1, tz="Asia/Seoul"),
    schedule_interval="0 8 * * *",  
    catchup=False,
    tags=["daily", "s3"]
)
def total_pipeline():
    # 전날 날짜 계산 (어제)
    target_date="{{ (data_interval_end - macros.timedelta(days=1)).format('YYYYMMDD') }}"
    
    @task 
    def extract_and_transform(dt: str) -> dict:
        print(f"추출 시작: {dt}")
        df_raw=get_daily_df(dt)
        
        if df_raw.empty:
            print(f"데이터가 없습니다: {dt}")
            return {'staging_key': None, 'dt': dt, 'success': False}
        
        df_transformed=transform(df_raw, dt)
        
        try:
            s3_hook=S3Hook(aws_conn_id='S3_CONN_ID')
            s3_key=f'staging/daily_{dt}.csv'
            csv_buffer = io.BytesIO()
            df_transformed.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            s3_hook.load_bytes(
                bytes_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name='de7-sixthsense',
                replace=True
            )
            print(f"Staging 저장 성공: {s3_key}")
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
            s3_hook=S3Hook(aws_conn_id='S3_CONN_ID')
            processed_key=f'daily/daily_{dt}.csv'
            
            s3_hook.copy_object(
                source_bucket_key=staging_key,
                dest_bucket_key=processed_key,
                source_bucket_name='de7-sixthsense',
                dest_bucket_name='de7-sixthsense'
            )
            print(f"Processed 저장 성공: {processed_key}")
            
            # staging 파일 삭제
            s3_hook.delete_objects(
                bucket='de7-sixthsense',
                keys=[staging_key]
            )
            print(f"Staging 파일 삭제 완료: {staging_key}")
        except Exception as e:
            print(f"처리 중 오류: {e}")
    
    staging_info=extract_and_transform(target_date)
    load_staging_to_processed(staging_info)

total_pipeline()