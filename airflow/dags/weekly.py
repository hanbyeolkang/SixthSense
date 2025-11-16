from airflow.decorators import dag, task 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable, Connection
import pendulum
import io 
import requests
import pandas as pd


date="20150101"

def gen_url(dt=date):
    base_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json"
    key=get_key()
    url=f"{base_url}?key={key}&targetDt={dt}"
    return url

def get_key():
    key=Variable.get('MOVIE_API_KEY') #variable 등록 필요 
    return key

def req(dt=date):
    url=gen_url(dt)
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
    df[['startRange', 'endRange']]=df['showRange'].str.split('~', expand=True)

    df['startRange']=pd.to_datetime(df['startRange'], format='%Y%m%d')
    df['endRange']=pd.to_datetime(df['endRange'], format='%Y%m%d')
    return df 


######### dag 시작 

@dag(
    dag_id="weekly_to_s3",
    start_date=pendulum.datetime(2025,10,1,tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False
)

def total_pipeline():
    target_date="{{ data_interval_end.format('YYYYMMDD')}}"

    @task 
    def extract_and_transform(dt: str) -> dict:
        df_raw=get_weekly_df(dt)
        df_transformed=transform(df_raw)
        
        #s3 hook 연결 
        s3_hook=S3Hook(aws_conn_id='S3_CONN_ID')
        s3_key=f'staging/weekly_{dt}.csv'

        csv_buffer = io.BytesIO()
        df_transformed.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

        s3_hook.load_bytes(
            bytes_data=csv_buffer.getvalue(),
            key=s3_key,
            bucket_name= 'de7-sixthsense', #나중에 수정 
            replace=True
        )

        return {'staging_key': s3_key, 'dt':dt}
    @task
    def load_staging_to_processed(upstream_data:dict):

        staging_key, dt = upstream_data.get('staging_key'),upstream_data.get('dt')
        
        s3_hook=S3Hook(aws_conn_id='S3_CONN_ID') #variable 설정 
        processed_key=f'weekly/weekly_{dt}.csv'

        #s3 내에서 파일 복사 
        s3_hook.copy_object(
            source_bucket_key=staging_key,
            dest_bucket_key=processed_key,
            source_bucket_name= 'de7-sixthsense', #나중에 수정 
            dest_bucket_name= 'de7-sixthsense' #나중에 수정 
            
        )

        #s3 원본 파일 삭제 
        s3_hook.delete_objects(
            bucket='de7-sixthsense', #나중에 수정 
            keys=[staging_key]
        )
    staging_info=extract_and_transform(target_date)
    load_staging_to_processed(staging_info)

total_pipeline()