'''
1. 주간/매일 영화 리스트 api를 호출해 movie 목록을 가져온다 
2. 1의 movie 목록으로 redshift 조회. 해당하지 않는 영화만 가져온다 
3. 2 목록으로 detail api를 호출한다. 배우, 직원 수 가공해서 s3에 저장 
4. s3에 올라온 json 파일을 redshift에 copy 

'''

import json
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

aws_conn_id='S3_CONN_ID'
postgres_conn_id='redshift_default'

s3_bucket=Variable.get("DE7_SIXTHSENSE_BUCKET")
s3_prefix="movie_details"
redshift_schema="public"
redshift_table="movie_details"
REDSHIFT_IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE")

kobis_api_key=Variable.get('MOVIE_API_KEY')
kobis_daily_url="http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
kobis_detail_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

@dag(
    dag_id="daily_movieDetail_S3_Redshift",
    start_date=datetime(2025,10,1),
    schedule_interval="@daily",
    catchup=False
)

def daily_movieDetail_S3_Redshift():

    create_redshift_table=PostgresOperator(
        task_id="create_redshift_table",
        postgres_conn_id=postgres_conn_id,  
        sql=f""" 
        create table if not exists {redshift_schema}.{redshift_table} (
            movieCd VARCHAR(20) PRIMARY KEY,
                movieNm VARCHAR(256),
                movieNmEn VARCHAR(256),
                movieNmOg VARCHAR(256),
                showTm VARCHAR(10),
                prdtYear VARCHAR(4),
                openDt VARCHAR(8),
                prdtStatNm VARCHAR(50),
                typeNm VARCHAR(50),
                nations SUPER,
                genres SUPER,
                directors SUPER,
                actors SUPER
                );
                """
    )

    @task
    def get_daily_movie_list(target_dt):

        
        params = {"key": kobis_api_key, "targetDt": target_dt}

        response=requests.get(kobis_daily_url, params=params)
        response.raise_for_status()
        data=response.json()   
        movie_cds=[movie["movieCd"] for movie in data["boxOfficeResult"]['dailyBoxOfficeList']]

        try:
            if not movie_cds: 
                logging.warning("영화 목록이 없다")
            return list(set(movie_cds))
        except Exception as e:
            logging.error(f"weekly api 호출 실패:{e}")
            raise

    @task
    def find_new_movies(movie_cds):
        if not movie_cds:
            logging.info("조회할 영화 목록 없음")
            return []
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        movie_cd_tuple=tuple(movie_cds)

        if len(movie_cd_tuple) ==1:
            sql_in_clause=f"('{movie_cd_tuple[0]}')"
        else:
            sql_in_clause=str(movie_cd_tuple)

        sql=f"""
        select distinct movieCd
        from {redshift_schema}.{redshift_table}
        where movieCd IN {sql_in_clause};
        """

        try: 
            existing_movies=hook.get_records(sql)
        except Exception as e:
            #테이블이 아예 없는 첫 실행일 경우, 모든 영화를 신규로 간주
            return movie_cds
        
        existing_movie_set={row[0] for row in existing_movies}
        api_movie_set=set(movie_cds)

        new_movie_cd=list(api_movie_set-existing_movie_set)

        logging.info(f"API 목록: {api_movie_set}")
        logging.info(f"Redshift에 이미 있는 목록: {existing_movie_set}")
        logging.info(f"새로 추가할 영화: {new_movie_cd}")

        return new_movie_cd

    @task
    def fetch_details(movie_cd,s3_key_prefix):
        logging.info(f"[{movie_cd}] 상세 정보 API(B) 호출 시작...")
        params = {"key": kobis_api_key, "movieCd": movie_cd}

        response=requests.get(kobis_detail_url, params=params)
        response.raise_for_status()
        raw_data=response.json()

        movie_info=raw_data.get("movieInfoResult", {}).get("movieInfo", {})

        if not movie_info:
            logging.warning(f"[{movie_cd}] API(B)에서 유효한 movieInfo를 받지 못했습니다.")
            return ""
        sliced_actors=[]
        if "actors" in movie_info and isinstance(movie_info["actors"], list):
            sliced_actors = movie_info.get("actors", [])[:10]

        processed_data = {
                "movieCd": movie_cd,
                "movieNm": movie_info.get("movieNm"),
                "movieNmEn": movie_info.get("movieNmEn"),
                "movieNmOg": movie_info.get("movieNmOg"),
                "showTm": movie_info.get("showTm"),
                "prdtYear": movie_info.get("prdtYear"),
                "openDt": movie_info.get("openDt"),
                "prdtStatNm": movie_info.get("prdtStatNm"),
                "typeNm": movie_info.get("typeNm"),
                "nations": movie_info.get("nations", []),
                "genres": movie_info.get("genres", []),
                "directors": movie_info.get("directors", []),
                "actors": sliced_actors
            }

        s3_hook=S3Hook(aws_conn_id=aws_conn_id)
        s3_key=f"{s3_key_prefix}/{movie_cd}.json" #copy 경로랑 일치할 것 

        s3_hook.load_string(
            string_data=json.dumps(processed_data, ensure_ascii=False),
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )
        logging.info(f"[{movie_cd}] 가공 완료. S3 저장 성공: s3://{s3_bucket}/{s3_key}")
                
        return s3_key
    
    @task
    def filter_none(movie_list):
        if not movie_list:
            return []
        return [m for m in movie_list if m]


#4시작
    exec_date = "{{ data_interval_end.subtract(days=1).strftime('%Y%m%d') }}"
    s3_prefix_templated = f"{s3_prefix}/{exec_date}"

    load_to_redshift=PostgresOperator(
        task_id="load_to_redshift",
        postgres_conn_id=postgres_conn_id,
        sql=f"""
        copy {redshift_schema}.{redshift_table}
        from 's3://{s3_bucket}/{s3_prefix_templated}/'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        format as json 'auto ignorecase'
        truncatecolumns
        timeformat 'auto'
        region 'ap-northeast-2';
        """,
    )

    

    daily_list = get_daily_movie_list(target_dt=exec_date)    
    new_movies = find_new_movies(movie_cds=daily_list)
    valid_movies=filter_none(new_movies)

    create_redshift_table >> valid_movies

    s3_files = fetch_details.expand(
        movie_cd=valid_movies,
        s3_key_prefix=[s3_prefix_templated]
        
    )

    
    s3_files >> load_to_redshift

daily_movieDetail_S3_Redshift()


