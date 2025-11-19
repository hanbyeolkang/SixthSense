'''
1. 주간/매일 영화 리스트 api를 호출해 movie 목록을 가져온다 
2. 1의 movie 목록으로 redshift 조회. 해당하지 않는 영화만 가져온다 
3. 2 목록으로 detail api를 호출한다. 배우, 직원 수 가공해서 s3에 저장 
4. s3에 올라온 json 파일을 redshift에 copy 

'''

import json
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context


aws_conn_id='S3_CONN_ID'
postgres_conn_id='redshift_default'

s3_bucket=Variable.get("DE7_SIXTHSENSE_BUCKET")
s3_prefix="movie_details"
redshift_schema="raw_data"
redshift_table="movie_details"
REDSHIFT_IAM_ROLE = Variable.get("REDSHIFT_IAM_ROLE")

kobis_api_key=Variable.get('MOVIE_API_KEY')
kobis_daily_url="http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
kobis_detail_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

@dag(
    dag_id="movie_details_to_s3_redshift",
    start_date=datetime(2025,10,1),
    schedule_interval="@daily",
    catchup=False,
    tags=["movie_details", "s3", "redshift"]
)

def daily_movieDetail_S3_Redshift():

    create_table_sql = f"""
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

    @task
    def create_redshift_table():
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        hook.run(create_table_sql)
        logging.info("Redshift 테이블 생성/존재확인 완료")

    @task
    def get_target_date_str():
        ctx=get_current_context()
        data_interval_end=ctx.get("data_interval_end")
        if not data_interval_end:
            data_interval_end=ctx.get("execution_date")
        target=(data_interval_end - timedelta(days=1)).strftime("%Y%m%d")
        logging.info(f"target_dt: {target}")
        return target

    @task
    def get_daily_movie_list(target_dt):
        
        params = {"key": kobis_api_key, "targetDt": target_dt}
        response=requests.get(kobis_daily_url, params=params)
        response.raise_for_status()
        data=response.json()   
        movies=data.get("boxOfficeResult", {}).get("dailyBoxOfficeList",[])
        movie_cds=[movie.get("movieCd") for movie in movies if movie.get("movieCd")]
        movie_cds = list(dict.fromkeys(movie_cds))  # preserve order, dedupe
        logging.info(f"API로부터 {len(movie_cds)}개의 영화코드 수신")
        return movie_cds
    
        

    @task
    def find_new_movies(movie_cds):
        if not movie_cds:
            logging.info("조회할 영화 목록 없음")
            return []
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        if len(movie_cds) == 1:
            in_clause = "('%s')" % movie_cds[0]
        else:
            in_clause = str(tuple(movie_cds))

        sql=f"""
        select distinct movieCd
        from {redshift_schema}.{redshift_table}
        where movieCd IN {in_clause};
        """

        try: 
            existing_movies=hook.get_records(sql)
            existing_set = {r[0] for r in existing_movies}
        except Exception as e:
            #테이블이 아예 없는 첫 실행일 경우, 모든 영화를 신규로 간주
            logging.warning("Redshift 조회 실패(테이블 없음 등). 모든 영화를 신규로 간주합니다. 상세: %s", e)
            existing_set = set()
        
        new=[m for m in movie_cds if m not in existing_set]
        logging.info(f"신규 영화 수: {len(new)}")
        return new 

    @task
    def fetch_details(movie_cds):

        if not movie_cds:
            logging.info("업로드할 신규 영화 없음.")
            return []
        
        s3 = S3Hook(aws_conn_id=aws_conn_id)
        
        uploaded_keys = []

        for movie_cd in movie_cds:
            logging.info(f"[{movie_cd}] 상세정보 호출 시작")
            try:
                params = {"key": kobis_api_key, "movieCd": movie_cd}
                response=requests.get(kobis_detail_url, params=params, timeout=20)
                response.raise_for_status()
                raw_data=response.json()

                movie_info=raw_data.get("movieInfoResult", {}).get("movieInfo", {})

                if not movie_info:
                    logging.warning(f"[{movie_cd}] API(B)에서 유효한 movieInfo를 받지 못했습니다.")
                    continue
        
                sliced_actors = movie_info.get("actors", [])[:10] if isinstance(movie_info["actors"], list) else []
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
                        "actors": sliced_actors,
                    }
                s3_key=f"{s3_prefix}/{movie_cd}.json" #copy 경로랑 일치할 것 

                s3.load_string(
                    string_data=json.dumps(processed_data, ensure_ascii=False),
                    key=s3_key,
                    bucket_name=s3_bucket,
                    replace=True
                )
                full_s3_path = f"s3://{s3_bucket}/{s3_key}"
                uploaded_keys.append(full_s3_path)
                logging.info(f"[{movie_cd}] 가공 완료. S3 저장 성공: s3://{s3_bucket}/{s3_key}")
            except Exception as e:
                logging.error(f"[{movie_cd}] 처리 실패: {e}", exc_info=True)
                # 실패한 영화는 건너뜀(나머지는 진행)
                continue  

        logging.info(f"총 업로드 파일 개수: {len(uploaded_keys)}")
        return uploaded_keys
    
    @task
    def load_to_redshift(s3_uris):
        if not s3_uris:
            logging.info("업로드된 S3 파일 없음 -> Redshift 로드 건너뜀")
            return "skipped"
        s3=S3Hook(aws_conn_id=aws_conn_id)

        manifest_entries = [{"url": uri, "mandatory": True} for uri in s3_uris]
        manifest_content = {"entries": manifest_entries}
        
        # 2. Manifest 파일을 S3 임시 경로에 저장 (매 실행마다 덮어써도 됨)
        manifest_key = f"{s3_prefix}/_manifests/latest_load_manifest.json"

        s3.load_string(
            string_data=json.dumps(manifest_content),
            key=manifest_key,
            bucket_name=s3_bucket,
            replace=True
        )
        logging.info(f"Manifest 생성 완료: {manifest_key}")

        # 3. Redshift COPY 명령어 (MANIFEST 옵션 사용)
        manifest_s3_path = f"s3://{s3_bucket}/{manifest_key}"
        
        copy_sql=f"""
        copy {redshift_schema}.{redshift_table}
        from '{manifest_s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        format as json 'auto ignorecase'
        MANIFEST
        truncatecolumns
        timeformat 'auto'
        region 'ap-northeast-2';
        """
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        hook.run(copy_sql)
        logging.info("Redshift COPY 완료")
        return "loaded"
        
    create_table=create_redshift_table()
    target_dt=get_target_date_str()
    daily_list = get_daily_movie_list(target_dt)    
    new_movies = find_new_movies(daily_list)
    fetch_details_task=fetch_details(new_movies)
    load_result=load_to_redshift(fetch_details_task)

    create_table >> daily_list >> new_movies >> fetch_details_task >> load_result


daily_movieDetail_S3_Redshift()


