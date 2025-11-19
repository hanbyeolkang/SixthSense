# Airflow

## 폴더 구조

    airflow/
    │
    ├── dags
    │   ├── daily_boxoffice_to_redshift.py      # 일별 박스오피스 추출 및 적재(Redshift)
    │   ├── daily_boxoffice_to_s3.py            # 일별 박스오피스 추출 및 적재(S3)
    │   ├── dbt_run_marts.py                    # dbt marts 모델 실행
    │   ├── dbt_run_stg.py                      # dbt staging 모델 실행
    │   ├── movie_details_to_s3_redshift.py     # 영화 상세정보 추출 및 적재(S3, Redshift)
    │   └── weekly_boxoffice_to_s3_redshift.py  # 주간/주말 박스오피스 추출 및 적재(S3, Redshift)
    │
    ├── Dockerfile
    ├── requirements.txt
    └── README.md
