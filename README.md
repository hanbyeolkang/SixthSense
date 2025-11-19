# SixthSense
**DE7 3차 프로젝트 - End-to-end 데이터 파이프라인 구성하기**

## 프로젝트 개요
**주제** : 영화진흥위원회 일별/주간/주말 박스오피스 및 영화 상세정보 API를 활용한 파이프라인 구성

**목표**
- 데이터를 자동으로 수집·처리·적재하는 파이프라인 구축 경험
- Airflow 기반의 개발·운영 경험
- dbt 를 이용한 데이터 모델링 경험
- Redshift 활용 능력 향상


## 실행 방법
### 1. 환경 변수 설정 파일 .env 생성
    sample.env 파일을 참고하여 같은 위치에 .env 파일을 생성합니다. (수동 작업)

### 2. 이미지 빌드
    docker compose build

### 3. 서비스 실행
    docker compose up -d

### 4. Airflow 초기 설정
airflow-init 실행 종료 후, airflow-webserver 가 실행되면 설정

    docker compose exec airflow-webserver airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin

### 5. Superset 초기 설정
    docker compose exec superset superset fab create-admin \
        --username admin \
        --firstname admin \
        --lastname user \
        --email admin@example.com \
        --password admin

    docker compose exec superset superset db upgrade

    docker compose exec superset superset init

    docker compose restart superset

### 6. 서로 다른 브라우저에서 실행 확인
    http://localhost:8081
    http://localhost:8088

### 7. Airflow Variables, Connections 등록
    - REDSHIFT_IAM_ROLE : AWS ROLE
    - MOVIE_API_KEY : 영화진흥위원회 OPEN API 키
    - DE7_SIXTHSENSE_BUCKET : S3 버킷명

    - redshift_default : redshift 연결 (conncetion Type: Postgres)
    - S3_CONN_ID : amazon web servie 연결


## 폴더 구조
    project_root/
    │
    ├── airflow/        # Airflow 관련
    │   ├── dags/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── README.md
    │
    ├── dbt/            # dbt 관련
    │   ├── macros/
    │   ├── models/
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    │   └── README.md
    │ 
    ├── superset/       # Superset 관련
    │   ├── images/
    │   ├── Dockerfile
    │   ├── superset_config.py
    │   └── README.md
    │
    ├── .env
    ├── .gitignore
    ├── docker-compose.yml
    └── README.md
