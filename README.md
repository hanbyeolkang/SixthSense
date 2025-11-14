# SixthSense
DE7 3차 프로젝트 - End-to-end 데이터 파이프라인 구성하기


### 실행 방법
## 1. 환경 변수 설정 파일 .env 생성
    sample.env 파일을 참고하여 같은 위치에 .env 파일을 생성합니다. (수동 작업)

## 2. 이미지 빌드
    docker compose build

## 3. 모든 서비스 실행
    docker compose up -d

## 4. Airflow 초기 설정
airflow-init 실행 후, airflow-webserver 가 실행되면..

    docker compose exec airflow-webserver airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin

## 5. Superset 초기 설정
    docker compose exec superset superset fab create-admin \
        --username admin \
        --firstname admin \
        --lastname user \
        --email admin@example.com \
        --password admin

    docker compose exec superset superset db upgrade

    docker compose exec superset superset init

    docker compose restart superset

## 6. 서로 다른 브라우저에서 실행 확인
    http://localhost:8080
    http://localhost:8088


### Airflow Variables 등록
- username
- slack_url