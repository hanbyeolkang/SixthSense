# dbt

## 폴더 구조

    dbt/
    │
    ├── macros
    │   └── schema.sql
    │
    ├── models
    │   ├── marts               # 2단계: analytics 스키마 내 테이블 생성
    │   │   ├── dim
    │   │   │   └── dim_movie.sql
    │   │   └── fact
    │   │       ├── fact_daily_movie_stats.sql
    │   │       └── fact_weekly_movie_stats.sql
    │   │
    │   └── staging             # 1단계: raw_data 스키마 내 뷰 생성
    │       ├── sources.yml
    │       ├── stg_daily_boxoffice.sql
    │       ├── stg_movie_info.sql
    │       └── stg_weekly_boxoffice.sql
    │
    ├── dbt_project.yml
    ├── profiles.yml
    └── README.md
