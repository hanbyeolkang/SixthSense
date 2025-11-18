-- 컬럼명 표준화(snake_case), 날짜 변환, 타입 캐스팅
WITH base AS (
    SELECT
        boxofficetype    AS boxoffice_type,
        showrange        AS show_range,
        yearweektime     AS year_week_time,
        rnum,
        rank,
        rankinten        AS rank_inten,
        rankoldandnew    AS rank_old_and_new,
        moviecd          AS movie_cd,
        movienm          AS movie_nm,
        opendt           AS open_dt_raw,
        salesamt         AS sales_amt,
        salesshare       AS sales_share,
        salesinten       AS sales_inten,
        saleschange      AS sales_change,
        salesacc         AS sales_acc,
        audicnt          AS audi_cnt,
        audiinten        AS audi_inten,
        audichange       AS audi_change,
        audiacc          AS audi_acc,
        scrncnt          AS scrn_cnt,
        showcnt          AS show_cnt,
        startrange       AS start_range_raw,
        endrange         AS end_range_raw,
        load_dt
    FROM {{ source('raw_data', 'weekly_boxoffice') }}
)
SELECT
    show_range, -- 원본 주차 문자열 (YYYYMMDD ~ YYYYMMDD)
    year_week_time, -- 연도+주차
    rnum,   -- 순번
    rank,   -- 주간 순위
    rank_inten, -- 순위 증감
    rank_old_and_new,   -- 신규/기존 여부
    movie_cd,   -- 영화 코드
    movie_nm,   -- 영화명
    TRY_CAST(to_date(open_dt_raw, 'YYYYMMDD') AS date) AS open_dt,  -- 개봉일 DATE 변환
    sales_amt,  -- 매출액
    sales_share,    -- 매출 비중(%)
    sales_inten,    -- 매출 증감액
    sales_change,   -- 매출 증감률(%)
    sales_acc,  -- 누적 매출액
    audi_cnt,   -- 관객수
    audi_inten, -- 관객 증감
    audi_change,    -- 관객 증감률
    audi_acc,   -- 누적 관객수
    scrn_cnt,   -- 스크린 수
    show_cnt,   -- 상영 횟수
    TRY_CAST(to_date(start_range_raw, 'YYYYMMDD') AS date) AS start_range,  -- 주차 시작일
    TRY_CAST(to_date(end_range_raw,   'YYYYMMDD') AS date) AS end_range,    -- 주차 종료일
    TRY_CAST(load_dt AS date) AS load_dt    -- 데이터 적재일자
FROM base