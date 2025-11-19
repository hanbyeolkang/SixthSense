-- 컬럼명 표준화(snake_case), 날짜 변환, 타입 캐스팅
WITH base AS (
    SELECT DISTINCT
        boxofficetype    AS boxoffice_type,
        showrange        AS show_range,
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
        load_dt,
        ROW_NUMBER() OVER (
            PARTITION BY showrange, moviecd 
            ORDER BY load_dt DESC
        ) AS rn
    FROM {{ source('raw_data', 'daily_boxoffice') }}
)
SELECT
    show_range, -- 조회 일자 (문자열 원본)
    TRY_CAST(to_date(show_range, 'YYYYMMDD') AS date) AS dt,    -- 조회 일자 (FACT PK에 사용)
    rnum,   -- 순번
    rank,   -- 일별 순위
    rank_inten, -- 전일 대비 순위 증감
    rank_old_and_new,   -- 신규/기존 여부
    movie_cd,   -- 영화 코드 (PK 조인 컬럼)
    movie_nm,   -- 영화 이름
    TO_DATE(TRIM(open_dt_raw), 'YYYY-MM-DD') AS open_dt,  -- 개봉일
    sales_amt,  -- 매출액
    sales_share,    -- 매출 비중(%)
    sales_inten,    -- 매출 증감액
    sales_change,   -- 매출 증감률(%)
    sales_acc,  -- 누적 매출액
    audi_cnt,   -- 일 관객수
    audi_inten, -- 관객수 증감
    audi_change,    -- 관객 증감률(%)
    audi_acc,   -- 누적 관객수
    scrn_cnt,   -- 스크린 수
    show_cnt,   -- 상영 횟수
    TRY_CAST(load_dt AS date) AS load_dt    -- 데이터 적재일자
FROM base
where rn = 1