-- 컬럼명 표준화(snake_case), 날짜 변환, 타입 캐스팅
-- 배열 컬럼(nations, genres, directors, actors) 보존

WITH base AS (

    SELECT
        moviecd      AS movie_cd,
        movienm      AS movie_nm,
        movienmen    AS movie_nm_en,
        movienmog    AS movie_nm_og,
        showtm       AS show_tm_raw,
        prdtyear     AS prdt_year_raw,
        opendt       AS open_dt_raw,
        prdtstatnm   AS prdt_stat_nm,
        typenm       AS type_nm,
        nations      AS nations_str,
        genres       AS genres_str,
        directors    AS directors_str,
        actors       AS actors_str
    FROM {{ source('raw_data', 'movie_details') }}
)

SELECT
    movie_cd,   -- 영화 코드 (PK)
    movie_nm,   -- 국문명
    movie_nm_en,    -- 영문명
    movie_nm_og,    -- 원제
    TRY_CAST(prdt_year_raw AS integer) AS prdt_year,    -- 제작연도
    TRY_CAST(show_tm_raw   AS integer) AS show_tm,  -- 상영시간(분)
    TRY_CAST(to_date(open_dt_raw, 'YYYYMMDD') AS date) AS open_dt,  -- 개봉일
    prdt_stat_nm,   -- 제작 상태
    type_nm,    -- 영화 유형
    json_parse(nations_str) AS nations, -- 제작 국가
    json_parse(genres_str) AS genres,   -- 장르
    json_parse(directors_str) AS directors, -- 감독
    json_parse(actors_str) AS actors    -- 배우
FROM base;
